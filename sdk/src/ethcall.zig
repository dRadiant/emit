/// Flat-file cache for immutable eth_call results at `<data_dir>/ethcall.dat`.
/// Survives entity-state wipes (separate file).
///
/// On-disk record (length-prefixed):
///   target           [20]u8
///   calldata_hash    [32]u8       keccak256(calldata), cache key suffix
///   status           u8           0 = success, 1 = reverted
///   value_len        u32 LE
///   value            [value_len]u8
///
/// Entries never expire. Results are immutable at the call site (decimals,
/// symbol, factory address) and always called at `latest`. Advisory cache.
/// Corruption detected on open, file reset, next prefetch repopulates from RPC.
///
/// O(1) lookups via in-memory hash map built on open. Writes append to file
/// and update the map. A crashed append leaves a truncated last record which
/// `open` truncates away so the next append continues from a clean boundary.
const std = @import("std");

const core = @import("core");
const eth = @import("eth");

pub const DEFAULT_BATCH_SIZE: usize = 500;

const MAGIC: core.flat_format.Magic = "EMITCALL".*;
const HEADER_SIZE: usize = core.flat_format.MAGIC_SIZE;
const RECORD_HEADER_SIZE: usize = 20 + 32 + 1 + 4;

pub const Call = struct {
    target: [20]u8,
    calldata: []const u8,
};

/// Borrowed view of a cached entry. `bytes` is owned by the cache, valid
/// until the next overwrite (rare, entries are upserted) or `close`.
/// Callers must not free.
pub const CachedEntry = struct {
    status: u8,
    bytes: []const u8,
};

pub fn selectorOf(comptime method: []const u8) [4]u8 {
    return comptime blk: {
        @setEvalBranchQuota(200_000);
        const h = eth.keccak.hash(method);
        break :blk h[0..4].*;
    };
}

/// Comptime-precomputed `keccak256(selector_of(method))`. The 32-byte tail
/// of the cache key for a no-arg method call. Hoisting to comptime saves a
/// keccak per `ctx.ethCall` invocation, meaningful when handlers fire on
/// every block.
pub fn calldataHashOf(comptime method: []const u8) [32]u8 {
    return comptime blk: {
        @setEvalBranchQuota(400_000);
        const selector = selectorOf(method);
        break :blk eth.keccak.hash(&selector);
    };
}

/// Head words a fixed-size return type occupies: one per primitive, the sum
/// for a struct/tuple of fixed fields. Dynamic types are not counted here.
fn wordCount(comptime T: type) comptime_int {
    return switch (@typeInfo(T)) {
        .int, .bool, .array => 1,
        .@"struct" => |s| blk: {
            var n: comptime_int = 0;
            for (s.fields) |f| n += wordCount(f.type);
            break :blk n;
        },
        else => @compileError(
            "ethcall.decodeAs: unsupported field type `" ++ @typeName(T) ++ "` in a multi-return struct",
        ),
    };
}

/// An ABI-dynamic return: `string`/`bytes` decoded as `[]const u8`. In a
/// tuple its head slot carries a tail offset, not the value inline.
fn isDynamic(comptime T: type) bool {
    return @typeInfo(T) == .pointer;
}

/// Head slots `T` occupies in a tuple head: one offset word if dynamic, its
/// full fixed width otherwise.
fn headWords(comptime T: type) comptime_int {
    return if (comptime isDynamic(T)) 1 else wordCount(T);
}

/// Decode a dynamic `[]const u8` whose `[length][data]` tail starts at
/// `bytes[off]`. The slice aliases `bytes` (no copy). `off` and the length
/// word are bounds checked against the buffer. `off` is relative to the
/// encoding base, the same base the head offsets resolve against.
fn decodeTail(comptime T: type, bytes: []const u8, off: usize) error{MalformedResult}![]const u8 {
    comptime {
        const ptr = @typeInfo(T).pointer;
        if (ptr.size != .slice or ptr.child != u8 or !ptr.is_const) @compileError(
            "ethcall.decodeAs: only `[]const u8` is supported for a dynamic `string`/`bytes` return; got `" ++ @typeName(T) ++ "`",
        );
    }
    if (off > bytes.len or bytes.len - off < 32) return error.MalformedResult;
    const length: usize = @truncate(std.mem.readInt(u256, bytes[off..][0..32], .big));
    if (length > bytes.len - off - 32) return error.MalformedResult;
    return bytes[off + 32 ..][0..length];
}

pub fn decodeAs(comptime T: type, bytes: []const u8) !T {
    const info = @typeInfo(T);
    // Struct or tuple return. The head is one slot per field: a static field
    // holds its value(s) inline, a dynamic field holds a tail offset relative
    // to the head start. `getReserves() -> (uint112,uint112,uint32)` is all
    // static, `totalSupplyAndName() -> (uint256,string)` mixes both.
    if (info == .@"struct") {
        const head = comptime blk: {
            var n: usize = 0;
            for (info.@"struct".fields) |f| n += headWords(f.type);
            break :blk n;
        };
        if (bytes.len < head * 32) return error.MalformedResult;
        var out: T = undefined;
        comptime var off: usize = 0;
        inline for (info.@"struct".fields) |f| {
            if (comptime isDynamic(f.type)) {
                const tail_off: usize = @truncate(std.mem.readInt(u256, bytes[off * 32 ..][0..32], .big));
                @field(out, f.name) = try decodeTail(f.type, bytes, tail_off);
            } else {
                @field(out, f.name) = try decodeAs(f.type, bytes[off * 32 ..]);
            }
            off += comptime headWords(f.type);
        }
        return out;
    }

    if (bytes.len < 32) return error.MalformedResult;
    const word = bytes[0..32];
    return switch (info) {
        .int => |int_info| if (int_info.signedness == .unsigned)
            @truncate(std.mem.readInt(u256, word, .big))
        else
            @truncate(@as(i256, @bitCast(std.mem.readInt(u256, word, .big)))),
        .bool => word[31] != 0,
        // Dynamic `string`/`bytes` return. The head word is the tail offset.
        // The slice borrows `bytes` (the cache entry's owned buffer), valid
        // for the cache's lifetime. Copy it to hold past a re-prefetch that
        // overwrites the same key.
        .pointer => try decodeTail(T, bytes, @truncate(std.mem.readInt(u256, word, .big))),
        .array => |arr| blk: {
            if (arr.child != u8) @compileError(
                "ethcall.decodeAs: arrays must be `[N]u8`; got `" ++ @typeName(T) ++ "`",
            );
            if (arr.len == 20) break :blk word[12..32].*;
            if (arr.len > 32) @compileError(
                "ethcall.decodeAs: arrays larger than 32 bytes cannot fit in one ABI word",
            );
            break :blk word[0..arr.len].*;
        },
        else => @compileError(
            "ethcall.decodeAs: type `" ++ @typeName(T) ++ "` is not supported. " ++
                "Use an int (u8..u256, i8..i256), bool, [20]u8, [N]u8 for N <= 32, " ++
                "or a struct/tuple of those for a multi-return.",
        ),
    };
}

/// Encode one fixed-size value as a 32-byte ABI word for calldata. `intN`
/// right-aligns big-endian (sign-extended for signed), `bool` is the LSB,
/// `[20]u8` (address) right-aligns, other `[N]u8` (`bytesN`) left-aligns.
/// Mirrors `manifest.paramWord` so a handler's `ethCallArgs` value and the
/// prefetch's resolved event-param word produce the same calldata.
pub fn encodeArg(value: anytype) [32]u8 {
    const V = @TypeOf(value);
    var word: [32]u8 = std.mem.zeroes([32]u8);
    switch (@typeInfo(V)) {
        .int => |int_info| {
            const u: u256 = if (int_info.signedness == .unsigned)
                @intCast(value)
            else
                @bitCast(@as(i256, value));
            std.mem.writeInt(u256, &word, u, .big);
        },
        .bool => word[31] = @intFromBool(value),
        .array => |arr| {
            if (arr.child != u8) @compileError("ethcall.encodeArg: arrays must be `[N]u8`; got `" ++ @typeName(V) ++ "`");
            if (arr.len > 32) @compileError("ethcall.encodeArg: `" ++ @typeName(V) ++ "` exceeds one ABI word");
            if (arr.len == 20) @memcpy(word[12..32], &value) else @memcpy(word[0..arr.len], &value);
        },
        else => @compileError(
            "ethcall.encodeArg: type `" ++ @typeName(V) ++ "` is not an encodable fixed-size arg (int, bool, [N]u8)",
        ),
    }
    return word;
}

pub fn cacheKey(target: [20]u8, calldata: []const u8) [52]u8 {
    return cacheKeyFromHash(target, eth.keccak.hash(calldata));
}

pub fn cacheKeyFromHash(target: [20]u8, calldata_hash: [32]u8) [52]u8 {
    var k: [52]u8 = undefined;
    @memcpy(k[0..20], &target);
    @memcpy(k[20..52], &calldata_hash);
    return k;
}

const OwnedEntry = struct {
    status: u8,
    bytes: []u8,
};

pub const Cache = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    entries: std.AutoHashMapUnmanaged([52]u8, OwnedEntry),

    /// Open or create `<dir>/ethcall.dat`. A corrupted-on-disk file (bad
    /// magic, truncated record) is reset to empty. Next prefetch rebuilds
    /// the relevant entries from RPC.
    pub fn open(allocator: std.mem.Allocator, dir: std.fs.Dir) !Cache {
        var self = Cache{
            .allocator = allocator,
            .file = core.flat_format.openOrCreateWithMagic(dir, "ethcall.dat", MAGIC) catch |err| switch (err) {
                error.InvalidMagic => try core.flat_format.createWithMagic(dir, "ethcall.dat", MAGIC),
                else => return err,
            },
            .entries = .{},
        };

        self.loadFromFile() catch {
            // Corruption past the magic. Reset to empty.
            self.clearEntries();
            self.file.close();
            self.file = try core.flat_format.createWithMagic(dir, "ethcall.dat", MAGIC);
        };
        return self;
    }

    /// Scan records past the magic into the in-memory map. A truncated last
    /// record is truncated back to the last good boundary so the next append
    /// continues cleanly. Unrecoverable corruption (mid-record garbage,
    /// impossible value_len) raises so the caller's reset-to-empty path fires.
    fn loadFromFile(self: *Cache) !void {
        const stat = try self.file.stat();
        if (stat.size < HEADER_SIZE) return error.Truncated;

        var pos: u64 = HEADER_SIZE;
        while (pos < stat.size) {
            if (pos + RECORD_HEADER_SIZE > stat.size) {
                try self.file.setEndPos(pos);
                return;
            }
            var hdr: [RECORD_HEADER_SIZE]u8 = undefined;
            if ((try self.file.pread(&hdr, pos)) != RECORD_HEADER_SIZE) return error.Truncated;

            const value_len = std.mem.readInt(u32, hdr[53..57], .little);
            const record_end = pos + RECORD_HEADER_SIZE + value_len;
            if (record_end > stat.size) {
                try self.file.setEndPos(pos);
                return;
            }

            const value = try self.allocator.alloc(u8, value_len);
            errdefer self.allocator.free(value);
            if ((try self.file.pread(value, pos + RECORD_HEADER_SIZE)) != value_len) return error.Truncated;

            var key: [52]u8 = undefined;
            @memcpy(key[0..20], hdr[0..20]);
            @memcpy(key[20..52], hdr[20..52]);

            const gop = try self.entries.getOrPut(self.allocator, key);
            if (gop.found_existing) self.allocator.free(gop.value_ptr.bytes);
            gop.value_ptr.* = .{ .status = hdr[52], .bytes = value };

            pos = record_end;
        }
    }

    fn clearEntries(self: *Cache) void {
        var it = self.entries.valueIterator();
        while (it.next()) |v| self.allocator.free(v.bytes);
        self.entries.deinit(self.allocator);
        self.entries = .{};
    }

    pub fn deinit(self: *Cache) void {
        self.clearEntries();
        self.file.close();
    }

    /// Idempotent upsert. Status 1 records a revert so re-runs don't refetch.
    pub fn put(
        self: *Cache,
        target: [20]u8,
        calldata: []const u8,
        status: u8,
        bytes: []const u8,
    ) !void {
        const key = cacheKey(target, calldata);

        const total = RECORD_HEADER_SIZE + bytes.len;
        const buf = try self.allocator.alloc(u8, total);
        defer self.allocator.free(buf);
        @memcpy(buf[0..20], &target);
        @memcpy(buf[20..52], key[20..52]);
        buf[52] = status;
        std.mem.writeInt(u32, buf[53..57], @intCast(bytes.len), .little);
        @memcpy(buf[57..], bytes);

        const end = try self.file.getEndPos();
        try self.file.pwriteAll(buf, end);

        const owned = try self.allocator.dupe(u8, bytes);
        errdefer self.allocator.free(owned);
        const gop = try self.entries.getOrPut(self.allocator, key);
        if (gop.found_existing) self.allocator.free(gop.value_ptr.bytes);
        gop.value_ptr.* = .{ .status = status, .bytes = owned };
    }

    /// Borrow the cached entry for `(target, calldata)`. Returned slice valid
    /// until the next `put` for the same key, or `close`.
    pub fn get(self: *const Cache, target: [20]u8, calldata: []const u8) ?CachedEntry {
        return self.getByHash(target, eth.keccak.hash(calldata));
    }

    /// Borrow the cached entry for `(target, keccak(calldata))`. Use when the
    /// calldata hash is already known (e.g. comptime via `calldataHashOf`) to
    /// skip the per-call keccak.
    pub fn getByHash(self: *const Cache, target: [20]u8, calldata_hash: [32]u8) ?CachedEntry {
        const key = cacheKeyFromHash(target, calldata_hash);
        if (self.entries.getPtr(key)) |e| {
            return .{ .status = e.status, .bytes = e.bytes };
        }
        return null;
    }

    pub fn contains(self: *const Cache, target: [20]u8, calldata: []const u8) bool {
        return self.entries.contains(cacheKey(target, calldata));
    }

    /// Chunk `calls` through Multicall3, writing each result via `put`.
    /// One HTTP RTT per `batch_size`. On network or decode failure the
    /// in-flight batch is dropped. Prior batches retain their writes.
    pub fn preload(
        self: *Cache,
        allocator: std.mem.Allocator,
        mc: *eth.multicall.Multicall,
        calls: []const Call,
        batch_size: usize,
    ) !void {
        var i: usize = 0;
        while (i < calls.len) : (i += batch_size) {
            const end = @min(i + batch_size, calls.len);
            const batch = calls[i..end];

            mc.reset();
            for (batch) |c| try mc.addCall(c.target, c.calldata, true);
            const results = mc.execute() catch return error.MulticallFailed;
            defer eth.multicall.freeResults(allocator, results);
            if (results.len != batch.len) return error.MulticallFailed;

            for (batch, results) |c, r| {
                const status: u8 = if (r.success) 0 else 1;
                try self.put(c.target, c.calldata, status, r.return_data);
            }
            // One fsync per batch, not per record. 500 individual fsyncs
            // (~13 ms each) would dominate a cold prefetch. Batched calls
            // share a single Multicall3 RTT and durability boundary.
            try self.file.sync();
        }
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

test "encodeArg right-aligns an address and a uint, LSB for bool" {
    const A = [_]u8{0xAB} ** 20;
    const wa = encodeArg(A);
    try testing.expectEqualSlices(u8, &([_]u8{0} ** 12), wa[0..12]);
    try testing.expectEqualSlices(u8, &A, wa[12..32]);

    try testing.expectEqual(@as(u256, 0xDEADBEEF), std.mem.readInt(u256, &encodeArg(@as(u256, 0xDEADBEEF)), .big));
    try testing.expectEqual(@as(u256, 42), std.mem.readInt(u256, &encodeArg(@as(u64, 42)), .big));
    try testing.expectEqual(@as(u8, 1), encodeArg(true)[31]);
    try testing.expectEqual(@as(u8, 0), encodeArg(false)[31]);
}

test "encodeArg sign-extends a negative signed integer" {
    const w = encodeArg(@as(i32, -1));
    try testing.expectEqualSlices(u8, &([_]u8{0xFF} ** 32), &w);
}

test "encodeArg left-aligns a bytesN value" {
    const w = encodeArg([_]u8{ 0xAA, 0xBB, 0xCC, 0xDD });
    try testing.expectEqualSlices(u8, &[_]u8{ 0xAA, 0xBB, 0xCC, 0xDD }, w[0..4]);
    try testing.expectEqualSlices(u8, &([_]u8{0} ** 28), w[4..32]);
}

test "cacheKey is deterministic and packs address+hash" {
    const TARGET = [_]u8{0xAB} ** 20;
    const CALLDATA = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    const k1 = cacheKey(TARGET, &CALLDATA);
    const k2 = cacheKey(TARGET, &CALLDATA);
    try testing.expectEqualSlices(u8, &k1, &k2);
    try testing.expectEqualSlices(u8, &TARGET, k1[0..20]);
    var any_nonzero = false;
    for (k1[20..52]) |b| if (b != 0) {
        any_nonzero = true;
        break;
    };
    try testing.expect(any_nonzero);
}

test "cacheKey differentiates target and calldata" {
    const A = [_]u8{0xAA} ** 20;
    const B = [_]u8{0xBB} ** 20;
    const CALL1 = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    const CALL2 = [_]u8{ 0x95, 0xd8, 0x9b, 0x41 };
    try testing.expect(!std.mem.eql(u8, &cacheKey(A, &CALL1), &cacheKey(B, &CALL1)));
    try testing.expect(!std.mem.eql(u8, &cacheKey(A, &CALL1), &cacheKey(A, &CALL2)));
}

test "put + get round-trips status and bytes" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try Cache.open(testing.allocator, tmp.dir);
    defer cache.deinit();

    const TARGET = [_]u8{0xCC} ** 20;
    const CALLDATA = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    var payload: [32]u8 = std.mem.zeroes([32]u8);
    payload[31] = 18;
    try cache.put(TARGET, &CALLDATA, 0, &payload);

    const entry = cache.get(TARGET, &CALLDATA) orelse return error.TestUnexpectedNull;
    try testing.expectEqual(@as(u8, 0), entry.status);
    try testing.expectEqualSlices(u8, &payload, entry.bytes);
}

test "get returns null when the key is absent" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try Cache.open(testing.allocator, tmp.dir);
    defer cache.deinit();

    const TARGET = [_]u8{0xDD} ** 20;
    const CALLDATA = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    try testing.expectEqual(@as(?CachedEntry, null), cache.get(TARGET, &CALLDATA));
}

test "put with status=1 round-trips an empty-payload revert" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try Cache.open(testing.allocator, tmp.dir);
    defer cache.deinit();

    const TARGET = [_]u8{0xEE} ** 20;
    const CALLDATA = [_]u8{ 0x95, 0xd8, 0x9b, 0x41 };
    try cache.put(TARGET, &CALLDATA, 1, &.{});

    const entry = cache.get(TARGET, &CALLDATA) orelse return error.TestUnexpectedNull;
    try testing.expectEqual(@as(u8, 1), entry.status);
    try testing.expectEqual(@as(usize, 0), entry.bytes.len);
}

test "put is idempotent (overwrite preserves last value)" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try Cache.open(testing.allocator, tmp.dir);
    defer cache.deinit();

    const TARGET = [_]u8{0xFF} ** 20;
    const CALLDATA = [_]u8{ 0x06, 0xfd, 0xde, 0x03 };
    var first: [32]u8 = std.mem.zeroes([32]u8);
    first[31] = 6;
    var second: [32]u8 = std.mem.zeroes([32]u8);
    second[31] = 18;
    try cache.put(TARGET, &CALLDATA, 0, &first);
    try cache.put(TARGET, &CALLDATA, 0, &second);

    const entry = cache.get(TARGET, &CALLDATA) orelse return error.TestUnexpectedNull;
    try testing.expectEqualSlices(u8, &second, entry.bytes);
}

test "contains tracks presence" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try Cache.open(testing.allocator, tmp.dir);
    defer cache.deinit();

    const TARGET = [_]u8{0x11} ** 20;
    const CALLDATA = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    try testing.expect(!cache.contains(TARGET, &CALLDATA));

    var payload: [32]u8 = std.mem.zeroes([32]u8);
    payload[31] = 8;
    try cache.put(TARGET, &CALLDATA, 0, &payload);
    try testing.expect(cache.contains(TARGET, &CALLDATA));
}

test "decodeAs reads u8 from the trailing byte of a 32-byte word" {
    var word: [32]u8 = std.mem.zeroes([32]u8);
    word[31] = 18;
    try testing.expectEqual(@as(u8, 18), try decodeAs(u8, &word));
}

test "decodeAs reads u256 from the full 32-byte word" {
    var word: [32]u8 = undefined;
    std.mem.writeInt(u256, &word, 0xdead_beef_cafe_babe, .big);
    try testing.expectEqual(@as(u256, 0xdead_beef_cafe_babe), try decodeAs(u256, &word));
}

test "decodeAs reads u112 by truncating the u256 view" {
    var word: [32]u8 = std.mem.zeroes([32]u8);
    word[19] = 0xAB;
    std.mem.writeInt(u256, &word, 0x1234_5678, .big);
    const got = try decodeAs(u112, &word);
    try testing.expectEqual(@as(u112, 0x1234_5678), got);
}

test "decodeAs reads a signed i32 with sign extension" {
    var word: [32]u8 = std.mem.zeroes([32]u8);
    @memset(&word, 0xFF);
    try testing.expectEqual(@as(i32, -1), try decodeAs(i32, &word));
}

test "decodeAs reads bool from the LSB" {
    var word_true: [32]u8 = std.mem.zeroes([32]u8);
    word_true[31] = 1;
    var word_false: [32]u8 = std.mem.zeroes([32]u8);
    try testing.expect(try decodeAs(bool, &word_true));
    try testing.expect(!(try decodeAs(bool, &word_false)));
}

test "decodeAs reads an EVM address from the trailing 20 bytes" {
    var word: [32]u8 = std.mem.zeroes([32]u8);
    const ADDR = [_]u8{0xAB} ** 20;
    @memcpy(word[12..32], &ADDR);
    try testing.expectEqualSlices(u8, &ADDR, &(try decodeAs([20]u8, &word)));
}

test "decodeAs reads bytes32-class from the left-aligned head" {
    var word: [32]u8 = std.mem.zeroes([32]u8);
    word[0..4].* = [_]u8{ 0xAA, 0xBB, 0xCC, 0xDD };
    try testing.expectEqualSlices(u8, &[_]u8{ 0xAA, 0xBB, 0xCC, 0xDD }, &(try decodeAs([4]u8, &word)));
}

test "decodeAs returns MalformedResult for a short payload" {
    var short: [4]u8 = .{ 1, 2, 3, 4 };
    try testing.expectError(error.MalformedResult, decodeAs(u8, &short));
}

test "decodeAs reads a fixed-size multi-return struct, one word per field" {
    const Reserves = struct { reserve0: u112, reserve1: u112, ts: u32 };
    var buf: [96]u8 = std.mem.zeroes([96]u8);
    std.mem.writeInt(u256, buf[0..32], 0x1111, .big);
    std.mem.writeInt(u256, buf[32..64], 0x2222, .big);
    std.mem.writeInt(u256, buf[64..96], 1_700_000_000, .big);

    const r = try decodeAs(Reserves, &buf);
    try testing.expectEqual(@as(u112, 0x1111), r.reserve0);
    try testing.expectEqual(@as(u112, 0x2222), r.reserve1);
    try testing.expectEqual(@as(u32, 1_700_000_000), r.ts);
}

test "decodeAs reads a tuple multi-return and mixed field types" {
    const T = struct { addr: [20]u8, amount: u256, flag: bool };
    var buf: [96]u8 = std.mem.zeroes([96]u8);
    const A = [_]u8{0xAB} ** 20;
    @memcpy(buf[12..32], &A); // address right-aligned
    std.mem.writeInt(u256, buf[32..64], 0xDEAD, .big);
    buf[95] = 1; // bool true

    const r = try decodeAs(T, &buf);
    try testing.expectEqualSlices(u8, &A, &r.addr);
    try testing.expectEqual(@as(u256, 0xDEAD), r.amount);
    try testing.expect(r.flag);
}

test "decodeAs multi-return rejects a payload short of the field count" {
    const Reserves = struct { a: u256, b: u256, c: u256 };
    var buf: [64]u8 = std.mem.zeroes([64]u8); // only 2 words, need 3
    try testing.expectError(error.MalformedResult, decodeAs(Reserves, &buf));
}

test "decodeAs reads a dynamic string borrowing the source bytes" {
    // ABI: [offset=0x20][length=5]["hello" padded to 32].
    var buf: [96]u8 = std.mem.zeroes([96]u8);
    std.mem.writeInt(u256, buf[0..32], 0x20, .big);
    std.mem.writeInt(u256, buf[32..64], 5, .big);
    @memcpy(buf[64..69], "hello");

    const s = try decodeAs([]const u8, &buf);
    try testing.expectEqualStrings("hello", s);
    // Slice aliases the input buffer, no copy.
    try testing.expectEqual(@intFromPtr(&buf[64]), @intFromPtr(s.ptr));
}

test "decodeAs reads an empty dynamic bytes" {
    var buf: [64]u8 = std.mem.zeroes([64]u8);
    std.mem.writeInt(u256, buf[0..32], 0x20, .big);
    // length word already zero.
    const s = try decodeAs([]const u8, &buf);
    try testing.expectEqual(@as(usize, 0), s.len);
}

test "decodeAs reads a dynamic return at a non-canonical offset" {
    // A padding word precedes the tail. Offset points past it.
    var buf: [128]u8 = std.mem.zeroes([128]u8);
    std.mem.writeInt(u256, buf[0..32], 0x40, .big); // tail at byte 64
    std.mem.writeInt(u256, buf[64..96], 3, .big);
    @memcpy(buf[96..99], "abc");
    try testing.expectEqualStrings("abc", try decodeAs([]const u8, &buf));
}

test "decodeAs dynamic rejects an out-of-bounds offset" {
    var buf: [64]u8 = std.mem.zeroes([64]u8);
    std.mem.writeInt(u256, buf[0..32], 0x1000, .big); // offset past the payload
    try testing.expectError(error.MalformedResult, decodeAs([]const u8, &buf));
}

test "decodeAs dynamic rejects a length running past the payload" {
    var buf: [96]u8 = std.mem.zeroes([96]u8);
    std.mem.writeInt(u256, buf[0..32], 0x20, .big);
    std.mem.writeInt(u256, buf[32..64], 1000, .big); // claims 1000 bytes, only 32 follow
    try testing.expectError(error.MalformedResult, decodeAs([]const u8, &buf));
}

test "decodeAs reads a tuple trailing a dynamic field after a fixed one" {
    // `(string name, uint256 supply)`. head[0]=offset, head[1]=supply.
    const T = struct { name: []const u8, supply: u256 };
    var buf: [128]u8 = std.mem.zeroes([128]u8);
    std.mem.writeInt(u256, buf[0..32], 0x40, .big); // name tail at byte 64
    std.mem.writeInt(u256, buf[32..64], 1_000_000, .big);
    std.mem.writeInt(u256, buf[64..96], 5, .big); // name length
    @memcpy(buf[96..101], "hello");

    const r = try decodeAs(T, &buf);
    try testing.expectEqualStrings("hello", r.name);
    try testing.expectEqual(@as(u256, 1_000_000), r.supply);
}

test "decodeAs reads a dynamic field between two fixed fields" {
    // `(uint256 id, string label, address owner)`.
    const T = struct { id: u256, label: []const u8, owner: [20]u8 };
    var buf: [160]u8 = std.mem.zeroes([160]u8);
    const OWNER = [_]u8{0xCC} ** 20;
    std.mem.writeInt(u256, buf[0..32], 42, .big);
    std.mem.writeInt(u256, buf[32..64], 0x60, .big); // label tail at byte 96
    @memcpy(buf[76..96], &OWNER); // address right-aligned in head[2]
    std.mem.writeInt(u256, buf[96..128], 4, .big); // label length
    @memcpy(buf[128..132], "usdc");

    const r = try decodeAs(T, &buf);
    try testing.expectEqual(@as(u256, 42), r.id);
    try testing.expectEqualStrings("usdc", r.label);
    try testing.expectEqualSlices(u8, &OWNER, &r.owner);
}

test "decodeAs reads two dynamic fields sharing one tail" {
    // `(string a, string b)`. Both head slots are offsets.
    const T = struct { a: []const u8, b: []const u8 };
    var buf: [192]u8 = std.mem.zeroes([192]u8);
    std.mem.writeInt(u256, buf[0..32], 0x40, .big); // a tail at 64
    std.mem.writeInt(u256, buf[32..64], 0x80, .big); // b tail at 128
    std.mem.writeInt(u256, buf[64..96], 3, .big);
    @memcpy(buf[96..99], "foo");
    std.mem.writeInt(u256, buf[128..160], 3, .big);
    @memcpy(buf[160..163], "bar");

    const r = try decodeAs(T, &buf);
    try testing.expectEqualStrings("foo", r.a);
    try testing.expectEqualStrings("bar", r.b);
}

test "decodeAs tuple rejects a dynamic field offset past the payload" {
    const T = struct { a: []const u8, n: u256 };
    var buf: [128]u8 = std.mem.zeroes([128]u8);
    std.mem.writeInt(u256, buf[0..32], 0x9999, .big); // offset past the buffer
    try testing.expectError(error.MalformedResult, decodeAs(T, &buf));
}

test "cache survives close and re-open" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const TARGET = [_]u8{0x22} ** 20;
    const CALLDATA = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    var payload: [32]u8 = std.mem.zeroes([32]u8);
    payload[31] = 18;

    {
        var cache = try Cache.open(testing.allocator, tmp.dir);
        try cache.put(TARGET, &CALLDATA, 0, &payload);
        cache.deinit();
    }

    var cache = try Cache.open(testing.allocator, tmp.dir);
    defer cache.deinit();
    const entry = cache.get(TARGET, &CALLDATA) orelse return error.TestUnexpectedNull;
    try testing.expectEqualSlices(u8, &payload, entry.bytes);
}

test "truncated last record is dropped on open without raising" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const TARGET = [_]u8{0x33} ** 20;
    const CALLDATA = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    var payload: [32]u8 = std.mem.zeroes([32]u8);
    payload[31] = 18;

    {
        var cache = try Cache.open(testing.allocator, tmp.dir);
        try cache.put(TARGET, &CALLDATA, 0, &payload);
        cache.deinit();
    }

    // Simulate a crashed append. Partial record missing payload bytes.
    {
        const f = try tmp.dir.openFile("ethcall.dat", .{ .mode = .read_write });
        defer f.close();
        const end = try f.getEndPos();
        var partial: [RECORD_HEADER_SIZE]u8 = undefined;
        @memset(&partial, 0xAA);
        std.mem.writeInt(u32, partial[53..57], 100, .little); // claims 100 bytes that don't follow
        try f.pwriteAll(&partial, end);
    }

    // Reopen drops the partial record, the good entry survives.
    var cache = try Cache.open(testing.allocator, tmp.dir);
    defer cache.deinit();
    const entry = cache.get(TARGET, &CALLDATA) orelse return error.TestUnexpectedNull;
    try testing.expectEqualSlices(u8, &payload, entry.bytes);
}

test "corrupted magic resets the cache to empty without raising" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("ethcall.dat", .{});
        defer f.close();
        try f.writeAll(&[_]u8{0xFF} ** 32);
    }

    var cache = try Cache.open(testing.allocator, tmp.dir);
    defer cache.deinit();

    // Cache is empty after corruption recovery.
    try testing.expect(!cache.contains([_]u8{0} ** 20, &[_]u8{0}));

    // Future puts work normally against the reset file.
    const TARGET = [_]u8{0x44} ** 20;
    const CALLDATA = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    var payload: [32]u8 = std.mem.zeroes([32]u8);
    payload[31] = 7;
    try cache.put(TARGET, &CALLDATA, 0, &payload);
    try testing.expect(cache.contains(TARGET, &CALLDATA));
}
