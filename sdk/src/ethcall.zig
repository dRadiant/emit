/// Flat-file cache for immutable eth_call results, kept at
/// `<data_dir>/ethcall.dat` so wiping the entity state doesn't invalidate it.
///
/// On-disk record (length-prefixed):
///   target           [20]u8
///   calldata_hash    [32]u8       keccak256(calldata), forms the cache key suffix
///   status           u8           0 = success, 1 = reverted
///   value_len        u32 LE
///   value            [value_len]u8
///
/// Entries never expire: results are immutable at the call site (decimals,
/// symbol, factory address, etc.) and we always call at `latest`. The cache
/// is advisory; corruption is detected on open, the file is reset, and the
/// next prefetch pass repopulates from RPC.
///
/// Lookups are O(1) via an in-memory hash map built on open. Writes append
/// to the file and update the map; an out-of-band crash leaves a truncated
/// last record which `open` detects and truncates away, so the next append
/// continues from a clean boundary.
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

/// Borrowed view of a cached entry. `bytes` is owned by the cache and
/// remains valid until the next overwrite (rare; entries are upserted) or
/// `close`. Callers must not free.
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

/// Comptime-precomputed `keccak256(selector_of(method))`. This is the
/// 32-byte tail of the cache key for a no-arg method call. Hoisting it
/// to comptime saves a keccak per `ctx.ethCall` invocation — meaningful
/// when handlers fire on every block.
pub fn calldataHashOf(comptime method: []const u8) [32]u8 {
    return comptime blk: {
        @setEvalBranchQuota(400_000);
        const selector = selectorOf(method);
        break :blk eth.keccak.hash(&selector);
    };
}

pub fn decodeAs(comptime T: type, bytes: []const u8) !T {
    if (bytes.len < 32) return error.MalformedResult;
    const word = bytes[0..32];
    const info = @typeInfo(T);
    return switch (info) {
        .int => |int_info| if (int_info.signedness == .unsigned)
            @truncate(std.mem.readInt(u256, word, .big))
        else
            @truncate(@as(i256, @bitCast(std.mem.readInt(u256, word, .big)))),
        .bool => word[31] != 0,
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
                "Use an int (u8..u256, i8..i256), bool, [20]u8, or [N]u8 for fixed N <= 32.",
        ),
    };
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
    /// magic, truncated record) is reset to empty; the next prefetch pass
    /// rebuilds the relevant entries from RPC.
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
            // Corruption past the magic: reset to empty.
            self.clearEntries();
            self.file.close();
            self.file = try core.flat_format.createWithMagic(dir, "ethcall.dat", MAGIC);
        };
        return self;
    }

    /// Scan records past the magic into the in-memory map. A truncated
    /// last record is detected and the file is truncated back to the last
    /// good boundary so the next append continues cleanly. Any unrecoverable
    /// corruption (mid-record garbage, impossible value_len) raises so the
    /// caller's reset-to-empty path fires.
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

    /// Borrow the cached entry for `(target, calldata)`. The returned slice
    /// is valid until the next `put` for the same key, or `close`.
    pub fn get(self: *const Cache, target: [20]u8, calldata: []const u8) ?CachedEntry {
        return self.getByHash(target, eth.keccak.hash(calldata));
    }

    /// Borrow the cached entry for `(target, keccak(calldata))`. Use this
    /// when the calldata hash is already known (e.g. precomputed at
    /// comptime via `calldataHashOf`) to skip the per-call keccak.
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
    /// in-flight batch is dropped; prior batches retain their writes.
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
            // One fsync per batch instead of per record: 500 individual
            // fsyncs (~13 ms each) would dominate a cold prefetch. The
            // batched calls share a single Multicall3 RTT and a single
            // durability boundary.
            try self.file.sync();
        }
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

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

    // Simulate a crashed append: tack on a partial record (missing payload bytes).
    {
        const f = try tmp.dir.openFile("ethcall.dat", .{ .mode = .read_write });
        defer f.close();
        const end = try f.getEndPos();
        var partial: [RECORD_HEADER_SIZE]u8 = undefined;
        @memset(&partial, 0xAA);
        std.mem.writeInt(u32, partial[53..57], 100, .little); // claim 100 bytes that don't follow
        try f.pwriteAll(&partial, end);
    }

    // Reopen: the partial record gets truncated; the good entry survives.
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
