/// MDBX-backed cache for immutable eth_call results, kept in its own env at
/// `<data_dir>/ethcall/` so wiping the entity store doesn't invalidate it.
///
/// Key layout (52 bytes): address [20]u8 || keccak256(calldata) [32]u8
/// Value layout (1 + N): status u8(0 ok, 1 revert) || raw return bytes
///
/// Entries never expire: results are immutable at the call site (decimals,
/// symbol, factory address, etc.) and we always call at `latest`.
const std = @import("std");

const eth = @import("eth");
const lmdbx = @import("lmdbx");

/// Default Multicall3 chunk size; fits comfortably under typical RPC payload
/// limits and the Multicall3 gas budget.
pub const DEFAULT_BATCH_SIZE: usize = 500;

/// One declared eth_call. `calldata` is the full call payload (4-byte
/// selector for no-argument methods).
pub const Call = struct {
    target: [20]u8,
    calldata: []const u8,
};

/// Result of a cache lookup. `bytes` points into the mmap'd MDBX value and
/// remains valid for the lifetime of the env (entries are never deleted).
pub const CachedEntry = struct {
    status: u8,
    bytes: []const u8,
};

/// Shared between `prefetch.zig` (queue calldata) and `entry.zig` (cache
/// lookup) so the same method string produces the same selector at both
/// sites — without this, an off-by-one would silently miss every cache hit.
pub fn selectorOf(comptime method: []const u8) [4]u8 {
    return comptime blk: {
        @setEvalBranchQuota(200_000);
        const h = eth.keccak.hash(method);
        break :blk h[0..4].*;
    };
}

/// Decode an ABI-encoded 32-byte word into `T`. Supports any int (truncated
/// from the big-endian u256), `bool` (LSB), `[20]u8` (trailing bytes — EVM
/// address layout), and `[N]u8` for N ≤ 32 (left-aligned bytes32-class).
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
            // [20]u8 is the EVM address layout: trailing 20 bytes of the word.
            // Any other fixed length is bytes32-class: left-aligned.
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

/// Form the 52-byte cache key for a `(target, calldata)` pair. Stable across
/// SDK versions so the cache survives upgrades.
pub fn cacheKey(target: [20]u8, calldata: []const u8) [52]u8 {
    var k: [52]u8 = undefined;
    @memcpy(k[0..20], &target);
    const h = eth.keccak.hash(calldata);
    @memcpy(k[20..52], &h);
    return k;
}

pub const Cache = struct {
    env: lmdbx.Environment,
    dbi: lmdbx.Database.DBI,

    /// Open or create the cache at `dir_path` (caller mkdir's).
    pub fn open(dir_path: [*:0]const u8) !Cache {
        const env = try lmdbx.Environment.init(dir_path, .{ .max_dbs = 1 });
        errdefer env.deinit() catch {};

        const txn = try lmdbx.Transaction.init(env, .{});
        errdefer txn.abort() catch {};
        const db = try lmdbx.Database.open(txn, "calls", .{ .create = true });
        try txn.commit();

        return .{ .env = env, .dbi = db.dbi };
    }

    pub fn close(self: *Cache) void {
        self.env.deinit() catch {};
    }

    /// Idempotent upsert. Status 1 records a revert so re-runs don't refetch.
    pub fn put(
        self: *Cache,
        allocator: std.mem.Allocator,
        target: [20]u8,
        calldata: []const u8,
        status: u8,
        bytes: []const u8,
    ) !void {
        const txn = try lmdbx.Transaction.init(self.env, .{});
        errdefer txn.abort() catch {};
        try self.writeInTxn(allocator, txn, target, calldata, status, bytes);
        try txn.commit();
    }

    fn writeInTxn(
        self: *Cache,
        allocator: std.mem.Allocator,
        txn: lmdbx.Transaction,
        target: [20]u8,
        calldata: []const u8,
        status: u8,
        bytes: []const u8,
    ) !void {
        const value = try allocator.alloc(u8, 1 + bytes.len);
        defer allocator.free(value);
        value[0] = status;
        @memcpy(value[1..], bytes);

        const k = cacheKey(target, calldata);
        const db = lmdbx.Database{ .txn = txn, .dbi = self.dbi };
        try db.set(&k, value, .Upsert);
    }

    /// `null` when absent. `bytes` is mmap-resident and outlives the call.
    pub fn get(
        self: *Cache,
        target: [20]u8,
        calldata: []const u8,
    ) !?CachedEntry {
        const txn = try lmdbx.Transaction.init(self.env, .{ .mode = .ReadOnly });
        defer txn.abort() catch {};

        const k = cacheKey(target, calldata);
        const db = lmdbx.Database{ .txn = txn, .dbi = self.dbi };
        const raw = (try db.get(&k)) orelse return null;
        if (raw.len < 1) return error.MalformedResult;
        return .{ .status = raw[0], .bytes = raw[1..] };
    }

    pub fn contains(self: *Cache, target: [20]u8, calldata: []const u8) !bool {
        return (try self.get(target, calldata)) != null;
    }

    /// Chunk `calls` through Multicall3 (one HTTP RTT per `batch_size`),
    /// write every result under a single MDBX write txn per batch — N+1
    /// fsync-per-result is the classic regression to watch for here. On
    /// network or decode failure: `error.MulticallFailed`, in-flight batch
    /// rolls back, prior batches retain their writes.
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

            const txn = try lmdbx.Transaction.init(self.env, .{});
            errdefer txn.abort() catch {};
            for (batch, results) |c, r| {
                const status: u8 = if (r.success) 0 else 1;
                try self.writeInTxn(allocator, txn, c.target, c.calldata, status, r.return_data);
            }
            try txn.commit();
        }
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

fn openTestCache(tmp: *std.testing.TmpDir) !Cache {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpathZ(".", &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    return try Cache.open(@ptrCast(&path_z));
}

test "cacheKey is deterministic and packs address+hash" {
    const TARGET = [_]u8{0xAB} ** 20;
    const CALLDATA = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 }; // decimals() selector
    const k1 = cacheKey(TARGET, &CALLDATA);
    const k2 = cacheKey(TARGET, &CALLDATA);
    try std.testing.expectEqualSlices(u8, &k1, &k2);
    try std.testing.expectEqualSlices(u8, &TARGET, k1[0..20]);
    // Hash bytes are non-zero (keccak of any non-empty input is non-zero w.h.p.).
    var any_nonzero = false;
    for (k1[20..52]) |b| if (b != 0) {
        any_nonzero = true;
        break;
    };
    try std.testing.expect(any_nonzero);
}

test "cacheKey differentiates target and calldata" {
    const A = [_]u8{0xAA} ** 20;
    const B = [_]u8{0xBB} ** 20;
    const CALL1 = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    const CALL2 = [_]u8{ 0x95, 0xd8, 0x9b, 0x41 };
    try std.testing.expect(!std.mem.eql(u8, &cacheKey(A, &CALL1), &cacheKey(B, &CALL1)));
    try std.testing.expect(!std.mem.eql(u8, &cacheKey(A, &CALL1), &cacheKey(A, &CALL2)));
}

test "put + get round-trips status and bytes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try openTestCache(&tmp);
    defer cache.close();

    const TARGET = [_]u8{0xCC} ** 20;
    const CALLDATA = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    // ABI-encoded u8 = 18 (right-aligned in 32-byte word).
    var payload: [32]u8 = std.mem.zeroes([32]u8);
    payload[31] = 18;
    try cache.put(std.testing.allocator, TARGET, &CALLDATA, 0, &payload);

    const entry = (try cache.get(TARGET, &CALLDATA)) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqual(@as(u8, 0), entry.status);
    try std.testing.expectEqualSlices(u8, &payload, entry.bytes);
}

test "get returns null when the key is absent" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try openTestCache(&tmp);
    defer cache.close();

    const TARGET = [_]u8{0xDD} ** 20;
    const CALLDATA = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    try std.testing.expectEqual(@as(?CachedEntry, null), try cache.get(TARGET, &CALLDATA));
}

test "put with status=1 round-trips an empty-payload revert" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try openTestCache(&tmp);
    defer cache.close();

    const TARGET = [_]u8{0xEE} ** 20;
    const CALLDATA = [_]u8{ 0x95, 0xd8, 0x9b, 0x41 };
    try cache.put(std.testing.allocator, TARGET, &CALLDATA, 1, &.{});

    const entry = (try cache.get(TARGET, &CALLDATA)) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqual(@as(u8, 1), entry.status);
    try std.testing.expectEqual(@as(usize, 0), entry.bytes.len);
}

test "put is idempotent (overwrite preserves last value)" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try openTestCache(&tmp);
    defer cache.close();

    const TARGET = [_]u8{0xFF} ** 20;
    const CALLDATA = [_]u8{ 0x06, 0xfd, 0xde, 0x03 };
    var first: [32]u8 = std.mem.zeroes([32]u8);
    first[31] = 6;
    var second: [32]u8 = std.mem.zeroes([32]u8);
    second[31] = 18;
    try cache.put(std.testing.allocator, TARGET, &CALLDATA, 0, &first);
    try cache.put(std.testing.allocator, TARGET, &CALLDATA, 0, &second);

    const entry = (try cache.get(TARGET, &CALLDATA)) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqualSlices(u8, &second, entry.bytes);
}

test "contains tracks presence without value-read cost (interface contract)" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try openTestCache(&tmp);
    defer cache.close();

    const TARGET = [_]u8{0x11} ** 20;
    const CALLDATA = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    try std.testing.expect(!(try cache.contains(TARGET, &CALLDATA)));

    var payload: [32]u8 = std.mem.zeroes([32]u8);
    payload[31] = 8;
    try cache.put(std.testing.allocator, TARGET, &CALLDATA, 0, &payload);
    try std.testing.expect(try cache.contains(TARGET, &CALLDATA));
}

test "decodeAs reads u8 from the trailing byte of a 32-byte word" {
    var word: [32]u8 = std.mem.zeroes([32]u8);
    word[31] = 18;
    try std.testing.expectEqual(@as(u8, 18), try decodeAs(u8, &word));
}

test "decodeAs reads u256 from the full 32-byte word" {
    var word: [32]u8 = undefined;
    std.mem.writeInt(u256, &word, 0xdead_beef_cafe_babe, .big);
    try std.testing.expectEqual(@as(u256, 0xdead_beef_cafe_babe), try decodeAs(u256, &word));
}

test "decodeAs reads u112 by truncating the u256 view" {
    // Sync(uint112,uint112): low 112 bits of the word are the value.
    var word: [32]u8 = std.mem.zeroes([32]u8);
    word[19] = 0xAB; // bit 152 (above u112 range, must be ignored if upper bits were set)
    std.mem.writeInt(u256, &word, 0x1234_5678, .big);
    const got = try decodeAs(u112, &word);
    try std.testing.expectEqual(@as(u112, 0x1234_5678), got);
}

test "decodeAs reads a signed i32 with sign extension" {
    var word: [32]u8 = std.mem.zeroes([32]u8);
    // -1 in i256 has all bits set.
    @memset(&word, 0xFF);
    try std.testing.expectEqual(@as(i32, -1), try decodeAs(i32, &word));
}

test "decodeAs reads bool from the LSB" {
    var word_true: [32]u8 = std.mem.zeroes([32]u8);
    word_true[31] = 1;
    var word_false: [32]u8 = std.mem.zeroes([32]u8);
    try std.testing.expect(try decodeAs(bool, &word_true));
    try std.testing.expect(!(try decodeAs(bool, &word_false)));
}

test "decodeAs reads an EVM address from the trailing 20 bytes" {
    var word: [32]u8 = std.mem.zeroes([32]u8);
    const ADDR = [_]u8{0xAB} ** 20;
    @memcpy(word[12..32], &ADDR);
    try std.testing.expectEqualSlices(u8, &ADDR, &(try decodeAs([20]u8, &word)));
}

test "decodeAs reads bytes32-class from the left-aligned head" {
    var word: [32]u8 = std.mem.zeroes([32]u8);
    word[0..4].* = [_]u8{ 0xAA, 0xBB, 0xCC, 0xDD };
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xAA, 0xBB, 0xCC, 0xDD }, &(try decodeAs([4]u8, &word)));
}

test "decodeAs returns MalformedResult for a short payload" {
    var short: [4]u8 = .{ 1, 2, 3, 4 };
    try std.testing.expectError(error.MalformedResult, decodeAs(u8, &short));
}

test "cache survives close and re-open" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const TARGET = [_]u8{0x22} ** 20;
    const CALLDATA = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };
    var payload: [32]u8 = std.mem.zeroes([32]u8);
    payload[31] = 18;

    {
        var cache = try openTestCache(&tmp);
        try cache.put(std.testing.allocator, TARGET, &CALLDATA, 0, &payload);
        cache.close();
    }

    var cache = try openTestCache(&tmp);
    defer cache.close();
    const entry = (try cache.get(TARGET, &CALLDATA)) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqualSlices(u8, &payload, entry.bytes);
}
