/// MDBX-backed cache for immutable eth_call results.
///
/// The cache lives in its own MDBX env at `<entity_data_dir>/ethcall.mdbx`,
/// separate from the entity store so users can `rm -rf entity/` and re-run
/// without paying Phase 4 again. Variable-length values (token symbols,
/// names, etc.) don't fit the fixed-size entity-store machinery, so this is
/// a thin purpose-built layer rather than another `MutableStore` instance.
///
/// Key layout (52 bytes):
///   address  [20]u8           target contract
///   hash     [32]u8           keccak256(calldata)
///
/// Value layout (1 + N bytes):
///   status   u8               0 = success, 1 = revert (per Multicall3)
///   bytes    [N]u8            raw payload (ABI-encoded return data)
///
/// Persistence is permanent: results are time-invariant as we only fetch immutable data.
/// eth_call is made at head and is not meant for `x data at y block`.
const std = @import("std");

const eth = @import("eth");
const lmdbx = @import("lmdbx");

/// Default Multicall3 chunk size. Tuned to fit comfortably under typical
/// RPC payload limits (Alchemy/Infura ~1 MB, Multicall3 gas budget). Override
/// via `preload`'s `batch_size` arg if the RPC provider has tighter limits.
pub const DEFAULT_BATCH_SIZE: usize = 500;

/// One declared eth_call. `calldata` is the full call payload: 4-byte
/// selector for no-argument methods today; `selector ++ ABI-encode(args)`
/// when method-with-args support lands in M3.x.
pub const Call = struct {
    target: [20]u8,
    calldata: []const u8,
};

/// Result of a single cache lookup. `bytes` is borrowed from the active
/// MDBX read transaction and is valid until the next cache op on the same
/// txn (M3 uses short-lived per-call read txns, so copy if you need the
/// value past the lookup).
pub const CachedEntry = struct {
    status: u8,
    bytes: []const u8,
};

/// Errors returned by the typed-decode path layered on top of `get` in
/// `handler.zig`'s `ethCall(comptime T, ...)`. Kept here so the public
/// surface lives next to the cache it describes.
pub const Error = error{
    NotPrefetched,
    CallReverted,
    MalformedResult,
    MulticallFailed,
};

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

    /// Open (or create) the ethcall cache rooted at `dir_path`. The directory
    /// must already exist; the SDK's `entry.run` is responsible for mkdir-ing
    /// `<entity_data_dir>/ethcall/` before calling this.
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

    /// Insert or overwrite the cached entry for `(target, calldata)`. Idempotent.
    /// Status = 0 (success) writes a typical entry. Status = 1 (revert) records
    /// that the call ran but failed; the handler will see `error.CallReverted`
    /// instead of a silent re-fetch on the next run.
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

        const value = try allocator.alloc(u8, 1 + bytes.len);
        defer allocator.free(value);
        value[0] = status;
        @memcpy(value[1..], bytes);

        const k = cacheKey(target, calldata);
        const db = lmdbx.Database{ .txn = txn, .dbi = self.dbi };
        try db.set(&k, value, .Upsert);
        try txn.commit();
    }

    /// Read the cached entry for `(target, calldata)`. Returns `null` when
    /// absent. The returned `bytes` slice is valid for the lifetime of the
    /// read transaction; this method opens and commits a short-lived ro-txn
    /// per call. Callers that need the value beyond the call return must copy.
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

    /// True if the cache contains an entry for this pair. Used by the
    /// prefetch filterUncached step to skip already-warm calls without
    /// paying the value-read cost.
    pub fn contains(self: *Cache, target: [20]u8, calldata: []const u8) !bool {
        return (try self.get(target, calldata)) != null;
    }

    /// Batch-execute `calls` through Multicall3 and write every result to
    /// the cache. Calls are chunked into batches of `batch_size`; each batch
    /// is one HTTP RTT to the configured provider. Per Multicall3, every
    /// call's `allow_failure` is set so one revert doesn't sink the batch.
    ///
    /// On any batch failure (network, malformed response, etc.) this returns
    /// `error.MulticallFailed` after aborting the in-flight batch. The cache
    /// retains whatever previous batches succeeded in writing; subsequent
    /// runs will retry the missing pairs through the dedup + filterUncached
    /// path.
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
                try self.put(allocator, c.target, c.calldata, status, r.return_data);
            }
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
