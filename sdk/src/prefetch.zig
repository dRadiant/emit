/// Phase 4 gather → dedupe → filterUncached. All allocations come from a
/// caller-owned arena so the entire phase frees in one `arena.deinit()`.
const std = @import("std");

const core = @import("core");
const lmdbx = @import("lmdbx");

const ethcall = @import("ethcall.zig");
const filter_builder = @import("filter_builder.zig");
const sdk_manifest = @import("manifest.zig");

const RawLog = core.RawLog;
const log_serial = core.log_serial;
const types = core.types;

/// One `Call` per `static_prefetch` entry. Selectors dupe into arena memory
/// so every `Call.calldata` shares the same lifetime as the dynamic-channel.
pub fn gatherStatic(
    arena: std.mem.Allocator,
    comptime m: sdk_manifest.Manifest,
) ![]ethcall.Call {
    if (comptime m.static_prefetch.len == 0) return &.{};
    var out = try arena.alloc(ethcall.Call, m.static_prefetch.len);
    inline for (m.static_prefetch, 0..) |sc, i| {
        const sel = comptime ethcall.selectorOf(sc.method);
        const calldata = try arena.dupe(u8, &sel);
        out[i] = .{ .target = sc.address, .calldata = calldata };
    }
    return out;
}

/// Live-mode counterpart to `gatherDynamic`: walks the logs of a single
/// pending block and emits one `Call` per `(matching log, declared
/// PrefetchCall)`. The live loop calls this before child-event dispatch
/// so factory children's metadata lands in the cache in time.
pub fn gatherOneBlock(
    arena: std.mem.Allocator,
    logs: []const RawLog,
    comptime m: sdk_manifest.Manifest,
) ![]ethcall.Call {
    if (comptime m.prefetch.len == 0) return &.{};

    var out: std.ArrayList(ethcall.Call) = .empty;
    for (logs) |*log| {
        if (log.topic_count == 0) continue;
        inline for (m.prefetch) |def| {
            const on_topic = comptime sdk_manifest.eventTopic0(def.on_event);
            if (std.mem.eql(u8, &log.topics[0], &on_topic)) {
                inline for (def.calls) |pc| {
                    const sel = comptime ethcall.selectorOf(pc.method);
                    const addr = sdk_manifest.extractAddress(
                        def.on_event,
                        pc.address,
                        log.address,
                        &log.topics,
                        log.data,
                    );
                    const calldata = try arena.dupe(u8, &sel);
                    try out.append(arena, .{ .target = addr, .calldata = calldata });
                }
            }
        }
    }
    return out.toOwnedSlice(arena);
}

/// Walk the filtered index (`BLOCKS_PRIMARY` + `BLOCKS_CHILDREN` when
/// present) and emit one `Call` per `(matching log, declared PrefetchCall)`.
pub fn gatherDynamic(
    arena: std.mem.Allocator,
    env: lmdbx.Environment,
    comptime m: sdk_manifest.Manifest,
) ![]ethcall.Call {
    if (comptime m.prefetch.len == 0) return &.{};

    var out: std.ArrayList(ethcall.Call) = .empty;

    const txn = try env.transaction(.{ .mode = .ReadOnly });
    defer txn.abort() catch {};

    const decompress_buf = try arena.alloc(u8, types.BLOCK_BUF_SIZE);
    const log_buf = try arena.alloc(RawLog, types.MAX_LOGS_PER_BLOCK);

    inline for (.{ filter_builder.DBI_PRIMARY, filter_builder.DBI_CHILDREN }) |dbi_name| {
        if (lmdbx.Database.open(txn, dbi_name, .{})) |db| {
            var cursor = try db.cursor();
            defer cursor.deinit();
            var key_opt = cursor.goToFirst() catch null;
            while (key_opt) |_| : (key_opt = cursor.goToNext() catch null) {
                const value = try cursor.getCurrentValue();
                const decoded = log_serial.decompressEntry(value, decompress_buf) catch continue;
                const log_count = log_serial.deserializeLogs(decoded, log_buf);

                for (log_buf[0..log_count]) |*log| {
                    if (log.topic_count == 0) continue;
                    inline for (m.prefetch) |def| {
                        const on_topic = comptime sdk_manifest.eventTopic0(def.on_event);
                        if (std.mem.eql(u8, &log.topics[0], &on_topic)) {
                            inline for (def.calls) |pc| {
                                const sel = comptime ethcall.selectorOf(pc.method);
                                const addr = sdk_manifest.extractAddress(
                                    def.on_event,
                                    pc.address,
                                    log.address,
                                    &log.topics,
                                    log.data,
                                );
                                const calldata = try arena.dupe(u8, &sel);
                                try out.append(arena, .{ .target = addr, .calldata = calldata });
                            }
                        }
                    }
                }
            }
        } else |_| {}
    }

    return out.toOwnedSlice(arena);
}

/// Collapse by `(target, keccak(calldata))`, preserving first occurrence.
pub fn dedupe(arena: std.mem.Allocator, calls: []const ethcall.Call) ![]ethcall.Call {
    if (calls.len == 0) return &.{};

    var seen = std.AutoHashMap([52]u8, void).init(arena);
    defer seen.deinit();
    try seen.ensureTotalCapacity(@intCast(calls.len));

    var out: std.ArrayList(ethcall.Call) = .empty;
    try out.ensureTotalCapacity(arena, calls.len);

    for (calls) |c| {
        const k = ethcall.cacheKey(c.target, c.calldata);
        const gop = seen.getOrPutAssumeCapacity(k);
        if (gop.found_existing) continue;
        gop.value_ptr.* = {};
        try out.append(arena, c);
    }
    return out.toOwnedSlice(arena);
}

/// Drop entries already in the cache; order-preserving. Holds one ro-txn
/// for the entire scan so we don't pay N txn opens for N calls.
pub fn filterUncached(
    arena: std.mem.Allocator,
    cache: *ethcall.Cache,
    calls: []const ethcall.Call,
) ![]ethcall.Call {
    if (calls.len == 0) return &.{};

    const txn = try cache.beginRead();
    defer txn.abort() catch {};

    var out: std.ArrayList(ethcall.Call) = .empty;
    try out.ensureTotalCapacity(arena, calls.len);

    for (calls) |c| {
        if ((try cache.lookupInTxn(txn, c.target, c.calldata)) == null) {
            try out.append(arena, c);
        }
    }
    return out.toOwnedSlice(arena);
}

// ── Tests ────────────────────────────────────────────────────────────────

const Transfer = struct {
    pub const signature = "Transfer(address indexed from, address indexed to, uint256 value)";
};

const PairCreated = struct {
    pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair, uint256 allPairsLength)";
};

test "gatherStatic returns empty for a manifest with no static_prefetch" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: sdk_manifest.Manifest = .{ .name = "x", .chain_id = 1, .start_block = 0 };
    const calls = try gatherStatic(arena, m);
    try std.testing.expectEqual(@as(usize, 0), calls.len);
}

test "gatherStatic emits one Call per static_prefetch entry" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: sdk_manifest.Manifest = .{
        .name = "x",
        .chain_id = 1,
        .start_block = 0,
        .static_prefetch = &.{
            .{ .address = [_]u8{0xAA} ** 20, .method = "decimals()" },
            .{ .address = [_]u8{0xAA} ** 20, .method = "symbol()" },
            .{ .address = [_]u8{0xBB} ** 20, .method = "decimals()" },
        },
    };
    const calls = try gatherStatic(arena, m);
    try std.testing.expectEqual(@as(usize, 3), calls.len);
    try std.testing.expectEqualSlices(u8, &([_]u8{0xAA} ** 20), &calls[0].target);
    try std.testing.expectEqual(@as(usize, 4), calls[0].calldata.len);

    // Same method → same selector bytes
    try std.testing.expectEqualSlices(u8, calls[0].calldata, calls[2].calldata);
    // Different methods → different selectors
    try std.testing.expect(!std.mem.eql(u8, calls[0].calldata, calls[1].calldata));
}

test "dedupe collapses identical (target, method) entries" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: sdk_manifest.Manifest = .{
        .name = "x",
        .chain_id = 1,
        .start_block = 0,
        .static_prefetch = &.{
            .{ .address = [_]u8{0xAA} ** 20, .method = "decimals()" },
            .{ .address = [_]u8{0xAA} ** 20, .method = "decimals()" }, // duplicate
            .{ .address = [_]u8{0xAA} ** 20, .method = "symbol()" },
            .{ .address = [_]u8{0xBB} ** 20, .method = "decimals()" },
        },
    };
    const raw = try gatherStatic(arena, m);
    try std.testing.expectEqual(@as(usize, 4), raw.len);
    const unique = try dedupe(arena, raw);
    try std.testing.expectEqual(@as(usize, 3), unique.len);
}

test "dedupe preserves order of first occurrence" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const SEL1 = [_]u8{ 0x11, 0x11, 0x11, 0x11 };
    const SEL2 = [_]u8{ 0x22, 0x22, 0x22, 0x22 };
    const A = [_]u8{0xAA} ** 20;
    const B = [_]u8{0xBB} ** 20;

    var raw = try arena.alloc(ethcall.Call, 4);
    raw[0] = .{ .target = A, .calldata = &SEL1 };
    raw[1] = .{ .target = B, .calldata = &SEL2 };
    raw[2] = .{ .target = A, .calldata = &SEL1 }; // dup of [0]
    raw[3] = .{ .target = B, .calldata = &SEL1 };

    const unique = try dedupe(arena, raw);
    try std.testing.expectEqual(@as(usize, 3), unique.len);
    try std.testing.expectEqualSlices(u8, &A, &unique[0].target);
    try std.testing.expectEqualSlices(u8, &SEL1, unique[0].calldata);
    try std.testing.expectEqualSlices(u8, &B, &unique[1].target);
    try std.testing.expectEqualSlices(u8, &SEL2, unique[1].calldata);
    try std.testing.expectEqualSlices(u8, &B, &unique[2].target);
    try std.testing.expectEqualSlices(u8, &SEL1, unique[2].calldata);
}

test "filterUncached drops entries already present in the cache" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpathZ(".", &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    var cache = try ethcall.Cache.open(@ptrCast(&path_z));
    defer cache.close();

    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const A = [_]u8{0xAA} ** 20;
    const B = [_]u8{0xBB} ** 20;
    const SEL = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };

    // Pre-warm A's decimals() in the cache.
    var payload: [32]u8 = std.mem.zeroes([32]u8);
    payload[31] = 18;
    try cache.put(std.testing.allocator, A, &SEL, 0, &payload);

    var raw = try arena.alloc(ethcall.Call, 2);
    raw[0] = .{ .target = A, .calldata = &SEL }; // already cached
    raw[1] = .{ .target = B, .calldata = &SEL }; // not cached

    const remaining = try filterUncached(arena, &cache, raw);
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
    try std.testing.expectEqualSlices(u8, &B, &remaining[0].target);
}

test "gatherDynamic returns empty for a manifest with no prefetch" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpathZ(".", &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const env = try lmdbx.Environment.init(@ptrCast(&path_z), .{ .max_dbs = 2 });
    defer env.deinit() catch {};

    const m: sdk_manifest.Manifest = .{ .name = "x", .chain_id = 1, .start_block = 0 };
    const calls = try gatherDynamic(arena, env, m);
    try std.testing.expectEqual(@as(usize, 0), calls.len);
}
