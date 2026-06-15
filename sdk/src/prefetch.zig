/// gather → dedupe → filterUncached. All allocations come from a caller-owned
/// arena so the entire phase frees in one `arena.deinit()`.
const std = @import("std");

const core = @import("core");
const filtered_store_mod = @import("filtered_store.zig");

const ethcall = @import("ethcall.zig");
const filter_builder = @import("filter_builder.zig");
const sdk_manifest = @import("manifest.zig");

const RawLog = core.RawLog;
const log_serial = core.log_serial;
const types = core.types;

/// One `Call` per `static_prefetch` entry. Selectors dupe into arena memory so
/// every `Call.calldata` shares the dynamic-channel lifetime.
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

/// Live-mode counterpart to `gatherDynamic`. Walks one pending block's logs and
/// emits one `Call` per `(matching log, declared PrefetchCall)`. Live loop calls
/// it before child-event dispatch so factory children's metadata is cached in time.
pub fn gatherOneBlock(
    arena: std.mem.Allocator,
    logs: []const RawLog,
    comptime m: sdk_manifest.Manifest,
    cache: *const ethcall.Cache,
) ![]ethcall.Call {
    if (comptime m.prefetch.len == 0) return &.{};

    var out: std.ArrayList(ethcall.Call) = .empty;
    try matchAndAppend(arena, &out, logs, m, cache);
    return out.toOwnedSlice(arena);
}

/// Write `selector ++ abi_word*` for `pc` into `buf` (length `4 + pc.args.len*32`).
/// A `.param` arg resolves to the event's word, a `.word` arg is the literal.
/// Shared by the append path and the producer-key lookup in `resolveTarget`, so
/// a chained call and its producer derive byte-identical calldata.
fn fillCalldata(buf: []u8, comptime E: type, comptime pc: sdk_manifest.PrefetchCall, log: *const RawLog) void {
    const sel = comptime ethcall.selectorOf(pc.method);
    @memcpy(buf[0..4], &sel);
    inline for (pc.args, 0..) |arg, ai| {
        const w: [32]u8 = switch (arg) {
            .word => |lit| lit,
            .param => |name| sdk_manifest.paramWord(E, name, &log.topics, log.data),
        };
        @memcpy(buf[4 + ai * 32 ..][0..32], &w);
    }
}

/// Target address for `def.calls[idx]` against a matching log. `.log`/`.param`
/// resolve from the log alone. `.of`/`.of_return` chain off an earlier call's
/// cached result, returning `null` when that producer isn't cached yet (a later
/// prefetch round resolves it) or reverted, so the chained call is dropped this
/// round rather than queued against a wrong target.
fn resolveTarget(comptime def: sdk_manifest.PrefetchDef, comptime idx: usize, log: *const RawLog, cache: *const ethcall.Cache) ?[20]u8 {
    return switch (def.calls[idx].address) {
        .log => log.address,
        .param => |name| sdk_manifest.extractAddress(def.on_event, .{ .param = name }, log.address, &log.topics, log.data),
        .of => |m| resolveChainTarget(def, idx, m, 0, log, cache),
        .of_return => |r| resolveChainTarget(def, idx, r.call, r.index, log, cache),
    };
}

/// Address in return word `ret_index` of the earlier call named `producer_method`.
/// Rebuilds the producer's `(target, calldata)` exactly as it was queued, looks
/// it up in the cache, and slices the trailing 20 bytes of the chosen word.
/// `null` when the producer target is itself unresolved, the result is uncached
/// or reverted, or the result is too short to hold `ret_index`.
fn resolveChainTarget(
    comptime def: sdk_manifest.PrefetchDef,
    comptime idx: usize,
    comptime producer_method: []const u8,
    comptime ret_index: u16,
    log: *const RawLog,
    cache: *const ethcall.Cache,
) ?[20]u8 {
    const pj = comptime sdk_manifest.producerIndex(def, idx, producer_method);
    const producer = def.calls[pj];
    const ptarget = resolveTarget(def, pj, log, cache) orelse return null;
    var pbuf: [4 + producer.args.len * 32]u8 = undefined;
    fillCalldata(&pbuf, def.on_event, producer, log);
    const entry = cache.get(ptarget, &pbuf) orelse return null;
    if (entry.status != 0) return null;
    const off = @as(usize, ret_index) * 32;
    if (entry.bytes.len < off + 32) return null;
    return entry.bytes[off + 12 ..][0..20].*;
}

/// Append one `Call` to `out` per `(matching log, resolvable PrefetchCall)`.
/// Matching shape is identical between live (one block's logs) and backfill
/// (every block in the filtered index). Single source so the paths can't drift.
/// `cache` resolves chained `.of`/`.of_return` targets; an unresolved chain link
/// drops its call, re-tried next round once its producer lands in the cache.
fn matchAndAppend(
    arena: std.mem.Allocator,
    out: *std.ArrayList(ethcall.Call),
    logs: []const RawLog,
    comptime m: sdk_manifest.Manifest,
    cache: *const ethcall.Cache,
) !void {
    for (logs) |*log| {
        if (log.topic_count == 0) continue;
        inline for (m.prefetch) |def| {
            const on_topic = comptime sdk_manifest.eventTopic0(def.on_event);
            if (std.mem.eql(u8, &log.topics[0], &on_topic)) {
                inline for (def.calls, 0..) |pc, idx| {
                    if (resolveTarget(def, idx, log, cache)) |target| {
                        const calldata = try arena.alloc(u8, 4 + pc.args.len * 32);
                        fillCalldata(calldata, def.on_event, pc, log);
                        try out.append(arena, .{ .target = target, .calldata = calldata });
                    }
                }
            }
        }
    }
}

/// Walk the filtered index (primary + children pairs when present), emitting one
/// `Call` per `(matching log, declared PrefetchCall)`.
pub fn gatherDynamic(
    arena: std.mem.Allocator,
    dir: std.fs.Dir,
    comptime m: sdk_manifest.Manifest,
    cache: *const ethcall.Cache,
) ![]ethcall.Call {
    if (comptime m.prefetch.len == 0) return &.{};

    var out: std.ArrayList(ethcall.Call) = .empty;

    const decompress_buf = try arena.alloc(u8, types.BLOCK_BUF_SIZE);
    const payload_buf = try arena.alloc(u8, types.BLOCK_BUF_SIZE);
    const log_buf = try arena.alloc(RawLog, types.MAX_LOGS_PER_BLOCK);

    inline for (.{ filter_builder.BASE_PRIMARY, filter_builder.BASE_CHILDREN }) |base| {
        if (filtered_store_mod.FilteredStore.open(arena, dir, base)) |store_init| {
            var store = store_init;
            defer store.deinit();
            var i: u64 = 0;
            while (i < store.count()) : (i += 1) {
                // A corrupt entry would silently drop prefetch targets, surfacing
                // later as scattered `error.NotPrefetched`. The filtered store is
                // SDK-written and atomic-committed, so a read or decompress
                // failure is real corruption. Fail loud at the source.
                const payload = try store.readPayload(i, payload_buf);
                const decoded = try log_serial.decompressEntry(payload, decompress_buf);
                const log_count = log_serial.deserializeLogs(decoded, log_buf);
                try matchAndAppend(arena, &out, log_buf[0..log_count], m, cache);
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

/// Drop entries already in the cache, order-preserving. Lookups are in-memory
/// hash hits, so no transaction or batch helper is needed.
pub fn filterUncached(
    arena: std.mem.Allocator,
    cache: *const ethcall.Cache,
    calls: []const ethcall.Call,
) ![]ethcall.Call {
    if (calls.len == 0) return &.{};

    var out: std.ArrayList(ethcall.Call) = .empty;
    try out.ensureTotalCapacity(arena, calls.len);

    for (calls) |c| {
        if (cache.get(c.target, c.calldata) == null) {
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

const Swap = struct {
    pub const signature = "Swap(address indexed sender, uint256 amount0Out, uint256 amount1Out, address indexed to)";
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

test "gatherOneBlock builds parameterized calldata; arg words match encodeArg" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const FACTORY = [_]u8{0xF0} ** 20;
    const TOKEN0 = [_]u8{0x11} ** 20;
    const TOKEN1 = [_]u8{0x22} ** 20;
    var topics: [4][32]u8 = std.mem.zeroes([4][32]u8);
    topics[0] = sdk_manifest.eventTopic0(PairCreated);
    @memcpy(topics[1][12..32], &TOKEN0); // token0 indexed
    @memcpy(topics[2][12..32], &TOKEN1); // token1 indexed
    const log: RawLog = .{
        .block_number = 1,
        .tx_index = 0,
        .log_index = 0,
        .address = FACTORY,
        .topic_count = 3,
        .topics = topics,
        .data = &.{},
        .tx_hash = [_]u8{0} ** 32,
    };

    const m: sdk_manifest.Manifest = .{
        .name = "x",
        .chain_id = 1,
        .start_block = 0,
        .prefetch = &.{.{
            .on_event = PairCreated,
            .calls = &.{.{
                .address = .log,
                .method = "getPair(address,address)",
                .args = &.{ .{ .param = "token0" }, .{ .param = "token1" } },
            }},
        }},
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try ethcall.Cache.open(std.testing.allocator, tmp.dir);
    defer cache.deinit();
    const calls = try gatherOneBlock(arena, &.{log}, m, &cache);
    try std.testing.expectEqual(@as(usize, 1), calls.len);
    try std.testing.expectEqualSlices(u8, &FACTORY, &calls[0].target);
    try std.testing.expectEqual(@as(usize, 4 + 64), calls[0].calldata.len);

    const sel = ethcall.selectorOf("getPair(address,address)");
    try std.testing.expectEqualSlices(u8, &sel, calls[0].calldata[0..4]);
    // Each arg word equals the event's topic word AND encodeArg of the value,
    // so the handler's ethCallArgs(.{token0, token1}) yields the same calldata.
    try std.testing.expectEqualSlices(u8, &ethcall.encodeArg(TOKEN0), calls[0].calldata[4..36]);
    try std.testing.expectEqualSlices(u8, &ethcall.encodeArg(TOKEN1), calls[0].calldata[36..68]);
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

    var cache = try ethcall.Cache.open(std.testing.allocator, tmp.dir);
    defer cache.deinit();

    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const A = [_]u8{0xAA} ** 20;
    const B = [_]u8{0xBB} ** 20;
    const SEL = [_]u8{ 0x31, 0x3c, 0xe5, 0x67 };

    // Pre-warm A's decimals() in the cache.
    var payload: [32]u8 = std.mem.zeroes([32]u8);
    payload[31] = 18;
    try cache.put(A, &SEL, 0, &payload);

    var raw = try arena.alloc(ethcall.Call, 2);
    raw[0] = .{ .target = A, .calldata = &SEL }; // already cached
    raw[1] = .{ .target = B, .calldata = &SEL }; // not cached

    const remaining = try filterUncached(arena, &cache, raw);
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
    try std.testing.expectEqualSlices(u8, &B, &remaining[0].target);
}

test "gatherOneBlock emits one Call per matching log per PrefetchCall" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const Sync = struct {
        pub const signature = "Sync(uint112 reserve0, uint112 reserve1)";
    };
    const FACTORY: [20]u8 = [_]u8{0xF0} ** 20;
    const PAIR: [20]u8 = [_]u8{0xC1} ** 20;

    const m: sdk_manifest.Manifest = .{
        .name = "uni",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FACTORY,
            .create_event = PairCreated,
            .spawn_param = "pair",
            .child_events = &.{Sync},
        }},
        .prefetch = &.{.{
            .on_event = PairCreated,
            .calls = &.{.{ .address = .{ .param = "pair" }, .method = "decimals()" }},
        }},
    };

    // Plant one PairCreated log with the pair address in data[0..32].
    const create_topic = sdk_manifest.eventTopic0(PairCreated);
    var data: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(data[12..32], &PAIR);
    const log: RawLog = .{
        .block_number = 100,
        .tx_index = 0,
        .log_index = 0,
        .address = FACTORY,
        .topic_count = 1,
        .topics = .{ create_topic, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &data,
        .tx_hash = [_]u8{0xFE} ** 32,
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try ethcall.Cache.open(std.testing.allocator, tmp.dir);
    defer cache.deinit();
    const calls = try gatherOneBlock(arena, &.{log}, m, &cache);
    try std.testing.expectEqual(@as(usize, 1), calls.len);
    try std.testing.expectEqualSlices(u8, &PAIR, &calls[0].target);
    const expected_sel = ethcall.selectorOf("decimals()");
    try std.testing.expectEqualSlices(u8, &expected_sel, calls[0].calldata);
}

test "gatherOneBlock returns empty when no log matches a PrefetchDef" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: sdk_manifest.Manifest = .{
        .name = "uni",
        .chain_id = 1,
        .start_block = 0,
        .prefetch = &.{.{
            .on_event = PairCreated,
            .calls = &.{.{ .address = .{ .param = "pair" }, .method = "decimals()" }},
        }},
    };

    // A Transfer log. topic0 doesn't match PairCreated.
    const transfer_topic = sdk_manifest.eventTopic0(Transfer);
    const log: RawLog = .{
        .block_number = 100,
        .tx_index = 0,
        .log_index = 0,
        .address = [_]u8{0xAA} ** 20,
        .topic_count = 1,
        .topics = .{ transfer_topic, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0} ** 32,
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try ethcall.Cache.open(std.testing.allocator, tmp.dir);
    defer cache.deinit();
    const calls = try gatherOneBlock(arena, &.{log}, m, &cache);
    try std.testing.expectEqual(@as(usize, 0), calls.len);
}

test "gatherDynamic returns empty for a manifest with no prefetch" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const m: sdk_manifest.Manifest = .{ .name = "x", .chain_id = 1, .start_block = 0 };
    var cache = try ethcall.Cache.open(std.testing.allocator, tmp.dir);
    defer cache.deinit();
    const calls = try gatherDynamic(arena, tmp.dir, m, &cache);
    try std.testing.expectEqual(@as(usize, 0), calls.len);
}

test "gatherOneBlock chains .of: skipped until producer cached, then resolved" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try ethcall.Cache.open(std.testing.allocator, tmp.dir);
    defer cache.deinit();

    const POOL = [_]u8{0xC1} ** 20;
    const TOKEN0 = [_]u8{0x77} ** 20;

    const m: sdk_manifest.Manifest = .{
        .name = "uni",
        .chain_id = 1,
        .start_block = 0,
        .prefetch = &.{.{
            .on_event = Swap,
            .calls = &.{
                .{ .address = .log, .method = "token0()" },
                .{ .address = .{ .of = "token0()" }, .method = "decimals()" },
            },
        }},
    };

    var topics: [4][32]u8 = std.mem.zeroes([4][32]u8);
    topics[0] = sdk_manifest.eventTopic0(Swap);
    const log: RawLog = .{
        .block_number = 1,
        .tx_index = 0,
        .log_index = 0,
        .address = POOL,
        .topic_count = 1,
        .topics = topics,
        .data = &.{},
        .tx_hash = [_]u8{0} ** 32,
    };

    // Round 0: token0() uncached, so the chained decimals() can't resolve its
    // target and is dropped. Only the producer call is queued.
    const r0 = try gatherOneBlock(arena, &.{log}, m, &cache);
    try std.testing.expectEqual(@as(usize, 1), r0.len);
    try std.testing.expectEqualSlices(u8, &POOL, &r0[0].target);

    // Warm token0()'s result (address right-aligned in the word), then the
    // chain resolves: decimals() now targets TOKEN0.
    const token0_sel = ethcall.selectorOf("token0()");
    var word: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(word[12..32], &TOKEN0);
    try cache.put(POOL, &token0_sel, 0, &word);

    const r1 = try gatherOneBlock(arena, &.{log}, m, &cache);
    try std.testing.expectEqual(@as(usize, 2), r1.len);
    try std.testing.expectEqualSlices(u8, &POOL, &r1[0].target);
    const decimals_sel = ethcall.selectorOf("decimals()");
    try std.testing.expectEqualSlices(u8, &TOKEN0, &r1[1].target);
    try std.testing.expectEqualSlices(u8, &decimals_sel, r1[1].calldata);
}

test "gatherOneBlock drops a chained call when the producer reverted" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try ethcall.Cache.open(std.testing.allocator, tmp.dir);
    defer cache.deinit();

    const POOL = [_]u8{0xC1} ** 20;

    const m: sdk_manifest.Manifest = .{
        .name = "uni",
        .chain_id = 1,
        .start_block = 0,
        .prefetch = &.{.{
            .on_event = Swap,
            .calls = &.{
                .{ .address = .log, .method = "token0()" },
                .{ .address = .{ .of = "token0()" }, .method = "decimals()" },
            },
        }},
    };

    var topics: [4][32]u8 = std.mem.zeroes([4][32]u8);
    topics[0] = sdk_manifest.eventTopic0(Swap);
    const log: RawLog = .{
        .block_number = 1,
        .tx_index = 0,
        .log_index = 0,
        .address = POOL,
        .topic_count = 1,
        .topics = topics,
        .data = &.{},
        .tx_hash = [_]u8{0} ** 32,
    };

    // A reverted producer (status 1) yields no address, so the chained call
    // stays dropped and never re-resolves.
    const token0_sel = ethcall.selectorOf("token0()");
    try cache.put(POOL, &token0_sel, 1, &.{});

    const calls = try gatherOneBlock(arena, &.{log}, m, &cache);
    try std.testing.expectEqual(@as(usize, 1), calls.len);
    try std.testing.expectEqualSlices(u8, &POOL, &calls[0].target);
}
