/// Filtered-index scanner. Two entry points:
///
/// - `scanCreations`: walks the `primary` filtered pair, dispatches only
///   factory creation events to a comptime-restricted collector returning
///   discovered child addresses. No user handlers run.
///
/// - `replay`: walks the `primary` pair (always) and `children` pair (when
///   present), k-way-merges logs by `(block_number, tx_index, log_index)`,
///   dispatches each to the user's handler via
///   `handler.dispatcherFor(manifest).dispatch`.
///
/// Both assume the pairs were produced by `filter_builder`, so file names
/// follow `BASE_PRIMARY` / `BASE_CHILDREN`.
const std = @import("std");

const core = @import("core");

const filter_builder = @import("filter_builder.zig");
const filtered_store_mod = @import("filtered_store.zig");
const handler_mod = @import("handler.zig");
const humanize = @import("humanize.zig");
const sdk_manifest = @import("manifest.zig");

const FilteredStore = filtered_store_mod.FilteredStore;

const RawLog = core.RawLog;
const log_serial = core.log_serial;
const parallel = core.parallel;
const types = core.types;

pub const ReplayOptions = struct {
    commit_interval: u32 = 100_000,
    /// Skip every filtered-index block whose number is at or below `start_block`.
    /// `entry.init` seeds this from `state.snap.cursor`, so replay against an
    /// existing entity store re-dispatches only the uncovered range. Default 0
    /// starts from the oldest entry.
    start_block: u64 = 0,
};

pub const ReplayResult = struct {
    logs_dispatched: u64 = 0,
    blocks_dispatched: u64 = 0,
    elapsed_ns: u64 = 0,
};

// ── scanCreations ────────────────────────────────────────────────────────

/// Walk `BLOCKS_PRIMARY` and collect spawned-contract addresses from every
/// log whose `topic0` matches a factory's `create_event`. Caller owns the
/// returned hashmap. Returns an empty map for factory-free manifests, for
/// envs missing `BLOCKS_PRIMARY`, and for empty filtered indexes.
pub fn scanCreations(
    dir: std.fs.Dir,
    comptime m: sdk_manifest.Manifest,
    allocator: std.mem.Allocator,
) !std.AutoHashMap([20]u8, void) {
    var discovered = std.AutoHashMap([20]u8, void).init(allocator);
    if (comptime m.factories.len == 0) return discovered;

    var primary = FilteredStore.open(allocator, dir, filter_builder.BASE_PRIMARY) catch return discovered;
    defer primary.deinit();

    const decompress_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(decompress_buf);
    const payload_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(payload_buf);
    const log_buf = try allocator.alloc(RawLog, types.MAX_LOGS_PER_BLOCK);
    defer allocator.free(log_buf);

    var i: u64 = 0;
    while (i < primary.count()) : (i += 1) {
        const entry = try primary.readEntry(i);
        // A corrupt entry here would silently drop a factory child spawned in
        // this block. The filtered store is engine-written and atomic-committed,
        // so a read or decompress failure is real corruption: fail loud.
        const payload = try primary.readPayload(i, payload_buf);
        const decoded = try log_serial.decompressEntry(payload, decompress_buf);
        const log_count = log_serial.deserializeLogs(decoded, log_buf);

        for (log_buf[0..log_count]) |*log| {
            log.block_number = entry.block_number;
            if (log.topic_count == 0) continue;
            inline for (m.factories) |f| {
                const create_topic = comptime sdk_manifest.eventTopic0(f.create_event);
                if (std.mem.eql(u8, &log.topics[0], &create_topic)) {
                    const addr = sdk_manifest.extractFactoryAddress(f, &log.topics, log.data);
                    try discovered.put(addr, {});
                }
            }
        }
    }
    return discovered;
}

// ── replay ───────────────────────────────────────────────────────────────

/// Walk the filtered index in canonical `(block_number, tx_index, log_index)`
/// order, dispatching each log via the comptime topic0 dispatcher. Logs with
/// no matching manifest event (bloom false positives) are skipped by the
/// dispatcher, not here.
///
/// Working buffers are heap-allocated from the ctx's allocator (main-thread
/// paths get the heap, per `core.parallel`).
pub fn replay(
    dir: std.fs.Dir,
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    options: ReplayOptions,
) !ReplayResult {
    var result = ReplayResult{};
    var timer = try std.time.Timer.start();
    var events_since_commit: u32 = 0;

    comptime handler_mod.dispatcherFor(m).validateHandler(Handler);

    const allocator = ctxAllocator(ctx);

    // Primary pair may not exist when filter_builder.build matched zero blocks
    // (e.g. fresh `--follow` before any historical match). Treat like the
    // children pair: empty walker, no work.
    var primary_storage: CursorWalker = undefined;
    var primary: ?*CursorWalker = null;
    if (CursorWalker.open(allocator, dir, filter_builder.BASE_PRIMARY, options.start_block)) |w| {
        primary_storage = w;
        primary = &primary_storage;
    } else |_| {}
    defer if (primary) |p| p.deinit();

    const has_children = comptime m.factories.len > 0;
    var children_storage: CursorWalker = undefined;
    var children: ?*CursorWalker = null;
    if (has_children) {
        if (CursorWalker.open(allocator, dir, filter_builder.BASE_CHILDREN, options.start_block)) |w| {
            children_storage = w;
            children = &children_storage;
        } else |_| {}
    }
    defer if (children) |c| c.deinit();

    const decompress_primary = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(decompress_primary);
    const log_buf_primary = try allocator.alloc(RawLog, types.MAX_LOGS_PER_BLOCK);
    defer allocator.free(log_buf_primary);
    const merge_buf = try allocator.alloc(RawLog, 2 * types.MAX_LOGS_PER_BLOCK);
    defer allocator.free(merge_buf);
    const decompress_children = if (has_children) try allocator.alloc(u8, types.BLOCK_BUF_SIZE) else &[_]u8{};
    defer if (has_children) allocator.free(decompress_children);
    const log_buf_children = if (has_children) try allocator.alloc(RawLog, types.MAX_LOGS_PER_BLOCK) else &[_]RawLog{};
    defer if (has_children) allocator.free(log_buf_children);

    // Verbose-only live counter. `show_progress` short-circuits the per-block
    // mask test off the hot path when the level is normal or silent.
    const show_progress = core.log.getLevel() == .verbose;
    const total_blocks: u64 = (if (primary) |p| p.store.count() else 0) +
        (if (children) |c| c.store.count() else 0);

    while (true) {
        const next_p: ?u64 = if (primary) |p| try p.peek() else null;
        const next_c: ?u64 = if (children) |c| try c.peek() else null;
        if (next_p == null and next_c == null) break;

        const block_number = pickMin(next_p, next_c);
        var block_ts: u32 = 0;

        var merge_count: usize = 0;
        if (primary) |p| if (next_p) |bn| if (bn == block_number) {
            block_ts = p.peekedTimestamp();
            const logs = try p.consume(block_number, decompress_primary, log_buf_primary);
            for (logs) |log| {
                merge_buf[merge_count] = log;
                merge_count += 1;
            }
        };
        if (children) |c| {
            if (next_c) |bn| if (bn == block_number) {
                if (block_ts == 0) block_ts = c.peekedTimestamp();
                const logs = try c.consume(block_number, decompress_children, log_buf_children);
                for (logs) |log| {
                    merge_buf[merge_count] = log;
                    merge_count += 1;
                }
            };
        }

        if (merge_count == 0) continue;
        std.mem.sort(RawLog, merge_buf[0..merge_count], {}, lessByTxLog);

        // Prefer the exact time carried in the FilteredStore entry. The remote
        // stream fills it from the PUSH frame. 0 means a local build, fall
        // back to the engine's timestamps.bin via `timestampOf`.
        ctx.block_number = block_number;
        ctx.timestamp = if (block_ts != 0) @as(u64, block_ts) else humanize.timestampOf(ctx, block_number);
        result.blocks_dispatched += 1;
        if (show_progress and (result.blocks_dispatched & 0x3FFF) == 0)
            core.log.debug("\r  replaying {d}/{d} blocks", .{ result.blocks_dispatched, total_blocks });

        for (merge_buf[0..merge_count]) |log| {
            try handler_mod.dispatchLog(m, Handler, ctx, log);
            result.logs_dispatched += 1;
            events_since_commit += 1;
        }

        // Block boundary. Record the cursor and gate the commit check here.
        // Mid-block commits would leave the cursor at a partially-dispatched
        // block.
        setLastDispatched(ctx, block_number);
        if (events_since_commit >= options.commit_interval) {
            try maybeCommit(ctx);
            events_since_commit = 0;
        }
    }

    if (show_progress and total_blocks > 0)
        core.log.debug("\r  replaying {d}/{d} blocks\n", .{ result.blocks_dispatched, total_blocks });

    result.elapsed_ns = timer.read();
    return result;
}

/// Pull an allocator off the ctx. `entry.Context` exposes `_allocator`, the
/// test `Counter` uses the bare name `allocator`.
inline fn ctxAllocator(ctx: anytype) std.mem.Allocator {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime @hasField(T, "_allocator")) return ctx._allocator;
    if (comptime @hasField(T, "allocator")) return ctx.allocator;
    @compileError("scanner.replay: ctx of type '" ++ @typeName(T) ++ "' must expose `_allocator` or `allocator` so replay can size its merge buffers off the heap.");
}

/// Calls `ctx.commitCycle()` if the context type defines one. Counter-shaped
/// test contexts (no entity stores) skip the commit. entry.zig's Context
/// provides commitCycle and gets the every-N-events flush.
inline fn maybeCommit(ctx: anytype) !void {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime @hasDecl(T, "commitCycle")) {
        try ctx.commitCycle();
    }
}

/// Set the ctx's cursor-boundary field if it has one. Counter-shaped test
/// contexts don't, and skip silently.
inline fn setLastDispatched(ctx: anytype, block: u64) void {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime @hasField(T, "_last_dispatched_block")) {
        ctx._last_dispatched_block = block;
    }
}

fn pickMin(a: ?u64, b: ?u64) u64 {
    if (a) |va| if (b) |vb| return @min(va, vb) else return va;
    if (b) |vb| return vb;
    unreachable;
}

fn lessByTxLog(_: void, a: RawLog, b: RawLog) bool {
    if (a.tx_index != b.tx_index) return a.tx_index < b.tx_index;
    return a.log_index < b.log_index;
}

/// Position-based iterator over a `FilteredStore`. peek/consume let replay's
/// merge loop advance one pair at a time without juggling cursor state across
/// two streams.
const CursorWalker = struct {
    store: FilteredStore,
    /// Owns the next-payload scratch buffer. Sized for one block's LZ4 entry.
    payload_buf: []u8,
    next_index: u64,
    /// IndexEntry at `next_index`, cached by `peek` and reused by `consume`.
    /// Without it every iteration reads the same entry twice (block_number,
    /// then offset+length).
    peeked: ?filtered_store_mod.IndexEntry = null,

    /// `start_block == 0` walks from the oldest entry. Any other value seeks
    /// past entries whose block_number is `<= start_block`.
    fn open(
        allocator: std.mem.Allocator,
        dir: std.fs.Dir,
        comptime base: []const u8,
        start_block: u64,
    ) !CursorWalker {
        var store = try FilteredStore.open(allocator, dir, base);
        errdefer store.deinit();
        const payload_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
        errdefer allocator.free(payload_buf);
        const next_index = if (start_block == 0) @as(u64, 0) else try store.seekPast(start_block);
        return .{ .store = store, .payload_buf = payload_buf, .next_index = next_index };
    }

    fn deinit(self: *CursorWalker) void {
        self.store.allocator.free(self.payload_buf);
        self.store.deinit();
    }

    fn peek(self: *CursorWalker) !?u64 {
        if (self.next_index >= self.store.count()) return null;
        if (self.peeked == null) {
            // A read failure here is corruption, not end of stream. Propagate so
            // replay fails loud instead of silently truncating the dispatch.
            self.peeked = try self.store.readEntry(self.next_index);
        }
        return self.peeked.?.block_number;
    }

    /// Exact block time of the peeked entry (0 = unknown). Valid only after
    /// `peek` cached the entry. The merge loop reads it before `consume`.
    fn peekedTimestamp(self: *const CursorWalker) u32 {
        return if (self.peeked) |e| e.timestamp else 0;
    }

    fn consume(
        self: *CursorWalker,
        block_number: u64,
        decompress_buf: []u8,
        log_buf: []RawLog,
    ) ![]RawLog {
        const entry = self.peeked orelse try self.store.readEntry(self.next_index);
        const payload = try self.store.readPayloadFor(entry, self.payload_buf);
        const decoded = try log_serial.decompressEntry(payload, decompress_buf);
        const log_count = log_serial.deserializeLogs(decoded, log_buf);
        for (log_buf[0..log_count]) |*log| log.block_number = block_number;
        self.next_index += 1;
        self.peeked = null;
        return log_buf[0..log_count];
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;
const FlatStoreReader = core.FlatStoreReader;

const TestLog = core.flat_reader.TestLog;
const TestBlock = core.flat_reader.TestBlock;

const Transfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
};
const Approval = struct {
    pub const signature = "Approval(address,address,uint256)";
};
const PairCreated = struct {
    pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair, uint256 allPairsLength)";
};
const Sync = struct {
    pub const signature = "Sync(uint112,uint112)";
};

const ADDR_TOKEN: [20]u8 = [_]u8{0xAE} ** 20;
const FACTORY_ADDR: [20]u8 = [_]u8{0xF0} ** 20;
const CHILD_1: [20]u8 = [_]u8{0xC1} ** 20;
const CHILD_2: [20]u8 = [_]u8{0xC2} ** 20;

fn topicOf(comptime E: type) [32]u8 {
    return sdk_manifest.eventTopic0(E);
}

const Counter = struct {
    transfers: u32 = 0,
    approvals: u32 = 0,
    syncs: u32 = 0,
    pair_creations: u32 = 0,
    dispatched: std.ArrayListUnmanaged(DispatchedLog) = .{},
    block_number: u64 = 0,
    timestamp: u64 = 0,
    allocator: std.mem.Allocator,

    const DispatchedLog = struct {
        block_number: u64,
        tx_index: u16,
        log_index: u16,
        topic0_first_byte: u8,
    };

    fn deinit(self: *Counter) void {
        self.dispatched.deinit(self.allocator);
    }

    /// `log` is `anytype` so the same helper accepts every Log(E) shape.
    fn record(self: *Counter, log: anytype) !void {
        try self.dispatched.append(self.allocator, .{
            .block_number = log.block_number,
            .tx_index = log.tx_index,
            .log_index = log.log_index,
            .topic0_first_byte = log.topics[0][0],
        });
    }

    pub fn handleTransfer(log: handler_mod.Log(Transfer), self: *Counter) !void {
        self.transfers += 1;
        try self.record(log);
    }
    pub fn handleApproval(log: handler_mod.Log(Approval), self: *Counter) !void {
        self.approvals += 1;
        try self.record(log);
    }
    pub fn handleSync(log: handler_mod.Log(Sync), self: *Counter) !void {
        self.syncs += 1;
        try self.record(log);
    }
    pub fn handlePairCreated(log: handler_mod.Log(PairCreated), self: *Counter) !void {
        self.pair_creations += 1;
        try self.record(log);
    }
};

test "scanCreations: extracts spawned addresses from factory creation events" {
    const allocator = testing.allocator;

    // Plant a flat store containing a PairCreated log at block 100 whose
    // `pair` address (the spawn_param target) sits at data[0..32].
    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();

    const create_topic = topicOf(PairCreated);

    // writeTestStore stores an empty `data` field, so craft a single block
    // manually with the spawned address in topic[1]. Exercises the indexed
    // slot path of extractFactoryAddress.
    const flat_reader = core.flat_reader;
    const bloom = core.bloom;

    var blocks_file = try src_tmp.dir.createFile("blocks.dat", .{});
    defer blocks_file.close();
    var idx_file = try src_tmp.dir.createFile("blocks.idx", .{});
    defer idx_file.close();
    var blooms_file = try src_tmp.dir.createFile("blooms.bin", .{});
    defer blooms_file.close();

    const FACTORY_BLOCK: u64 = 100;
    var idx_hdr: [flat_reader.INDEX_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, idx_hdr[0..8], FACTORY_BLOCK, .little);
    std.mem.writeInt(u64, idx_hdr[8..16], 1, .little);
    try idx_file.writeAll(&idx_hdr);

    var blooms_hdr: [flat_reader.BLOOM_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, &blooms_hdr, 1, .little);
    try blooms_file.writeAll(&blooms_hdr);

    var spawned_topic1: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(spawned_topic1[12..32], &CHILD_1);

    const log: RawLog = .{
        .block_number = FACTORY_BLOCK,
        .tx_index = 0,
        .log_index = 0,
        .address = FACTORY_ADDR,
        .topic_count = 2,
        .topics = .{ create_topic, spawned_topic1, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };

    var serialize_buf: [4096]u8 = undefined;
    var compress_buf: [4096]u8 = undefined;
    const written = log_serial.serializeLogs(&[_]RawLog{log}, &serialize_buf);
    const entry_len = try log_serial.compressEntry(serialize_buf[0..written], &compress_buf);
    try blocks_file.writeAll(compress_buf[0..entry_len]);

    var idx_entry: [flat_reader.INDEX_ENTRY_SIZE]u8 = undefined;
    std.mem.writeInt(u64, idx_entry[0..8], 0, .little);
    std.mem.writeInt(u32, idx_entry[8..12], @intCast(entry_len), .little);
    try idx_file.writeAll(&idx_entry);

    const tb = log_serial.buildTopicBloom(&[_]RawLog{log});
    const ab = log_serial.buildAddrBloom(&[_]RawLog{log});
    var bloom_entry: [flat_reader.BLOOM_ENTRY_SIZE]u8 = std.mem.zeroes([flat_reader.BLOOM_ENTRY_SIZE]u8);
    std.mem.writeInt(u64, bloom_entry[0..8], FACTORY_BLOCK, .big);
    @memcpy(bloom_entry[flat_reader.TOPIC_BLOOM_OFFSET..][0..bloom.BLOOM_SIZE], &tb.bits);
    @memcpy(bloom_entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE], &ab.bits);
    try blooms_file.writeAll(&bloom_entry);

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    const FactoryManifest: sdk_manifest.Manifest = .{
        .name = "factory",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FACTORY_ADDR,
            .create_event = PairCreated,
            .spawn_param = "token0",
            .child_events = &.{Sync},
        }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    _ = try filter_builder.build(&reader, FactoryManifest, dst_tmp.dir, allocator);

    var discovered = try scanCreations(dst_tmp.dir, FactoryManifest, allocator);
    defer discovered.deinit();

    try testing.expectEqual(@as(u32, 1), discovered.count());
    try testing.expect(discovered.contains(CHILD_1));
}

test "replay: dispatches logs in canonical (block, tx, log_index) order across one DBI" {
    const allocator = testing.allocator;

    // Block 100 has logs at (tx=0, log=0) and (tx=0, log=1). writeTestStore
    // preserves the given order, so plant them out of order to verify the
    // scanner sorts within a block.
    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();

    const tt = topicOf(Transfer);
    const blocks = [_]TestBlock{
        .{ .block_number = 100, .logs = &.{
            .{ .tx_index = 0, .log_index = 1, .address = ADDR_TOKEN, .topic0 = tt },
            .{ .tx_index = 0, .log_index = 0, .address = ADDR_TOKEN, .topic0 = tt },
        } },
        .{ .block_number = 101, .logs = &.{
            .{ .tx_index = 0, .log_index = 0, .address = ADDR_TOKEN, .topic0 = tt },
        } },
    };
    try core.flat_reader.writeTestStore(src_tmp.dir, &blocks, allocator);

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    const Manifest: sdk_manifest.Manifest = .{
        .name = "test",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    _ = try filter_builder.build(&reader, Manifest, dst_tmp.dir, allocator);

    var counter = Counter{ .allocator = allocator };
    defer counter.deinit();

    const result = try replay(dst_tmp.dir, Manifest, Counter, &counter, .{});
    try testing.expectEqual(@as(u64, 3), result.logs_dispatched);
    try testing.expectEqual(@as(u64, 2), result.blocks_dispatched);
    try testing.expectEqual(@as(u32, 3), counter.transfers);

    // Canonical order: (100,0,0), (100,0,1), (101,0,0).
    try testing.expectEqual(@as(usize, 3), counter.dispatched.items.len);
    try testing.expectEqual(@as(u64, 100), counter.dispatched.items[0].block_number);
    try testing.expectEqual(@as(u16, 0), counter.dispatched.items[0].log_index);
    try testing.expectEqual(@as(u64, 100), counter.dispatched.items[1].block_number);
    try testing.expectEqual(@as(u16, 1), counter.dispatched.items[1].log_index);
    try testing.expectEqual(@as(u64, 101), counter.dispatched.items[2].block_number);
}

test "replay seeks past start_block so already-dispatched range is skipped" {
    // When init reads a non-zero cursor from `_meta`, scanner seeks past the
    // already-covered range without per-block iteration. Plant three blocks,
    // run replay with start_block = 100, expect only blocks 101 and 102.
    const allocator = testing.allocator;
    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();

    const tt = topicOf(Transfer);
    const blocks = [_]TestBlock{
        .{ .block_number = 100, .logs = &.{.{ .address = ADDR_TOKEN, .topic0 = tt }} },
        .{ .block_number = 101, .logs = &.{.{ .address = ADDR_TOKEN, .topic0 = tt }} },
        .{ .block_number = 102, .logs = &.{.{ .address = ADDR_TOKEN, .topic0 = tt }} },
    };
    try core.flat_reader.writeTestStore(src_tmp.dir, &blocks, allocator);

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    const Manifest: sdk_manifest.Manifest = .{
        .name = "test",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    _ = try filter_builder.build(&reader, Manifest, dst_tmp.dir, allocator);

    var counter = Counter{ .allocator = allocator };
    defer counter.deinit();

    const result = try replay(dst_tmp.dir, Manifest, Counter, &counter, .{ .start_block = 100 });
    try testing.expectEqual(@as(u64, 2), result.logs_dispatched);
    try testing.expectEqual(@as(u64, 2), result.blocks_dispatched);
    try testing.expectEqual(@as(u32, 2), counter.transfers);
    try testing.expectEqual(@as(u64, 101), counter.dispatched.items[0].block_number);
    try testing.expectEqual(@as(u64, 102), counter.dispatched.items[1].block_number);

    // start_block at or beyond the max key dispatches nothing.
    var counter2 = Counter{ .allocator = allocator };
    defer counter2.deinit();
    const result2 = try replay(dst_tmp.dir, Manifest, Counter, &counter2, .{ .start_block = 102 });
    try testing.expectEqual(@as(u64, 0), result2.logs_dispatched);
    try testing.expectEqual(@as(u64, 0), result2.blocks_dispatched);
}

test "replay: k-way merge across BLOCKS_PRIMARY and BLOCKS_CHILDREN preserves block order" {
    const allocator = testing.allocator;

    // Plant a flat store with:
    //   block 100: factory address emits PairCreated → primary
    //   block 101: child 1 emits Sync
    //   block 102: factory address emits another PairCreated → primary
    //   block 103: child 2 emits Sync
    // After build + appendChildren, PRIMARY has {100, 102} and CHILDREN has
    // {101, 103}. replay's k-way merge must dispatch in (100, 101, 102, 103).
    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();

    const create_topic = topicOf(PairCreated);
    const sync_topic = topicOf(Sync);

    const blocks = [_]TestBlock{
        .{ .block_number = 100, .logs = &.{.{ .address = FACTORY_ADDR, .topic0 = create_topic }} },
        .{ .block_number = 101, .logs = &.{.{ .address = CHILD_1, .topic0 = sync_topic }} },
        .{ .block_number = 102, .logs = &.{.{ .address = FACTORY_ADDR, .topic0 = create_topic }} },
        .{ .block_number = 103, .logs = &.{.{ .address = CHILD_2, .topic0 = sync_topic }} },
    };
    try core.flat_reader.writeTestStore(src_tmp.dir, &blocks, allocator);

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    const FactoryManifest: sdk_manifest.Manifest = .{
        .name = "factory",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FACTORY_ADDR,
            .create_event = PairCreated,
            .spawn_param = "pair",
            .child_events = &.{Sync},
        }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    _ = try filter_builder.build(&reader, FactoryManifest, dst_tmp.dir, allocator);
    const child_addrs = [_][20]u8{ CHILD_1, CHILD_2 };
    _ = try filter_builder.appendChildren(&reader, FactoryManifest, &child_addrs, dst_tmp.dir, allocator);

    var counter = Counter{ .allocator = allocator };
    defer counter.deinit();

    const result = try replay(dst_tmp.dir, FactoryManifest, Counter, &counter, .{});
    try testing.expectEqual(@as(u64, 4), result.logs_dispatched);
    try testing.expectEqual(@as(u64, 4), result.blocks_dispatched);
    try testing.expectEqual(@as(u32, 2), counter.pair_creations);
    try testing.expectEqual(@as(u32, 2), counter.syncs);

    // (100, 101, 102, 103) in dispatch order.
    try testing.expectEqual(@as(usize, 4), counter.dispatched.items.len);
    try testing.expectEqual(@as(u64, 100), counter.dispatched.items[0].block_number);
    try testing.expectEqual(@as(u64, 101), counter.dispatched.items[1].block_number);
    try testing.expectEqual(@as(u64, 102), counter.dispatched.items[2].block_number);
    try testing.expectEqual(@as(u64, 103), counter.dispatched.items[3].block_number);
}

test "factory orchestration: build → scanCreations → appendChildren → replay" {
    const allocator = testing.allocator;

    // Block 100: factory creates child_1.
    // Block 101: child_1 emits Sync (child event).
    // Block 102: noise. Unrelated address with unrelated topic.
    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();

    const create_topic = topicOf(PairCreated);
    const sync_topic = topicOf(Sync);

    // Factory log needs data with the spawned address at offset 0. writeTestStore
    // plants empty data, so build a custom log with non-empty data.
    const flat_reader = core.flat_reader;
    const bloom = core.bloom;

    var blocks_file = try src_tmp.dir.createFile("blocks.dat", .{});
    defer blocks_file.close();
    var idx_file = try src_tmp.dir.createFile("blocks.idx", .{});
    defer idx_file.close();
    var blooms_file = try src_tmp.dir.createFile("blooms.bin", .{});
    defer blooms_file.close();

    var idx_hdr: [flat_reader.INDEX_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, idx_hdr[0..8], 100, .little);
    std.mem.writeInt(u64, idx_hdr[8..16], 3, .little);
    try idx_file.writeAll(&idx_hdr);

    var blooms_hdr: [flat_reader.BLOOM_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, &blooms_hdr, 3, .little);
    try blooms_file.writeAll(&blooms_hdr);

    var spawned_data: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(spawned_data[12..32], &CHILD_1);

    const factory_log: RawLog = .{
        .block_number = 100,
        .tx_index = 0,
        .log_index = 0,
        .address = FACTORY_ADDR,
        .topic_count = 1,
        .topics = .{ create_topic, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &spawned_data,
        .tx_hash = [_]u8{0xFE} ** 32,
    };
    const child_log: RawLog = .{
        .block_number = 101,
        .tx_index = 0,
        .log_index = 0,
        .address = CHILD_1,
        .topic_count = 1,
        .topics = .{ sync_topic, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };
    const noise_topic = topicOf(Approval);
    const noise_log: RawLog = .{
        .block_number = 102,
        .tx_index = 0,
        .log_index = 0,
        .address = [_]u8{0x99} ** 20,
        .topic_count = 1,
        .topics = .{ noise_topic, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };

    const all_logs = [_][]const RawLog{ &.{factory_log}, &.{child_log}, &.{noise_log} };
    var serialize_buf: [4096]u8 = undefined;
    var compress_buf: [4096]u8 = undefined;
    var offset: u64 = 0;
    for (all_logs, 0..) |logs, i| {
        const written = log_serial.serializeLogs(logs, &serialize_buf);
        const entry_len = try log_serial.compressEntry(serialize_buf[0..written], &compress_buf);
        try blocks_file.writeAll(compress_buf[0..entry_len]);

        var idx_entry: [flat_reader.INDEX_ENTRY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, idx_entry[0..8], offset, .little);
        std.mem.writeInt(u32, idx_entry[8..12], @intCast(entry_len), .little);
        try idx_file.writeAll(&idx_entry);
        offset += entry_len;

        const tb = log_serial.buildTopicBloom(logs);
        const ab = log_serial.buildAddrBloom(logs);
        var bloom_entry: [flat_reader.BLOOM_ENTRY_SIZE]u8 = std.mem.zeroes([flat_reader.BLOOM_ENTRY_SIZE]u8);
        std.mem.writeInt(u64, bloom_entry[0..8], 100 + @as(u64, i), .big);
        @memcpy(bloom_entry[flat_reader.TOPIC_BLOOM_OFFSET..][0..bloom.BLOOM_SIZE], &tb.bits);
        @memcpy(bloom_entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE], &ab.bits);
        try blooms_file.writeAll(&bloom_entry);
    }

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    const FactoryManifest: sdk_manifest.Manifest = .{
        .name = "factory",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FACTORY_ADDR,
            .create_event = PairCreated,
            .spawn_param = "pair",
            .child_events = &.{Sync},
        }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    _ = try filter_builder.build(&reader, FactoryManifest, dst_tmp.dir, allocator);

    {
        var discovered = try scanCreations(dst_tmp.dir, FactoryManifest, allocator);
        defer discovered.deinit();
        try testing.expectEqual(@as(u32, 1), discovered.count());
        try testing.expect(discovered.contains(CHILD_1));
    }

    const child_addrs = [_][20]u8{CHILD_1};
    _ = try filter_builder.appendChildren(&reader, FactoryManifest, &child_addrs, dst_tmp.dir, allocator);

    var counter = Counter{ .allocator = allocator };
    defer counter.deinit();

    const result = try replay(dst_tmp.dir, FactoryManifest, Counter, &counter, .{});
    try testing.expectEqual(@as(u64, 2), result.logs_dispatched);
    try testing.expectEqual(@as(u32, 1), counter.pair_creations);
    try testing.expectEqual(@as(u32, 1), counter.syncs);
}
