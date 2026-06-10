/// SDK head-following loop. Reads the engine's pending ring on each inotify
/// wakeup, classifies the diff against the prior tick, dispatches new blocks
/// through the per-block overlay, commits finalized blocks via state.snap,
/// recovers from reorgs.
const std = @import("std");

const core = @import("core");

const eth = @import("eth");

const ethcall = @import("ethcall.zig");
const handler_mod = @import("handler.zig");
const humanize = @import("humanize.zig");
const prefetch = @import("prefetch.zig");
const sdk_manifest = @import("manifest.zig");

const head_watch = core.head_watch;
const log_serial = core.log_serial;
const pending_format = core.pending_format;
const types = core.types;

const Entry = pending_format.Entry;

/// Re-exported for `entry.zig`'s follow gap-fill, which reads the finality boundary.
pub const readMeta = head_watch.readMeta;

// ── Live session ─────────────────────────────────────────────────────────

pub const RunOptions = struct {
    engine_data_dir: []const u8,
    /// Maximum wait between inotify wakeups. On non-Linux hosts the stub
    /// sleeps for this duration before every tick.
    tick_timeout_ms: u32 = 500,
    /// Optional Multicall3 pointer for per-block prefetch of factory
    /// children. `null` keeps the live loop offline. Handlers that hit
    /// uncached `ethCall` get `error.NotPrefetched`.
    multicall: ?*eth.multicall.Multicall = null,
    multicall_batch_size: usize = ethcall.DEFAULT_BATCH_SIZE,
};

/// Resources that persist across ticks: watcher, last-seen pending
/// snapshot, per-block decompress + log buffers. Held here so `tick`
/// doesn't reallocate per call. `run` constructs one session and loops.
/// Tests drive `tick` directly.
const LiveSession = struct {
    allocator: std.mem.Allocator,
    engine_data_dir: []const u8,
    watcher: head_watch.Watcher,
    prev: head_watch.PendingSnapshot,
    decompress_buf: []u8,
    log_buf: []core.RawLog,
    /// Optional Multicall3 for per-block prefetch of factory children.
    /// Null leaves uncached calls to surface as `error.NotPrefetched`.
    multicall: ?*eth.multicall.Multicall = null,
    multicall_batch_size: usize = ethcall.DEFAULT_BATCH_SIZE,

    pub fn init(allocator: std.mem.Allocator, engine_data_dir: []const u8) !LiveSession {
        var watcher = try head_watch.Watcher.init(engine_data_dir);
        errdefer watcher.deinit();

        var prev = try head_watch.readPending(allocator, engine_data_dir);
        errdefer prev.deinit(allocator);

        const decompress_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
        errdefer allocator.free(decompress_buf);
        const log_buf = try allocator.alloc(core.RawLog, types.MAX_LOGS_PER_BLOCK);
        errdefer allocator.free(log_buf);

        return .{
            .allocator = allocator,
            .engine_data_dir = engine_data_dir,
            .watcher = watcher,
            .prev = prev,
            .decompress_buf = decompress_buf,
            .log_buf = log_buf,
        };
    }

    pub fn deinit(self: *LiveSession) void {
        self.allocator.free(self.log_buf);
        self.allocator.free(self.decompress_buf);
        self.prev.deinit(self.allocator);
        self.watcher.deinit();
    }

    /// One pass of the live loop: wait, classify, dispatch new blocks, commit.
    pub fn tick(
        self: *LiveSession,
        comptime m: sdk_manifest.Manifest,
        comptime Handler: type,
        ctx: anytype,
        timeout_ms: u32,
    ) !void {
        try self.watcher.wait(timeout_ms);

        // Lock the mutation body only, not the wait above (that would stall
        // API readers for up to timeout_ms).
        lockCtx(ctx);
        defer unlockCtx(ctx);

        var curr = try head_watch.readPending(self.allocator, self.engine_data_dir);
        errdefer curr.deinit(self.allocator);

        const last_finalized = try readMeta(self.engine_data_dir);

        var classification = try head_watch.classifyChanges(
            self.allocator,
            self.prev.entries,
            curr.entries,
            last_finalized,
        );
        defer classification.deinit(self.allocator);

        // Promote first so a finalized block's overlay slice is committed
        // through state.snap before this tick's dispatches could overwrite
        // its tag.
        try promoteFinalized(ctx, classification.finalized);

        if (classification.reorg_from != null or classification.reorged_out.len > 0) {
            if (classification.reorg_from) |rf| {
                if (rf <= ctx._last_dispatched_block) return error.ReorgExceedsFinalityDepth;
            }
            discardAllOverlays(ctx);
            // Re-dispatch every block in fresh pending oldest to newest.
            // Re-applying mutations against an empty overlay is idempotent.
            for (curr.entries) |entry| {
                setLiveBlock(ctx, entry.block_number);
                try self.dispatchBlock(m, Handler, ctx, entry);
            }
        } else {
            for (classification.new_blocks) |entry| {
                setLiveBlock(ctx, entry.block_number);
                try self.dispatchBlock(m, Handler, ctx, entry);
            }
        }

        self.prev.deinit(self.allocator);
        self.prev = curr;
    }

    fn dispatchBlock(
        self: *LiveSession,
        comptime m: sdk_manifest.Manifest,
        comptime Handler: type,
        ctx: anytype,
        entry: Entry,
    ) !void {
        const decoded = try log_serial.decompressEntry(entry.lz4_entry, self.decompress_buf);
        const log_count = log_serial.deserializeLogs(decoded, self.log_buf);

        for (self.log_buf[0..log_count]) |*log| {
            log.block_number = entry.block_number;
        }

        // Pending blocks are raw. Unlike the backfill path, nothing filtered
        // them by address. Discover factory children spawned in this block
        // first (a create-event is emitted by the factory, which always
        // passes the gate), then compact to the dispatchable logs so prefetch
        // and dispatch both operate on a trusted slice. Same
        // filter→prefetch→dispatch order as the historical pipeline.
        try discoverChildren(m, ctx, self.log_buf[0..log_count]);

        var keep: usize = 0;
        for (self.log_buf[0..log_count]) |log| {
            if (!shouldDispatch(m, ctx, log.address)) continue;
            self.log_buf[keep] = log;
            keep += 1;
        }

        try self.maybePrefetchBlock(m, ctx, self.log_buf[0..keep]);

        ctx.block_number = entry.block_number;
        // The follower carries the exact header timestamp in pending.bin. Use
        // it directly. timestamps.bin (mmap'd at init) can't see blocks appended
        // past the import cutoff, so the formula fallback would otherwise drift
        // for live blocks. 0 = unknown (pre-magic engine) → derive as before.
        ctx.timestamp = if (entry.timestamp != 0)
            entry.timestamp
        else
            humanize.timestampOf(ctx, entry.block_number);

        for (self.log_buf[0..keep]) |log| {
            try handler_mod.dispatchLog(m, Handler, ctx, log);
        }
    }

    /// Stage-1 address gate for raw live logs. Mirrors `filter_builder`'s
    /// per-log address predicate: keep emitters that are statically declared
    /// (contracts + factories) or runtime-discovered factory children. A
    /// manifest declaring no addresses keeps nothing, correct since its
    /// dispatcher matches no event either.
    fn shouldDispatch(comptime m: sdk_manifest.Manifest, ctx: anytype, address: [20]u8) bool {
        const known = comptime sdk_manifest.knownAddresses(m);
        inline for (known) |addr| {
            if (std.mem.eql(u8, &address, &addr)) return true;
        }
        if (comptime m.factories.len > 0) {
            const T = std.meta.Child(@TypeOf(ctx));
            if (comptime @hasField(T, "_child_addresses")) {
                if (ctx._child_addresses) |set| {
                    if (set.contains(address)) return true;
                }
            }
        }
        return false;
    }

    /// Insert factory children spawned by create-events in `logs` into the
    /// runtime child set, so their own events later in the same block (or in
    /// later blocks) pass `shouldDispatch`. No-op for factory-free manifests
    /// or when ctx carries no `_child_addresses` (counter-shaped tests).
    fn discoverChildren(comptime m: sdk_manifest.Manifest, ctx: anytype, logs: []const core.RawLog) !void {
        if (comptime m.factories.len == 0) return;
        const T = std.meta.Child(@TypeOf(ctx));
        if (comptime !@hasField(T, "_child_addresses")) return;
        const set = ctx._child_addresses orelse return;
        for (logs) |log| {
            if (log.topic_count == 0) continue;
            inline for (m.factories) |f| {
                // Only the factory's own address legitimately spawns children.
                // Gating on it also guards extractFactoryAddress against a stray
                // contract whose topic0 collides with create_event but whose
                // data is too short to hold the spawn param.
                const create_topic = comptime sdk_manifest.eventTopic0(f.create_event);
                if (std.mem.eql(u8, &log.address, &f.address) and
                    std.mem.eql(u8, &log.topics[0], &create_topic))
                {
                    const addr = sdk_manifest.extractFactoryAddress(f, &log.topics, log.data);
                    try set.put(addr, {});
                }
            }
        }
    }

    /// Gather → dedupe → filterUncached → preload. Skips when the manifest
    /// declares no prefetch, when ctx has no cache (counter-shaped tests),
    /// or when the block has no matching factory events. Missing entries
    /// surface as `error.NotPrefetched` in handlers when multicall is null.
    fn maybePrefetchBlock(
        self: *LiveSession,
        comptime m: sdk_manifest.Manifest,
        ctx: anytype,
        logs: []const core.RawLog,
    ) !void {
        if (comptime m.prefetch.len == 0) return;
        const T = std.meta.Child(@TypeOf(ctx));
        if (comptime !@hasField(T, "_cache")) return;
        const cache = ctx._cache orelse return;

        var arena_state = std.heap.ArenaAllocator.init(ctx._allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        const calls = try prefetch.gatherOneBlock(arena, logs, m);
        if (calls.len == 0) return;

        const unique = try prefetch.dedupe(arena, calls);
        const missing = try prefetch.filterUncached(arena, cache, unique);
        if (missing.len == 0) return;

        const mc = self.multicall orelse return;
        try cache.preload(ctx._allocator, mc, missing, self.multicall_batch_size);
    }
};

/// For each finalized block: drain that block's overlay slice from every
/// store into the dirty cache / pending-append queue, advance
/// `_last_dispatched_block`, then `commitCycle` so the cursor + every
/// store flushes via one `state.snap` rename. A crash mid-promotion rolls
/// the whole batch back. Counter-shaped tests without `stores` skip.
pub fn promoteFinalized(ctx: anytype, finalized: []const u64) !void {
    if (finalized.len == 0) return;
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime !@hasField(T, "stores")) return;
    const Stores = @FieldType(T, "stores");
    for (finalized) |block| {
        inline for (std.meta.fields(Stores)) |f| {
            try @field(ctx.stores, f.name).commitBlock(block);
        }
        if (comptime @hasField(T, "_last_dispatched_block")) {
            ctx._last_dispatched_block = block;
        }
        if (comptime @hasDecl(T, "commitCycle")) {
            try ctx.commitCycle();
        }
    }
}

/// Block forever, dispatching new pending blocks as they arrive.
pub fn run(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    options: RunOptions,
) !void {
    comptime handler_mod.dispatcherFor(m).validateHandler(Handler);

    enter(ctx);

    var session = try LiveSession.init(ctx._allocator, options.engine_data_dir);
    defer session.deinit();
    session.multicall = options.multicall;
    session.multicall_batch_size = options.multicall_batch_size;

    while (!stopRequested(ctx)) {
        try session.tick(m, Handler, ctx, options.tick_timeout_ms);
    }
}

/// `deinit` sets `_stop`. The loop exits within one `tick`. Test contexts have
/// no `_stop` field and never stop here.
pub fn stopRequested(ctx: anytype) bool {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime @hasField(T, "_stop")) return ctx._stop.load(.seq_cst);
    return false;
}

/// Guards a `tick`'s mutations against API readers. No-op for test contexts
/// without a `_lock` field.
pub fn lockCtx(ctx: anytype) void {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime @hasField(T, "_lock")) ctx._lock.lock();
}
pub fn unlockCtx(ctx: anytype) void {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime @hasField(T, "_lock")) ctx._lock.unlock();
}

/// Flip `live = true` on every entity store. Saves from this point go
/// to the per-block overlay instead of the dirty cache. `commitBlock` is
/// the only path that drains overlay state into the cache/append queue
/// for the next `state.snap` commit. Counter-shaped test contexts without
/// a `stores` field are skipped.
pub fn enter(ctx: anytype) void {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime !@hasField(T, "stores")) return;
    const Stores = @FieldType(T, "stores");
    inline for (std.meta.fields(Stores)) |f| {
        @field(ctx.stores, f.name).live = true;
    }
}

pub fn setLiveBlock(ctx: anytype, block: u64) void {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime !@hasField(T, "stores")) return;
    const Stores = @FieldType(T, "stores");
    inline for (std.meta.fields(Stores)) |f| {
        @field(ctx.stores, f.name).live_block = block;
    }
}

pub fn discardAllOverlays(ctx: anytype) void {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime !@hasField(T, "stores")) return;
    const Stores = @FieldType(T, "stores");
    inline for (std.meta.fields(Stores)) |f| {
        @field(ctx.stores, f.name).discardAll();
    }
}

/// Roll back overlay blocks at or above `fork` across every store, keeping
/// canonical blocks below the fork. For a streamed REORG, where the engine
/// re-streams only the canonical tail and finalized-but-uncommitted blocks
/// below the fork must survive.
pub fn discardOverlaysFrom(ctx: anytype, fork: u64) void {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime !@hasField(T, "stores")) return;
    const Stores = @FieldType(T, "stores");
    inline for (std.meta.fields(Stores)) |f| {
        @field(ctx.stores, f.name).discardFrom(fork);
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

// ── Live tick integration ────────────────────────────────────────────────

const fake_engine = @import("testing/fake_engine.zig");

const TestTransfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
};

const TEST_CONTRACT: [20]u8 = [_]u8{0xAB} ** 20;

const TestManifest: sdk_manifest.Manifest = .{
    .name = "live-tick",
    .chain_id = 1,
    .start_block = 0,
    .contracts = &.{.{ .name = "T", .address = TEST_CONTRACT, .events = &.{TestTransfer} }},
};

/// Counter-shaped ctx + handler. Cheap stand-in for a real Context that
/// still exercises the dispatch pipeline. No entity stores, so the
/// overlay path is a no-op via the @hasField gate on `enter` / `tick`.
const TestRunner = struct {
    _allocator: std.mem.Allocator,
    block_number: u64 = 0,
    timestamp: u64 = 0,
    transfers: u32 = 0,
    _last_dispatched_block: u64 = 0,

    pub fn handleTransfer(_: handler_mod.Log(TestTransfer), self: *TestRunner) !void {
        self.transfers += 1;
    }
};

fn makeTransferLog(from: [20]u8, to: [20]u8, value: u64, data_buf: *[32]u8) core.RawLog {
    var from_topic: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(from_topic[12..32], &from);
    var to_topic: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(to_topic[12..32], &to);
    @memset(data_buf, 0);
    std.mem.writeInt(u64, data_buf[24..32], value, .big);
    return .{
        .block_number = 0,
        .tx_index = 0,
        .log_index = 0,
        .address = TEST_CONTRACT,
        .topic_count = 3,
        .topics = .{ sdk_manifest.eventTopic0(TestTransfer), from_topic, to_topic, [_]u8{0} ** 32 },
        .data = data_buf,
        .tx_hash = [_]u8{0xFE} ** 32,
    };
}

test "tick dispatches a pending block ingested by FakeEngine" {
    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &path_buf);

    // Session must initialize BEFORE the ingest so its `prev` snapshot
    // starts empty. Otherwise the planted block would already be in
    // `prev` and the diff would report no new blocks.
    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    var data_buf: [32]u8 = undefined;
    const log = makeTransferLog([_]u8{0} ** 20, ALICE, 100, &data_buf);
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{log});

    var runner = TestRunner{ ._allocator = testing.allocator };
    try session.tick(TestManifest, TestRunner, &runner, 200);

    try testing.expectEqual(@as(u32, 1), runner.transfers);
    try testing.expectEqual(@as(u64, 100), runner.block_number);
}

test "tick drops a pending log whose emitter is not in the manifest" {
    // Regression: foreign emitters must be rejected. A raw pending block can
    // carry Ethereum-wide ERC-20 Transfers. Only manifest contracts dispatch.
    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &path_buf);

    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    var data_buf: [32]u8 = undefined;
    var log = makeTransferLog([_]u8{0} ** 20, ALICE, 100, &data_buf);
    log.address = [_]u8{0xCC} ** 20; // not TEST_CONTRACT
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{log});

    var runner = TestRunner{ ._allocator = testing.allocator };
    try session.tick(TestManifest, TestRunner, &runner, 200);

    try testing.expectEqual(@as(u32, 0), runner.transfers);
}

test "tick discovers a factory child spawned live and dispatches its events" {
    const PairCreated = struct {
        pub const signature = "PairCreated(address indexed t0, address indexed t1, address pair, uint256 n)";
    };
    const Sync = struct {
        pub const signature = "Sync(uint112 r0, uint112 r1)";
    };
    const FACTORY: [20]u8 = [_]u8{0xF0} ** 20;
    const CHILD: [20]u8 = [_]u8{0xC1} ** 20;
    const M: sdk_manifest.Manifest = .{
        .name = "live-factory",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FACTORY,
            .create_event = PairCreated,
            .spawn_param = "pair",
            .child_events = &.{Sync},
        }},
    };
    const Runner = struct {
        _allocator: std.mem.Allocator,
        block_number: u64 = 0,
        timestamp: u64 = 0,
        _last_dispatched_block: u64 = 0,
        _child_addresses: ?*std.AutoHashMap([20]u8, void) = null,
        creates: u32 = 0,
        syncs: u32 = 0,
        pub fn handlePairCreated(_: handler_mod.Log(PairCreated), self: *@This()) !void {
            self.creates += 1;
        }
        pub fn handleSync(_: handler_mod.Log(Sync), self: *@This()) !void {
            self.syncs += 1;
        }
    };

    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &path_buf);

    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    // Empty runtime child set. The child is unknown until the create-event in
    // this very block adds it (the warm-start set starts empty here).
    var children = std.AutoHashMap([20]u8, void).init(testing.allocator);
    defer children.deinit();

    // Block 100: factory spawns CHILD, then CHILD emits Sync in the same block.
    var create_data: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(create_data[12..32], &CHILD);
    const create_log: core.RawLog = .{
        .block_number = 0,
        .tx_index = 0,
        .log_index = 0,
        .address = FACTORY,
        .topic_count = 1,
        .topics = .{ sdk_manifest.eventTopic0(PairCreated), [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &create_data,
        .tx_hash = [_]u8{0xFE} ** 32,
    };
    const sync_log: core.RawLog = .{
        .block_number = 0,
        .tx_index = 0,
        .log_index = 1,
        .address = CHILD,
        .topic_count = 1,
        .topics = .{ sdk_manifest.eventTopic0(Sync), [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{ create_log, sync_log });

    var runner = Runner{ ._allocator = testing.allocator, ._child_addresses = &children };
    try session.tick(M, Runner, &runner, 200);

    try testing.expectEqual(@as(u32, 1), runner.creates);
    try testing.expectEqual(@as(u32, 1), runner.syncs); // same-block child event caught
    try testing.expect(children.contains(CHILD));
}

test "tick routes saves through the per-block overlay, not the slab" {
    const root = @import("root.zig");
    const mutable_store = @import("mutable_store.zig");

    const Account = struct {
        pub const storage: root.StorageMode = .mutable;
        id: [20]u8,
        balance: u64,
    };

    const OverlayHandler = struct {
        pub fn handleTransfer(log: handler_mod.Log(TestTransfer), ctx: anytype) !void {
            const to = log.topics[2][12..32].*;
            const value: u64 = std.mem.readInt(u64, log.data[24..32], .big);
            var receiver = try ctx.stores.accounts.loadOrInit(to);
            receiver.balance +%= value;
            try ctx.stores.accounts.save(receiver);
        }
    };

    const Manifest: sdk_manifest.Manifest = .{
        .name = "overlay",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = TEST_CONTRACT, .events = &.{TestTransfer} }},
    };

    const TestStores = struct { accounts: mutable_store.MutableStore(Account) };
    const TestCtx = struct {
        _allocator: std.mem.Allocator,
        block_number: u64 = 0,
        timestamp: u64 = 0,
        stores: TestStores,
        _last_dispatched_block: u64 = 0,
    };

    // Engine + SDK data dirs.
    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();
    var engine_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &engine_path_buf);

    const ctx = try testing.allocator.create(TestCtx);
    defer testing.allocator.destroy(ctx);
    ctx.* = .{
        ._allocator = testing.allocator,
        .stores = undefined,
    };
    ctx.stores.accounts = mutable_store.MutableStore(Account).open(testing.allocator, &.{});
    defer ctx.stores.accounts.deinit();

    enter(ctx);

    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    // Three pending blocks, each crediting a different recipient.
    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    const BOB: [20]u8 = [_]u8{0xB2} ** 20;
    const CARL: [20]u8 = [_]u8{0xC3} ** 20;
    var bufs: [3][32]u8 = undefined;
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, ALICE, 100, &bufs[0])});
    try fake.ingest(101, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, BOB, 200, &bufs[1])});
    try fake.ingest(102, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, CARL, 300, &bufs[2])});

    try session.tick(Manifest, OverlayHandler, ctx, 200);

    // Per-block isolation: each block's submap holds exactly its own writes.
    try testing.expectEqual(@as(u32, 3), ctx.stores.accounts.pendingCount());

    const block_100 = ctx.stores.accounts.pending.get(100).?;
    try testing.expectEqual(@as(u32, 1), block_100.count());
    try testing.expectEqual(@as(u64, 100), block_100.get(ALICE).?.balance);

    const block_101 = ctx.stores.accounts.pending.get(101).?;
    try testing.expectEqual(@as(u32, 1), block_101.count());
    try testing.expectEqual(@as(u64, 200), block_101.get(BOB).?.balance);

    const block_102 = ctx.stores.accounts.pending.get(102).?;
    try testing.expectEqual(@as(u32, 1), block_102.count());
    try testing.expectEqual(@as(u64, 300), block_102.get(CARL).?.balance);

    // Nothing committed to disk. The slab is empty, only the overlay holds the data.
    try testing.expectEqual(@as(u32, 0), ctx.stores.accounts.count());

    // `load` returns the overlay value (not the slab).
    const loaded = (try ctx.stores.accounts.load(ALICE)).?;
    try testing.expectEqual(@as(u64, 100), loaded.balance);
}

test "reorg recovery drops overlay and re-dispatches fresh pending" {
    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &path_buf);

    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    var runner = TestRunner{ ._allocator = testing.allocator };

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    var bufs: [3][32]u8 = undefined;
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, ALICE, 1, &bufs[0])});
    try fake.ingest(101, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, ALICE, 2, &bufs[1])});
    try fake.ingest(102, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, ALICE, 3, &bufs[2])});

    try session.tick(TestManifest, TestRunner, &runner, 200);
    try testing.expectEqual(@as(u32, 3), runner.transfers);

    // Reorg block 102 to a different hash. classifyChanges sees the
    // mismatch, tick drops overlay (no-op for counter ctx) and
    // re-dispatches all three curr blocks.
    try fake.reorg(102);
    var buf_b: [32]u8 = undefined;
    try fake.ingest(102, [_]u8{0xBB} ** 32, &.{makeTransferLog([_]u8{0} ** 20, ALICE, 99, &buf_b)});

    try session.tick(TestManifest, TestRunner, &runner, 200);
    try testing.expectEqual(@as(u32, 6), runner.transfers);
}

test "reorg below the cursor raises ReorgExceedsFinalityDepth" {
    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &path_buf);

    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    // Simulate prior finalization having advanced the cursor past block 200.
    var runner = TestRunner{ ._allocator = testing.allocator, ._last_dispatched_block = 200 };

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    var buf_a: [32]u8 = undefined;
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, ALICE, 1, &buf_a)});
    try session.tick(TestManifest, TestRunner, &runner, 200);

    // Reorg below cursor. Engine truncates and re-ingests block 100 with a
    // new hash. The bound check refuses recovery.
    try fake.reorg(100);
    var buf_b: [32]u8 = undefined;
    try fake.ingest(100, [_]u8{0xBB} ** 32, &.{makeTransferLog([_]u8{0} ** 20, ALICE, 2, &buf_b)});

    try testing.expectError(
        error.ReorgExceedsFinalityDepth,
        session.tick(TestManifest, TestRunner, &runner, 200),
    );
}

test "live prefetch hits warm cache, issues no Multicall" {
    // A pending block carrying a PairCreated factory event runs through
    // `maybePrefetchBlock`. With the cache pre-warmed for the expected
    // decimals() call, `filterUncached` returns empty and no Multicall
    // round-trip is attempted. Proven by passing `multicall = null`
    // (any attempt would have been a null deref).
    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &path_buf);

    var cache_tmp = testing.tmpDir(.{});
    defer cache_tmp.cleanup();

    var cache = try ethcall.Cache.open(testing.allocator, cache_tmp.dir);
    defer cache.deinit();

    const PairCreated = struct {
        pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair, uint256 allPairsLength)";
    };
    const FACTORY: [20]u8 = [_]u8{0xF0} ** 20;
    const PAIR: [20]u8 = [_]u8{0xC1} ** 20;

    // Warm the cache with the decimals() result the prefetch would fetch.
    const sel = ethcall.selectorOf("decimals()");
    var payload: [32]u8 = std.mem.zeroes([32]u8);
    payload[31] = 18;
    try cache.put(PAIR, &sel, 0, &payload);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "uni",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FACTORY,
            .create_event = PairCreated,
            .spawn_param = "pair",
            .child_events = &.{},
        }},
        .prefetch = &.{.{
            .on_event = PairCreated,
            .calls = &.{.{ .address = .{ .param = "pair" }, .method = "decimals()" }},
        }},
    };

    const PrefetchRunner = struct {
        _allocator: std.mem.Allocator,
        _cache: ?*ethcall.Cache,
        block_number: u64 = 0,
        timestamp: u64 = 0,
        _last_dispatched_block: u64 = 0,
        creates: u32 = 0,

        pub fn handlePairCreated(_: handler_mod.Log(PairCreated), self: *@This()) !void {
            self.creates += 1;
        }
    };

    var runner = PrefetchRunner{ ._allocator = testing.allocator, ._cache = &cache };

    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();
    // session.multicall stays null. The warm cache must satisfy every
    // prefetch call. A miss would surface here.

    // Plant a PairCreated log with the pair address in data[0..32].
    var data: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(data[12..32], &PAIR);
    const create_topic = sdk_manifest.eventTopic0(PairCreated);
    const log: core.RawLog = .{
        .block_number = 100,
        .tx_index = 0,
        .log_index = 0,
        .address = FACTORY,
        .topic_count = 1,
        .topics = .{ create_topic, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &data,
        .tx_hash = [_]u8{0xFE} ** 32,
    };
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{log});

    try session.tick(Manifest, PrefetchRunner, &runner, 200);

    try testing.expectEqual(@as(u32, 1), runner.creates);
}

test "finalized blocks commit to state.snap and advance the cursor" {
    const root = @import("root.zig");
    const mutable_store = @import("mutable_store.zig");
    const entity_serial = @import("entity_serial.zig");

    const Account = struct {
        pub const storage: root.StorageMode = .mutable;
        id: [20]u8,
        balance: u64,
    };

    const OverlayHandler = struct {
        pub fn handleTransfer(log: handler_mod.Log(TestTransfer), ctx: anytype) !void {
            const to = log.topics[2][12..32].*;
            const value: u64 = std.mem.readInt(u64, log.data[24..32], .big);
            var receiver = try ctx.stores.accounts.loadOrInit(to);
            receiver.balance +%= value;
            try ctx.stores.accounts.save(receiver);
        }
    };

    const Manifest: sdk_manifest.Manifest = .{
        .name = "finalize",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = TEST_CONTRACT, .events = &.{TestTransfer} }},
    };

    const state_snap = @import("state_snap.zig");
    const Snap = state_snap.StateSnap(1, 0);
    const TestStores = struct { accounts: mutable_store.MutableStore(Account) };
    const TestCtx = struct {
        _allocator: std.mem.Allocator,
        _state_snap: *Snap,
        block_number: u64 = 0,
        timestamp: u64 = 0,
        stores: TestStores,
        _last_dispatched_block: u64 = 0,

        pub fn commitCycle(self: *@This()) !void {
            const buf = try self.stores.accounts.materialize(self._allocator);
            defer self._allocator.free(buf);
            try self._state_snap.commit(self._last_dispatched_block, &.{buf}, &.{});
            self.stores.accounts.refreshSlab(self._state_snap.mutableSlab(0));
        }
    };

    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();
    var engine_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &engine_path_buf);

    var entity_tmp = testing.tmpDir(.{});
    defer entity_tmp.cleanup();

    var snap = try Snap.open(testing.allocator, entity_tmp.dir);
    defer snap.deinit();

    const ctx = try testing.allocator.create(TestCtx);
    defer testing.allocator.destroy(ctx);
    ctx.* = .{
        ._allocator = testing.allocator,
        ._state_snap = &snap,
        .stores = undefined,
    };
    ctx.stores.accounts = mutable_store.MutableStore(Account).open(testing.allocator, snap.mutableSlab(0));
    defer ctx.stores.accounts.deinit();

    enter(ctx);

    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    // Two pending blocks. Tick once → both land in the overlay.
    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    const BOB: [20]u8 = [_]u8{0xB2} ** 20;
    var bufs: [2][32]u8 = undefined;
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, ALICE, 100, &bufs[0])});
    try fake.ingest(101, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, BOB, 200, &bufs[1])});
    try session.tick(Manifest, OverlayHandler, ctx, 200);
    try testing.expectEqual(@as(u32, 2), ctx.stores.accounts.pendingCount());

    // Finalize block 100 on the engine side. Next tick's classifyChanges
    // sees block 100 disappeared with last_finalized=100 → commitCycle.
    try fake.finalize(100);
    try session.tick(Manifest, OverlayHandler, ctx, 200);

    // Block 100's entry made it to the cache (dirty=false after refresh).
    // Block 101 still pending.
    try testing.expectEqual(@as(u32, 1), ctx.stores.accounts.pendingCount());
    try testing.expectEqual(@as(u64, 100), ctx._last_dispatched_block);

    // Reopen state.snap and verify alice's balance is durable.
    {
        var reopened = try Snap.open(testing.allocator, entity_tmp.dir);
        defer reopened.deinit();
        try testing.expectEqual(@as(u64, 100), reopened.cursor);

        const Store = mutable_store.MutableStore(Account);
        const slab = reopened.mutableSlab(0);
        try testing.expectEqual(@as(usize, Store.value_size), slab.len);
        const alice_acct = entity_serial.deserialize(Account, slab[0..Store.value_size]);
        try testing.expectEqualSlices(u8, &ALICE, &alice_acct.id);
        try testing.expectEqual(@as(u64, 100), alice_acct.balance);
    }
}

test "tick is a no-op when pending is unchanged" {
    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &path_buf);

    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    var data_buf: [32]u8 = undefined;
    const log = makeTransferLog([_]u8{0} ** 20, ALICE, 100, &data_buf);
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{log});

    var runner = TestRunner{ ._allocator = testing.allocator };
    try session.tick(TestManifest, TestRunner, &runner, 100);
    try testing.expectEqual(@as(u32, 1), runner.transfers);

    try session.tick(TestManifest, TestRunner, &runner, 100);
    try testing.expectEqual(@as(u32, 1), runner.transfers);
}

test "live dispatch uses the exact pending.bin timestamp, not the slot formula" {
    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &path_buf);

    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    var data_buf: [32]u8 = undefined;
    // An exact header time the slot formula would never produce for block 100.
    const exact_ts: u32 = 1_700_000_123;
    try fake.ingestAt(100, exact_ts, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, ALICE, 1, &data_buf)});

    var runner = TestRunner{ ._allocator = testing.allocator };
    try session.tick(TestManifest, TestRunner, &runner, 100);

    try testing.expectEqual(@as(u32, 1), runner.transfers);
    try testing.expectEqual(@as(u64, exact_ts), runner.timestamp);
    try testing.expect(runner.timestamp != humanize.blockTimestamp(100));
}

test "live dispatch falls back to the formula when pending carries no timestamp" {
    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &path_buf);

    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    var data_buf: [32]u8 = undefined;
    // Plain ingest leaves timestamp 0 (a pre-magic engine) → formula fallback.
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, ALICE, 1, &data_buf)});

    var runner = TestRunner{ ._allocator = testing.allocator };
    try session.tick(TestManifest, TestRunner, &runner, 100);

    try testing.expectEqual(@as(u64, humanize.blockTimestamp(100)), runner.timestamp);
}

test "readPending + readMeta against a FakeEngine snapshot" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(tmp.dir, testing.allocator);
    defer fake.deinit();

    const hash_a = [_]u8{0xAA} ** 32;
    try fake.ingest(100, hash_a, &.{});
    try fake.ingest(101, hash_a, &.{});
    try fake.finalize(100); // meta now reflects 100

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);

    var snap = try head_watch.readPending(testing.allocator, path);
    defer snap.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 1), snap.entries.len);
    try testing.expectEqual(@as(u64, 101), snap.entries[0].block_number);

    try testing.expectEqual(@as(u64, 100), try readMeta(path));
}
