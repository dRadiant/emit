/// `sdk.run` and `sdk.init`: orchestrate the five-phase pipeline.
///
/// Phase 1: filter_builder.build          (static + factory addresses)
/// Phase 2: scanner.scanCreations         (only when factories declared)
/// Phase 3: filter_builder.appendChildren (only when Phase 2 found any)
/// Phase 4: prefetch.gather + ethcall.preload (only when prefetch declared)
/// Phase 5: scanner.replay                (with commit batching via Context)
///
/// Phases 1-3 are skipped when an existing filter env is present (the
/// handler-only re-run path). Phase 4 is skipped when the manifest declares
/// no prefetch. Three entry points share the pipeline:
///   `init`  — backfill, return a caught-up `Context` (does not follow).
///   `run`   — backfill, then (if `follow`) run the live loop inline, blocking.
///   `spawn` — backfill, then run the live loop on a background thread and
///             return the `Context`, so an in-process API can read the stores
///             under `ctx.lock()` (tip-fresh, reorg-aware).
const std = @import("std");

const core = @import("core");

const eth = @import("eth");
const ethcall = @import("ethcall.zig");
const event_log_mod = @import("event_log.zig");
const filter_builder = @import("filter_builder.zig");
const filtered_store_mod = @import("filtered_store.zig");
const immutable_store_mod = @import("immutable_store.zig");
const live = @import("live.zig");
const mutable_store_mod = @import("mutable_store.zig");
const prefetch = @import("prefetch.zig");
const root = @import("root.zig");
const scanner = @import("scanner.zig");
const sdk_manifest = @import("manifest.zig");
const state_snap_mod = @import("state_snap.zig");

pub const Options = struct {
    /// Directory containing the engine's flat store
    /// (blocks.dat / blocks.idx / blooms.bin / meta.bin). Read-only.
    engine_data_dir: []const u8,
    /// SDK-managed data root. The SDK creates `<data_dir>/entity/` for the
    /// state.snap + per-entity events.dat files, `<data_dir>/filter/` for
    /// the filtered-index pair, and `<data_dir>/ethcall/` for the eth_call
    /// cache on first run; all three are mkdir'd if missing.
    data_dir: []const u8,
    /// Flush + commit cadence during handler replay, in dispatched logs.
    commit_interval: u32 = 100_000,
    /// JSON-RPC HTTP URL for Phase 4. `null` skips the network fetch
    /// (warm-cache re-runs and tests); uncached calls stay uncached.
    node_rpc: ?[]const u8 = null,
    /// Multicall3 address; canonical on every major chain. Override only
    /// for chains without the canonical deployment.
    multicall_address: [20]u8 = CANONICAL_MULTICALL3,
    multicall_batch_size: usize = ethcall.DEFAULT_BATCH_SIZE,
    /// When true, `run` and `init` block after backfill and enter the
    /// live head-following loop; `init` never returns.
    follow: bool = false,
};

pub const CANONICAL_MULTICALL3: [20]u8 = .{
    0xca, 0x11, 0xbd, 0xe0, 0x59, 0x77, 0xb3, 0x63, 0x11, 0x67,
    0x02, 0x88, 0x62, 0xbe, 0x2a, 0x17, 0x39, 0x76, 0xca, 0x11,
};


/// Result of a backfill run; returned from `run` and embedded in `Context`.
/// `elapsed_ns - (filter_build_ns + scan_creations_ns + append_children_ns +
/// replay_ns)` is overhead (entity-store open, final commit, env init).
pub const RunStats = struct {
    filter_blocks_scanned: u64 = 0,
    filter_blocks_matched: u64 = 0,
    filter_total_logs: u64 = 0,
    children_blocks_matched: u64 = 0,
    children_total_logs: u64 = 0,
    discovered_children: u32 = 0,
    logs_dispatched: u64 = 0,
    blocks_dispatched: u64 = 0,
    commits_performed: u32 = 0,
    prefetch_calls_gathered: u64 = 0,
    prefetch_calls_executed: u64 = 0,
    /// True when an existing filter env let init skip Phases 1-3.
    phases_skipped: bool = false,
    filter_build_ns: u64 = 0,
    scan_creations_ns: u64 = 0,
    append_children_ns: u64 = 0,
    prefetch_ns: u64 = 0,
    replay_ns: u64 = 0,
    elapsed_ns: u64 = 0,
};

/// Comptime-generate the long-lived context type. `entities` is the user's
/// tuple of entity types; each entity declares
/// `pub const storage: sdk.StorageMode = .mutable | .immutable;`.
///
/// Heap-allocated by `init` so each `MutableStore`'s borrowed slab and
/// each `ImmutableStore`'s `*EventLog` stay valid across the Context's
/// lifetime. `commitCycle` reassigns the slabs via `refreshSlab` after
/// every `state_snap.commit` — pointer addresses don't move.
///
/// Underscore-prefixed fields are SDK internals — handlers should not read
/// or mutate them. Public surface for handlers is `block_number`,
/// `timestamp`, `stores`, `stats`, and `ethCall`.
pub fn Context(comptime entities: anytype) type {
    const Stores = StoresStruct(entities);
    const EventLogs = EventLogsStruct(entities);
    return struct {
        const Self = @This();
        pub const Snap = state_snap_mod.StateSnap(mutableCount(entities), immutableCount(entities));

        block_number: u64 = 0,
        timestamp: u64 = 0,
        stores: Stores,
        stats: RunStats = .{},

        _allocator: std.mem.Allocator,
        _entity_dir: std.fs.Dir,
        _state_snap: Snap,
        _event_logs: EventLogs,
        /// Heap-allocated ethcall cache, owned by Context. Null when init
        /// runs without prefetch declared — every `ethCall` then returns
        /// `error.NotPrefetched`, matching the strict-mode semantics.
        _cache: ?*ethcall.Cache = null,
        /// Highest fully-dispatched block. Updated at block boundaries.
        /// `commitCycle` writes it into `state.snap.cursor` inside the same
        /// rename as the entity-slab flush so cursor and state are byte-atomic.
        _last_dispatched_block: u64 = 0,
        /// Factory-discovered child addresses, or `null` for factory-free
        /// manifests. Seeded from the historical `scanCreations` pass (on both
        /// cold build and warm reuse) and extended live by `discoverChildren`
        /// as new create-events arrive. `live.shouldDispatch` consults it so
        /// child logs pass the address gate alongside statically declared
        /// contracts. Owned by the Context; freed in `deinit`.
        _child_addresses: ?*std.AutoHashMap([20]u8, void) = null,
        /// Coarse lock: the follow thread holds it per tick, API readers per
        /// query. Serializing reads is what makes the caching `load` reusable.
        _lock: std.Thread.Mutex = .{},
        /// Non-null only under `spawn` (live loop on a thread); `deinit` joins it.
        _follow_thread: ?std.Thread = null,
        _stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        /// First error the follow thread hit before exiting; surfaced via `followError`.
        _follow_error: ?anyerror = null,

        /// Locked point read for in-process API callers — a value copy of
        /// mutable entity `T` for `key`. Not for handlers: they already run
        /// under the loop's lock, so `read` there would deadlock.
        pub fn read(self: *Self, comptime T: type, key: anytype) !?T {
            self.lock();
            defer self.unlock();
            return @field(self.stores, entityFieldName(T)).load(key);
        }

        /// Manual guard for multi-key snapshots; prefer `read` for single keys.
        pub fn lock(self: *Self) void {
            self._lock.lock();
        }
        pub fn unlock(self: *Self) void {
            self._lock.unlock();
        }
        /// Non-null once the follow thread has exited on an error.
        pub fn followError(self: *Self) ?anyerror {
            self.lock();
            defer self.unlock();
            return self._follow_error;
        }

        pub fn deinit(self: *Self) void {
            if (self._follow_thread) |t| {
                self._stop.store(true, .seq_cst);
                t.join();
                self._follow_thread = null;
            }
            inline for (std.meta.fields(Stores)) |f| {
                var s = &@field(self.stores, f.name);
                s.deinit();
            }
            inline for (std.meta.fields(EventLogs)) |f| {
                var log = &@field(self._event_logs, f.name);
                log.deinit();
            }
            self._state_snap.deinit();
            self._entity_dir.close();
            if (self._cache) |c| {
                c.deinit();
                self._allocator.destroy(c);
            }
            if (self._child_addresses) |set| {
                set.deinit();
                self._allocator.destroy(set);
            }
            self._allocator.destroy(self);
        }

        /// Flush every store, advance the cursor, and rename `state.snap`
        /// atomically. Cursor + every MutableStore slab + every ImmutableStore
        /// count are all published together; a crash inside leaves the prior
        /// `state.snap` intact.
        pub fn commitCycle(self: *Self) !void {
            // Flush ImmutableStore appends to events.dat first; their new
            // record counts feed the next state.snap.
            inline for (comptime resolveEntities(entities)) |T| {
                if (comptime T.storage == .immutable) {
                    const field_name = comptime entityFieldName(T);
                    var store = &@field(self.stores, field_name);
                    try store.flushAppends();
                }
            }
            inline for (comptime resolveEntities(entities)) |T| {
                if (comptime T.storage == .immutable) {
                    const field_name = comptime entityFieldName(T);
                    var log = &@field(self._event_logs, field_name);
                    try log.sync();
                }
            }

            // Materialize every MutableStore slab. Buffers freed after rename.
            var slabs: [Snap.mutable_count][]const u8 = undefined;
            var slab_bufs: [Snap.mutable_count][]u8 = undefined;
            comptime var slab_idx_init: usize = 0;
            inline for (comptime resolveEntities(entities)) |T| {
                if (comptime T.storage == .mutable) {
                    const field_name = comptime entityFieldName(T);
                    var store = &@field(self.stores, field_name);
                    const buf = try store.materialize(self._allocator);
                    slab_bufs[slab_idx_init] = buf;
                    slabs[slab_idx_init] = buf;
                    slab_idx_init += 1;
                }
            }
            defer for (slab_bufs) |b| self._allocator.free(b);

            // Collect new immutable record counts.
            var counts: [Snap.immutable_count]u64 = undefined;
            comptime var count_idx_init: usize = 0;
            inline for (comptime resolveEntities(entities)) |T| {
                if (comptime T.storage == .immutable) {
                    const field_name = comptime entityFieldName(T);
                    const store = &@field(self.stores, field_name);
                    counts[count_idx_init] = store.nextCommittedCount();
                    count_idx_init += 1;
                }
            }

            try self._state_snap.commit(self._last_dispatched_block, &slabs, &counts);

            // Rebind each MutableStore's slab to the new state.snap body.
            comptime var refresh_idx: usize = 0;
            inline for (comptime resolveEntities(entities)) |T| {
                if (comptime T.storage == .mutable) {
                    const field_name = comptime entityFieldName(T);
                    var store = &@field(self.stores, field_name);
                    store.refreshSlab(self._state_snap.mutableSlab(refresh_idx));
                    refresh_idx += 1;
                }
            }

            // Advance each ImmutableStore's committed count.
            inline for (comptime resolveEntities(entities)) |T| {
                if (comptime T.storage == .immutable) {
                    const field_name = comptime entityFieldName(T);
                    var store = &@field(self.stores, field_name);
                    store.markCommitted();
                }
            }

            self.stats.commits_performed += 1;
        }

        /// Strict cache read — never issues HTTP. Returns `error.NotPrefetched`
        /// for undeclared pairs, `error.CallReverted` for status=1 entries.
        pub fn ethCall(
            self: *Self,
            comptime T: type,
            to: [20]u8,
            comptime method: []const u8,
        ) !T {
            const cache = self._cache orelse return error.NotPrefetched;
            const calldata_hash = comptime ethcall.calldataHashOf(method);
            const entry = cache.getByHash(to, calldata_hash) orelse return error.NotPrefetched;
            if (entry.status != 0) return error.CallReverted;
            return ethcall.decodeAs(T, entry.bytes);
        }
    };
}

fn mutableCount(comptime entities: anytype) usize {
    comptime {
        var n: usize = 0;
        for (resolveEntities(entities)) |T| if (T.storage == .mutable) {
            n += 1;
        };
        return n;
    }
}

fn immutableCount(comptime entities: anytype) usize {
    comptime {
        var n: usize = 0;
        for (resolveEntities(entities)) |T| if (T.storage == .immutable) {
            n += 1;
        };
        return n;
    }
}

fn EventLogsStruct(comptime entities: anytype) type {
    const list = resolveEntities(entities);
    var fields: []const std.builtin.Type.StructField = &.{};
    for (list) |T| {
        if (T.storage == .immutable) {
            const name = entityFieldName(T);
            const Log = event_log_mod.EventLog(T);
            fields = fields ++ &[_]std.builtin.Type.StructField{.{
                .name = name,
                .type = Log,
                .default_value_ptr = null,
                .is_comptime = false,
                .alignment = @alignOf(Log),
            }};
        }
    }
    return @Type(.{ .@"struct" = .{
        .layout = .auto,
        .fields = fields,
        .decls = &.{},
        .is_tuple = false,
    } });
}

/// Backfill to completion and tear down. The entity stores are committed
/// via `state.snap`; every handle in the Context is closed. To keep the
/// stores readable after backfill, use `init` instead.
pub fn run(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    comptime entities: anytype,
    options: Options,
    allocator: std.mem.Allocator,
) !RunStats {
    const ctx = try init(m, Handler, entities, options, allocator);
    defer ctx.deinit();
    // Headless follow: run the live loop inline on this thread (blocks forever
    // under normal operation). Backfill-only (`follow = false`) returns stats.
    if (options.follow) try followLoop(m, Handler, ctx, options);
    return ctx.stats;
}

/// Backfill to completion and return the live `Context`. Caller owns the
/// pointer and must call `deinit` when done reading. Heap-allocated so
/// each `MutableStore`'s borrowed slab pointer and each `ImmutableStore`'s
/// `*EventLog` stay valid across the Context's lifetime.
pub fn init(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    comptime entities: anytype,
    options: Options,
    allocator: std.mem.Allocator,
) !*Context(entities) {
    var timer = try std.time.Timer.start();

    // Derive and mkdir the entity / filter / ethcall subdirs under `data_dir`.
    const entity_dir = try std.fs.path.join(allocator, &.{ options.data_dir, "entity" });
    defer allocator.free(entity_dir);
    const filter_dir = try std.fs.path.join(allocator, &.{ options.data_dir, "filter" });
    defer allocator.free(filter_dir);
    const ethcall_dir = try std.fs.path.join(allocator, &.{ options.data_dir, "ethcall" });
    defer allocator.free(ethcall_dir);
    try std.fs.cwd().makePath(entity_dir);
    try std.fs.cwd().makePath(filter_dir);
    try std.fs.cwd().makePath(ethcall_dir);

    var reader = try core.FlatStoreReader.open(options.engine_data_dir);
    defer reader.deinit();

    const C = Context(entities);
    const ctx = try allocator.create(C);
    errdefer allocator.destroy(ctx);

    var entity_dh = try std.fs.cwd().openDir(entity_dir, .{});
    errdefer entity_dh.close();

    ctx.* = .{
        ._allocator = allocator,
        ._entity_dir = entity_dh,
        ._state_snap = try C.Snap.open(allocator, entity_dh),
        ._event_logs = undefined,
        ._last_dispatched_block = 0,
        .stores = undefined,
    };
    errdefer ctx._state_snap.deinit();
    ctx._last_dispatched_block = ctx._state_snap.cursor;

    // Open every ImmutableStore's events.dat. Logs live on the Context so
    // each ImmutableStore can hold a stable pointer into the field.
    inline for (comptime resolveEntities(entities)) |T| {
        if (comptime T.storage == .immutable) {
            const field_name = comptime entityFieldName(T);
            const log_file_name = comptime field_name ++ ".events.dat";
            @field(ctx._event_logs, field_name) = try event_log_mod.EventLog(T).open(allocator, entity_dh, log_file_name);
        }
    }
    errdefer inline for (comptime resolveEntities(entities)) |T| {
        if (comptime T.storage == .immutable) {
            const field_name = comptime entityFieldName(T);
            @field(ctx._event_logs, field_name).deinit();
        }
    };

    var filter_dh = try std.fs.cwd().openDir(filter_dir, .{});
    defer filter_dh.close();

    const fp = comptime sdk_manifest.fingerprint(m);

    if (shouldSkipFilterBuild(filter_dh, allocator, fp)) {
        ctx.stats.phases_skipped = true;
        // Even with the filter reused, factory children must be rediscovered
        // so the live address gate admits them. The primary store always
        // holds the factory create-events, so scanCreations rebuilds the same
        // set without re-running the (skipped) full build.
        if (comptime m.factories.len > 0) {
            const discovered = try scanner.scanCreations(filter_dh, m, allocator);
            ctx.stats.discovered_children = discovered.count();
            try setChildAddresses(C, ctx, allocator, discovered);
        }
    } else {
        // A stale filter on disk (fingerprint mismatch or torn write) would
        // otherwise make build() try to append blocks <= the existing tail,
        // raising error.OutOfOrder. Clear first so the rebuild starts clean.
        try clearFilterFiles(filter_dh);
        const primary_result = try filter_builder.build(&reader, m, filter_dh, allocator);
        try requireCompleteFilter("phase 1 (build)", primary_result);
        ctx.stats.filter_blocks_scanned = primary_result.blocks_scanned;
        ctx.stats.filter_blocks_matched = primary_result.blocks_matched;
        ctx.stats.filter_total_logs = primary_result.total_logs;
        ctx.stats.filter_build_ns = primary_result.elapsed_ns;

        // Phases 2 + 3 (factory-only).
        if (comptime m.factories.len > 0) {
            var phase23_timer = try std.time.Timer.start();
            const discovered = try scanner.scanCreations(filter_dh, m, allocator);
            ctx.stats.scan_creations_ns = phase23_timer.read();

            ctx.stats.discovered_children = discovered.count();

            if (discovered.count() > 0) {
                const child_addrs = try allocator.alloc([20]u8, discovered.count());
                defer allocator.free(child_addrs);
                var i: usize = 0;
                var it = discovered.keyIterator();
                while (it.next()) |addr| : (i += 1) child_addrs[i] = addr.*;

                const child_result = try filter_builder.appendChildren(
                    &reader,
                    m,
                    child_addrs,
                    filter_dh,
                    allocator,
                );
                try requireCompleteFilter("phase 3 (appendChildren)", child_result);
                ctx.stats.children_blocks_matched = child_result.blocks_matched;
                ctx.stats.children_total_logs = child_result.total_logs;
                ctx.stats.append_children_ns = child_result.elapsed_ns;
            }

            try setChildAddresses(C, ctx, allocator, discovered);
        }

        try writeFilterFingerprint(filter_dh, fp);
    }

    // Cache + Phase 4 are prefetch-only. Opening the cache unconditionally
    // would let handlers read stale entries from a previous manifest after
    // the prefetch declaration is removed.
    if (comptime (m.prefetch.len > 0 or m.static_prefetch.len > 0)) {
        var ethcall_dh = try std.fs.cwd().openDir(ethcall_dir, .{});
        defer ethcall_dh.close();
        const cache = try allocator.create(ethcall.Cache);
        errdefer allocator.destroy(cache);
        cache.* = try ethcall.Cache.open(allocator, ethcall_dh);
        errdefer cache.deinit();
        ctx._cache = cache;
    } else {
        ctx._cache = null;
    }

    if (comptime (m.prefetch.len > 0 or m.static_prefetch.len > 0)) {
        var phase4_timer = try std.time.Timer.start();
        try runPhase4(m, options, ctx, filter_dh);
        ctx.stats.prefetch_ns = phase4_timer.read();
    }

    // Open every entity store. MutableStores borrow a slab from state_snap;
    // ImmutableStores wrap their owning EventLog with the authoritative count
    // from state_snap.immutable_counts. Slot indices are comptime-tracked in
    // entities-tuple order.
    {
        comptime var mut_slot: usize = 0;
        comptime var imm_slot: usize = 0;
        inline for (comptime resolveEntities(entities)) |T| {
            const field_name = comptime entityFieldName(T);
            if (comptime T.storage == .mutable) {
                @field(ctx.stores, field_name) = mutable_store_mod.MutableStore(T).open(
                    allocator,
                    ctx._state_snap.mutableSlab(mut_slot),
                );
                mut_slot += 1;
            } else {
                const log_ptr = &@field(ctx._event_logs, field_name);
                @field(ctx.stores, field_name) = try immutable_store_mod.ImmutableStore(T).open(
                    allocator,
                    log_ptr,
                    ctx._state_snap.immutableCount(imm_slot),
                );
                imm_slot += 1;
            }
        }
    }

    const replay_result = try scanner.replay(
        filter_dh,
        m,
        Handler,
        ctx,
        .{
            .commit_interval = options.commit_interval,
            .start_block = ctx._last_dispatched_block,
        },
    );
    ctx.stats.logs_dispatched = replay_result.logs_dispatched;
    ctx.stats.blocks_dispatched = replay_result.blocks_dispatched;
    ctx.stats.replay_ns = replay_result.elapsed_ns;

    // Final commit so any logs since the last commit boundary land.
    try ctx.commitCycle();
    ctx.stats.elapsed_ns = timer.read();

    if (options.follow) {
        // Gap fill: the engine may have advanced during backfill. Re-open
        // the reader so any flat-store entries added since the start of
        // init are visible (the original `reader` mmap was sized at open).
        const engine_last = try live.readMeta(options.engine_data_dir);
        if (engine_last > ctx._last_dispatched_block) {
            var gap_reader = try core.FlatStoreReader.open(options.engine_data_dir);
            defer gap_reader.deinit();

            const gap_from = ctx._last_dispatched_block + 1;
            const gap_result = try filter_builder.appendBlocks(
                &gap_reader,
                m,
                gap_from,
                engine_last,
                filter_dh,
                allocator,
            );
            try requireCompleteFilter("follow gap-fill", gap_result);

            // Factories: the gap may hold create-events for children unknown to
            // the backfill pass (and child events from already-known children).
            // Rediscover over the now-extended primary, merge into the live set,
            // and extend children.dat across the gap so replay sees them. The
            // gap range is strictly above the children store's tail, so the
            // append stays monotonic.
            if (comptime m.factories.len > 0) {
                var gap_children = try scanner.scanCreations(filter_dh, m, allocator);
                defer gap_children.deinit();
                if (ctx._child_addresses) |set| {
                    var it = gap_children.keyIterator();
                    while (it.next()) |addr| try set.put(addr.*, {});
                    if (set.count() > 0) {
                        const addrs = try allocator.alloc([20]u8, set.count());
                        defer allocator.free(addrs);
                        var i: usize = 0;
                        var ks = set.keyIterator();
                        while (ks.next()) |a| : (i += 1) addrs[i] = a.*;
                        const cres = try filter_builder.appendChildrenBlocks(
                            &gap_reader,
                            m,
                            addrs,
                            gap_from,
                            engine_last,
                            filter_dh,
                            allocator,
                        );
                        try requireCompleteFilter("follow gap-fill children", cres);
                    }
                }
            }

            _ = try scanner.replay(filter_dh, m, Handler, ctx, .{
                .commit_interval = options.commit_interval,
                .start_block = ctx._last_dispatched_block,
            });
            try ctx.commitCycle();
        }
    }
    // init never enters the live loop; run/spawn drive followLoop below.
    return ctx;
}

/// Live loop body: set up the Multicall (when a node RPC is configured) and run
/// `live.run` until stop/error. Inline under `run`, on a thread under `spawn`;
/// the multicall stays on this frame for the loop's lifetime.
fn followLoop(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    options: Options,
) !void {
    if (options.node_rpc) |rpc_url| {
        var http = eth.http_transport.HttpTransport.init(ctx._allocator, rpc_url);
        var provider = eth.provider.Provider.init(ctx._allocator, &http);
        var mc = eth.multicall.Multicall.init(ctx._allocator, &provider, options.multicall_address);
        defer mc.deinit();
        try live.run(m, Handler, ctx, .{
            .engine_data_dir = options.engine_data_dir,
            .multicall = &mc,
            .multicall_batch_size = options.multicall_batch_size,
        });
    } else {
        try live.run(m, Handler, ctx, .{ .engine_data_dir = options.engine_data_dir });
    }
}

/// Backfill, then run the live loop on a background thread and return the
/// caught-up `*Context` for in-process reads (`ctx.read` / `ctx.lock`).
/// `deinit` stops + joins the thread. Forces `follow = true` so `init`
/// gap-fills before the thread starts.
pub fn spawn(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    comptime entities: anytype,
    options: Options,
    allocator: std.mem.Allocator,
) !*Context(entities) {
    var opts = options;
    opts.follow = true;
    const ctx = try init(m, Handler, entities, opts, allocator);
    errdefer ctx.deinit();

    const Ctx = Context(entities);
    const Thunk = struct {
        fn entry(c: *Ctx, o: Options) void {
            followLoop(m, Handler, c, o) catch |e| {
                c.lock();
                c._follow_error = e;
                c.unlock();
            };
        }
    };
    ctx._stop.store(false, .seq_cst);
    ctx._follow_thread = try std.Thread.spawn(.{}, Thunk.entry, .{ ctx, opts });
    return ctx;
}

/// Gather → dedupe → filterUncached → preload. Runs once between Phases 3 and 5.
/// All gather allocations live in a local arena that frees on return; the
/// only state that escapes is the cache writes from `preload`.
fn runPhase4(
    comptime m: sdk_manifest.Manifest,
    options: Options,
    ctx: anytype,
    filter_dh: std.fs.Dir,
) !void {
    const allocator = ctx._allocator;
    const cache = ctx._cache.?;

    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const static_calls = try prefetch.gatherStatic(arena, m);
    const dynamic_calls = try prefetch.gatherDynamic(arena, filter_dh, m);

    const merged = try arena.alloc(ethcall.Call, static_calls.len + dynamic_calls.len);
    @memcpy(merged[0..static_calls.len], static_calls);
    @memcpy(merged[static_calls.len..], dynamic_calls);

    const unique = try prefetch.dedupe(arena, merged);
    ctx.stats.prefetch_calls_gathered = unique.len;

    const missing = try prefetch.filterUncached(arena, cache, unique);
    if (missing.len == 0) return;

    // No RPC configured: gather is informational, fetch is a no-op. Handlers
    // that hit an uncached pair will see `error.NotPrefetched` at replay.
    const rpc_url = options.node_rpc orelse return;

    var http = eth.http_transport.HttpTransport.init(allocator, rpc_url);
    var provider = eth.provider.Provider.init(allocator, &http);
    var mc = eth.multicall.Multicall.init(allocator, &provider, options.multicall_address);
    defer mc.deinit();

    try cache.preload(allocator, &mc, missing, options.multicall_batch_size);
    ctx.stats.prefetch_calls_executed = missing.len;
}

/// Skip the filter build only when the on-disk filter has at least one entry
/// AND its persisted manifest fingerprint matches the current manifest. Any
/// open or read failure degrades to "rebuild" so a torn fingerprint cannot
/// admit stale data.
fn shouldSkipFilterBuild(filter_dh: std.fs.Dir, allocator: std.mem.Allocator, fp: [32]u8) bool {
    var store = filtered_store_mod.FilteredStore.open(allocator, filter_dh, filter_builder.BASE_PRIMARY) catch return false;
    defer store.deinit();
    if (store.count() == 0) return false;

    var on_disk: [32]u8 = undefined;
    const file = filter_dh.openFile("manifest.fingerprint", .{}) catch return false;
    defer file.close();
    const n = file.readAll(&on_disk) catch return false;
    if (n != 32) return false;
    return std.mem.eql(u8, &on_disk, &fp);
}

fn writeFilterFingerprint(filter_dh: std.fs.Dir, fp: [32]u8) !void {
    try core.atomic_file.write(filter_dh, "manifest.fingerprint.tmp", "manifest.fingerprint", &fp);
}

/// Move `discovered` onto the heap and hand ownership to `ctx`. The set has
/// the lifetime of the Context (same allocator); `Context.deinit` frees it.
/// The live address gate and child-discovery pre-pass both read/extend it.
fn setChildAddresses(
    comptime C: type,
    ctx: *C,
    allocator: std.mem.Allocator,
    discovered: std.AutoHashMap([20]u8, void),
) !void {
    const set_ptr = try allocator.create(std.AutoHashMap([20]u8, void));
    set_ptr.* = discovered;
    ctx._child_addresses = set_ptr;
}

fn clearFilterFiles(filter_dh: std.fs.Dir) !void {
    const names = [_][]const u8{
        "primary.dat",     "primary.idx",
        "children.dat",    "children.idx",
        "manifest.fingerprint",
    };
    for (names) |n| filter_dh.deleteFile(n) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
}

/// Hard-fail when a filter phase dropped blocks. Better than shipping a partial index.
fn requireCompleteFilter(phase: []const u8, r: filter_builder.BuildResult) !void {
    if (r.dropped_blocks == 0) return;
    std.debug.print(
        \\
        \\ERROR: filter {s} dropped {d} of {d} matching blocks.
        \\Index is incomplete; aborting. Likely cause: a block's serialized log
        \\data exceeded BLOCK_BUF_SIZE or its compressed entry exceeded the
        \\io_uring slot buffer. Bump the relevant constant in core/src/types.zig
        \\(MAX_LOGS_PER_BLOCK or BLOCK_BUF_SIZE) and rerun.
        \\
    ,
        .{ phase, r.dropped_blocks, r.blocks_matched + r.dropped_blocks },
    );
    return error.FilterBuildIncomplete;
}

/// Comptime-build the inner struct that holds one entity store per tuple
/// element, used as the type of `Context.stores`. Field names derive from
/// the entity type's basename: lowercase the first byte and append `s`
/// (e.g. `Account` → `accounts`). An entity type may override this with
/// `pub const store_name = "balances";` — useful when the auto-derived
/// name is ugly (`LBTCBalance` → `lBTCBalances`) or collides with another
/// entity. Two entities whose effective store names collide raise
/// `@compileError`.
fn StoresStruct(comptime entities: anytype) type {
    const list = comptime resolveEntities(entities);

    comptime var derived: [list.len][:0]const u8 = undefined;
    inline for (list, 0..) |T, i| {
        // storeFor enforces `pub const storage: sdk.StorageMode`.
        _ = root.storeFor(T);
        derived[i] = entityFieldName(T);
    }

    inline for (derived, 0..) |a, i| {
        if (i + 1 >= derived.len) break;
        inline for (derived[i + 1 ..], i + 1..) |b, j| {
            if (std.mem.eql(u8, a, b)) @compileError(std.fmt.comptimePrint(
                "sdk.Context: entity types '{s}' and '{s}' both derive store field name '{s}'. Rename one of the entity types.",
                .{ @typeName(list[i]), @typeName(list[j]), a },
            ));
        }
    }

    var struct_fields: [list.len]std.builtin.Type.StructField = undefined;
    inline for (list, 0..) |T, i| {
        const Store = root.storeFor(T);
        struct_fields[i] = .{
            .name = derived[i],
            .type = Store,
            .default_value_ptr = null,
            .is_comptime = false,
            .alignment = @alignOf(Store),
        };
    }

    return @Type(.{ .@"struct" = .{
        .layout = .auto,
        .fields = &struct_fields,
        .decls = &.{},
        .is_tuple = false,
    } });
}

/// Normalize `entities` to a `[]const type`. Accepts either the tuple
/// form (`.{ A, B, C }`) or a module type (`@import("entities.zig")`).
fn resolveEntities(comptime entities: anytype) []const type {
    const T = @TypeOf(entities);

    // Module form: `entities` is a type whose pub decls include the
    // entity structs (those declaring `pub const storage`).
    if (T == type) {
        var out: []const type = &.{};
        inline for (@typeInfo(entities).@"struct".decls) |d| {
            const member = @field(entities, d.name);
            if (@TypeOf(member) == type and @hasDecl(member, "storage")) {
                out = out ++ &[_]type{member};
            }
        }
        if (out.len == 0) @compileError(
            "sdk.Context: module `" ++ @typeName(entities) ++ "` has no entities (no pub structs declaring `pub const storage: StorageMode`)",
        );
        return out;
    }

    // Tuple form: `entities` is a tuple value of entity types.
    const info = @typeInfo(T);
    if (info == .@"struct" and info.@"struct".is_tuple) {
        var out: []const type = &.{};
        inline for (info.@"struct".fields, 0..) |f, i| {
            const t = @field(entities, f.name);
            if (@TypeOf(t) != type) @compileError(std.fmt.comptimePrint(
                "sdk.Context: entities[{d}] is not a type. Pass entity types: `.{{ Account, Allowance, Transfer, Approval }}`.",
                .{i},
            ));
            out = out ++ &[_]type{t};
        }
        return out;
    }

    @compileError(
        "sdk.Context: expected tuple of entity types or entities module, got `" ++ @typeName(T) ++ "`",
    );
}

fn entityFieldName(comptime T: type) [:0]const u8 {
    return comptime blk: {
        @setEvalBranchQuota(20_000);
        if (@hasDecl(T, "store_name")) {
            const override: []const u8 = T.store_name;
            if (override.len == 0) @compileError(
                "sdk.Context: entity type '" ++ @typeName(T) ++ "' declared `pub const store_name` but it is empty.",
            );
            break :blk std.fmt.comptimePrint("{s}", .{override});
        }
        const full = @typeName(T);
        const start = if (std.mem.lastIndexOfScalar(u8, full, '.')) |idx| idx + 1 else 0;
        const basename = full[start..];
        if (basename.len == 0) @compileError(
            "sdk.Context: entity type '" ++ full ++ "' has empty basename. Cannot derive store field name. Add `pub const store_name = \"...\";` to override.",
        );
        break :blk std.fmt.comptimePrint("{c}{s}s", .{ std.ascii.toLower(basename[0]), basename[1..] });
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;
const types = core.types;
const log_serial = core.log_serial;
const flat_reader = core.flat_reader;
const bloom = core.bloom;

const Transfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
};

const Account = struct {
    pub const storage: root.StorageMode = .mutable;
    id: [20]u8,
    balance: u64,
};

const LBTCBalance = struct {
    pub const storage: root.StorageMode = .mutable;
    pub const store_name = "balances";
    id: [20]u8,
    amount: u64,
};

const ADDR_TOKEN: [20]u8 = [_]u8{0xAE} ** 20;

test "commit boundary lands at block end, not mid-block" {
    // commit_interval = 1 with a 3-log single-block fixture: the old
    // mid-block commit would have produced 3 in-replay commits + 1 final.
    // The new block-boundary discipline produces 1 in-replay commit + 1
    // final = 2. This is the invariant the cursor scheme depends on:
    // every commit reflects a fully-dispatched block, never a partial.
    const allocator = testing.allocator;
    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    const BOB: [20]u8 = [_]u8{0xB2} ** 20;
    const CARL: [20]u8 = [_]u8{0xC3} ** 20;

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();

    var data_bufs: [3][32]u8 = undefined;
    const logs = [_]core.RawLog{
        makeTransferLog(100, 0, [_]u8{0} ** 20, ALICE, 10, &data_bufs[0]),
        makeTransferLog(100, 1, [_]u8{0} ** 20, BOB, 20, &data_bufs[1]),
        makeTransferLog(100, 2, [_]u8{0} ** 20, CARL, 30, &data_bufs[2]),
    };
    const blocks = [_][]const core.RawLog{&logs};
    try writeFlatStoreFromLogs(src_tmp.dir, &blocks, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);

    var data_tmp = testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data_tmp.dir.realpath(".", &data_path_buf);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "erc20",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    const ctx = try init(
        Manifest,
        TransferHandler,
        .{Account},
        .{ .engine_data_dir = src_path, .data_dir = data_path, .commit_interval = 1 },
        allocator,
    );
    defer ctx.deinit();

    try testing.expectEqual(@as(u64, 3), ctx.stats.logs_dispatched);
    try testing.expectEqual(@as(u64, 1), ctx.stats.blocks_dispatched);
    // 1 in-replay commit (after block 100's last log crossed interval=1) +
    // 1 final commit from init = 2.
    try testing.expectEqual(@as(u32, 2), ctx.stats.commits_performed);
}

test "requireCompleteFilter: zero drops succeeds, any drops escalate to error" {
    try requireCompleteFilter("test", .{ .blocks_matched = 100, .dropped_blocks = 0 });
    try testing.expectError(
        error.FilterBuildIncomplete,
        requireCompleteFilter("test", .{ .blocks_matched = 99, .dropped_blocks = 1 }),
    );
}

test "store_name override beats the default basename derivation" {
    const Ctx = Context(.{LBTCBalance});
    const Stores = std.meta.fieldInfo(Ctx, .stores).type;
    try testing.expect(@hasField(Stores, "balances"));
    try testing.expect(!@hasField(Stores, "lBTCBalances"));
}

test "Context accepts a module type and produces the same Context as the tuple form" {
    // Inline module fixture: a struct with two pub entity decls plus a
    // non-entity helper that the resolver must skip.
    const FixtureModule = struct {
        pub const A = struct {
            pub const storage: root.StorageMode = .mutable;
            id: [20]u8,
            balance: u256,
        };
        pub const B = struct {
            pub const storage: root.StorageMode = .immutable;
            id: [16]u8,
            value: u64,
        };
        // Non-entity decl — must be ignored by the resolver.
        pub const helper_constant: u32 = 42;
    };

    const FromModule = Context(FixtureModule);
    const FromTuple = Context(.{ FixtureModule.A, FixtureModule.B });

    // Both forms yield identical Stores layouts (the user-observable contract).
    const stores_from_module = std.meta.fieldInfo(FromModule, .stores).type;
    const stores_from_tuple = std.meta.fieldInfo(FromTuple, .stores).type;
    try testing.expectEqual(stores_from_module, stores_from_tuple);
    try testing.expect(@hasField(stores_from_module, "as"));
    try testing.expect(@hasField(stores_from_module, "bs"));
}

const TransferHandler = struct {
    pub fn handleTransfer(log: @import("handler.zig").Log(Transfer), ctx: anytype) !void {
        // Transfer's signature is unnamed, so we still read positionally
        // here; named-parameter access (`log.params.value`) is exercised by the
        // example indexers and the parser tests.
        const from = log.topics[1][12..32].*;
        const to = log.topics[2][12..32].*;
        const value: u64 = std.mem.readInt(u64, log.data[24..32], .big);

        if (!std.mem.eql(u8, &from, &([_]u8{0} ** 20))) {
            var sender = try ctx.stores.accounts.loadOrInit(from);
            sender.balance -%= value;
            try ctx.stores.accounts.save(sender);
        }
        var receiver = try ctx.stores.accounts.loadOrInit(to);
        receiver.balance +%= value;
        try ctx.stores.accounts.save(receiver);
    }
};

/// Caller owns `data_buf` and must keep it alive as long as the returned
/// RawLog is used; we used to stash data in a threadlocal static, which
/// silently aliased across calls and corrupted the test fixtures.
fn makeTransferLog(block: u64, log_index: u16, from: [20]u8, to: [20]u8, value: u64, data_buf: *[32]u8) core.RawLog {
    var from_topic: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(from_topic[12..32], &from);
    var to_topic: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(to_topic[12..32], &to);
    @memset(data_buf, 0);
    std.mem.writeInt(u64, data_buf[24..32], value, .big);
    return .{
        .block_number = block,
        .tx_index = 0,
        .log_index = log_index,
        .address = ADDR_TOKEN,
        .topic_count = 3,
        .topics = .{
            sdk_manifest.eventTopic0(Transfer),
            from_topic,
            to_topic,
            [_]u8{0} ** 32,
        },
        .data = data_buf,
        .tx_hash = [_]u8{0xFE} ** 32,
    };
}

fn writeFlatStoreFromLogs(dir: std.fs.Dir, blocks: []const []const core.RawLog, allocator: std.mem.Allocator) !void {
    var blocks_file = try dir.createFile("blocks.dat", .{});
    defer blocks_file.close();
    var idx_file = try dir.createFile("blocks.idx", .{});
    defer idx_file.close();
    var blooms_file = try dir.createFile("blooms.bin", .{});
    defer blooms_file.close();

    var idx_hdr: [flat_reader.INDEX_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, idx_hdr[0..8], blocks[0][0].block_number, .little);
    std.mem.writeInt(u64, idx_hdr[8..16], blocks.len, .little);
    try idx_file.writeAll(&idx_hdr);

    var blooms_hdr: [flat_reader.BLOOM_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, &blooms_hdr, blocks.len, .little);
    try blooms_file.writeAll(&blooms_hdr);

    const serialize_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(serialize_buf);
    const compress_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(compress_buf);

    var offset: u64 = 0;
    for (blocks) |logs| {
        const written = log_serial.serializeLogs(logs, serialize_buf);
        const entry_len = try log_serial.compressEntry(serialize_buf[0..written], compress_buf);
        try blocks_file.writeAll(compress_buf[0..entry_len]);

        var idx_entry: [flat_reader.INDEX_ENTRY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, idx_entry[0..8], offset, .little);
        std.mem.writeInt(u32, idx_entry[8..12], @intCast(entry_len), .little);
        try idx_file.writeAll(&idx_entry);

        const tb = log_serial.buildTopicBloom(logs);
        const ab = log_serial.buildAddrBloom(logs);
        var bloom_entry: [flat_reader.BLOOM_ENTRY_SIZE]u8 = std.mem.zeroes([flat_reader.BLOOM_ENTRY_SIZE]u8);
        std.mem.writeInt(u64, bloom_entry[0..8], logs[0].block_number, .big);
        @memcpy(bloom_entry[flat_reader.TOPIC_BLOOM_OFFSET..][0..bloom.BLOOM_SIZE], &tb.bits);
        @memcpy(bloom_entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE], &ab.bits);
        try blooms_file.writeAll(&bloom_entry);

        offset += entry_len;
    }
}

test "init: backfills planted Transfers and final balances match" {
    const allocator = testing.allocator;

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    const BOB: [20]u8 = [_]u8{0xB2} ** 20;
    const CARL: [20]u8 = [_]u8{0xC3} ** 20;

    // Plant four Transfers: mint 100 to alice, mint 50 to bob,
    // alice → carl 30, bob → alice 20.
    var data_bufs: [4][32]u8 = undefined;
    const log_b100 = makeTransferLog(100, 0, [_]u8{0} ** 20, ALICE, 100, &data_bufs[0]);
    const log_b101 = makeTransferLog(101, 0, [_]u8{0} ** 20, BOB, 50, &data_bufs[1]);
    const log_b102 = makeTransferLog(102, 0, ALICE, CARL, 30, &data_bufs[2]);
    const log_b103 = makeTransferLog(103, 0, BOB, ALICE, 20, &data_bufs[3]);

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    const blocks = [_][]const core.RawLog{
        &.{log_b100},
        &.{log_b101},
        &.{log_b102},
        &.{log_b103},
    };
    try writeFlatStoreFromLogs(src_tmp.dir, &blocks, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);

    var data_tmp = testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data_tmp.dir.realpath(".", &data_path_buf);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "erc20",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    const ctx = try init(
        Manifest,
        TransferHandler,
        .{Account},
        .{
            .engine_data_dir = src_path,
            .data_dir = data_path,
            .commit_interval = 100_000,
        },
        allocator,
    );
    defer ctx.deinit();

    try testing.expectEqual(@as(u64, 4), ctx.stats.logs_dispatched);
    try testing.expectEqual(@as(u64, 4), ctx.stats.blocks_dispatched);
    try testing.expect(ctx.stats.commits_performed >= 1);

    // alice: +100 -30 +20 = 90
    // bob:   +50 -20      = 30
    // carl:  +30          = 30
    const alice = (try ctx.stores.accounts.load(ALICE)) orelse return error.MissingAlice;
    const bob = (try ctx.stores.accounts.load(BOB)) orelse return error.MissingBob;
    const carl = (try ctx.stores.accounts.load(CARL)) orelse return error.MissingCarl;
    try testing.expectEqual(@as(u64, 90), alice.balance);
    try testing.expectEqual(@as(u64, 30), bob.balance);
    try testing.expectEqual(@as(u64, 30), carl.balance);
}

test "spawn: follows on a background thread, reads under lock, deinit joins" {
    const allocator = testing.allocator;
    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;

    // Two finalized mints to alice (100 + 50). No pending.bin, so the follow
    // thread ticks on an empty ring and idles — we're exercising spawn's
    // thread lifecycle + locked reads + deinit join, not live dispatch (which
    // the live.zig tick tests already cover).
    var data_bufs: [2][32]u8 = undefined;
    const log_b100 = makeTransferLog(100, 0, [_]u8{0} ** 20, ALICE, 100, &data_bufs[0]);
    const log_b101 = makeTransferLog(101, 0, [_]u8{0} ** 20, ALICE, 50, &data_bufs[1]);

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    const blocks = [_][]const core.RawLog{ &.{log_b100}, &.{log_b101} };
    try writeFlatStoreFromLogs(src_tmp.dir, &blocks, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);

    var data_tmp = testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data_tmp.dir.realpath(".", &data_path_buf);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "erc20",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    const ctx = try spawn(
        Manifest,
        TransferHandler,
        .{Account},
        .{ .engine_data_dir = src_path, .data_dir = data_path, .commit_interval = 100_000 },
        allocator,
    );
    defer ctx.deinit(); // sets _stop + joins the follow thread; hangs the test if join fails

    try testing.expect(ctx._follow_thread != null);
    {
        ctx.lock();
        defer ctx.unlock();
        const alice = (try ctx.stores.accounts.load(ALICE)) orelse return error.MissingAlice;
        try testing.expectEqual(@as(u256, 150), alice.balance);
    }
    try testing.expectEqual(@as(?anyerror, null), ctx.followError());
}

fn pollBalance(ctx: anytype, key: [20]u8, want: u256, max_ms: u32) !void {
    var waited: u32 = 0;
    while (waited <= max_ms) : (waited += 20) {
        if (try ctx.read(Account, key)) |a| if (a.balance == want) return;
        std.Thread.sleep(20 * std.time.ns_per_ms);
    }
    return error.TipNotObserved;
}

test "spawn: follow thread dispatches a live pending block + reorg; reader sees the tip" {
    const allocator = testing.allocator;
    const fake_engine = @import("testing/fake_engine.zig");
    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;

    // Backfill two finalized mints → alice = 150.
    var data_bufs: [2][32]u8 = undefined;
    const b100 = makeTransferLog(100, 0, [_]u8{0} ** 20, ALICE, 100, &data_bufs[0]);
    const b101 = makeTransferLog(101, 0, [_]u8{0} ** 20, ALICE, 50, &data_bufs[1]);
    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    const blocks = [_][]const core.RawLog{ &.{b100}, &.{b101} };
    try writeFlatStoreFromLogs(src_tmp.dir, &blocks, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);

    var data_tmp = testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data_tmp.dir.realpath(".", &data_path_buf);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "erc20",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    const ctx = try spawn(
        Manifest,
        TransferHandler,
        .{Account},
        .{ .engine_data_dir = src_path, .data_dir = data_path, .commit_interval = 100_000 },
        allocator,
    );
    defer ctx.deinit(); // joins the follow thread before the dirs below are removed

    var fake = fake_engine.FakeEngine.init(src_tmp.dir, allocator);
    fake.last_finalized = 101; // keep meta consistent with the backfilled flat store
    defer fake.deinit();

    // Pending block 102 mints +25 to alice; the follow thread must dispatch it
    // into the overlay so a reader sees the tip value 175.
    var pbuf: [32]u8 = undefined;
    try fake.ingest(102, [_]u8{0xAA} ** 32, &.{makeTransferLog(102, 0, [_]u8{0} ** 20, ALICE, 25, &pbuf)});
    try pollBalance(ctx, ALICE, 175, 5000);

    // Reorg 102 to a version minting +99; the reader must converge to 249.
    try fake.reorg(102);
    var pbuf2: [32]u8 = undefined;
    try fake.ingest(102, [_]u8{0xBB} ** 32, &.{makeTransferLog(102, 0, [_]u8{0} ** 20, ALICE, 99, &pbuf2)});
    try pollBalance(ctx, ALICE, 249, 5000);

    try testing.expectEqual(@as(?anyerror, null), ctx.followError());
}

test "run: returns stats and tears down without leaking" {
    const allocator = testing.allocator;

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    const BOB: [20]u8 = [_]u8{0xB2} ** 20;
    var data_bufs: [2][32]u8 = undefined;
    const log = makeTransferLog(100, 0, [_]u8{0} ** 20, ALICE, 100, &data_bufs[0]);
    const log2 = makeTransferLog(101, 0, ALICE, BOB, 25, &data_bufs[1]);

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    const blocks = [_][]const core.RawLog{ &.{log}, &.{log2} };
    try writeFlatStoreFromLogs(src_tmp.dir, &blocks, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);

    var data_tmp = testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data_tmp.dir.realpath(".", &data_path_buf);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "erc20",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    const stats = try run(
        Manifest,
        TransferHandler,
        .{Account},
        .{
            .engine_data_dir = src_path,
            .data_dir = data_path,
        },
        allocator,
    );
    try testing.expectEqual(@as(u64, 2), stats.logs_dispatched);
    try testing.expectEqual(@as(u64, 2), stats.blocks_dispatched);
}

test "init + replay commit batching: ctx.commitCycle fires per commit_interval" {
    const allocator = testing.allocator;

    // 12 Transfers split across 12 blocks. With commit_interval = 5,
    // commitCycle fires at events 5 and 10 (during replay) plus once
    // post-replay = 3 commits.
    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();

    var raw_logs: [12]core.RawLog = undefined;
    var data_bufs: [12][32]u8 = undefined;
    var blocks_storage: [12][1]core.RawLog = undefined;
    var blocks_slices: [12][]const core.RawLog = undefined;
    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    const BOB: [20]u8 = [_]u8{0xB2} ** 20;
    for (0..12) |i| {
        raw_logs[i] = makeTransferLog(@as(u64, 100 + i), 0, ALICE, BOB, 1, &data_bufs[i]);
        blocks_storage[i] = .{raw_logs[i]};
        blocks_slices[i] = &blocks_storage[i];
    }
    try writeFlatStoreFromLogs(src_tmp.dir, &blocks_slices, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);

    var data_tmp = testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data_tmp.dir.realpath(".", &data_path_buf);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "erc20",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    const ctx = try init(
        Manifest,
        TransferHandler,
        .{Account},
        .{
            .engine_data_dir = src_path,
            .data_dir = data_path,
            .commit_interval = 5,
        },
        allocator,
    );
    defer ctx.deinit();

    // 12 events, interval 5 → commits at events 5 and 10 during replay, plus
    // a final commit from init. Total 3.
    try testing.expectEqual(@as(u32, 3), ctx.stats.commits_performed);
    try testing.expectEqual(@as(u64, 12), ctx.stats.logs_dispatched);
}

// ── ethCall (BlockContext typed cache read) ──────────────────────────────

/// Build a minimal Context whose only used field is `_cache`.
/// `_entity_dir` and `_state_snap` are left `undefined` because `ethCall`
/// never touches them; the caller must NOT invoke `deinit` (which would
/// dereference them).
fn ethCallTestContext(cache: *ethcall.Cache) Context(.{}) {
    return .{
        .stores = .{},
        ._allocator = testing.allocator,
        ._entity_dir = undefined,
        ._state_snap = undefined,
        ._event_logs = .{},
        ._cache = cache,
    };
}

fn openTestCache(tmp: *std.testing.TmpDir) !ethcall.Cache {
    return try ethcall.Cache.open(testing.allocator, tmp.dir);
}

test "ethCall returns the cached u8 for a prefetched decimals() pair" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try openTestCache(&tmp);
    defer cache.deinit();

    const USDC = [_]u8{0xA0} ** 20;
    const SEL = ethcall.selectorOf("decimals()");
    var payload: [32]u8 = std.mem.zeroes([32]u8);
    payload[31] = 6;
    try cache.put(USDC, &SEL, 0, &payload);

    var ctx = ethCallTestContext(&cache);
    try testing.expectEqual(@as(u8, 6), try ctx.ethCall(u8, USDC, "decimals()"));
}

test "ethCall returns NotPrefetched when the cache lacks the pair" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try openTestCache(&tmp);
    defer cache.deinit();

    const UNKNOWN = [_]u8{0xDE} ** 20;
    var ctx = ethCallTestContext(&cache);
    try testing.expectError(error.NotPrefetched, ctx.ethCall(u8, UNKNOWN, "decimals()"));
}

test "ethCall returns NotPrefetched when the cache is null" {
    var ctx: Context(.{}) = .{
        .stores = .{},
        ._allocator = testing.allocator,
        ._entity_dir = undefined,
        ._state_snap = undefined,
        ._event_logs = .{},
        ._cache = null,
    };
    const ANY = [_]u8{0xAA} ** 20;
    try testing.expectError(error.NotPrefetched, ctx.ethCall(u8, ANY, "decimals()"));
}

test "ethCall returns CallReverted for a status=1 cached entry" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try openTestCache(&tmp);
    defer cache.deinit();

    const MKR = [_]u8{0x9F} ** 20;
    const SEL = ethcall.selectorOf("decimals()");
    try cache.put(MKR, &SEL, 1, &.{});

    var ctx = ethCallTestContext(&cache);
    try testing.expectError(error.CallReverted, ctx.ethCall(u8, MKR, "decimals()"));
}

test "ethCall decodes [20]u8 from the trailing word bytes" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try openTestCache(&tmp);
    defer cache.deinit();

    const ROUTER = [_]u8{0x7A} ** 20;
    const FACTORY = [_]u8{0x5C} ** 20;
    const SEL = ethcall.selectorOf("factory()");
    var payload: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(payload[12..32], &FACTORY);
    try cache.put(ROUTER, &SEL, 0, &payload);

    var ctx = ethCallTestContext(&cache);
    const got = try ctx.ethCall([20]u8, ROUTER, "factory()");
    try testing.expectEqualSlices(u8, &FACTORY, &got);
}

test "ethCall decodes u256 from the full word" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var cache = try openTestCache(&tmp);
    defer cache.deinit();

    const TOKEN = [_]u8{0xBB} ** 20;
    const SEL = ethcall.selectorOf("totalSupply()");
    var payload: [32]u8 = undefined;
    std.mem.writeInt(u256, &payload, 1_000_000_000_000_000_000_000, .big);
    try cache.put(TOKEN, &SEL, 0, &payload);

    var ctx = ethCallTestContext(&cache);
    try testing.expectEqual(
        @as(u256, 1_000_000_000_000_000_000_000),
        try ctx.ethCall(u256, TOKEN, "totalSupply()"),
    );
}

// ── Phase 4 wiring ───────────────────────────────────────────────────────

fn writeSingleTransferStore(dir: std.fs.Dir, allocator: std.mem.Allocator) !void {
    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    var data_buf: [32]u8 = undefined;
    const log = makeTransferLog(100, 0, [_]u8{0} ** 20, ALICE, 1, &data_buf);
    const blocks = [_][]const core.RawLog{&.{log}};
    try writeFlatStoreFromLogs(dir, &blocks, allocator);
}

test "phase 4 gathers static_prefetch and skips preload without node_rpc" {
    const allocator = testing.allocator;

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try writeSingleTransferStore(src_tmp.dir, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);

    var data_tmp = testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data_tmp.dir.realpath(".", &data_path_buf);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "erc20",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
        .static_prefetch = &.{
            .{ .address = [_]u8{0xC0} ** 20, .method = "decimals()" },
            .{ .address = [_]u8{0xC0} ** 20, .method = "symbol()" },
            .{ .address = [_]u8{0xC1} ** 20, .method = "decimals()" },
        },
    };

    const ctx = try init(
        Manifest,
        TransferHandler,
        .{Account},
        .{ .engine_data_dir = src_path, .data_dir = data_path },
        allocator,
    );
    defer ctx.deinit();

    try testing.expectEqual(@as(u64, 3), ctx.stats.prefetch_calls_gathered);
    try testing.expectEqual(@as(u64, 0), ctx.stats.prefetch_calls_executed);
    try testing.expect(!ctx.stats.phases_skipped);
}

test "phase 4 runs zero work for a manifest with no prefetch" {
    const allocator = testing.allocator;

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try writeSingleTransferStore(src_tmp.dir, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);

    var data_tmp = testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data_tmp.dir.realpath(".", &data_path_buf);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "erc20",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    const ctx = try init(
        Manifest,
        TransferHandler,
        .{Account},
        .{ .engine_data_dir = src_path, .data_dir = data_path },
        allocator,
    );
    defer ctx.deinit();

    try testing.expectEqual(@as(u64, 0), ctx.stats.prefetch_calls_gathered);
    try testing.expectEqual(@as(u64, 0), ctx.stats.prefetch_ns);
}

test "handler-only re-run skips phases 1-3 and the cursor blocks re-dispatch" {
    // cursor: the first init writes `state.snap.cursor` reflecting the
    // last dispatched block. The second init reads it, seeds `start_block`,
    // and `scanner.replay` seeks past the already-covered range. The
    // single planted block (number 100) is therefore *not* re-dispatched
    const allocator = testing.allocator;

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try writeSingleTransferStore(src_tmp.dir, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);

    var data_tmp = testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data_tmp.dir.realpath(".", &data_path_buf);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "erc20",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    const ctx1 = try init(
        Manifest,
        TransferHandler,
        .{Account},
        .{ .engine_data_dir = src_path, .data_dir = data_path },
        allocator,
    );
    try testing.expect(!ctx1.stats.phases_skipped);
    try testing.expect(ctx1.stats.filter_build_ns > 0);
    try testing.expectEqual(@as(u64, 1), ctx1.stats.logs_dispatched);
    try testing.expectEqual(@as(u64, 100), ctx1._last_dispatched_block);
    ctx1.deinit();

    const ctx2 = try init(
        Manifest,
        TransferHandler,
        .{Account},
        .{ .engine_data_dir = src_path, .data_dir = data_path },
        allocator,
    );
    defer ctx2.deinit();
    try testing.expect(ctx2.stats.phases_skipped);
    try testing.expectEqual(@as(u64, 0), ctx2.stats.filter_build_ns);
    try testing.expectEqual(@as(u64, 100), ctx2._last_dispatched_block);
    try testing.expectEqual(@as(u64, 0), ctx2.stats.logs_dispatched);
}
