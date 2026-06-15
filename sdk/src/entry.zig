/// `sdk.run` and `sdk.init`: orchestrate the five-phase pipeline.
///
/// Phase 1: filter_builder.build          (static + factory addresses)
/// Phase 2: scanner.scanCreations         (only when factories declared)
/// Phase 3: filter_builder.appendChildren (only when Phase 2 found any)
/// Phase 4: prefetch.gather + ethcall.preload (only when prefetch declared)
/// Phase 5: scanner.replay                (with commit batching via Context)
///
/// Phases 1-3 skipped when an existing filter env is present (handler-only
/// re-run path). Phase 4 skipped when the manifest declares no prefetch.
/// Three entry points share the pipeline:
///   `init`  backfill, return a caught-up `Context` (does not follow).
///   `run`   backfill, then (if `follow`) run the live loop inline, blocking.
///   `spawn` backfill, then run the live loop on a background thread and
///           return the `Context`, so an in-process API reads the stores
///           under `ctx.lock()` (tip-fresh, reorg-aware).
const std = @import("std");

const core = @import("core");

const eth = @import("eth");
const entity_serial = @import("entity_serial.zig");
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
const tcp_client = @import("tcp_client.zig");

pub const Options = struct {
    /// Directory containing the engine's flat store
    /// (blocks.dat / blocks.idx / blooms.bin / meta.bin). Read-only.
    engine_data_dir: []const u8,
    /// SDK-managed data root. Creates `<data_dir>/entity/` (state.snap +
    /// per-entity events.dat), `<data_dir>/filter/` (filtered-index pair),
    /// and `<data_dir>/ethcall/` (eth_call cache) on first run. All three
    /// mkdir'd if missing.
    data_dir: []const u8,
    /// Flush + commit cadence during handler replay, in dispatched logs.
    commit_interval: u32 = 100_000,
    /// JSON-RPC HTTP URL for Phase 4. `null` skips the network fetch
    /// (warm-cache re-runs and tests). Uncached calls stay uncached.
    node_rpc: ?[]const u8 = null,
    /// Multicall3 address, canonical on every major chain. Override only
    /// for chains without the canonical deployment.
    multicall_address: [20]u8 = CANONICAL_MULTICALL3,
    multicall_batch_size: usize = ethcall.DEFAULT_BATCH_SIZE,
    /// When true, `run` blocks after backfill in the live head-following
    /// loop and `spawn` runs that loop on a background thread. `init`
    /// backfills (including the follow gap-fill) and returns without
    /// entering the loop.
    follow: bool = false,
    /// When set, stream the filtered backfill from a remote engine `serve`
    /// listener instead of reading a local flat store at `engine_data_dir`.
    /// With `follow`, live blocks stream over the same connection
    /// (`tcp_client.follow`), reconnecting from the committed cursor.
    remote_engine: ?RemoteEngine = null,
};

/// Address of a remote engine `serve` listener. Reached over an SSH tunnel in
/// production, so `host` is normally a localhost forward.
pub const RemoteEngine = struct {
    host: []const u8,
    port: u16,
};

pub const CANONICAL_MULTICALL3: [20]u8 = .{
    0xca, 0x11, 0xbd, 0xe0, 0x59, 0x77, 0xb3, 0x63, 0x11, 0x67,
    0x02, 0x88, 0x62, 0xbe, 0x2a, 0x17, 0x39, 0x76, 0xca, 0x11,
};

/// Result of a backfill run, returned from `run` and embedded in `Context`.
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
    // Inclusive block span the run covered, from the engine store index.
    start_block: u64 = 0,
    end_block: u64 = 0,
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

/// Render the full stats block at info level. Shown automatically under
/// `--verbose` at run completion. Default runs get a one-line summary
/// instead. Factory fields read zero for non-factory indexers, kept visible
/// so users can confirm no children were unexpectedly discovered.
pub fn printStats(prog_name: []const u8, stats: RunStats) void {
    const ms = std.time.ns_per_ms;
    const phases = stats.filter_build_ns + stats.scan_creations_ns + stats.append_children_ns + stats.prefetch_ns + stats.replay_ns;
    const overhead_ns = if (stats.elapsed_ns > phases) stats.elapsed_ns - phases else 0;
    // Batch count derived from executed pairs and the default Multicall3
    // chunk. Exact count would need the per-run override routed through stats.
    const batches = (stats.prefetch_calls_executed + ethcall.DEFAULT_BATCH_SIZE - 1) / ethcall.DEFAULT_BATCH_SIZE;
    core.log.info(
        \\{s} indexer complete
        \\  start block:       {d}
        \\  end block:         {d}
        \\  blocks scanned:    {d}
        \\  blocks matched:    {d}
        \\  filter logs:       {d}
        \\  discovered child:  {d}
        \\  child blocks:      {d}
        \\  child logs:        {d}
        \\  logs dispatched:   {d}
        \\  blocks dispatched: {d}
        \\  commits:           {d}
        \\  phases skipped:    {}
        \\  prefetch gathered: {d}
        \\  prefetch executed: {d}
        \\  prefetch batches:  {d}
        \\  ── timing ──
        \\  filter build:      {d} ms
        \\  scan creations:    {d} ms
        \\  append children:   {d} ms
        \\  prefetch:          {d} ms
        \\  replay:            {d} ms
        \\  overhead:          {d} ms
        \\  elapsed:           {d} ms
        \\
    , .{
        prog_name,
        stats.start_block,
        stats.end_block,
        stats.filter_blocks_scanned,
        stats.filter_blocks_matched,
        stats.filter_total_logs,
        stats.discovered_children,
        stats.children_blocks_matched,
        stats.children_total_logs,
        stats.logs_dispatched,
        stats.blocks_dispatched,
        stats.commits_performed,
        stats.phases_skipped,
        stats.prefetch_calls_gathered,
        stats.prefetch_calls_executed,
        batches,
        stats.filter_build_ns / ms,
        stats.scan_creations_ns / ms,
        stats.append_children_ns / ms,
        stats.prefetch_ns / ms,
        stats.replay_ns / ms,
        overhead_ns / ms,
        stats.elapsed_ns / ms,
    });
}

/// Backfill-completion announcement for the blocking entry points that never
/// return (`run --follow`, `spawn`). One line at the default level, the full
/// stats block under `--verbose`. Backfill-only `run` returns stats and the
/// caller owns the print.
fn announceBackfill(comptime m: sdk_manifest.Manifest, stats: RunStats) void {
    if (core.log.getLevel() == .verbose)
        printStats(m.name, stats)
    else
        core.log.info("{s}: backfill done in {d} ms ({d} logs dispatched)\n", .{ m.name, stats.elapsed_ns / std.time.ns_per_ms, stats.logs_dispatched });
    core.log.info("{s}: following the chain head\n", .{m.name});
}

/// Comptime-generate the long-lived context type. `entities` is the user's
/// tuple of entity types, each declaring
/// `pub const storage: sdk.StorageMode = .mutable | .immutable;`.
///
/// Heap-allocated by `init` so each `MutableStore`'s borrowed slab and
/// each `ImmutableStore`'s `*EventLog` stay valid across the Context's
/// lifetime. `commitCycle` reassigns the slabs via `refreshSlab` after
/// every `state_snap.commit`. Pointer addresses don't move.
///
/// Underscore-prefixed fields are SDK internals, not for handler read or
/// mutation. Public surface for handlers is `block_number`, `timestamp`,
/// `stores`, `stats`, and `ethCall`.
pub fn Context(comptime entities: anytype) type {
    const Stores = StoresStruct(entities);
    const EventLogs = EventLogsStruct(entities);
    return struct {
        const Self = @This();
        /// Resolved entity type list, evaluated once for every `inline for`.
        const entity_list = resolveEntities(entities);
        pub const Snap = state_snap_mod.StateSnap(mutableCount(entities), immutableCount(entities), blobStoreCount(entities));

        block_number: u64 = 0,
        timestamp: u64 = 0,
        stores: Stores,
        stats: RunStats = .{},

        _allocator: std.mem.Allocator,
        _entity_dir: std.fs.Dir,
        _state_snap: Snap,
        _event_logs: EventLogs,
        /// Heap-allocated ethcall cache, owned by Context. Null when init
        /// runs without prefetch declared, where every `ethCall` then returns
        /// `error.NotPrefetched` matching the strict-mode semantics.
        _cache: ?*ethcall.Cache = null,
        /// Engine's per-block timestamp index, or null when the store predates
        /// the feature. `humanize.timestampOf` reads it for exact `timestamp`,
        /// falling back to the derivation formula when absent.
        _timestamps: ?core.timestamps.TimestampReader = null,
        /// Highest fully-dispatched block. Updated at block boundaries.
        /// `commitCycle` writes it into `state.snap.cursor` inside the same
        /// rename as the entity-slab flush so cursor and state are byte-atomic.
        _last_dispatched_block: u64 = 0,
        /// Factory-discovered child addresses, or `null` for factory-free
        /// manifests. Seeded from the historical `scanCreations` pass (on both
        /// cold build and warm reuse) and extended live by `discoverChildren`
        /// as new create-events arrive. `live.shouldDispatch` consults it so
        /// child logs pass the address gate alongside statically declared
        /// contracts. Owned by the Context, freed in `deinit`.
        _child_addresses: ?*std.AutoHashMap([20]u8, void) = null,
        /// Coarse lock. The follow thread holds it per tick, API readers per
        /// query. Serializing reads is what makes the caching `load` reusable.
        _lock: std.Thread.Mutex = .{},
        /// Non-null only under `spawn` (live loop on a thread). `deinit` joins it.
        _follow_thread: ?std.Thread = null,
        _stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        /// First error the follow thread hit before exiting. Surfaced via `followError`.
        _follow_error: ?anyerror = null,

        /// Locked point read for in-process API callers. A value copy of
        /// entity `T` for `key`: the mutable store's tip-fresh state, or the
        /// immutable store's live log (finalized records plus the overlay). Not
        /// for handlers, which already run under the loop's lock so a read here
        /// would deadlock.
        ///
        /// Blob entities (ADR-005) cannot use this path: a blob field is a
        /// slice borrowing the store mmap, and `read` releases the lock on
        /// return, so a concurrent commit could remap under the borrow. Read
        /// them through `readView`, which holds the lock across the borrow.
        pub fn read(self: *Self, comptime T: type, key: anytype) !?T {
            comptime assertNoBlobs(T, "read");
            self.lock();
            defer self.unlock();
            return self.readLocked(T, key);
        }

        /// Live record count of immutable entity `T` (finalized + overlay).
        /// Locked. Mutable entities are point-read by key via `read`.
        pub fn count(self: *Self, comptime T: type) u64 {
            comptime assertImmutable(T, "count");
            self.lock();
            defer self.unlock();
            return @field(self.stores, entityFieldName(T)).count();
        }

        /// Fill `out` with immutable records [start, start+out.len) of entity
        /// `T` in ascending key order, returning the filled prefix. Tip-overlay
        /// aware and taken under the Context lock, so callers never lock
        /// directly. Build a newest-first page with `start = count(T) - n`.
        /// `count` and `range` take the lock separately, so a commit between
        /// the two can shift the page by a few records. Never incoherent data,
        /// just a moved window. Wrap both in `lock`/`unlock` for a pinned page.
        /// Mutable entities are point-read by key via `read`. Blob entities use
        /// `readView` (the filled `out` would borrow the mmap past the lock).
        pub fn range(self: *Self, comptime T: type, start: u64, out: []T) ![]T {
            comptime assertImmutable(T, "range");
            comptime assertNoBlobs(T, "range");
            self.lock();
            defer self.unlock();
            return @field(self.stores, entityFieldName(T)).range(start, out);
        }

        fn readLocked(self: *Self, comptime T: type, key: anytype) !?T {
            const store = &@field(self.stores, entityFieldName(T));
            if (comptime T.storage == .immutable) return store.get(key);
            return store.load(key);
        }

        /// Open a read view: holds the Context lock so blob field slices stay
        /// valid for the borrow's use, then released on `deinit`. The sound way
        /// to read blob entities from an external thread (ADR-005). Numeric
        /// entities can use it too for a pinned multi-read snapshot. Copy any
        /// blob bytes out before `deinit` if they must outlive the view. Never
        /// open a view from a handler, which already holds the lock.
        pub fn readView(self: *Self) ReadView {
            self.lock();
            return .{ .ctx = self };
        }

        /// RAII read guard from `readView`. Reads borrow the store mmap and are
        /// valid until `deinit` releases the lock.
        pub const ReadView = struct {
            ctx: *Self,

            pub fn read(self: ReadView, comptime T: type, key: anytype) !?T {
                return self.ctx.readLocked(T, key);
            }

            pub fn count(self: ReadView, comptime T: type) u64 {
                comptime assertImmutable(T, "count");
                return @field(self.ctx.stores, entityFieldName(T)).count();
            }

            pub fn range(self: ReadView, comptime T: type, start: u64, out: []T) ![]T {
                comptime assertImmutable(T, "range");
                return @field(self.ctx.stores, entityFieldName(T)).range(start, out);
            }

            pub fn deinit(self: ReadView) void {
                self.ctx.unlock();
            }
        };

        /// Manual guard for multi-key snapshots. Prefer `read` for single keys.
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

        /// Last fully-dispatched block, the indexer's cursor, for an honest
        /// health/progress readout. Locked. Not for handlers.
        pub fn cursor(self: *Self) u64 {
            self.lock();
            defer self.unlock();
            return self._last_dispatched_block;
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
            if (self._timestamps) |*ts| ts.deinit();
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
        /// count are published together. A crash inside leaves the prior
        /// `state.snap` intact.
        pub fn commitCycle(self: *Self) !void {
            // Flush ImmutableStore appends to events.dat first. Their new
            // record counts feed the next state.snap.
            inline for (entity_list) |T| {
                if (comptime T.storage == .immutable) {
                    const field_name = comptime entityFieldName(T);
                    var store = &@field(self.stores, field_name);
                    try store.flushAppends();
                }
            }
            inline for (entity_list) |T| {
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
            inline for (entity_list) |T| {
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
            inline for (entity_list) |T| {
                if (comptime T.storage == .immutable) {
                    const field_name = comptime entityFieldName(T);
                    const store = &@field(self.stores, field_name);
                    counts[count_idx_init] = store.nextCommittedCount();
                    count_idx_init += 1;
                }
            }

            // Flush each store's staged blob payloads to its blobs.dat (write
            // + fsync + remap) BEFORE the state.snap rename, then record the
            // committed lengths in blob-slot order. The ordering keeps a crash
            // between the two from leaving the snap referencing undurable
            // bytes (ADR-005). Blobless schemas make this a zero-length array.
            var blob_bytes: [Snap.blob_count]u64 = undefined;
            comptime var blob_idx_init: usize = 0;
            inline for (entity_list) |T| {
                if (comptime entity_serial.hasBlobs(T)) {
                    const field_name = comptime entityFieldName(T);
                    var store = &@field(self.stores, field_name);
                    try store.flushBlobs();
                    blob_bytes[blob_idx_init] = store.committedBlobLen();
                    blob_idx_init += 1;
                }
            }

            try self._state_snap.commit(self._last_dispatched_block, &slabs, &counts, &blob_bytes);

            // Rebind each MutableStore's slab to the new state.snap body.
            comptime var refresh_idx: usize = 0;
            inline for (entity_list) |T| {
                if (comptime T.storage == .mutable) {
                    const field_name = comptime entityFieldName(T);
                    var store = &@field(self.stores, field_name);
                    store.refreshSlab(self._state_snap.mutableSlab(refresh_idx));
                    refresh_idx += 1;
                }
            }

            // Advance each ImmutableStore's committed count.
            inline for (entity_list) |T| {
                if (comptime T.storage == .immutable) {
                    const field_name = comptime entityFieldName(T);
                    var store = &@field(self.stores, field_name);
                    store.markCommitted();
                }
            }

            self.stats.commits_performed += 1;
        }

        /// Strict cache read, never issues HTTP. Returns `error.NotPrefetched`
        /// for undeclared pairs, `error.CallReverted` for status=1 entries.
        /// Any `[]const u8` in the result (a dynamic `string`/`bytes`, bare or
        /// a tuple field) borrows the cache entry's bytes, stable until a
        /// re-prefetch overwrites the same key. Copy it to hold past the
        /// current handler.
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

        /// Strict cache read for a parameterized method. `args` is a tuple of
        /// fixed-size values (the same the prefetch resolved), encoded into the
        /// calldata `selector ++ word*` so the key matches its prefetched entry.
        /// `ethCall` is the no-arg fast path.
        pub fn ethCallArgs(
            self: *Self,
            comptime T: type,
            to: [20]u8,
            comptime method: []const u8,
            args: anytype,
        ) !T {
            const cache = self._cache orelse return error.NotPrefetched;
            const sel = comptime ethcall.selectorOf(method);
            const nargs = std.meta.fields(@TypeOf(args)).len;
            var calldata: [4 + nargs * 32]u8 = undefined;
            @memcpy(calldata[0..4], &sel);
            inline for (args, 0..) |a, i| {
                const w = ethcall.encodeArg(a);
                @memcpy(calldata[4 + i * 32 ..][0..32], &w);
            }
            const entry = cache.get(to, &calldata) orelse return error.NotPrefetched;
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

/// Count of blob-bearing stores (either kind), the `state.snap` blob slot
/// count. Slot order is entity-declaration order among blob-bearing types.
fn blobStoreCount(comptime entities: anytype) usize {
    comptime {
        var n: usize = 0;
        for (resolveEntities(entities)) |T| if (entity_serial.hasBlobs(T)) {
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
/// via `state.snap` and every handle in the Context is closed. To keep the
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
    // Headless follow runs the live loop inline on this thread (blocks forever
    // under normal operation). Backfill-only (`follow = false`) returns stats.
    if (options.follow) {
        announceBackfill(m, ctx.stats);
        try followLoop(m, Handler, ctx, options);
    }
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
    // Fail the build on a malformed manifest (empty method strings, a factory
    // spawn_param of the wrong type that would otherwise yield garbage child
    // addresses at runtime).
    comptime sdk_manifest.validateManifest(m);

    var timer = try std.time.Timer.start();

    // Banner is verbose-only. Per-phase progress prints at the default level
    // (the test suite stays quiet via the is_test silent default).
    core.log.debug(
        \\
        \\   ███████ ███    ███ ██ ████████
        \\   ██      ████  ████ ██    ██
        \\   █████   ██ ████ ██ ██    ██
        \\   ██      ██  ██  ██ ██    ██
        \\   ███████ ██      ██ ██    ██
        \\
        \\
    , .{});

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

    // Local mode opens the engine flat store. Remote mode streams instead, so
    // there is no local store to read.
    var reader: ?core.FlatStoreReader = if (options.remote_engine == null)
        try core.FlatStoreReader.open(options.engine_data_dir)
    else
        null;
    defer if (reader) |*r| r.deinit();

    const C = Context(entities);
    const entity_list = comptime resolveEntities(entities);
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

    // Local path reports the engine store's covered span from the flat reader.
    // Both the cold build and the warm re-run reach here. The remote path has
    // no reader and sets the span from the streamed store in remoteBackfill.
    if (reader) |r| {
        ctx.stats.start_block = r.first_block;
        ctx.stats.end_block = if (r.index_count == 0) r.first_block else r.first_block + r.index_count - 1;
        core.log.info("  indexing blocks {d} → {d}\n", .{ ctx.stats.start_block, ctx.stats.end_block });
    }

    // Open the engine's per-block timestamp index if present. Absence (older
    // stores) or a corrupt file leaves it null, and `humanize.timestampOf`
    // falls back to the derivation formula. The mmap outlives the dir handle.
    // Local mode reads the engine timestamps.bin for exact times. Remote mode
    // carries each block's timestamp in its FilteredStore entry instead.
    if (options.remote_engine == null) {
        var engine_dh = try std.fs.cwd().openDir(options.engine_data_dir, .{});
        defer engine_dh.close();
        ctx._timestamps = core.timestamps.TimestampReader.open(engine_dh) catch null;
    }
    errdefer if (ctx._timestamps) |*ts| ts.deinit();
    // Function-scope cleanup for ownership set inside blocks below. The
    // block-local errdefers expire when their blocks exit, so a later init
    // failure (replay, gap-fill) must free through the ctx fields. Matters to
    // spawn-embedding callers that catch the error and retry instead of
    // exiting.
    errdefer if (ctx._cache) |c| {
        c.deinit();
        allocator.destroy(c);
    };
    errdefer if (ctx._child_addresses) |set| {
        set.deinit();
        allocator.destroy(set);
    };

    // Open every ImmutableStore's events.dat. Logs live on the Context so
    // each ImmutableStore can hold a stable pointer into the field.
    inline for (entity_list) |T| {
        if (comptime T.storage == .immutable) {
            const field_name = comptime entityFieldName(T);
            const log_file_name = comptime field_name ++ ".events.dat";
            @field(ctx._event_logs, field_name) = try event_log_mod.EventLog(T).open(allocator, entity_dh, log_file_name);
        }
    }
    errdefer inline for (entity_list) |T| {
        if (comptime T.storage == .immutable) {
            const field_name = comptime entityFieldName(T);
            @field(ctx._event_logs, field_name).deinit();
        }
    };

    var filter_dh = try std.fs.cwd().openDir(filter_dir, .{});
    defer filter_dh.close();

    const fp = comptime sdk_manifest.fingerprint(m);

    if (options.remote_engine) |re| {
        // A manifest change invalidates the streamed store. Clear so the stream
        // restarts from cursor 0 instead of appending past a stale tail.
        if (!shouldSkipFilterBuild(filter_dh, allocator, fp)) try clearFilterFiles(filter_dh);
        try remoteBackfill(m, C, re, ctx, filter_dh, allocator);
        try writeFilterFingerprint(filter_dh, fp);
    } else if (shouldSkipFilterBuild(filter_dh, allocator, fp)) {
        ctx.stats.phases_skipped = true;
        core.log.info("  reusing filtered index, skipping build\n", .{});
        // Even with the filter reused, factory children must be rediscovered
        // so the live address gate admits them. The primary store always
        // holds the factory create-events, so scanCreations rebuilds the same
        // set without re-running the skipped full build.
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

        // Tx-fields carry. The whole build range must be covered up front,
        // a hole would surface as a dropped block mid-build.
        var txs_storage: core.txs.TxsReader = undefined;
        var txs_reader: ?*const core.txs.TxsReader = null;
        if (comptime sdk_manifest.wantsTxFields(m)) {
            // Clamp to what the flat store actually holds: the build scans no
            // block below first_block or above the tip regardless of manifest.
            const build_from = @max(m.start_block, reader.?.first_block);
            const build_end = @min(
                m.end_block orelse std.math.maxInt(u64),
                reader.?.first_block + reader.?.index_count - 1,
            );
            txs_storage = try openTxsRequired(options.engine_data_dir, build_from, build_end);
            txs_reader = &txs_storage;
        }
        defer if (comptime sdk_manifest.wantsTxFields(m)) txs_storage.deinit();

        const primary_result = try filter_builder.build(&reader.?, m, txs_reader, filter_dh, allocator);
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
                const child_addrs = try keysToSlice(&discovered, allocator);
                defer allocator.free(child_addrs);

                const child_result = try filter_builder.appendChildren(
                    &reader.?,
                    m,
                    txs_reader,
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
    // a prefetch declaration is removed.
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

    // Open every entity store. MutableStores borrow a slab from state_snap.
    // ImmutableStores wrap their owning EventLog with the authoritative count
    // from state_snap.immutable_counts. Slot indices are comptime-tracked in
    // entities-tuple order.
    {
        comptime var mut_slot: usize = 0;
        comptime var imm_slot: usize = 0;
        comptime var blob_slot: usize = 0;
        inline for (entity_list) |T| {
            const field_name = comptime entityFieldName(T);
            if (comptime T.storage == .mutable) {
                if (comptime entity_serial.hasBlobs(T)) {
                    @field(ctx.stores, field_name) = try mutable_store_mod.MutableStore(T).openWithBlobs(
                        allocator,
                        ctx._state_snap.mutableSlab(mut_slot),
                        entity_dh,
                        comptime field_name ++ ".blobs.dat",
                        ctx._state_snap.blobBytes(blob_slot),
                    );
                    blob_slot += 1;
                } else {
                    @field(ctx.stores, field_name) = mutable_store_mod.MutableStore(T).open(
                        allocator,
                        ctx._state_snap.mutableSlab(mut_slot),
                    );
                }
                mut_slot += 1;
            } else {
                const log_ptr = &@field(ctx._event_logs, field_name);
                if (comptime entity_serial.hasBlobs(T)) {
                    @field(ctx.stores, field_name) = try immutable_store_mod.ImmutableStore(T).openWithBlobs(
                        allocator,
                        log_ptr,
                        ctx._state_snap.immutableCount(imm_slot),
                        entity_dh,
                        comptime field_name ++ ".blobs.dat",
                        ctx._state_snap.blobBytes(blob_slot),
                    );
                    blob_slot += 1;
                } else {
                    @field(ctx.stores, field_name) = try immutable_store_mod.ImmutableStore(T).open(
                        allocator,
                        log_ptr,
                        ctx._state_snap.immutableCount(imm_slot),
                    );
                }
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

    // Local follow gap-fills the flat-store advance the engine made during
    // backfill so `spawn` returns a caught-up ctx. Remote follow re-streams from
    // the cursor via REGISTER instead. followLoop runs the gap-fill again at the
    // live handoff to catch any advance since this point.
    if (options.follow and options.remote_engine == null) {
        try followGapFill(m, Handler, ctx, options, allocator);
    }
    // init never enters the live loop. run/spawn drive followLoop below.
    return ctx;
}

/// Replay the flat-store advance the engine made past the cursor. Re-reads meta
/// and dispatches `(cursor, last_finalized]` from a freshly opened reader. The
/// reader `init` mmap'd at open never sees blocks appended during backfill. Run
/// once after backfill so `spawn` returns a caught-up ctx, and again at the live
/// handoff. The engine can finalize more blocks between the two, and the live
/// loop reads only the pending ring, so a block finalizing in that window would
/// be dispatched by no one. Idempotent when meta has not advanced.
///
/// A sub-tick residual remains. meta and pending are separate files read
/// non-atomically, so a block whose full finalize lands between this meta read
/// and the live loop's first pending read is still missed. Closing it needs a
/// pending-before-meta read with boundary dedup, or an atomic engine checkpoint.
fn followGapFill(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    options: Options,
    allocator: std.mem.Allocator,
) !void {
    const engine_last = try live.readMeta(options.engine_data_dir);
    if (engine_last <= ctx._last_dispatched_block) return;

    const filter_dir = try std.fs.path.join(allocator, &.{ options.data_dir, "filter" });
    defer allocator.free(filter_dir);
    var filter_dh = try std.fs.cwd().openDir(filter_dir, .{});
    defer filter_dh.close();

    var gap_reader = try core.FlatStoreReader.open(options.engine_data_dir);
    defer gap_reader.deinit();

    const gap_from = ctx._last_dispatched_block + 1;

    // Fresh open per gap fill. The engine extends txs.dat with each finalize,
    // an init-scoped reader would not see the new coverage.
    var txs_storage: core.txs.TxsReader = undefined;
    var txs_reader: ?*const core.txs.TxsReader = null;
    if (comptime sdk_manifest.wantsTxFields(m)) {
        txs_storage = try openTxsRequired(options.engine_data_dir, gap_from, engine_last);
        txs_reader = &txs_storage;
    }
    defer if (comptime sdk_manifest.wantsTxFields(m)) txs_storage.deinit();

    const gap_result = try filter_builder.appendBlocks(&gap_reader, m, txs_reader, gap_from, engine_last, filter_dh, allocator);
    try requireCompleteFilter("follow gap-fill", gap_result);

    // The gap may hold create-events for children unknown to the backfill pass
    // (and child events from already-known children). Rediscover over the
    // now-extended primary, merge into the live set, and extend children.dat so
    // replay sees them. The gap range sits above the children tail, append stays
    // monotonic.
    if (comptime m.factories.len > 0) {
        var gap_children = try scanner.scanCreations(filter_dh, m, allocator);
        defer gap_children.deinit();
        if (ctx._child_addresses) |set| {
            var it = gap_children.keyIterator();
            while (it.next()) |addr| try set.put(addr.*, {});
            if (set.count() > 0) {
                const addrs = try keysToSlice(set, allocator);
                defer allocator.free(addrs);
                const cres = try filter_builder.appendChildrenBlocks(&gap_reader, m, txs_reader, addrs, gap_from, engine_last, filter_dh, allocator);
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

/// Live loop body. Sets up the Multicall (when a node RPC is configured) and
/// runs `live.run` until stop/error. Inline under `run`, on a thread under
/// `spawn`. The multicall stays on this frame for the loop's lifetime.
fn followLoop(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    options: Options,
) !void {
    if (options.remote_engine) |re| {
        return tcp_client.follow(
            m,
            Handler,
            ctx,
            re.host,
            re.port,
            comptime filter_builder.collectKnownAddresses(m),
            comptime filter_builder.collectFollowTopics(m),
            ctx._allocator,
        );
    }

    // Close the init->live handoff window. The engine may have finalized more
    // blocks since init's gap-fill. Replay that advance from the flat store
    // before the live loop (which reads only the pending ring) takes over.
    try followGapFill(m, Handler, ctx, options, ctx._allocator);

    if (options.node_rpc) |rpc_url| {
        var http = eth.http_transport.HttpTransport.init(ctx._allocator, rpc_url);
        // Frees the std.http.Client connection pool (kept-alive sockets).
        defer http.deinit();
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
    announceBackfill(m, ctx.stats);

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
/// All gather allocations live in a local arena that frees on return. The
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
    const dynamic_calls = try prefetch.gatherDynamic(arena, filter_dh, m, cache);

    const merged = try arena.alloc(ethcall.Call, static_calls.len + dynamic_calls.len);
    @memcpy(merged[0..static_calls.len], static_calls);
    @memcpy(merged[static_calls.len..], dynamic_calls);

    const unique = try prefetch.dedupe(arena, merged);
    ctx.stats.prefetch_calls_gathered = unique.len;

    const missing = try prefetch.filterUncached(arena, cache, unique);
    if (missing.len == 0) return;

    // No RPC configured: gather is informational, fetch is a no-op. Handlers
    // that hit an uncached pair see `error.NotPrefetched` at replay.
    const rpc_url = options.node_rpc orelse return;

    var http = eth.http_transport.HttpTransport.init(allocator, rpc_url);
    // Frees the std.http.Client connection pool (kept-alive sockets).
    defer http.deinit();
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

/// Stream one filtered store from the remote engine. The REGISTER cursor is the
/// store's current tail, so a re-run extends it rather than restreaming. Returns
/// the number of blocks written.
fn streamInto(
    re: RemoteEngine,
    filter_dh: std.fs.Dir,
    comptime base: []const u8,
    addresses: []const [20]u8,
    topics: []const [32]u8,
    exclude: []const [20]u8,
    tx_fields: bool,
    allocator: std.mem.Allocator,
) !u64 {
    var store = try filtered_store_mod.FilteredStore.open(allocator, filter_dh, base);
    defer store.deinit();
    const cursor = if (store.count() > 0)
        (try store.readEntry(store.count() - 1)).block_number
    else
        0;
    const result = try tcp_client.backfill(re.host, re.port, .{
        .cursor = cursor,
        .addresses = addresses,
        .topics = topics,
        .exclude_addresses = exclude,
        .tx_fields = tx_fields,
    }, &store, allocator);
    return result.blocks_received;
}

/// Remote analogue of the local build + scanCreations + appendChildren. Streams
/// the primary, and for a factory manifest discovers children from the streamed
/// creations and streams their events into the children store (excluding
/// static∪factory, matching the local children filter). Populates `ctx.stats`
/// and hands the discovered child set to `ctx` so the live follow admits them.
fn remoteBackfill(
    comptime m: sdk_manifest.Manifest,
    comptime C: type,
    re: RemoteEngine,
    ctx: *C,
    filter_dh: std.fs.Dir,
    allocator: std.mem.Allocator,
) !void {
    ctx.stats.filter_blocks_matched = try streamInto(
        re,
        filter_dh,
        filter_builder.BASE_PRIMARY,
        comptime filter_builder.collectKnownAddresses(m),
        comptime filter_builder.collectAllTopics(m),
        &.{},
        comptime sdk_manifest.wantsTxFields(m),
        allocator,
    );

    // Remote has no flat reader. Derive the indexed span from the streamed
    // primary store first and last block, so the stats and the verbose range
    // line match the local path instead of reading 0.
    {
        var ps = filtered_store_mod.FilteredStore.open(allocator, filter_dh, filter_builder.BASE_PRIMARY) catch null;
        if (ps) |*store| {
            defer store.deinit();
            if (store.count() > 0) {
                ctx.stats.start_block = (try store.readEntry(0)).block_number;
                ctx.stats.end_block = (try store.readEntry(store.count() - 1)).block_number;
                core.log.info("  indexing blocks {d} → {d}\n", .{ ctx.stats.start_block, ctx.stats.end_block });
            }
        }
    }

    if (comptime m.factories.len > 0) {
        const child_topics = comptime filter_builder.collectChildTopics(m);
        var discovered = try scanner.scanCreations(filter_dh, m, allocator);
        errdefer discovered.deinit();
        ctx.stats.discovered_children = discovered.count();

        if (discovered.count() > 0 and child_topics.len > 0) {
            const child_addrs = try keysToSlice(&discovered, allocator);
            defer allocator.free(child_addrs);

            ctx.stats.children_blocks_matched = try streamInto(
                re,
                filter_dh,
                filter_builder.BASE_CHILDREN,
                child_addrs,
                child_topics,
                comptime filter_builder.collectKnownAddresses(m),
                comptime sdk_manifest.wantsTxFields(m),
                allocator,
            );
        }

        // The follow REGISTER and the live address gate read this set. Ownership
        // moves to ctx, freed by Context.deinit.
        try setChildAddresses(C, ctx, allocator, discovered);
    }
}

/// Copy a `[20]u8` key set into a freshly-allocated slice. Caller frees.
fn keysToSlice(set: *const std.AutoHashMap([20]u8, void), allocator: std.mem.Allocator) ![][20]u8 {
    const out = try allocator.alloc([20]u8, set.count());
    var i: usize = 0;
    var it = set.keyIterator();
    while (it.next()) |a| : (i += 1) out[i] = a.*;
    return out;
}

/// Move `discovered` onto the heap and hand ownership to `ctx`. The set has
/// the lifetime of the Context (same allocator). `Context.deinit` frees it.
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
        "primary.dat",          "primary.idx",
        "children.dat",         "children.idx",
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
    core.log.err(
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

/// Open the engine's txs.{dat,idx} pair and require coverage of `[from, to]`.
/// Called only for manifests setting `tx_fields`. Absence or a coverage hole
/// is fatal at init, never a silent null `log.tx` at dispatch.
fn openTxsRequired(engine_data_dir: []const u8, from: u64, to: u64) !core.txs.TxsReader {
    var dh = try std.fs.cwd().openDir(engine_data_dir, .{});
    defer dh.close();
    var tr = (core.txs.TxsReader.open(dh) catch null) orelse {
        core.log.err(
            \\
            \\ERROR: manifest sets tx_fields but the engine store has no txs.{{dat,idx}}.
            \\Re-run the import without --no-tx-fields (or run `emit-engine import`
            \\against a store that already has logs to backfill the tx pass).
            \\
        , .{});
        return error.TxFieldsUnavailable;
    };
    if (!tr.covers(from, to)) {
        core.log.err(
            \\
            \\ERROR: manifest sets tx_fields but txs.dat covers [{d}, {d}], the
            \\indexed range needs [{d}, {d}]. Extend the import's tx pass first.
            \\
        , .{ tr.first_block, tr.first_block + tr.count -| 1, from, to });
        tr.deinit();
        return error.TxFieldsUnavailable;
    }
    return tr;
}

/// Comptime-build the inner struct that holds one entity store per tuple
/// element, used as the type of `Context.stores`. Field names derive from
/// the entity type's basename, lowercasing the first byte and appending `s`
/// (e.g. `Account` → `accounts`). An entity type may override this with
/// `pub const store_name = "balances";`, useful when the auto-derived
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

    // Module form. `entities` is a type whose pub decls include the
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

    // Tuple form. `entities` is a tuple value of entity types.
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

/// Compile-time guard for the immutable-only Context reads (`count`, `range`).
/// Mutable entities are point-read by key via `read`. Iterating them would
/// mean a union+dedup over the slab, cache, and overlays, a different and
/// unbuilt read shape.
fn assertImmutable(comptime T: type, comptime who: []const u8) void {
    if (T.storage != .immutable) @compileError(
        "Context." ++ who ++ "(): '" ++ @typeName(T) ++ "' is a mutable entity. " ++
            who ++ "() serves immutable event-log entities; use read() for keyed state.",
    );
}

/// Blob fields borrow the store mmap, so a value returned past the lock would
/// dangle on a concurrent commit. The auto-locking accessors reject them and
/// point at `readView`, which holds the lock across the borrow (ADR-005).
fn assertNoBlobs(comptime T: type, comptime who: []const u8) void {
    if (entity_serial.hasBlobs(T)) @compileError(
        "Context." ++ who ++ "(): '" ++ @typeName(T) ++ "' has blob fields whose slices " ++
            "borrow the store mmap. Read it through `ctx.readView()` so the borrow stays " ++
            "valid under the lock, and copy any blob bytes out before the view is released.",
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
    // commit_interval = 1 with a 3-log single-block fixture. Block-boundary
    // commit discipline produces 1 in-replay commit + 1 final = 2 (a mid-block
    // commit would yield 3 + 1). The invariant the cursor scheme depends on:
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
    // Inline module fixture. A struct with two pub entity decls plus a
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
        // Non-entity decl, must be ignored by the resolver.
        pub const helper_constant: u32 = 42;
    };

    const FromModule = Context(FixtureModule);
    const FromTuple = Context(.{ FixtureModule.A, FixtureModule.B });

    // Both forms yield identical Stores layouts, the user-observable contract.
    const stores_from_module = std.meta.fieldInfo(FromModule, .stores).type;
    const stores_from_tuple = std.meta.fieldInfo(FromTuple, .stores).type;
    try testing.expectEqual(stores_from_module, stores_from_tuple);
    try testing.expect(@hasField(stores_from_module, "as"));
    try testing.expect(@hasField(stores_from_module, "bs"));
}

const TransferHandler = struct {
    pub fn handleTransfer(log: @import("handler.zig").Log(Transfer), ctx: anytype) !void {
        // Transfer's signature is unnamed, so read positionally here.
        // Named-parameter access (`log.params.value`) is exercised by the
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
/// RawLog is used. A per-call buffer avoids aliasing across calls, which
/// would corrupt the test fixtures.
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

fn writeTestMeta(dir: std.fs.Dir, last_finalized: u64) !void {
    const meta = flat_reader.Meta{
        .last_finalized_block = last_finalized,
        .blocks_dat_size = 0,
        .blocks_idx_count = 0,
        .blooms_count = 0,
        .checksum = 0,
    };
    var buf: [flat_reader.META_SIZE]u8 = undefined;
    meta.serialize(&buf);
    var f = try dir.createFile("meta.bin", .{});
    defer f.close();
    try f.writeAll(&buf);
}

test "follow gap-fill dispatches blocks the engine finalized after backfill" {
    // Residual fix for the init->live handoff. The engine can finalize blocks
    // into the flat store after init's backfill, and the live loop reads only
    // the pending ring, so followGapFill must replay that advance or the blocks
    // are dispatched by no one. Backfill 100-101, grow the store to 100-103 with
    // meta past the cursor, then assert followGapFill dispatches 102-103 without
    // re-dispatching 100-101.
    const allocator = testing.allocator;

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    var d: [4][32]u8 = undefined;
    const log100 = makeTransferLog(100, 0, [_]u8{0} ** 20, ALICE, 100, &d[0]);
    const log101 = makeTransferLog(101, 0, [_]u8{0} ** 20, ALICE, 10, &d[1]);

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try writeFlatStoreFromLogs(src_tmp.dir, &.{ &.{log100}, &.{log101} }, allocator);
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
    const options: Options = .{ .engine_data_dir = src_path, .data_dir = data_path, .commit_interval = 100_000 };

    const ctx = try init(Manifest, TransferHandler, .{Account}, options, allocator);
    defer ctx.deinit();
    try testing.expectEqual(@as(u64, 101), ctx._last_dispatched_block);

    // The engine appends two finalized blocks and advances meta past the cursor.
    const log102 = makeTransferLog(102, 0, [_]u8{0} ** 20, ALICE, 5, &d[2]);
    const log103 = makeTransferLog(103, 0, [_]u8{0} ** 20, ALICE, 1, &d[3]);
    try writeFlatStoreFromLogs(src_tmp.dir, &.{ &.{log100}, &.{log101}, &.{log102}, &.{log103} }, allocator);
    try writeTestMeta(src_tmp.dir, 103);

    try followGapFill(Manifest, TransferHandler, ctx, options, allocator);

    try testing.expectEqual(@as(u64, 103), ctx._last_dispatched_block);
    const alice = (try ctx.stores.accounts.load(ALICE)) orelse return error.MissingAlice;
    try testing.expectEqual(@as(u64, 116), alice.balance); // 100 + 10 + 5 + 1
}

test "Context read/count/range/cursor over an immutable store after backfill" {
    const allocator = testing.allocator;
    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    const BOB: [20]u8 = [_]u8{0xB2} ** 20;
    const CARL: [20]u8 = [_]u8{0xC3} ** 20;

    // Immutable event entity plus a handler that records one per Transfer.
    const XferEvent = struct {
        pub const storage: root.StorageMode = .immutable;
        id: [16]u8,
        to: [20]u8,
        value: u64,
    };
    const XferHandler = struct {
        pub fn handleTransfer(log: @import("handler.zig").Log(Transfer), ctx: anytype) !void {
            const to = log.topics[2][12..32].*;
            const value: u64 = std.mem.readInt(u64, log.data[24..32], .big);
            try ctx.stores.xferEvents.save(.{ .id = log.eventId(), .to = to, .value = value });
        }
    };

    // Three transfers across three blocks -> monotonic immutable ids.
    var data_bufs: [3][32]u8 = undefined;
    const blocks = [_][]const core.RawLog{
        &.{makeTransferLog(100, 0, [_]u8{0} ** 20, ALICE, 100, &data_bufs[0])},
        &.{makeTransferLog(101, 0, [_]u8{0} ** 20, BOB, 200, &data_bufs[1])},
        &.{makeTransferLog(102, 0, [_]u8{0} ** 20, CARL, 300, &data_bufs[2])},
    };

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
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
        XferHandler,
        .{XferEvent},
        .{ .engine_data_dir = src_path, .data_dir = data_path, .commit_interval = 100_000 },
        allocator,
    );
    defer ctx.deinit();

    // count spans the finalized log. cursor is the last dispatched block.
    try testing.expectEqual(@as(u64, 3), ctx.count(XferEvent));
    try testing.expectEqual(@as(u64, 102), ctx.cursor());

    // range over the whole store, ascending by id (block order).
    var buf: [8]XferEvent = undefined;
    const all = try ctx.range(XferEvent, 0, &buf);
    try testing.expectEqual(@as(usize, 3), all.len);
    try testing.expectEqual(@as(u64, 100), all[0].value);
    try testing.expectEqualSlices(u8, &ALICE, &all[0].to);
    try testing.expectEqual(@as(u64, 300), all[2].value);
    try testing.expectEqualSlices(u8, &CARL, &all[2].to);

    // Newest-first page of size 2 (start = count - 2). The caller reverses.
    // Its own buffer so the `all` slice above (aliasing `buf`) stays valid.
    var page_buf: [2]XferEvent = undefined;
    const page = try ctx.range(XferEvent, ctx.count(XferEvent) - 2, &page_buf);
    try testing.expectEqual(@as(u64, 200), page[0].value);
    try testing.expectEqual(@as(u64, 300), page[1].value);

    // read by key round-trips the middle record.
    const got = (try ctx.read(XferEvent, all[1].id)) orelse return error.MissingMid;
    try testing.expectEqual(@as(u64, 200), got.value);
    try testing.expectEqualSlices(u8, &BOB, &got.to);
}

test "spawn: follows on a background thread, reads under lock, deinit joins" {
    const allocator = testing.allocator;
    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;

    // Two finalized mints to alice (100 + 50). No pending.bin, so the follow
    // thread ticks on an empty ring and idles. Exercises spawn's thread
    // lifecycle + locked reads + deinit join, not live dispatch (covered by
    // the live.zig tick tests).
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
    defer ctx.deinit(); // sets _stop + joins the follow thread, hangs the test if join fails

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

    // Pending block 102 mints +25 to alice. The follow thread must dispatch it
    // into the overlay so a reader sees the tip value 175.
    var pbuf: [32]u8 = undefined;
    try fake.ingest(102, [_]u8{0xAA} ** 32, &.{makeTransferLog(102, 0, [_]u8{0} ** 20, ALICE, 25, &pbuf)});
    try pollBalance(ctx, ALICE, 175, 5000);

    // Reorg 102 to a version minting +99. The reader must converge to 249.
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
/// never touches them. The caller must NOT invoke `deinit`, which would
/// dereference them.
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
    // The first init writes `state.snap.cursor` reflecting the last
    // dispatched block. The second init reads it, seeds `start_block`,
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
