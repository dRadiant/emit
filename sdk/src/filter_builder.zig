/// Filtered-index builder. Reads the engine's flat log store via `core`,
/// keeps logs matching the manifest, writes them into a flat-file pair
/// (`<base>.dat` + `<base>.idx`) via `sdk.filtered_store`. Per ADR-002.
///
/// Two entry points:
///   - `build`: phase 1, writes the `primary` pair for static + factory addresses.
///   - `appendChildren`: phase 3, writes the `children` pair for addresses
///     discovered by the scanner's factory pre-pass. No-op for an empty set.
///
/// Per-log keep rule (uniform across both phases via `Filter`):
///   keep = (address ∈ filter.match_addrs)
///       AND (topic0 ∈ filter.match_topics)
///       AND (address ∉ filter.exclude_addrs)
/// Phase 1 leaves exclude_addrs empty. Phase 3 sets it to static∪factory so a
/// static contract that's also a factory child does not appear in both pairs.
const std = @import("std");

const builtin = @import("builtin");
const core = @import("core");
const lz4 = @import("lz4");

const filtered_store_mod = @import("filtered_store.zig");
const sdk_manifest = @import("manifest.zig");

const FilteredStore = filtered_store_mod.FilteredStore;
const RawLog = core.RawLog;
const FlatStoreReader = core.FlatStoreReader;
const log_serial = core.log_serial;
const block_filter = core.block_filter;
const parallel = core.parallel;
const io_pipeline = core.io_pipeline;
const types = core.types;

pub const WORKER_QUEUE_DEPTH = 16;
/// Min matching blocks before spawning worker threads. Below this, thread
/// spin-up cost exceeds the parallel speedup.
pub const PARALLEL_THRESHOLD = 1_000;

pub const BASE_PRIMARY: []const u8 = "primary";
pub const BASE_CHILDREN: []const u8 = "children";

pub const BuildResult = struct {
    blocks_scanned: u64 = 0,
    blocks_matched: u64 = 0,
    total_logs: u64 = 0,
    /// Bloom-matched blocks the worker pipeline failed to materialize. Any
    /// non-zero value is a hard failure (incomplete index).
    dropped_blocks: u64 = 0,
    elapsed_ns: u64 = 0,
};

/// Per-log keep predicate, shared with the engine via `core.filter`. Phase 1
/// (`build`) leaves `exclude_addrs` empty. Phase 3 (`appendChildren`) sets it
/// to static∪factory.
const Filter = core.filter.Filter;

/// Phase 1: build the `primary` filtered-store pair under `dir` from the
/// manifest's static and factory addresses. Caller owns `reader` and `dir`.
/// Appending to a pre-existing pair extends it. Block numbers must be
/// strictly greater than the last recorded block.
pub fn build(
    reader: *const FlatStoreReader,
    comptime m: sdk_manifest.Manifest,
    dir: std.fs.Dir,
    allocator: std.mem.Allocator,
) !BuildResult {
    return appendBlocks(reader, m, m.start_block, m.end_block orelse std.math.maxInt(u64), dir, allocator);
}

/// Extend the primary filtered-store pair over `from_block..=to_block`.
/// `build` is the special case `from_block = manifest.start_block`.
/// Drives the follow-mode gap fill in `entry.init`: when the engine advances
/// during backfill, the SDK re-scans the new range and appends matching blocks
/// without rebuilding from scratch.
pub fn appendBlocks(
    reader: *const FlatStoreReader,
    comptime m: sdk_manifest.Manifest,
    from_block: u64,
    to_block: u64,
    dir: std.fs.Dir,
    allocator: std.mem.Allocator,
) !BuildResult {
    const known_addresses = comptime collectKnownAddresses(m);
    const all_topics = comptime collectAllTopics(m);
    return runPhase(
        reader,
        known_addresses,
        from_block,
        to_block,
        .{
            .match_addrs = known_addresses,
            .match_topics = all_topics,
            .exclude_addrs = &.{},
        },
        dir,
        BASE_PRIMARY,
        allocator,
    );
}

/// Phase 3: walk the engine's flat store filtered by scanner-discovered child
/// addresses, write matching child-event logs to the `children` pair under
/// `dir`. Spans the manifest's whole range. The follow-mode gap fill uses
/// `appendChildrenBlocks` for a sub-range instead.
pub fn appendChildren(
    reader: *const FlatStoreReader,
    comptime m: sdk_manifest.Manifest,
    child_addresses: []const [20]u8,
    dir: std.fs.Dir,
    allocator: std.mem.Allocator,
) !BuildResult {
    return appendChildrenBlocks(
        reader,
        m,
        child_addresses,
        m.start_block,
        m.end_block orelse std.math.maxInt(u64),
        dir,
        allocator,
    );
}

/// Extend the children pair over `from_block..=to_block` for `child_addresses`.
/// Returns a zero BuildResult when there are no children or child events. The
/// per-log filter excludes addresses already in `static∪factory` so a static
/// contract that's also a factory child does not produce duplicate entries
/// across pairs. Block numbers must exceed the children store's current tail
/// (caller fills strictly-increasing ranges).
pub fn appendChildrenBlocks(
    reader: *const FlatStoreReader,
    comptime m: sdk_manifest.Manifest,
    child_addresses: []const [20]u8,
    from_block: u64,
    to_block: u64,
    dir: std.fs.Dir,
    allocator: std.mem.Allocator,
) !BuildResult {
    if (child_addresses.len == 0) return .{};

    const known_addresses = comptime collectKnownAddresses(m);
    const child_topics = comptime collectChildTopics(m);
    if (child_topics.len == 0) return .{};

    return runPhase(
        reader,
        child_addresses,
        from_block,
        to_block,
        .{
            .match_addrs = child_addresses,
            .match_topics = child_topics,
            .exclude_addrs = known_addresses,
        },
        dir,
        BASE_CHILDREN,
        allocator,
    );
}

/// Shared phase runner. `bloom_addresses` feeds the bloom scan (block-level
/// prefilter). `filter` is the per-log keep predicate (post-decompression
/// precision filter). `base` selects the flat-store pair under `dir` to append
/// to (`BASE_PRIMARY` or `BASE_CHILDREN`).
fn runPhase(
    reader: *const FlatStoreReader,
    bloom_addresses: []const [20]u8,
    start_block: u64,
    end_block: u64,
    filter: Filter,
    dir: std.fs.Dir,
    comptime base: []const u8,
    allocator: std.mem.Allocator,
) !BuildResult {
    var result = BuildResult{};
    var timer = try std.time.Timer.start();

    var matching = std.ArrayListUnmanaged(u64){};
    defer matching.deinit(allocator);

    try block_filter.scanBloomsParallel(
        reader,
        bloom_addresses,
        filter.match_topics,
        start_block,
        end_block,
        &matching,
        &result.blocks_scanned,
        &result.dropped_blocks,
        allocator,
    );

    if (matching.items.len == 0) {
        result.elapsed_ns = timer.read();
        return result;
    }

    // blooms.bin can hold duplicate entries for one block_number when the
    // importer's RocksDB key parsing collapses multi-byte discriminators (reorg
    // entries) onto the same u64. The list is sorted, so adjacent dedup
    // suffices. Without it, FilteredStore.appendEntry raises OutOfOrder on the
    // second write and the build fails.
    var write_idx: usize = 1;
    for (1..matching.items.len) |read_idx| {
        if (matching.items[read_idx] == matching.items[read_idx - 1]) continue;
        matching.items[write_idx] = matching.items[read_idx];
        write_idx += 1;
    }
    matching.items.len = write_idx;

    const num_workers = parallel.workerCount(matching.items.len, PARALLEL_THRESHOLD);
    const ranges = parallel.chunkRanges(matching.items.len, num_workers);

    var worker_results: [parallel.MAX_WORKERS]std.ArrayListUnmanaged(FilteredBlock) = undefined;
    var worker_args: [parallel.MAX_WORKERS]FilterWorkerArgs = undefined;
    var worker_arenas: [parallel.MAX_WORKERS]std.heap.ArenaAllocator = undefined;

    for (0..num_workers) |i| {
        worker_arenas[i] = std.heap.ArenaAllocator.init(allocator);
        worker_results[i] = .{};
        worker_args[i] = .{
            .reader = reader,
            .matching_blocks = matching.items[ranges[i][0]..ranges[i][1]],
            .filter = filter,
            .results = &worker_results[i],
            .allocator = worker_arenas[i].allocator(),
        };
    }
    defer for (0..num_workers) |i| worker_arenas[i].deinit();

    try parallel.run(FilterWorkerArgs, worker_args[0..num_workers], num_workers, filterWorker);

    // Surface fatal pipeline errors before opening the writer. Per-block drops accumulate below.
    for (0..num_workers) |i| if (worker_args[i].err) |e| return e;

    var store = try FilteredStore.open(allocator, dir, base);
    defer store.deinit();

    for (0..num_workers) |i| {
        for (worker_results[i].items) |fb| {
            // Local build leaves the FilteredStore timestamp 0. The scanner
            // falls back to the engine's timestamps.bin via `timestampOf`. Only
            // the remote client fills it, from the PUSH frame.
            try store.appendEntry(fb.block_number, 0, fb.entry);
            result.blocks_matched += 1;
            result.total_logs += fb.log_count;
        }
    }
    try store.syncAll();

    for (0..num_workers) |i| result.dropped_blocks += worker_args[i].dropped_blocks;
    result.elapsed_ns = timer.read();
    return result;
}

// ── Manifest projections ─────────────────────────────────────────────────
// Public so the remote client builds its REGISTER filter from the manifest.

pub fn collectKnownAddresses(comptime m: sdk_manifest.Manifest) []const [20]u8 {
    comptime {
        var out: []const [20]u8 = &.{};
        for (m.contracts) |c| out = out ++ &[_][20]u8{c.address};
        for (m.factories) |f| out = out ++ &[_][20]u8{f.address};
        return out;
    }
}

pub fn collectAllTopics(comptime m: sdk_manifest.Manifest) []const [32]u8 {
    comptime {
        var out: []const [32]u8 = &.{};
        for (m.contracts) |c| {
            for (c.events) |E| {
                const t = sdk_manifest.eventTopic0(E);
                if (core.filter.containsTopic(out, &t)) continue;
                out = out ++ &[_][32]u8{t};
            }
        }
        for (m.factories) |f| {
            const t = sdk_manifest.eventTopic0(f.create_event);
            if (!core.filter.containsTopic(out, &t)) out = out ++ &[_][32]u8{t};
        }
        return out;
    }
}

pub fn collectChildTopics(comptime m: sdk_manifest.Manifest) []const [32]u8 {
    comptime {
        var out: []const [32]u8 = &.{};
        for (m.factories) |f| {
            for (f.child_events) |E| {
                const t = sdk_manifest.eventTopic0(E);
                if (!core.filter.containsTopic(out, &t)) out = out ++ &[_][32]u8{t};
            }
        }
        return out;
    }
}

// ── Worker ───────────────────────────────────────────────────────────────

const FilteredBlock = struct {
    block_number: u64,
    entry: []u8, // lz4_len(4) + lz4_data
    log_count: u32,
};

const FilterWorkerArgs = struct {
    reader: *const FlatStoreReader,
    matching_blocks: []const u64,
    filter: Filter,
    results: *std.ArrayListUnmanaged(FilteredBlock),
    allocator: std.mem.Allocator,
    /// Per-block recoverable failures (alloc, lz4, etc.), surfaced via BuildResult.
    dropped_blocks: u64 = 0,
    /// First fatal pipeline-level error (init/wait/oversize). Aborts the build.
    err: ?anyerror = null,
};

fn filterWorker(args: *FilterWorkerArgs) void {
    // Stack scratch is safe: `parallel.run` always spawns workers at
    // `WORKER_STACK_SIZE`.
    var decompress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var serialize_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var compress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;

    const reader = args.reader;

    if (comptime io_pipeline.supported) {
        const Pipeline = io_pipeline.ReadPipeline(WORKER_QUEUE_DEPTH);
        const pipeline = Pipeline.init(args.allocator, reader.blocks_file.handle) catch |e| {
            args.err = e;
            return;
        };
        defer pipeline.deinit();

        var submitted: usize = 0;
        var completed: usize = 0;
        const total = args.matching_blocks.len;

        while (completed < total) {
            while (submitted < total) {
                const slot = pipeline.claimSlot() orelse break;
                const loc = reader.getBlockLoc(args.matching_blocks[submitted]) catch {
                    pipeline.releaseSlot(slot);
                    submitted += 1;
                    completed += 1;
                    args.dropped_blocks += 1;
                    continue;
                };
                pipeline.submit(slot, args.matching_blocks[submitted], loc.offset, loc.length) catch |e| {
                    pipeline.releaseSlot(slot);
                    // EntryExceedsBuffer = corrupt store, fatal. Else SQE-full, drain and retry.
                    if (e == error.EntryExceedsBuffer) {
                        args.err = e;
                        return;
                    }
                    break;
                };
                submitted += 1;
            }
            _ = pipeline.flush() catch |e| {
                args.err = e;
                return;
            };

            var done: [WORKER_QUEUE_DEPTH]*io_pipeline.Completion = undefined;
            const n = pipeline.waitAtLeastOne(&done) catch |e| {
                args.err = e;
                return;
            };
            for (done[0..n]) |c| {
                const entry_data = pipeline.getBuffer(c);
                if (entry_data.len > 0) {
                    processBlockEntry(entry_data, c.block_number, args, &decompress_buf, &serialize_buf, &compress_buf);
                } else args.dropped_blocks += 1;
                pipeline.releaseSlot(c.buf_slot);
                completed += 1;
            }
        }
        // io_uring completes in NVMe order, not submission order. Sort so the
        // writer iterates worker outputs in ascending block order, satisfying
        // FilteredStore.appendEntry's monotonic invariant.
        std.mem.sort(FilteredBlock, args.results.items, {}, blockNumberLessThan);
        return;
    }

    // pread fallback (non-Linux). Reads in matching_blocks order. Sort anyway
    // for path-uniform output.
    var read_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    for (args.matching_blocks) |bn| {
        const entry_data = reader.readBlock(bn, &read_buf) catch {
            args.dropped_blocks += 1;
            continue;
        };
        processBlockEntry(entry_data, bn, args, &decompress_buf, &serialize_buf, &compress_buf);
    }
    std.mem.sort(FilteredBlock, args.results.items, {}, blockNumberLessThan);
}

fn blockNumberLessThan(_: void, a: FilteredBlock, b: FilteredBlock) bool {
    return a.block_number < b.block_number;
}

fn processBlockEntry(
    entry_data: []const u8,
    block_number: u64,
    args: *FilterWorkerArgs,
    decompress_buf: []u8,
    serialize_buf: []u8,
    compress_buf: []u8,
) void {
    // Every error path counts the block as dropped, propagating to
    // BuildResult.dropped_blocks so the entry point refuses to ship the index.
    // No silent failures here.
    const decompressed = log_serial.decompressEntry(entry_data, decompress_buf) catch {
        args.dropped_blocks += 1;
        return;
    };

    // Precision filter, shared with the engine's TCP server via core. Keeps
    // only matching logs, recompresses into `compress_buf`. A compress failure
    // on an oversize block counts as a drop. `null` = no match.
    const maybe = core.filter.filterBlockEntry(decompressed, args.filter, serialize_buf, compress_buf) catch {
        args.dropped_blocks += 1;
        return;
    };
    const filtered = maybe orelse return;

    const owned = args.allocator.alloc(u8, filtered.entry.len) catch {
        args.dropped_blocks += 1;
        return;
    };
    @memcpy(owned, filtered.entry);

    args.results.append(args.allocator, .{
        .block_number = block_number,
        .entry = owned,
        .log_count = filtered.log_count,
    }) catch {
        args.dropped_blocks += 1;
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;
const flat_reader = core.flat_reader;

const TestLog = flat_reader.TestLog;
const TestBlock = flat_reader.TestBlock;

const ContractA = struct {
    pub const signature = "EventA(uint256)";
};
const ContractB = struct {
    pub const signature = "EventB(uint256)";
};
const Other = struct {
    pub const signature = "Other(uint256)";
};

const ADDR_A: [20]u8 = [_]u8{0xAA} ** 20;
const ADDR_B: [20]u8 = [_]u8{0xBB} ** 20;
const ADDR_C: [20]u8 = [_]u8{0xCC} ** 20;

fn topicOf(comptime E: type) [32]u8 {
    return sdk_manifest.eventTopic0(E);
}

const SmallManifest: sdk_manifest.Manifest = .{
    .name = "test",
    .chain_id = 1,
    .start_block = 0,
    .contracts = &.{
        .{ .name = "A", .address = ADDR_A, .events = &.{ContractA} },
        .{ .name = "B", .address = ADDR_B, .events = &.{ContractB} },
    },
};

fn dumpDecodedBlocks(dir: std.fs.Dir, comptime base: []const u8, allocator: std.mem.Allocator) !std.ArrayListUnmanaged(DecodedBlock) {
    var store = FilteredStore.open(allocator, dir, base) catch {
        return std.ArrayListUnmanaged(DecodedBlock){};
    };
    defer store.deinit();

    var out: std.ArrayListUnmanaged(DecodedBlock) = .{};
    errdefer {
        for (out.items) |db_item| allocator.free(db_item.logs);
        out.deinit(allocator);
    }

    const decompress_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(decompress_buf);
    const payload_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(payload_buf);
    const log_scratch = try allocator.alloc(RawLog, types.MAX_LOGS_PER_BLOCK);
    defer allocator.free(log_scratch);

    var i: u64 = 0;
    while (i < store.count()) : (i += 1) {
        const entry = try store.readEntry(i);
        const payload = try store.readPayload(i, payload_buf);
        const decoded = try log_serial.decompressEntry(payload, decompress_buf);
        const n = log_serial.deserializeLogs(decoded, log_scratch);
        const owned = try allocator.alloc(LogSummary, n);
        for (log_scratch[0..n], 0..) |*l, j| {
            owned[j] = .{ .address = l.address, .topic0 = l.topics[0] };
        }
        try out.append(allocator, .{ .block_number = entry.block_number, .logs = owned });
    }
    return out;
}

const LogSummary = struct {
    address: [20]u8,
    topic0: [32]u8,
};

const DecodedBlock = struct {
    block_number: u64,
    logs: []LogSummary,
};

fn freeDecoded(decoded: *std.ArrayListUnmanaged(DecodedBlock), allocator: std.mem.Allocator) void {
    for (decoded.items) |db| allocator.free(db.logs);
    decoded.deinit(allocator);
}

test "build: filters multi-contract flat store, primary contains exactly the matches" {
    const allocator = testing.allocator;

    // Plant 1000 blocks. Even blocks: contract A+B logs (matching). Odd
    // blocks: contract C only (non-matching).
    const N: u64 = 1000;
    const a_topic = topicOf(ContractA);
    const b_topic = topicOf(ContractB);
    const other_topic = topicOf(Other);

    var blocks_list: std.ArrayListUnmanaged(TestBlock) = .{};
    defer blocks_list.deinit(allocator);
    var log_arena = std.heap.ArenaAllocator.init(allocator);
    defer log_arena.deinit();
    const arena = log_arena.allocator();

    var matching_count: u64 = 0;
    var matching_log_total: u64 = 0;

    for (0..N) |i| {
        const bn: u64 = 100 + i;
        const logs = if (bn % 2 == 0) blk: {
            const buf = try arena.alloc(TestLog, 2);
            buf[0] = .{ .address = ADDR_A, .topic0 = a_topic };
            buf[1] = .{ .address = ADDR_B, .topic0 = b_topic, .log_index = 1 };
            matching_count += 1;
            matching_log_total += 2;
            break :blk @as([]const TestLog, buf);
        } else blk: {
            const buf = try arena.alloc(TestLog, 1);
            buf[0] = .{ .address = ADDR_C, .topic0 = other_topic };
            break :blk @as([]const TestLog, buf);
        };
        try blocks_list.append(allocator, .{ .block_number = bn, .logs = logs });
    }

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try flat_reader.writeTestStore(src_tmp.dir, blocks_list.items, allocator);

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    const result = try build(&reader, SmallManifest, dst_tmp.dir, allocator);
    try testing.expectEqual(matching_count, result.blocks_matched);
    try testing.expectEqual(matching_log_total, result.total_logs);
    try testing.expectEqual(N, result.blocks_scanned);

    var decoded = try dumpDecodedBlocks(dst_tmp.dir, BASE_PRIMARY, allocator);
    defer freeDecoded(&decoded, allocator);

    try testing.expectEqual(matching_count, @as(u64, decoded.items.len));
    for (decoded.items) |entry| {
        try testing.expect(entry.block_number % 2 == 0);
        try testing.expectEqual(@as(usize, 2), entry.logs.len);
        try testing.expectEqualSlices(u8, &ADDR_A, &entry.logs[0].address);
        try testing.expectEqualSlices(u8, &a_topic, &entry.logs[0].topic0);
        try testing.expectEqualSlices(u8, &ADDR_B, &entry.logs[1].address);
        try testing.expectEqualSlices(u8, &b_topic, &entry.logs[1].topic0);
    }
}

test "build: rebuild produces decoded-identical output" {
    const allocator = testing.allocator;
    const a_topic = topicOf(ContractA);

    var blocks_list: std.ArrayListUnmanaged(TestBlock) = .{};
    defer blocks_list.deinit(allocator);
    var log_arena = std.heap.ArenaAllocator.init(allocator);
    defer log_arena.deinit();
    const arena = log_arena.allocator();

    for (0..200) |i| {
        const bn: u64 = 1000 + i;
        const buf = try arena.alloc(TestLog, 1);
        buf[0] = .{ .address = ADDR_A, .topic0 = a_topic };
        try blocks_list.append(allocator, .{ .block_number = bn, .logs = buf });
    }

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try flat_reader.writeTestStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    const RebuildManifest: sdk_manifest.Manifest = .{
        .name = "rebuild",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "A", .address = ADDR_A, .events = &.{ContractA} }},
    };

    var first = try buildIntoNewTmp(&reader, RebuildManifest, allocator);
    defer freeDecoded(&first.decoded, allocator);
    defer first.tmp.cleanup();

    var second = try buildIntoNewTmp(&reader, RebuildManifest, allocator);
    defer freeDecoded(&second.decoded, allocator);
    defer second.tmp.cleanup();

    try testing.expectEqual(first.decoded.items.len, second.decoded.items.len);
    for (first.decoded.items, second.decoded.items) |a, b| {
        try testing.expectEqual(a.block_number, b.block_number);
        try testing.expectEqual(a.logs.len, b.logs.len);
        for (a.logs, b.logs) |la, lb| {
            try testing.expectEqualSlices(u8, &la.address, &lb.address);
            try testing.expectEqualSlices(u8, &la.topic0, &lb.topic0);
        }
    }
}

const RebuildOutput = struct {
    tmp: std.testing.TmpDir,
    decoded: std.ArrayListUnmanaged(DecodedBlock),
};

fn buildIntoNewTmp(
    reader: *const FlatStoreReader,
    comptime m: sdk_manifest.Manifest,
    allocator: std.mem.Allocator,
) !RebuildOutput {
    const tmp = testing.tmpDir(.{});
    _ = try build(reader, m, tmp.dir, allocator);
    const decoded = try dumpDecodedBlocks(tmp.dir, BASE_PRIMARY, allocator);
    return .{ .tmp = tmp, .decoded = decoded };
}

test "build + appendChildren: primary holds creations, children holds child events, no duplication" {
    const allocator = testing.allocator;

    const FactoryAddr: [20]u8 = [_]u8{0xF0} ** 20;
    const ChildAddr1: [20]u8 = [_]u8{0xC1} ** 20;
    const ChildAddr2: [20]u8 = [_]u8{0xC2} ** 20;
    const ChildAddr3: [20]u8 = [_]u8{0xC3} ** 20;

    const Create = struct {
        pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair)";
    };
    const Sync = struct {
        pub const signature = "Sync(uint112,uint112)";
    };

    const FactoryManifest: sdk_manifest.Manifest = .{
        .name = "factory",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FactoryAddr,
            .create_event = Create,
            .spawn_param = "pair",
            .child_events = &.{Sync},
        }},
    };

    const create_topic = topicOf(Create);
    const sync_topic = topicOf(Sync);

    var blocks_list: std.ArrayListUnmanaged(TestBlock) = .{};
    defer blocks_list.deinit(allocator);
    var log_arena = std.heap.ArenaAllocator.init(allocator);
    defer log_arena.deinit();
    const arena = log_arena.allocator();

    // Block 100: factory emits a creation event.
    const create_logs = try arena.alloc(TestLog, 1);
    create_logs[0] = .{ .address = FactoryAddr, .topic0 = create_topic };
    try blocks_list.append(allocator, .{ .block_number = 100, .logs = create_logs });

    // Blocks 101..103: child contracts emit Sync.
    const sync1_logs = try arena.alloc(TestLog, 1);
    sync1_logs[0] = .{ .address = ChildAddr1, .topic0 = sync_topic };
    try blocks_list.append(allocator, .{ .block_number = 101, .logs = sync1_logs });
    const sync2_logs = try arena.alloc(TestLog, 1);
    sync2_logs[0] = .{ .address = ChildAddr2, .topic0 = sync_topic };
    try blocks_list.append(allocator, .{ .block_number = 102, .logs = sync2_logs });
    const sync3_logs = try arena.alloc(TestLog, 1);
    sync3_logs[0] = .{ .address = ChildAddr3, .topic0 = sync_topic };
    try blocks_list.append(allocator, .{ .block_number = 103, .logs = sync3_logs });

    // Block 104: unrelated address with unrelated topic. Must NOT appear in either pair.
    const noise_topic = topicOf(Other);
    const noise_logs = try arena.alloc(TestLog, 1);
    noise_logs[0] = .{ .address = ADDR_C, .topic0 = noise_topic };
    try blocks_list.append(allocator, .{ .block_number = 104, .logs = noise_logs });

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try flat_reader.writeTestStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    // Phase 1: build primary. Only the factory creation event qualifies, since
    // child addresses are not yet known.
    const primary = try build(&reader, FactoryManifest, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 1), primary.blocks_matched);
    try testing.expectEqual(@as(u64, 1), primary.total_logs);

    // Phase 3: append children for the addresses the pre-pass discovered.
    const discovered = [_][20]u8{ ChildAddr1, ChildAddr2, ChildAddr3 };
    const children = try appendChildren(&reader, FactoryManifest, &discovered, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 3), children.blocks_matched);
    try testing.expectEqual(@as(u64, 3), children.total_logs);

    var primary_blocks = try dumpDecodedBlocks(dst_tmp.dir, BASE_PRIMARY, allocator);
    defer freeDecoded(&primary_blocks, allocator);
    var child_blocks = try dumpDecodedBlocks(dst_tmp.dir, BASE_CHILDREN, allocator);
    defer freeDecoded(&child_blocks, allocator);

    try testing.expectEqual(@as(usize, 1), primary_blocks.items.len);
    try testing.expectEqual(@as(u64, 100), primary_blocks.items[0].block_number);
    try testing.expectEqualSlices(u8, &FactoryAddr, &primary_blocks.items[0].logs[0].address);
    try testing.expectEqualSlices(u8, &create_topic, &primary_blocks.items[0].logs[0].topic0);

    try testing.expectEqual(@as(usize, 3), child_blocks.items.len);
    try testing.expectEqual(@as(u64, 101), child_blocks.items[0].block_number);
    try testing.expectEqualSlices(u8, &ChildAddr1, &child_blocks.items[0].logs[0].address);
    try testing.expectEqualSlices(u8, &sync_topic, &child_blocks.items[0].logs[0].topic0);
    try testing.expectEqual(@as(u64, 102), child_blocks.items[1].block_number);
    try testing.expectEqualSlices(u8, &ChildAddr2, &child_blocks.items[1].logs[0].address);
    try testing.expectEqual(@as(u64, 103), child_blocks.items[2].block_number);
    try testing.expectEqualSlices(u8, &ChildAddr3, &child_blocks.items[2].logs[0].address);
}

test "appendChildren: returns zero-result for empty discovered set" {
    const allocator = testing.allocator;

    const FactoryAddr: [20]u8 = [_]u8{0xF0} ** 20;
    const Create = struct {
        pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair)";
    };
    const Sync = struct {
        pub const signature = "Sync(uint112,uint112)";
    };
    const FactoryManifest: sdk_manifest.Manifest = .{
        .name = "factory",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FactoryAddr,
            .create_event = Create,
            .spawn_param = "pair",
            .child_events = &.{Sync},
        }},
    };

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    var blocks: [1]TestBlock = .{.{ .block_number = 100, .logs = &.{} }};
    blocks[0].logs = &[_]TestLog{.{ .address = FactoryAddr, .topic0 = topicOf(Create) }};
    try flat_reader.writeTestStore(src_tmp.dir, &blocks, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    _ = try build(&reader, FactoryManifest, dst_tmp.dir, allocator);
    const result = try appendChildren(&reader, FactoryManifest, &.{}, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 0), result.blocks_scanned);
    try testing.expectEqual(@as(u64, 0), result.blocks_matched);
}

test "build: end_block clamps the scan range to a fixed window" {
    const allocator = testing.allocator;

    // Plant 50 contiguous blocks, each with a matching ContractA log. With
    // end_block = 119 (first block 100), the build matches exactly 20 blocks
    // (100..=119) and ignores 120..=149.
    const a_topic = topicOf(ContractA);
    var blocks_list: std.ArrayListUnmanaged(TestBlock) = .{};
    defer blocks_list.deinit(allocator);
    var log_arena = std.heap.ArenaAllocator.init(allocator);
    defer log_arena.deinit();
    const arena = log_arena.allocator();
    for (0..50) |i| {
        const buf = try arena.alloc(TestLog, 1);
        buf[0] = .{ .address = ADDR_A, .topic0 = a_topic };
        try blocks_list.append(allocator, .{ .block_number = 100 + i, .logs = buf });
    }

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try flat_reader.writeTestStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    const ClampedManifest: sdk_manifest.Manifest = .{
        .name = "clamped",
        .chain_id = 1,
        .start_block = 0,
        .end_block = 119,
        .contracts = &.{.{ .name = "A", .address = ADDR_A, .events = &.{ContractA} }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    const result = try build(&reader, ClampedManifest, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 20), result.blocks_matched);
    try testing.expectEqual(@as(u64, 20), result.total_logs);

    // On-disk contents: exactly blocks 100..=119, none past 119.
    var decoded = try dumpDecodedBlocks(dst_tmp.dir, BASE_PRIMARY, allocator);
    defer freeDecoded(&decoded, allocator);
    try testing.expectEqual(@as(usize, 20), decoded.items.len);
    try testing.expectEqual(@as(u64, 100), decoded.items[0].block_number);
    try testing.expectEqual(@as(u64, 119), decoded.items[19].block_number);
}

test "appendBlocks extends a primary filter env over the new range" {
    const allocator = testing.allocator;

    // 10 contiguous blocks, every one matches ContractA.
    const a_topic = topicOf(ContractA);
    var blocks_list: std.ArrayListUnmanaged(TestBlock) = .{};
    defer blocks_list.deinit(allocator);
    var log_arena = std.heap.ArenaAllocator.init(allocator);
    defer log_arena.deinit();
    const arena = log_arena.allocator();
    for (0..10) |i| {
        const buf = try arena.alloc(TestLog, 1);
        buf[0] = .{ .address = ADDR_A, .topic0 = a_topic };
        try blocks_list.append(allocator, .{ .block_number = 100 + i, .logs = buf });
    }

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try flat_reader.writeTestStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    const M: sdk_manifest.Manifest = .{
        .name = "ext",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "A", .address = ADDR_A, .events = &.{ContractA} }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    // First pass: cover blocks 100..=104.
    const first = try appendBlocks(&reader, M, 100, 104, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 5), first.blocks_matched);

    // Second pass extends the pair over 105..=109, same files, appended.
    const second = try appendBlocks(&reader, M, 105, 109, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 5), second.blocks_matched);

    var decoded = try dumpDecodedBlocks(dst_tmp.dir, BASE_PRIMARY, allocator);
    defer freeDecoded(&decoded, allocator);
    try testing.expectEqual(@as(usize, 10), decoded.items.len);
    try testing.expectEqual(@as(u64, 100), decoded.items[0].block_number);
    try testing.expectEqual(@as(u64, 109), decoded.items[9].block_number);
}

test "appendChildrenBlocks extends the children pair over a sub-range" {
    const allocator = testing.allocator;

    const ChildAddr: [20]u8 = [_]u8{0xC1} ** 20;
    const FactoryAddr: [20]u8 = [_]u8{0xF0} ** 20;
    const Create = struct {
        pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair)";
    };
    const Sync = struct {
        pub const signature = "Sync(uint112,uint112)";
    };
    const M: sdk_manifest.Manifest = .{
        .name = "gapchild",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FactoryAddr,
            .create_event = Create,
            .spawn_param = "pair",
            .child_events = &.{Sync},
        }},
    };

    const sync_topic = topicOf(Sync);
    var blocks_list: std.ArrayListUnmanaged(TestBlock) = .{};
    defer blocks_list.deinit(allocator);
    var log_arena = std.heap.ArenaAllocator.init(allocator);
    defer log_arena.deinit();
    const arena = log_arena.allocator();
    // The same child emits Sync in 10 contiguous blocks.
    for (0..10) |i| {
        const buf = try arena.alloc(TestLog, 1);
        buf[0] = .{ .address = ChildAddr, .topic0 = sync_topic };
        try blocks_list.append(allocator, .{ .block_number = 100 + i, .logs = buf });
    }

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try flat_reader.writeTestStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    const children = [_][20]u8{ChildAddr};
    // Backfill covers 100..=104, then the follow gap extends 105..=109.
    const first = try appendChildrenBlocks(&reader, M, &children, 100, 104, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 5), first.blocks_matched);
    const second = try appendChildrenBlocks(&reader, M, &children, 105, 109, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 5), second.blocks_matched);

    var decoded = try dumpDecodedBlocks(dst_tmp.dir, BASE_CHILDREN, allocator);
    defer freeDecoded(&decoded, allocator);
    try testing.expectEqual(@as(usize, 10), decoded.items.len);
    try testing.expectEqual(@as(u64, 100), decoded.items[0].block_number);
    try testing.expectEqual(@as(u64, 109), decoded.items[9].block_number);
}

test "processBlockEntry: corrupt entry counts as dropped_block (fail-loud safety net)" {
    const allocator = testing.allocator;

    var results: std.ArrayListUnmanaged(FilteredBlock) = .{};
    defer results.deinit(allocator);

    var args: FilterWorkerArgs = .{
        .reader = undefined,
        .matching_blocks = &.{},
        .filter = .{ .match_addrs = &.{}, .match_topics = &.{}, .exclude_addrs = &.{} },
        .results = &results,
        .allocator = allocator,
    };

    // lz4_len prefix claims more bytes than the entry holds, so decompressEntry
    // returns error.InvalidEntry. A regression re-introducing a silent drop
    // would leave dropped_blocks == 0 here.
    const corrupt_entry = [_]u8{ 0xFF, 0xFF, 0xFF, 0x7F, 0x42 };
    var decompress_buf: [256]u8 = undefined;
    var serialize_buf: [256]u8 = undefined;
    var compress_buf: [256]u8 = undefined;

    processBlockEntry(&corrupt_entry, 100, &args, &decompress_buf, &serialize_buf, &compress_buf);
    try testing.expectEqual(@as(u64, 1), args.dropped_blocks);
    try testing.expectEqual(@as(usize, 0), results.items.len);
}
