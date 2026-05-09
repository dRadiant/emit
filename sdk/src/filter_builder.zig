/// Filtered-index builder. Reads the engine's flat log store via `core`,
/// keeps logs that match the manifest, and writes them into the
/// `filtered_index.mdbx` env's BLOCKS_PRIMARY (and optional BLOCKS_CHILDREN)
/// DBI keyed by block number (u64 BE). Per ADR-002.
///
/// Two entry points:
///   - `build`: phase 1, writes BLOCKS_PRIMARY for static + factory addresses.
///   - `appendChildren`: phase 3, writes BLOCKS_CHILDREN for addresses
///     discovered by the scanner's factory pre-pass. No-op when the
///     discovered set is empty.
///
/// Per-log keep rule (uniform across both phases via the `Filter` struct):
///   keep = (address ∈ filter.match_addrs)
///       AND (topic0 ∈ filter.match_topics)
///       AND (address ∉ filter.exclude_addrs)
/// Phase 1 sets exclude_addrs empty; phase 3 sets it to static∪factory so a
/// static contract that's also a factory child does not appear in both DBIs.
const std = @import("std");

const builtin = @import("builtin");
const core = @import("core");
const lmdbx = @import("lmdbx");
const lz4 = @import("lz4");

const sdk_manifest = @import("manifest.zig");

const RawLog = core.RawLog;
const FlatStoreReader = core.FlatStoreReader;
const log_serial = core.log_serial;
const block_filter = core.block_filter;
const parallel = core.parallel;
const io_pipeline = core.io_pipeline;
const types = core.types;

pub const KEY_SIZE = 8;
pub const WORKER_QUEUE_DEPTH = 16;
/// Commit cadence during the MDBX write phase. Smaller than the entity-store
/// 100K because each filtered-index row is a whole block (one cursor step in
/// Stage 2), so the absolute work between commits stays in the same range.
pub const COMMIT_BLOCKS = 10_000;
/// Min matching blocks before we spawn worker threads. Below this, the
/// thread spin-up cost is larger than the parallel speedup.
pub const PARALLEL_THRESHOLD = 1_000;

pub const DBI_PRIMARY = "blocks_primary";
pub const DBI_CHILDREN = "blocks_children";

pub const BuildResult = struct {
    blocks_scanned: u64 = 0,
    blocks_matched: u64 = 0,
    total_logs: u64 = 0,
    elapsed_ns: u64 = 0,
};

pub fn blockKey(block_number: u64) [KEY_SIZE]u8 {
    var buf: [KEY_SIZE]u8 = undefined;
    std.mem.writeInt(u64, &buf, block_number, .big);
    return buf;
}

pub fn blockFromKey(key: []const u8) u64 {
    return std.mem.readInt(u64, key[0..KEY_SIZE], .big);
}

/// Per-log keep predicate. `match_addrs` and `match_topics` are positive
/// match sets; `exclude_addrs` is a negative filter applied after positives
/// pass. Used by both `build` (phase 1) and `appendChildren` (phase 3).
const Filter = struct {
    match_addrs: []const [20]u8,
    match_topics: []const [32]u8,
    exclude_addrs: []const [20]u8,
};

/// Phase 1: build BLOCKS_PRIMARY at `dest_path` from the manifest's static
/// and factory addresses. Caller owns `reader`. The destination directory
/// must exist; if it already contains data the build will fail at
/// `MDBX_APPEND` time.
pub fn build(
    reader: *const FlatStoreReader,
    comptime m: sdk_manifest.Manifest,
    dest_path: [*:0]const u8,
    allocator: std.mem.Allocator,
) !BuildResult {
    const known_addresses = comptime collectKnownAddresses(m);
    const all_topics = comptime collectAllTopics(m);
    return runPhase(
        reader,
        known_addresses,
        m.start_block,
        m.end_block orelse std.math.maxInt(u64),
        .{
            .match_addrs = known_addresses,
            .match_topics = all_topics,
            .exclude_addrs = &.{},
        },
        dest_path,
        DBI_PRIMARY,
        allocator,
    );
}

/// Phase 3: walk the engine's flat store filtered by the
/// scanner-discovered child addresses, write matching child-event logs to
/// `BLOCKS_CHILDREN` in the existing env at `dest_path`. Returns a zero
/// BuildResult immediately when `child_addresses` is empty. The per-log
/// filter excludes addresses already in `static∪factory` so a static
/// contract that's also a factory child does not produce duplicate entries
/// across DBIs.
pub fn appendChildren(
    reader: *const FlatStoreReader,
    comptime m: sdk_manifest.Manifest,
    child_addresses: []const [20]u8,
    dest_path: [*:0]const u8,
    allocator: std.mem.Allocator,
) !BuildResult {
    if (child_addresses.len == 0) return .{};

    const known_addresses = comptime collectKnownAddresses(m);
    const child_topics = comptime collectChildTopics(m);
    if (child_topics.len == 0) return .{};

    return runPhase(
        reader,
        child_addresses,
        m.start_block,
        m.end_block orelse std.math.maxInt(u64),
        .{
            .match_addrs = child_addresses,
            .match_topics = child_topics,
            .exclude_addrs = known_addresses,
        },
        dest_path,
        DBI_CHILDREN,
        allocator,
    );
}

/// Shared phase runner. `bloom_addresses` is what we feed the bloom scan
/// (block-level prefilter); `filter` is the per-log keep predicate
/// (post-decompression precision filter). `dbi_name` is the DBI to write
/// into; the env at `dest_path` is opened with `max_dbs = 2`.
fn runPhase(
    reader: *const FlatStoreReader,
    bloom_addresses: []const [20]u8,
    start_block: u64,
    end_block: u64,
    filter: Filter,
    dest_path: [*:0]const u8,
    dbi_name: [*:0]const u8,
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
        allocator,
    );

    if (matching.items.len == 0) {
        result.elapsed_ns = timer.read();
        return result;
    }

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

    const env = try lmdbx.Environment.init(dest_path, .{ .max_dbs = 2 });
    defer env.deinit() catch {};

    var txn = try env.transaction(.{});
    var dbi = (try lmdbx.Database.open(txn, dbi_name, .{ .create = true })).dbi;
    var writes_since_commit: u32 = 0;

    for (0..num_workers) |i| {
        for (worker_results[i].items) |fb| {
            const key = blockKey(fb.block_number);
            const db = lmdbx.Database{ .txn = txn, .dbi = dbi };
            try db.set(&key, fb.entry, .Append);
            result.blocks_matched += 1;
            result.total_logs += fb.log_count;
            writes_since_commit += 1;
            if (writes_since_commit >= COMMIT_BLOCKS) {
                try txn.commit();
                txn = try env.transaction(.{});
                dbi = (try lmdbx.Database.open(txn, dbi_name, .{ .create = true })).dbi;
                writes_since_commit = 0;
            }
        }
    }
    try txn.commit();

    result.elapsed_ns = timer.read();
    return result;
}

// ── Manifest projections ─────────────────────────────────────────────────

fn collectKnownAddresses(comptime m: sdk_manifest.Manifest) []const [20]u8 {
    comptime {
        var out: []const [20]u8 = &.{};
        for (m.contracts) |c| out = out ++ &[_][20]u8{c.address};
        for (m.factories) |f| out = out ++ &[_][20]u8{f.address};
        return out;
    }
}

fn collectAllTopics(comptime m: sdk_manifest.Manifest) []const [32]u8 {
    comptime {
        var out: []const [32]u8 = &.{};
        for (m.contracts) |c| {
            for (c.events) |E| {
                const t = sdk_manifest.eventTopic0(E);
                if (containsTopic(out, &t)) continue;
                out = out ++ &[_][32]u8{t};
            }
        }
        for (m.factories) |f| {
            const t = sdk_manifest.eventTopic0(f.create_event);
            if (!containsTopic(out, &t)) out = out ++ &[_][32]u8{t};
        }
        return out;
    }
}

fn collectChildTopics(comptime m: sdk_manifest.Manifest) []const [32]u8 {
    comptime {
        var out: []const [32]u8 = &.{};
        for (m.factories) |f| {
            for (f.child_events) |E| {
                const t = sdk_manifest.eventTopic0(E);
                if (!containsTopic(out, &t)) out = out ++ &[_][32]u8{t};
            }
        }
        return out;
    }
}

inline fn containsTopic(haystack: []const [32]u8, needle: *const [32]u8) bool {
    for (haystack) |t| if (std.mem.eql(u8, &t, needle)) return true;
    return false;
}

inline fn containsAddress(haystack: []const [20]u8, needle: *const [20]u8) bool {
    for (haystack) |a| if (std.mem.eql(u8, &a, needle)) return true;
    return false;
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
};

fn filterWorker(args: *FilterWorkerArgs) void {
    // Stack scratch is safe under the buffer rule in `core.parallel`:
    // `parallel.run` always spawns workers at `WORKER_STACK_SIZE`.
    var decompress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var log_buf: [types.MAX_LOGS_PER_BLOCK]RawLog = undefined;
    var keep_buf: [types.MAX_LOGS_PER_BLOCK]RawLog = undefined;
    var serialize_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var compress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;

    const reader = args.reader;

    if (comptime io_pipeline.supported) {
        const Pipeline = io_pipeline.ReadPipeline(WORKER_QUEUE_DEPTH);
        const pipeline = Pipeline.init(args.allocator, reader.blocks_file.handle) catch return;
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
                    continue;
                };
                pipeline.submit(slot, args.matching_blocks[submitted], loc.offset, loc.length) catch {
                    pipeline.releaseSlot(slot);
                    break;
                };
                submitted += 1;
            }
            _ = pipeline.flush() catch {};

            var done: [WORKER_QUEUE_DEPTH]*io_pipeline.Completion = undefined;
            const n = pipeline.waitAtLeastOne(&done) catch break;
            for (done[0..n]) |c| {
                const entry_data = pipeline.getBuffer(c);
                if (entry_data.len > 0) {
                    processBlockEntry(entry_data, c.block_number, args, &decompress_buf, &log_buf, &keep_buf, &serialize_buf, &compress_buf);
                }
                pipeline.releaseSlot(c.buf_slot);
                completed += 1;
            }
        }
        // io_uring completes in NVMe order, not submission order. Sort so the
        // writer can iterate worker outputs in ascending block order for
        // MDBX_APPEND.
        std.mem.sort(FilteredBlock, args.results.items, {}, blockNumberLessThan);
        return;
    }

    // pread fallback (non-Linux). Reads in matching_blocks order, no sort
    // needed but we sort anyway for path-uniform output.
    var read_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    for (args.matching_blocks) |bn| {
        const entry_data = reader.readBlock(bn, &read_buf) catch continue;
        processBlockEntry(entry_data, bn, args, &decompress_buf, &log_buf, &keep_buf, &serialize_buf, &compress_buf);
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
    log_buf: []RawLog,
    keep_buf: []RawLog,
    serialize_buf: []u8,
    compress_buf: []u8,
) void {
    const decompressed = log_serial.decompressEntry(entry_data, decompress_buf) catch return;
    const log_count = log_serial.deserializeLogs(decompressed, log_buf);

    var keep_count: usize = 0;
    for (log_buf[0..log_count]) |*log| {
        if (!keepLog(log, args.filter)) continue;
        log.block_number = block_number;
        keep_buf[keep_count] = log.*;
        keep_count += 1;
    }
    if (keep_count == 0) return;

    const serialized_len = log_serial.serializeLogs(keep_buf[0..keep_count], serialize_buf);
    const entry_len = log_serial.compressEntry(serialize_buf[0..serialized_len], compress_buf) catch return;

    const owned = args.allocator.alloc(u8, entry_len) catch return;
    @memcpy(owned, compress_buf[0..entry_len]);

    args.results.append(args.allocator, .{
        .block_number = block_number,
        .entry = owned,
        .log_count = @intCast(keep_count),
    }) catch {};
}

inline fn keepLog(log: *const RawLog, filter: Filter) bool {
    if (log.topic_count == 0) return false;
    if (!containsAddress(filter.match_addrs, &log.address)) return false;
    if (!containsTopic(filter.match_topics, &log.topics[0])) return false;
    if (containsAddress(filter.exclude_addrs, &log.address)) return false;
    return true;
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;
const bloom = core.bloom;
const flat_reader = core.flat_reader;

const TestLog = struct {
    tx_index: u16 = 0,
    log_index: u16 = 0,
    address: [20]u8,
    topic0: [32]u8,
    data: []const u8 = &.{},
};

const TestBlock = struct {
    block_number: u64,
    logs: []const TestLog,
};

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

/// Write a synthetic flat store (blocks.dat + blocks.idx + blooms.bin) into
/// `dir`. Returns nothing — caller opens via `FlatStoreReader.open(dir_path)`.
fn writeFlatStore(dir: std.fs.Dir, blocks: []const TestBlock, allocator: std.mem.Allocator) !void {
    var blocks_file = try dir.createFile("blocks.dat", .{});
    defer blocks_file.close();
    var idx_file = try dir.createFile("blocks.idx", .{});
    defer idx_file.close();
    var blooms_file = try dir.createFile("blooms.bin", .{});
    defer blooms_file.close();

    var idx_hdr: [flat_reader.INDEX_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, idx_hdr[0..8], blocks[0].block_number, .little);
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
    for (blocks) |blk| {
        var raw_logs: [types.MAX_LOGS_PER_BLOCK]RawLog = undefined;
        for (blk.logs, 0..) |tl, i| {
            raw_logs[i] = .{
                .block_number = blk.block_number,
                .tx_index = tl.tx_index,
                .log_index = tl.log_index,
                .address = tl.address,
                .topic_count = 1,
                .topics = .{ tl.topic0, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
                .data = tl.data,
                .tx_hash = [_]u8{0xFE} ** 32,
            };
        }
        const written = log_serial.serializeLogs(raw_logs[0..blk.logs.len], serialize_buf);
        const entry_len = try log_serial.compressEntry(serialize_buf[0..written], compress_buf);
        try blocks_file.writeAll(compress_buf[0..entry_len]);

        var idx_entry: [flat_reader.INDEX_ENTRY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, idx_entry[0..8], offset, .little);
        std.mem.writeInt(u32, idx_entry[8..12], @intCast(entry_len), .little);
        try idx_file.writeAll(&idx_entry);

        const tb = log_serial.buildTopicBloom(raw_logs[0..blk.logs.len]);
        const ab = log_serial.buildAddrBloom(raw_logs[0..blk.logs.len]);
        var bloom_entry: [flat_reader.BLOOM_ENTRY_SIZE]u8 = std.mem.zeroes([flat_reader.BLOOM_ENTRY_SIZE]u8);
        std.mem.writeInt(u64, bloom_entry[0..8], blk.block_number, .big);
        @memcpy(bloom_entry[flat_reader.TOPIC_BLOOM_OFFSET..][0..bloom.BLOOM_SIZE], &tb.bits);
        @memcpy(bloom_entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE], &ab.bits);
        try blooms_file.writeAll(&bloom_entry);

        offset += entry_len;
    }
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

fn dumpDecodedBlocks(env: lmdbx.Environment, dbi_name: [*:0]const u8, allocator: std.mem.Allocator) !std.ArrayListUnmanaged(DecodedBlock) {
    const txn = try env.transaction(.{ .mode = .ReadOnly });
    defer txn.abort() catch {};
    const db = try lmdbx.Database.open(txn, dbi_name, .{});
    var cursor = try db.cursor();
    defer cursor.deinit();

    var out: std.ArrayListUnmanaged(DecodedBlock) = .{};
    errdefer {
        for (out.items) |db_item| allocator.free(db_item.logs);
        out.deinit(allocator);
    }

    var key_opt = cursor.goToFirst() catch return out;
    var decompress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var log_scratch: [types.MAX_LOGS_PER_BLOCK]RawLog = undefined;
    while (key_opt) |k| {
        const bn = blockFromKey(k);
        const v = try cursor.getCurrentValue();
        const decoded = try log_serial.decompressEntry(v, &decompress_buf);
        const n = log_serial.deserializeLogs(decoded, &log_scratch);
        const owned = try allocator.alloc(LogSummary, n);
        for (log_scratch[0..n], 0..) |*l, i| {
            owned[i] = .{ .address = l.address, .topic0 = l.topics[0] };
        }
        try out.append(allocator, .{ .block_number = bn, .logs = owned });
        key_opt = try cursor.goToNext();
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

test "build: filters multi-contract flat store, MDBX contains exactly the matches" {
    const allocator = testing.allocator;

    // Plant 1000 blocks. Even-numbered → contract A and B logs (matching).
    // Odd-numbered → contract C only (non-matching).
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
    try writeFlatStore(src_tmp.dir, blocks_list.items, allocator);

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.close();

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();
    var dst_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dst_path = try dst_tmp.dir.realpath(".", &dst_path_buf);
    var dst_path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(dst_path_z[0..dst_path.len], dst_path);
    dst_path_z[dst_path.len] = 0;

    const result = try build(&reader, SmallManifest, @ptrCast(&dst_path_z), allocator);
    try testing.expectEqual(matching_count, result.blocks_matched);
    try testing.expectEqual(matching_log_total, result.total_logs);
    try testing.expectEqual(N, result.blocks_scanned);

    const env = try lmdbx.Environment.init(@ptrCast(&dst_path_z), .{ .max_dbs = 2 });
    defer env.deinit() catch {};
    var decoded = try dumpDecodedBlocks(env, DBI_PRIMARY, allocator);
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
    try writeFlatStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.close();

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
    var tmp = testing.tmpDir(.{});
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    _ = try build(reader, m, @ptrCast(&path_z), allocator);

    const env = try lmdbx.Environment.init(@ptrCast(&path_z), .{ .max_dbs = 2 });
    defer env.deinit() catch {};
    const decoded = try dumpDecodedBlocks(env, DBI_PRIMARY, allocator);
    return .{ .tmp = tmp, .decoded = decoded };
}

test "build + appendChildren: primary holds creations, children holds child events, no duplication" {
    const allocator = testing.allocator;

    const FactoryAddr: [20]u8 = [_]u8{0xF0} ** 20;
    const ChildAddr1: [20]u8 = [_]u8{0xC1} ** 20;
    const ChildAddr2: [20]u8 = [_]u8{0xC2} ** 20;
    const ChildAddr3: [20]u8 = [_]u8{0xC3} ** 20;

    const Create = struct {
        pub const signature = "PairCreated(address,address,address)";
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
            .address_param = .{ .data = 0 },
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

    // Block 104: unrelated address with unrelated topic — must NOT appear in either DBI.
    const noise_topic = topicOf(Other);
    const noise_logs = try arena.alloc(TestLog, 1);
    noise_logs[0] = .{ .address = ADDR_C, .topic0 = noise_topic };
    try blocks_list.append(allocator, .{ .block_number = 104, .logs = noise_logs });

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try writeFlatStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.close();

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();
    var dst_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dst_path = try dst_tmp.dir.realpath(".", &dst_path_buf);
    var dst_path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(dst_path_z[0..dst_path.len], dst_path);
    dst_path_z[dst_path.len] = 0;

    // Phase 1: build primary. Only the factory creation event qualifies
    // because child addresses are not yet known.
    const primary = try build(&reader, FactoryManifest, @ptrCast(&dst_path_z), allocator);
    try testing.expectEqual(@as(u64, 1), primary.blocks_matched);
    try testing.expectEqual(@as(u64, 1), primary.total_logs);

    // Phase 3: append children for the addresses the (would-be) pre-pass
    // discovered. Stand-in for `scanner.scanCreations` until Group 6 lands.
    const discovered = [_][20]u8{ ChildAddr1, ChildAddr2, ChildAddr3 };
    const children = try appendChildren(&reader, FactoryManifest, &discovered, @ptrCast(&dst_path_z), allocator);
    try testing.expectEqual(@as(u64, 3), children.blocks_matched);
    try testing.expectEqual(@as(u64, 3), children.total_logs);

    const env = try lmdbx.Environment.init(@ptrCast(&dst_path_z), .{ .max_dbs = 2 });
    defer env.deinit() catch {};

    var primary_blocks = try dumpDecodedBlocks(env, DBI_PRIMARY, allocator);
    defer freeDecoded(&primary_blocks, allocator);
    var child_blocks = try dumpDecodedBlocks(env, DBI_CHILDREN, allocator);
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
        pub const signature = "PairCreated(address,address,address)";
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
            .address_param = .{ .data = 0 },
            .child_events = &.{Sync},
        }},
    };

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    var blocks: [1]TestBlock = .{.{ .block_number = 100, .logs = &.{} }};
    blocks[0].logs = &[_]TestLog{.{ .address = FactoryAddr, .topic0 = topicOf(Create) }};
    try writeFlatStore(src_tmp.dir, &blocks, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.close();

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();
    var dst_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dst_path = try dst_tmp.dir.realpath(".", &dst_path_buf);
    var dst_path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(dst_path_z[0..dst_path.len], dst_path);
    dst_path_z[dst_path.len] = 0;

    _ = try build(&reader, FactoryManifest, @ptrCast(&dst_path_z), allocator);
    const result = try appendChildren(&reader, FactoryManifest, &.{}, @ptrCast(&dst_path_z), allocator);
    try testing.expectEqual(@as(u64, 0), result.blocks_scanned);
    try testing.expectEqual(@as(u64, 0), result.blocks_matched);
}

test "build: end_block clamps the scan range to a fixed window" {
    const allocator = testing.allocator;

    // Plant 50 contiguous blocks; every block has a matching ContractA log.
    // With end_block = 119 (start_block 0, first block 100), the build
    // should match exactly 20 blocks (100..=119) and ignore 120..=149.
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
    try writeFlatStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.close();

    const ClampedManifest: sdk_manifest.Manifest = .{
        .name = "clamped",
        .chain_id = 1,
        .start_block = 0,
        .end_block = 119,
        .contracts = &.{.{ .name = "A", .address = ADDR_A, .events = &.{ContractA} }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();
    var dst_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dst_path = try dst_tmp.dir.realpath(".", &dst_path_buf);
    var dst_path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(dst_path_z[0..dst_path.len], dst_path);
    dst_path_z[dst_path.len] = 0;

    const result = try build(&reader, ClampedManifest, @ptrCast(&dst_path_z), allocator);
    try testing.expectEqual(@as(u64, 20), result.blocks_matched);
    try testing.expectEqual(@as(u64, 20), result.total_logs);

    // Verify the MDBX contents: exactly blocks 100..=119, none past 119.
    const env = try lmdbx.Environment.init(@ptrCast(&dst_path_z), .{ .max_dbs = 2 });
    defer env.deinit() catch {};
    var decoded = try dumpDecodedBlocks(env, DBI_PRIMARY, allocator);
    defer freeDecoded(&decoded, allocator);
    try testing.expectEqual(@as(usize, 20), decoded.items.len);
    try testing.expectEqual(@as(u64, 100), decoded.items[0].block_number);
    try testing.expectEqual(@as(u64, 119), decoded.items[19].block_number);
}

test "blockKey/blockFromKey roundtrip and ordering" {
    const k = blockKey(18_600_002);
    try testing.expectEqual(@as(u64, 18_600_002), blockFromKey(&k));

    const k1 = blockKey(100);
    const k2 = blockKey(200);
    try testing.expect(std.mem.order(u8, &k1, &k2) == .lt);
}
