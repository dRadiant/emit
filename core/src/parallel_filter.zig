//! Parallel block read + precision filter, shared by the sdk filtered-index
//! builder and the engine's TCP backfill server. Bloom-scans a range, then
//! reads and filters matching blocks across a thread pool, each thread driving
//! its own io_uring ring. Survivors are emitted to a caller-supplied sink in
//! ascending block order.
//!
//! Work is processed in chunks: each chunk's blocks are filtered in parallel,
//! emitted, then freed before the next chunk. This bounds memory to one chunk's
//! filtered output and lets the sink stream (a socket flushes per chunk instead
//! of after the whole range). Linux uses io_uring; other platforms fall back to
//! sequential pread per worker.
const std = @import("std");

const types = @import("types.zig");
const flat_reader = @import("flat_reader.zig");
const filter_mod = @import("filter.zig");
const block_filter = @import("block_filter.zig");
const io_pipeline = @import("io_pipeline.zig");
const parallel = @import("parallel.zig");
const log_serial = @import("log_serial.zig");
const log = @import("log.zig");
const txs_mod = @import("txs.zig");

const FlatStoreReader = flat_reader.FlatStoreReader;
const Filter = filter_mod.Filter;

pub const WORKER_QUEUE_DEPTH = 16;
/// Min matching blocks before spawning worker threads. Below this, thread
/// spin-up cost exceeds the parallel speedup.
pub const PARALLEL_THRESHOLD = 1_000;
/// Blocks filtered per chunk. Bounds peak memory to one chunk's filtered
/// entries and the per-chunk emit cadence. Far above the 112 concurrent reads
/// (7 workers x QD 16) so each chunk keeps the pipeline saturated.
pub const CHUNK_BLOCKS = 16_384;

pub const Result = struct {
    blocks_scanned: u64 = 0,
    blocks_matched: u64 = 0,
    total_logs: u64 = 0,
    /// Bloom-matched blocks the pipeline failed to materialize. Any non-zero
    /// value means an incomplete result; the caller decides whether to reject.
    dropped_blocks: u64 = 0,
    elapsed_ns: u64 = 0,
};

/// Bloom-scan `(start_block, end_block]`, then read + precision-filter matching
/// blocks across the worker pool, emitting each survivor to `sink` in ascending
/// block order. `Sink` must expose
/// `fn emit(*Sink, block_number: u64, entry: []const u8, log_count: u32) !void`,
/// where `entry` is the recompressed filtered block (`lz4_len + lz4_data`),
/// valid only for the call. `bloom_addresses` feeds the block-level bloom scan;
/// `filter` is the per-log keep predicate applied after decompression.
///
/// `txs_reader`, when non-null, appends each survivor's tx subtable (the
/// kept logs' TxRecords, `core.txs` serialized form) after the lz4 payload.
/// `decompressEntry` tolerates the trailing bytes, so tx-blind readers parse
/// the entry unchanged. A block whose table is missing or incomplete counts
/// as dropped, fail loud, never a silent null `log.tx`.
pub fn run(
    reader: *const FlatStoreReader,
    bloom_addresses: []const [20]u8,
    filter: Filter,
    txs_reader: ?*const txs_mod.TxsReader,
    start_block: u64,
    end_block: u64,
    comptime Sink: type,
    sink: *Sink,
    allocator: std.mem.Allocator,
) !Result {
    var result = Result{};
    var timer = try std.time.Timer.start();

    // One filter serves every block in the range, so sort the address sets once
    // here and binary-search per log. Below the threshold the linear scan wins,
    // so leave the filter untouched and skip the copy.
    var owned_match: ?[][20]u8 = null;
    var owned_exclude: ?[][20]u8 = null;
    defer if (owned_match) |s| allocator.free(s);
    defer if (owned_exclude) |s| allocator.free(s);
    var filt = filter;
    if (filter.match_addrs.len > filter_mod.ADDR_BINARY_SEARCH_THRESHOLD or
        filter.exclude_addrs.len > filter_mod.ADDR_BINARY_SEARCH_THRESHOLD)
    {
        owned_match = try allocator.dupe([20]u8, filter.match_addrs);
        owned_exclude = try allocator.dupe([20]u8, filter.exclude_addrs);
        filter_mod.sortAddresses(owned_match.?);
        filter_mod.sortAddresses(owned_exclude.?);
        filt = .{
            .match_addrs = owned_match.?,
            .match_topics = filter.match_topics,
            .exclude_addrs = owned_exclude.?,
            .addrs_sorted = true,
        };
    }

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

    // blooms.bin can carry duplicate rows for one block (the importer collapses
    // multi-byte reorg discriminators onto the same u64). The list is sorted, so
    // adjacent dedup suffices. Without it a monotonic sink raises OutOfOrder.
    dedupAdjacent(&matching);

    var off: usize = 0;
    while (off < matching.items.len) {
        const end = @min(off + CHUNK_BLOCKS, matching.items.len);
        try filterChunk(reader, filt, txs_reader, matching.items[off..end], Sink, sink, &result, allocator);
        off = end;
        // Live progress at the default level: a `\r` line that ticks per
        // chunk. The gate is a single int compare under --silent, so the
        // 2M-events/s build is untouched.
        log.info("\r  filtering {d}/{d} matching blocks", .{ off, matching.items.len });
    }
    if (matching.items.len > 0) log.info("\n", .{});

    result.elapsed_ns = timer.read();
    return result;
}

/// Filter one chunk in parallel, then emit its survivors in ascending order and
/// free the chunk's arenas. Worker ranges are contiguous over the sorted chunk
/// and each worker sorts its own output, so a flat walk is globally ascending.
///
/// Per-chunk pipeline + arena teardown is deliberate. Freeing per chunk keeps
/// held memory at one chunk's working set instead of pinning the largest
/// chunk's for the whole run. Ring re-setup is marginal next to the NVMe
/// reads, and reuse would need run-scoped pipelines threaded through the
/// per-chunk workers. Revisit only with a measured win on a real build.
fn filterChunk(
    reader: *const FlatStoreReader,
    filter: Filter,
    txs_reader: ?*const txs_mod.TxsReader,
    chunk: []const u64,
    comptime Sink: type,
    sink: *Sink,
    result: *Result,
    allocator: std.mem.Allocator,
) !void {
    const num_workers = parallel.workerCount(chunk.len, PARALLEL_THRESHOLD);
    const ranges = parallel.chunkRanges(chunk.len, num_workers);

    var worker_results: [parallel.MAX_WORKERS]std.ArrayListUnmanaged(FilteredBlock) = undefined;
    var worker_args: [parallel.MAX_WORKERS]WorkerArgs = undefined;
    var worker_arenas: [parallel.MAX_WORKERS]std.heap.ArenaAllocator = undefined;

    for (0..num_workers) |i| {
        worker_arenas[i] = std.heap.ArenaAllocator.init(allocator);
        worker_results[i] = .{};
        worker_args[i] = .{
            .reader = reader,
            .matching_blocks = chunk[ranges[i][0]..ranges[i][1]],
            .filter = filter,
            .txs_reader = txs_reader,
            .results = &worker_results[i],
            .allocator = worker_arenas[i].allocator(),
        };
    }
    defer for (0..num_workers) |i| worker_arenas[i].deinit();

    try parallel.run(WorkerArgs, worker_args[0..num_workers], num_workers, worker);

    // Surface fatal pipeline errors before emitting. Per-block drops accumulate.
    for (0..num_workers) |i| if (worker_args[i].err) |e| return e;

    for (0..num_workers) |i| {
        for (worker_results[i].items) |fb| {
            try sink.emit(fb.block_number, fb.entry, fb.log_count);
            result.blocks_matched += 1;
            result.total_logs += fb.log_count;
        }
    }
    for (0..num_workers) |i| result.dropped_blocks += worker_args[i].dropped_blocks;
}

/// Collapse runs of equal block numbers in a sorted list, in place.
pub fn dedupAdjacent(list: *std.ArrayListUnmanaged(u64)) void {
    if (list.items.len < 2) return;
    var w: usize = 1;
    for (1..list.items.len) |r| {
        if (list.items[r] == list.items[r - 1]) continue;
        list.items[w] = list.items[r];
        w += 1;
    }
    list.items.len = w;
}

// ── Worker ───────────────────────────────────────────────────────────────

const FilteredBlock = struct {
    block_number: u64,
    entry: []u8, // lz4_len(4) + lz4_data
    log_count: u32,
};

const WorkerArgs = struct {
    reader: *const FlatStoreReader,
    matching_blocks: []const u64,
    filter: Filter,
    txs_reader: ?*const txs_mod.TxsReader = null,
    results: *std.ArrayListUnmanaged(FilteredBlock),
    allocator: std.mem.Allocator,
    /// Tx carry scratch, arena-allocated by the worker when txs_reader is set.
    tx_scratch: ?TxScratch = null,
    /// Per-block recoverable failures (alloc, lz4, etc.), surfaced via Result.
    dropped_blocks: u64 = 0,
    /// First fatal pipeline-level error (init/wait/oversize). Aborts the run.
    err: ?anyerror = null,
};

/// Per-worker tx carry buffers, sized to the structural u16 ceiling
/// (`txs.MAX_RECORDS`) so no legitimate block can overflow. ~20 MB per
/// worker, paid only when the tx carry is on.
const TxScratch = struct {
    mask: []u16,
    payload: []u8,
    decomp: []u8,
    records: []txs_mod.TxRecord,
    selected: []txs_mod.TxRecord,

    fn init(a: std.mem.Allocator) !TxScratch {
        // Compressed entries of incompressible tables exceed the raw size by
        // the LZ4 bound, mirror the writer's compress_buf slack.
        const payload_max = txs_mod.MAX_TABLE_SIZE + txs_mod.MAX_TABLE_SIZE / 128 + 64;
        return .{
            .mask = try a.alloc(u16, txs_mod.MAX_RECORDS),
            .payload = try a.alloc(u8, payload_max),
            .decomp = try a.alloc(u8, txs_mod.MAX_TABLE_SIZE),
            .records = try a.alloc(txs_mod.TxRecord, txs_mod.MAX_RECORDS),
            .selected = try a.alloc(txs_mod.TxRecord, txs_mod.MAX_RECORDS),
        };
    }
};

fn worker(args: *WorkerArgs) void {
    // Stack scratch is safe: `parallel.run` always spawns at WORKER_STACK_SIZE.
    var decompress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var serialize_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var compress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;

    if (args.txs_reader != null) {
        args.tx_scratch = TxScratch.init(args.allocator) catch |e| {
            args.err = e;
            return;
        };
    }

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
        // emit walk is ascending, satisfying a monotonic sink.
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
    args: *WorkerArgs,
    decompress_buf: []u8,
    serialize_buf: []u8,
    compress_buf: []u8,
) void {
    // Every error path counts the block as dropped, propagating to
    // Result.dropped_blocks so the caller can refuse an index with a hole.
    const decompressed = log_serial.decompressEntry(entry_data, decompress_buf) catch {
        args.dropped_blocks += 1;
        return;
    };

    const mask_buf: ?[]u16 = if (args.tx_scratch) |ts| ts.mask else null;
    const maybe = filter_mod.filterBlockEntry(decompressed, args.filter, serialize_buf, compress_buf, mask_buf) catch {
        args.dropped_blocks += 1;
        return;
    };
    const filtered = maybe orelse return;

    // Tx carry: select the kept logs' records from txs.dat and size the tail.
    // A missing or incomplete table is a hole in an advisory artifact the
    // manifest declared required, drop the block so the build fails loud.
    var selected: []const txs_mod.TxRecord = &.{};
    if (args.tx_scratch) |ts| {
        const full = args.txs_reader.?.readBlock(block_number, ts.payload, ts.decomp, ts.records) catch {
            args.dropped_blocks += 1;
            return;
        } orelse {
            args.dropped_blocks += 1;
            return;
        };
        selected = txs_mod.selectByMask(full, filtered.tx_mask, ts.selected);
        if (selected.len != filtered.tx_mask.len) {
            args.dropped_blocks += 1;
            return;
        }
    }

    const tail_len: usize = if (args.tx_scratch != null) 4 + selected.len * txs_mod.RECORD_SIZE else 0;
    const owned = args.allocator.alloc(u8, filtered.entry.len + tail_len) catch {
        args.dropped_blocks += 1;
        return;
    };
    @memcpy(owned[0..filtered.entry.len], filtered.entry);
    if (tail_len > 0) _ = txs_mod.serializeRecords(selected, owned[filtered.entry.len..]);

    args.results.append(args.allocator, .{
        .block_number = block_number,
        .entry = owned,
        .log_count = filtered.log_count,
    }) catch {
        args.dropped_blocks += 1;
    };
}

// ── Tests ──────────────────────────────────────────────────────────────────

const testing = std.testing;

test "processBlockEntry: corrupt entry counts as dropped_block (fail-loud safety net)" {
    const allocator = testing.allocator;

    var results: std.ArrayListUnmanaged(FilteredBlock) = .{};
    defer results.deinit(allocator);

    var args: WorkerArgs = .{
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

test "dedupAdjacent collapses runs of equal block numbers" {
    var list: std.ArrayListUnmanaged(u64) = .{};
    defer list.deinit(testing.allocator);
    try list.appendSlice(testing.allocator, &.{ 100, 100, 101, 102, 102, 102, 103 });
    dedupAdjacent(&list);
    try testing.expectEqualSlices(u64, &.{ 100, 101, 102, 103 }, list.items);
}

test "run crosses a chunk boundary with a globally ascending sink" {
    // CHUNK_BLOCKS + 100 matching blocks force two filterChunk calls. The
    // sink asserts strict ascent across the seam and the final count proves
    // no block is lost or duplicated at the boundary. Pins the invariant the
    // FilteredStore append and the TCP PUSH stream both depend on.
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const ADDR = [_]u8{0xAB} ** 20;
    const TOPIC = [_]u8{0xCD} ** 32;
    const total: usize = CHUNK_BLOCKS + 100;

    const logs = [_]flat_reader.TestLog{.{ .address = ADDR, .topic0 = TOPIC }};
    const blocks = try alloc.alloc(flat_reader.TestBlock, total);
    defer alloc.free(blocks);
    for (blocks, 0..) |*b, i| b.* = .{ .block_number = i + 1, .logs = &logs };
    try flat_reader.writeTestStore(tmp.dir, blocks, alloc);

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);
    var reader = try FlatStoreReader.open(path);
    defer reader.deinit();

    const Sink = struct {
        alloc: std.mem.Allocator,
        seen: std.ArrayListUnmanaged(u64) = .{},
        pub fn emit(self: *@This(), block_number: u64, entry: []const u8, log_count: u32) !void {
            _ = entry;
            if (log_count != 1) return error.WrongLogCount;
            if (self.seen.items.len > 0 and block_number <= self.seen.items[self.seen.items.len - 1])
                return error.OutOfOrder;
            try self.seen.append(self.alloc, block_number);
        }
    };
    var sink = Sink{ .alloc = alloc };
    defer sink.seen.deinit(alloc);

    const addrs = [_][20]u8{ADDR};
    const topics = [_][32]u8{TOPIC};
    const filter: Filter = .{ .match_addrs = &addrs, .match_topics = &topics, .exclude_addrs = &.{} };
    const r = try run(&reader, &addrs, filter, null, 1, total, Sink, &sink, alloc);

    try testing.expectEqual(@as(u64, 0), r.dropped_blocks);
    try testing.expectEqual(@as(usize, total), sink.seen.items.len);
    try testing.expectEqual(@as(u64, 1), sink.seen.items[0]);
    try testing.expectEqual(@as(u64, total), sink.seen.items[sink.seen.items.len - 1]);
}
