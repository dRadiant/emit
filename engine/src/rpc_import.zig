/// Backfill the flat log store via `eth_getLogs` range queries.
///
/// Fallback to `import --rocksdb` for chains without a local Nethermind receipts
/// DB, L2s, or L1 operators without a colocated node. Unfiltered `eth_getLogs`
/// over [from, to] returns every log in the range grouped by block.
/// Shares the `flat_writer` the RocksDB path uses. Block ranges sized
/// adaptively, grow while the node accepts them, halve on rejection.
///
/// Timestamps populated alongside logs. Per log batch a batched
/// `eth_getBlockByNumber` over the same range fills `timestamps.bin`, the only
/// way over RPC since there is no `headers` DB to read. Essential for L2s, whose
/// block time the L1 `blockTimestamp` formula does not model.
const std = @import("std");

const core = @import("core");
const eth = @import("eth");

const FlatStoreWriter = @import("flat_writer.zig").FlatStoreWriter;
const types = core.types;
const log_serial = core.log_serial;

/// Starting block span per `eth_getLogs`. Grows x2 on success up to
/// `MAX_BATCH`, halves on rejection (provider result-count or range cap) down to
/// a single block. A moderate start self-tunes within a few batches.
const INITIAL_BATCH: u64 = 512;
const MAX_BATCH: u64 = 8192;
/// Transient-error retries once the span is already a single block. A one-block
/// query rarely trips a result cap, so failures there are network.
const MAX_RETRIES: u32 = 5;
/// Progress line cadence, in blocks.
const LOG_EVERY: u64 = 100_000;
/// Blocks per batched `eth_getBlockByNumber` timestamp request. Each result is a
/// full header (~1 KB JSON), so 256 caps the response at a few hundred KB.
/// Adaptive like the log span, halves on a node batch-size cap, grows back.
const TS_CHUNK: u64 = 256;
const TS_CHUNK_MAX: u64 = 1024;

pub const Config = struct {
    rpc_url: []const u8,
    data_dir: []const u8,
    /// Inclusive lower bound. Ignored when the store already has data (import
    /// resumes from the next dense block). Defaults to 0 for a fresh store.
    from_block: ?u64 = null,
    /// Inclusive upper bound. Defaults to `chain_tip - FINALITY_DEPTH` so the
    /// flat store holds only finalized blocks. The follower covers the tail.
    to_block: ?u64 = null,
    /// Populate `timestamps.bin` via batched `eth_getBlockByNumber`. On by
    /// default and strict, a persistent timestamp failure aborts the import
    /// loudly. The L1 formula is wrong for L2s, so a silent fallback would be a
    /// correctness landmine. Set false (`--no-timestamps`) only for chains where
    /// the formula is acceptable.
    timestamps: bool = true,
};

/// Reusable per-block buffers, allocated once for the whole import.
const Scratch = struct {
    raw_logs: []types.RawLog,
    serialize_buf: []u8,
    compress_buf: []u8,
};

pub fn run(config: Config) !void {
    const page = std.heap.page_allocator;

    var writer = try FlatStoreWriter.open(config.data_dir);
    defer {
        writer.commitMeta() catch {};
        writer.deinit();
    }

    const end: u64 = config.to_block orelse blk: {
        const tip = try queryTip(config.rpc_url, page);
        break :blk if (tip > types.FINALITY_DEPTH) tip - types.FINALITY_DEPTH else 0;
    };

    // Pass 1, logs. Resumes from the next dense block. `appendBlock`'s
    // dense-append invariant guarantees `log_start` lands on `first_block + count`.
    const log_start: u64 = if (writer.meta.blocks_idx_count > 0)
        writer.first_block + writer.meta.blocks_idx_count
    else
        (config.from_block orelse 0);
    try importLogs(&writer, config.rpc_url, log_start, end, page);

    // Pass 2, timestamps (strict by default). Separate pass with its own resume
    // cursor (`timestamps.bin` count), so a re-run after a failure backfills
    // exactly the missing range with no silent formula fallback. Only meaningful
    // once the store has blocks to stamp.
    if (config.timestamps and writer.meta.blocks_idx_count > 0) {
        var dir = try std.fs.cwd().openDir(config.data_dir, .{});
        defer dir.close();
        var ts_writer = try core.timestamps.TimestampWriter.open(dir, writer.first_block);
        defer ts_writer.deinit();
        try importTimestamps(&ts_writer, config.rpc_url, end, page);
    } else if (!config.timestamps) {
        std.debug.print("Timestamps skipped (--no-timestamps); blocks use the formula fallback.\n", .{});
    }
}

/// Import logs over [start, end] via adaptively-sized `eth_getLogs`. Fails loud,
/// a span that can't be fetched even at one block after retries aborts. The
/// store stays consistent and a re-run resumes from the next block.
fn importLogs(writer: *FlatStoreWriter, rpc_url: []const u8, start: u64, end: u64, page: std.mem.Allocator) !void {
    if (start > end) {
        std.debug.print("Logs: flat store already covers through block {d}.\n", .{end});
        return;
    }
    std.debug.print("Importing logs [{d}, {d}] via eth_getLogs ({s}).\n", .{ start, end, rpc_url });

    const scratch = Scratch{
        .raw_logs = try page.alloc(types.RawLog, types.MAX_LOGS_PER_BLOCK),
        .serialize_buf = try page.alloc(u8, types.BLOCK_BUF_SIZE),
        .compress_buf = try page.alloc(u8, types.BLOCK_BUF_SIZE),
    };
    defer {
        page.free(scratch.raw_logs);
        page.free(scratch.serialize_buf);
        page.free(scratch.compress_buf);
    }

    var lo: u64 = start;
    var batch: u64 = INITIAL_BATCH;
    var retries: u32 = 0;
    var total_logs: u64 = 0;
    var next_log: u64 = start + LOG_EVERY;

    while (lo <= end) {
        const hi = @min(lo + batch - 1, end);
        if (importBatch(writer, rpc_url, lo, hi, scratch, page)) |n| {
            total_logs += n;
            lo = hi + 1;
            retries = 0;
            batch = @min(batch * 2, MAX_BATCH);
            if (hi >= next_log) {
                std.debug.print("  ... logs block {d}/{d} ({d} so far)\n", .{ hi, end, total_logs });
                next_log = hi + LOG_EVERY;
            }
        } else |err| {
            // Wide range rejected: halve and retry the same span. Already a
            // single block: treat as transient and back off, then fail loud.
            if (batch > 1) {
                batch = @max(1, batch / 2);
                continue;
            }
            retries += 1;
            if (retries > MAX_RETRIES) {
                std.debug.print("Logs block {d}: {s} after {d} retries — aborting.\n", .{ lo, @errorName(err), MAX_RETRIES });
                return err;
            }
            std.debug.print("Logs block {d}: {s}, retry {d}/{d}\n", .{ lo, @errorName(err), retries, MAX_RETRIES });
            std.Thread.sleep(std.time.ns_per_s * retries);
        }
    }

    try writer.commitMeta();
    std.debug.print("Logs done: [{d}, {d}] — {d} blocks, {d} logs.\n", .{ start, end, end - start + 1, total_logs });
}

/// Fill `timestamps.bin` over [resume, end] via batched `eth_getBlockByNumber`.
/// Strict and fail-loud, every block in a chunk must return a timestamp, and a
/// chunk that can't be fetched even at size one after retries aborts the import.
/// Independently resumable from the timestamps.bin count, so a re-run continues
/// exactly where it stopped.
fn importTimestamps(ts_writer: *core.timestamps.TimestampWriter, rpc_url: []const u8, end: u64, page: std.mem.Allocator) !void {
    const start = ts_writer.first_block + ts_writer.count;
    if (start > end) {
        std.debug.print("Timestamps: already cover through block {d}.\n", .{end});
        return;
    }
    std.debug.print("Fetching timestamps [{d}, {d}] via eth_getBlockByNumber.\n", .{ start, end });

    var lo: u64 = start;
    var chunk: u64 = TS_CHUNK;
    var retries: u32 = 0;
    var next_log: u64 = start + LOG_EVERY;

    while (lo <= end) {
        const hi = @min(lo + chunk - 1, end);
        if (timestampBatch(rpc_url, ts_writer, lo, hi, page)) |_| {
            try ts_writer.sync();
            lo = hi + 1;
            retries = 0;
            chunk = @min(chunk * 2, TS_CHUNK_MAX);
            if (hi >= next_log) {
                std.debug.print("  ... timestamps block {d}/{d}\n", .{ hi, end });
                next_log = hi + LOG_EVERY;
            }
        } else |err| {
            if (chunk > 1) {
                chunk = @max(1, chunk / 2);
                continue;
            }
            retries += 1;
            if (retries > MAX_RETRIES) {
                std.debug.print("Timestamps block {d}: {s} after {d} retries — aborting. Re-run to resume.\n", .{ lo, @errorName(err), MAX_RETRIES });
                return err;
            }
            std.debug.print("Timestamps block {d}: {s}, retry {d}/{d}\n", .{ lo, @errorName(err), retries, MAX_RETRIES });
            std.Thread.sleep(std.time.ns_per_s * retries);
        }
    }

    try ts_writer.sync();
    std.debug.print("Timestamps done: [{d}, {d}].\n", .{ start, end });
}

/// Fetch [lo, hi] in one `eth_getLogs` and write its blocks. Owns a per-batch
/// arena so the large JSON-RPC response and a fresh HTTP client are freed on
/// return. The writer and scratch live across batches. The provider and
/// transport share the arena allocator, `getLogs` frees the transport's
/// response with the provider's allocator, so they must be the same.
fn importBatch(
    writer: *FlatStoreWriter,
    rpc_url: []const u8,
    lo: u64,
    hi: u64,
    scratch: Scratch,
    parent_alloc: std.mem.Allocator,
) !u64 {
    var arena = std.heap.ArenaAllocator.init(parent_alloc);
    defer arena.deinit();
    const a = arena.allocator();

    var http = eth.http_transport.HttpTransport.init(a, rpc_url);
    defer http.deinit();
    var provider = eth.provider.Provider.init(a, &http);

    var lo_buf: [20]u8 = undefined;
    var hi_buf: [20]u8 = undefined;
    const lo_hex = try std.fmt.bufPrint(&lo_buf, "0x{x}", .{lo});
    const hi_hex = try std.fmt.bufPrint(&hi_buf, "0x{x}", .{hi});

    const logs = try provider.getLogs(.{ .fromBlock = lo_hex, .toBlock = hi_hex });
    return writeBatch(writer, logs, lo, hi, scratch);
}

/// Fetch and write timestamps for every block in [lo, hi] via one batched
/// `eth_getBlockByNumber`. Owns a per-call arena (the batch body, response, and
/// a fresh client). Strict, returns `error.MissingTimestamps` unless every block
/// in the range came back with a timestamp, so a partial node response surfaces
/// rather than silently leaving holes.
fn timestampBatch(
    rpc_url: []const u8,
    ts_writer: *core.timestamps.TimestampWriter,
    lo: u64,
    hi: u64,
    parent_alloc: std.mem.Allocator,
) !void {
    var arena = std.heap.ArenaAllocator.init(parent_alloc);
    defer arena.deinit();
    const a = arena.allocator();

    var http = eth.http_transport.HttpTransport.init(a, rpc_url);
    defer http.deinit();

    var body: std.ArrayList(u8) = .empty;
    try body.append(a, '[');
    var b = lo;
    while (b <= hi) : (b += 1) {
        if (b > lo) try body.append(a, ',');
        var item_buf: [128]u8 = undefined;
        // id = block - lo, so the possibly-reordered batch response maps back to a block.
        const item = try std.fmt.bufPrint(&item_buf, "{{\"jsonrpc\":\"2.0\",\"id\":{d},\"method\":\"eth_getBlockByNumber\",\"params\":[\"0x{x}\",false]}}", .{ b - lo, b });
        try body.appendSlice(a, item);
    }
    try body.append(a, ']');

    const raw = try httpPost(&http, body.items, a);
    const applied = try parseTimestampBatch(raw, lo, ts_writer, a);
    if (applied != hi - lo + 1) return error.MissingTimestamps;
}

/// Parse a JSON-RPC batch response of `eth_getBlockByNumber` results and write
/// each `result.timestamp` (hex) to `ts_writer` at block `lo + id`. Returns the
/// number of timestamps written. Items without a result (an error or a missing
/// block) are not counted, so the caller can enforce completeness. A disk write
/// error propagates (fail loud).
fn parseTimestampBatch(
    raw: []const u8,
    lo: u64,
    ts_writer: *core.timestamps.TimestampWriter,
    alloc: std.mem.Allocator,
) !usize {
    const BatchItem = struct {
        id: u64,
        result: ?struct { timestamp: []const u8 } = null,
    };
    const parsed = try std.json.parseFromSlice([]BatchItem, alloc, raw, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var applied: usize = 0;
    for (parsed.value) |item| {
        const r = item.result orelse continue;
        if (!std.mem.startsWith(u8, r.timestamp, "0x")) continue;
        const ts = std.fmt.parseInt(u64, r.timestamp[2..], 16) catch continue;
        try ts_writer.set(lo + item.id, std.math.cast(u32, ts) orelse 0);
        applied += 1;
    }
    return applied;
}

/// POST a raw JSON body (here a JSON-RPC batch array) over the transport's
/// client, reusing its connection. Mirrors `HttpTransport.request` but sends
/// the body verbatim instead of wrapping it as a single call.
fn httpPost(http: *eth.http_transport.HttpTransport, body: []const u8, alloc: std.mem.Allocator) ![]u8 {
    var response_body: std.Io.Writer.Allocating = .init(alloc);
    errdefer response_body.deinit();
    const result = http.client.fetch(.{
        .location = .{ .url = http.url },
        .method = .POST,
        .payload = body,
        .extra_headers = &.{.{ .name = "Content-Type", .value = "application/json" }},
        .response_writer = &response_body.writer,
    });
    if (result) |res| {
        if (res.status != .ok) {
            response_body.deinit();
            return error.HttpError;
        }
        return response_body.toOwnedSlice();
    } else |_| {
        response_body.deinit();
        return error.ConnectionFailed;
    }
}

/// Group already-fetched range logs by block and append every block in [lo, hi],
/// including those with no logs, keeping `blocks.idx` dense. Relies on
/// `eth_getLogs` returning logs in ascending (block, log_index) order (universal
/// across Geth/Nethermind/Erigon). Any log left unconsumed means out-of-range or
/// out-of-order data and fails loud rather than landing a misgrouped block.
/// Returns the log count written. Pure (no network) so the grouping/dense-emit
/// logic is unit-testable.
fn writeBatch(
    writer: *FlatStoreWriter,
    logs: []const eth.receipt.Log,
    lo: u64,
    hi: u64,
    scratch: Scratch,
) !u64 {
    var cursor: usize = 0;
    var bn: u64 = lo;
    while (bn <= hi) : (bn += 1) {
        const block_start = cursor;
        while (cursor < logs.len and (logs[cursor].block_number orelse 0) == bn) : (cursor += 1) {}
        const block_logs = logs[block_start..cursor];

        if (block_logs.len > types.MAX_LOGS_PER_BLOCK) {
            std.debug.print(
                "Block {d}: {d} logs exceeds MAX_LOGS_PER_BLOCK ({d}). Bump the constant in core/src/types.zig.\n",
                .{ bn, block_logs.len, types.MAX_LOGS_PER_BLOCK },
            );
            return error.TooManyLogsInBlock;
        }

        for (block_logs, 0..) |log, i| scratch.raw_logs[i] = toRawLog(log, bn);
        const raw = scratch.raw_logs[0..block_logs.len];

        const topic_bloom = log_serial.buildTopicBloom(raw);
        const addr_bloom = log_serial.buildAddrBloom(raw);
        const serialized_len = log_serial.serializeLogs(raw, scratch.serialize_buf);
        const entry_len = try log_serial.compressEntry(scratch.serialize_buf[0..serialized_len], scratch.compress_buf);

        try writer.appendBlock(bn, scratch.compress_buf[0..entry_len], &topic_bloom.bits, &addr_bloom.bits);
    }

    if (cursor != logs.len) return error.UnexpectedLogOrder;
    return @intCast(logs.len);
}

/// Convert an eth.zig log to a `RawLog`, borrowing `log.data` (consumed by
/// `serializeLogs` before the batch arena is freed, no copy needed).
fn toRawLog(log: eth.receipt.Log, block_number: u64) types.RawLog {
    var topics: [types.MAX_TOPICS][32]u8 = std.mem.zeroes([types.MAX_TOPICS][32]u8);
    var topic_count: u8 = 0;
    for (log.topics) |t| {
        if (topic_count >= types.MAX_TOPICS) break;
        topics[topic_count] = t;
        topic_count += 1;
    }
    return .{
        .block_number = block_number,
        .log_index = @intCast(log.log_index orelse 0),
        .tx_index = @intCast(log.transaction_index orelse 0),
        .address = log.address,
        .topic_count = topic_count,
        .topics = topics,
        .data = log.data,
        .tx_hash = log.transaction_hash orelse std.mem.zeroes([32]u8),
    };
}

/// One-off chain-tip query with its own short-lived arena.
fn queryTip(rpc_url: []const u8, parent_alloc: std.mem.Allocator) !u64 {
    var arena = std.heap.ArenaAllocator.init(parent_alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var http = eth.http_transport.HttpTransport.init(a, rpc_url);
    defer http.deinit();
    var provider = eth.provider.Provider.init(a, &http);
    return provider.getBlockNumber();
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

fn testLog(block: u64, log_index: u32, addr: u8, data: []const u8) eth.receipt.Log {
    const topic = [_]u8{0xAA} ** 32;
    return .{
        .address = [_]u8{addr} ** 20,
        .topics = &.{topic},
        .data = data,
        .block_number = block,
        .transaction_hash = null,
        .transaction_index = 0,
        .log_index = log_index,
        .block_hash = null,
        .removed = false,
    };
}

test "writeBatch groups range logs by block, emits empty blocks, stays dense" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);

    var writer = try FlatStoreWriter.open(path);
    defer writer.deinit();

    const scratch = Scratch{
        .raw_logs = try testing.allocator.alloc(types.RawLog, types.MAX_LOGS_PER_BLOCK),
        .serialize_buf = try testing.allocator.alloc(u8, types.BLOCK_BUF_SIZE),
        .compress_buf = try testing.allocator.alloc(u8, types.BLOCK_BUF_SIZE),
    };
    defer {
        testing.allocator.free(scratch.raw_logs);
        testing.allocator.free(scratch.serialize_buf);
        testing.allocator.free(scratch.compress_buf);
    }

    // Range [100, 102]: block 100 has two logs, 101 is empty, 102 has one.
    const logs = [_]eth.receipt.Log{
        testLog(100, 0, 0x11, &.{}),
        testLog(100, 1, 0x22, &.{ 0xDE, 0xAD }),
        testLog(102, 0, 0x33, &.{}),
    };
    const n = try writeBatch(&writer, &logs, 100, 102, scratch);

    try testing.expectEqual(@as(u64, 3), n);
    try testing.expectEqual(@as(u64, 3), writer.meta.blocks_idx_count); // 100, 101, 102 all present
    try testing.expectEqual(@as(u64, 102), writer.meta.last_finalized_block);
    try testing.expectEqual(@as(u64, 100), writer.first_block);
    try writer.commitMeta();

    // Read back: decompress + deserialize each block, verify per-block log counts.
    var reader = try core.flat_reader.FlatStoreReader.open(path);
    defer reader.deinit();

    var entry_buf: [4096]u8 = undefined;
    var decomp_buf: [4096]u8 = undefined;
    var log_buf: [8]types.RawLog = undefined;

    const expect_counts = [_]usize{ 2, 0, 1 };
    for (expect_counts, 100..) |want, block| {
        const entry = try reader.readBlock(@intCast(block), &entry_buf);
        const decoded = try log_serial.decompressEntry(entry, &decomp_buf);
        const count = log_serial.deserializeLogs(decoded, &log_buf);
        try testing.expectEqual(want, count);
    }
}

test "writeBatch fails loud on out-of-range or unordered logs" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);

    var writer = try FlatStoreWriter.open(path);
    defer writer.deinit();

    const scratch = Scratch{
        .raw_logs = try testing.allocator.alloc(types.RawLog, types.MAX_LOGS_PER_BLOCK),
        .serialize_buf = try testing.allocator.alloc(u8, types.BLOCK_BUF_SIZE),
        .compress_buf = try testing.allocator.alloc(u8, types.BLOCK_BUF_SIZE),
    };
    defer {
        testing.allocator.free(scratch.raw_logs);
        testing.allocator.free(scratch.serialize_buf);
        testing.allocator.free(scratch.compress_buf);
    }

    // A log for block 105 while the range is [100, 102] can never be consumed by
    // the lo..hi walk, so the leftover trips the guard.
    const logs = [_]eth.receipt.Log{testLog(105, 0, 0x11, &.{})};
    try testing.expectError(error.UnexpectedLogOrder, writeBatch(&writer, &logs, 100, 102, scratch));
}

test "parseTimestampBatch writes hex timestamps keyed by lo + id, skips missing" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var tw = try core.timestamps.TimestampWriter.open(tmp.dir, 100);
    defer tw.deinit();

    // id 0 -> block 100, id 2 -> block 102 (real results). id 1 -> block 101 is
    // an error item with no result. Response order shuffled to prove id-keying.
    const raw =
        \\[{"jsonrpc":"2.0","id":2,"result":{"timestamp":"0x65432118"}},
        \\ {"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"missing"}},
        \\ {"jsonrpc":"2.0","id":0,"result":{"number":"0x64","timestamp":"0x65432100","hash":"0xabcd"}}]
    ;
    const applied = try parseTimestampBatch(raw, 100, &tw, testing.allocator);
    try testing.expectEqual(@as(usize, 2), applied); // ids 0 and 2 had results, id 1 was an error
    tw.sync() catch {};

    var r = (try core.timestamps.TimestampReader.open(tmp.dir)).?;
    defer r.deinit();
    try testing.expectEqual(@as(?u64, 0x65432100), r.get(100));
    try testing.expectEqual(@as(?u64, null), r.get(101)); // error item -> unknown -> formula
    try testing.expectEqual(@as(?u64, 0x65432118), r.get(102));
}
