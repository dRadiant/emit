/// Bulk importer for Nethermind's receipts RocksDB into the flat log store.
/// Reads the CompactReceiptStore column family directly — no JSON-RPC.
///
/// Architecture: single reader thread fills a 512-slot ring from the RocksDB
/// iterator. N worker threads (one per slot modulo) decode RLP → build blooms
/// → serialize → LZ4 compress. The reader thread also drains completed slots
/// into the flat store writer in order. Lock-free via atomic slot states.
///
/// Only compiled when the rocksdb lazy dependency is available (`zig build import`).
/// Decode logic lives in receipt_decoder.zig (testable without rocksdb).
const std = @import("std");

const c = @import("rocksdb");
const core = @import("core");

const flat_writer = @import("flat_writer.zig");
const receipt_decoder = @import("receipt_decoder.zig");

const types = core.types;
const log_serial = core.log_serial;
const bloom = core.bloom;
const parallel = core.parallel;

const Iterator = ?*c.rocksdb_iterator_t;

// ── Parallel pipeline ────────────────────────────────────────────────────

/// Ring buffer of decode slots shared between reader and workers.
/// 512 slots keeps the pipeline full even when individual blocks vary in size.
const SLOT_COUNT = 512;
const MAX_RAW_VALUE = 2 * 1024 * 1024; // largest observed Nethermind receipt ~1.5 MB
const MAX_ENTRY_SIZE = 4 + types.BLOCK_BUF_SIZE; // lz4_len(4) + compressed payload

/// Decode pipeline slot. Transitions: EMPTY → FILLED (reader) → DONE (worker) → EMPTY.
/// Each worker owns slots where `slot_index % num_workers == worker_id`.
const Slot = struct {
    // Input (written by reader thread)
    block_number: u64 = 0,
    raw_value: [MAX_RAW_VALUE]u8 = undefined,
    raw_len: usize = 0,
    // Output (written by worker thread)
    entry: [MAX_ENTRY_SIZE]u8 = undefined,
    entry_len: usize = 0,
    topic_bloom: [bloom.BLOOM_SIZE]u8 = undefined,
    addr_bloom: [bloom.ADDR_BLOOM_SIZE]u8 = undefined,
    log_count: u64 = 0,
    has_error: bool = false,
    // Atomic state for lock-free handoff
    state: std.atomic.Value(u8) = .init(EMPTY),

    const EMPTY: u8 = 0;
    const FILLED: u8 = 1;
    const DONE: u8 = 2;
};

const WorkerArgs = struct {
    slots: *[SLOT_COUNT]Slot,
    worker_id: usize,
    allocator: std.mem.Allocator,
};

/// Worker: decode RLP receipts → build blooms → serialize → LZ4 compress.
/// Buffers are heap-allocated per the project rule (see `core.parallel`):
/// any buffer sized off BLOCK_BUF_SIZE / MAX_LOGS_PER_BLOCK lives on the
/// heap so the function is safe regardless of where it's spawned. Each
/// worker holds its own buffers for the run's lifetime — one alloc pair
/// total, no churn.
fn workerFn(args: *WorkerArgs) void {
    const log_buf = args.allocator.alloc(types.RawLog, types.MAX_LOGS_PER_BLOCK) catch return;
    defer args.allocator.free(log_buf);
    const data_buf = args.allocator.alloc(u8, types.BLOCK_BUF_SIZE) catch return;
    defer args.allocator.free(data_buf);
    const serialize_buf = args.allocator.alloc(u8, types.BLOCK_BUF_SIZE) catch return;
    defer args.allocator.free(serialize_buf);

    while (true) {
        for (0..SLOT_COUNT) |i| {
            if (i % parallel.MAX_WORKERS != args.worker_id) continue;
            const slot = &args.slots[i];
            if (slot.state.load(.acquire) != Slot.FILLED) continue;

            const log_count = receipt_decoder.decodeReceipts(
                slot.block_number, slot.raw_value[0..slot.raw_len], log_buf, data_buf,
            ) catch {
                slot.has_error = true;
                slot.log_count = 0;
                slot.entry_len = 0;
                slot.state.store(Slot.DONE, .release);
                continue;
            };

            if (log_count == 0) {
                slot.log_count = 0;
                slot.entry_len = 0;
                slot.state.store(Slot.DONE, .release);
                continue;
            }

            const topic_bloom = log_serial.buildTopicBloom(log_buf[0..log_count]);
            const addr_bloom = log_serial.buildAddrBloom(log_buf[0..log_count]);
            const serialized_len = log_serial.serializeLogs(log_buf[0..log_count], serialize_buf);

            slot.entry_len = log_serial.compressEntry(serialize_buf[0..serialized_len], &slot.entry) catch {
                slot.has_error = true;
                slot.state.store(Slot.DONE, .release);
                continue;
            };

            slot.topic_bloom = topic_bloom.bits;
            slot.addr_bloom = addr_bloom.bits;
            slot.log_count = log_count;
            slot.state.store(Slot.DONE, .release);
        }

        // Shutdown signal: reader sets slot 0's block_number to max after iterator exhausted
        if (args.slots[0].block_number == std.math.maxInt(u64)) break;
        std.atomic.spinLoopHint();
    }
}

/// Copy the current iterator entry into a slot. Keys are block numbers (u64 BE).
fn fillSlot(slot: *Slot, iter: Iterator) bool {
    var klen: usize = 0;
    var vlen: usize = 0;
    const key_ptr: [*]const u8 = @ptrCast(c.rocksdb_iter_key(iter, &klen) orelse return false);
    const val_ptr: [*]const u8 = @ptrCast(c.rocksdb_iter_value(iter, &vlen) orelse return false);
    if (klen < 8 or vlen > MAX_RAW_VALUE) return false;

    slot.block_number = std.mem.readInt(u64, key_ptr[0..8], .big);
    @memcpy(slot.raw_value[0..vlen], val_ptr[0..vlen]);
    slot.raw_len = vlen;
    slot.has_error = false;
    slot.log_count = 0;
    slot.entry_len = 0;
    slot.state.store(Slot.FILLED, .release);
    return true;
}

/// Convert a RocksDB error pointer to a Zig error. Frees the C string.
fn rocksErr(err_ptr: *?[*:0]u8) !void {
    if (err_ptr.*) |e| {
        std.debug.print("RocksDB error: {s}\n", .{std.mem.span(e)});
        c.rocksdb_free(@ptrCast(e));
        err_ptr.* = null;
        return error.RocksDBError;
    }
}

fn iterValid(iter: Iterator) bool {
    return c.rocksdb_iter_valid(iter) != 0;
}

// ── Entry points ─────────────────────────────────────────────────────────

pub fn main() !void {
    const alloc = std.heap.page_allocator;
    const args = try std.process.argsAlloc(alloc);
    if (args.len < 3) {
        std.debug.print("Usage: rocksdb-import <receipts_db_path> <data_dir>\n", .{});
        std.process.exit(1);
    }
    try run(args[1], args[2]);
}

pub fn run(receipts_path: [*:0]const u8, output_path: []const u8) !void {
    const allocator = std.heap.page_allocator;

    // Open Nethermind's receipts DB read-only with column families.
    // "Blocks" CF contains receipts keyed by block number (u64 BE).
    var err: ?[*:0]u8 = null;
    const opts = c.rocksdb_options_create();

    const cf_names = [_][*c]const u8{ @ptrCast("default"), @ptrCast("Transactions"), @ptrCast("Blocks") };
    const cf_opts = [3]?*const c.rocksdb_options_t{ opts, opts, opts };
    var cf_handles: [3]?*c.rocksdb_column_family_handle_t = .{ null, null, null };

    const db = c.rocksdb_open_for_read_only_column_families(
        opts, receipts_path, 3, &cf_names, &cf_opts, &cf_handles, 0, @ptrCast(&err),
    );
    try rocksErr(&err);
    if (db == null) return error.RocksDBError;
    const blocks_cf = cf_handles[2] orelse return error.RocksDBError;

    std.fs.cwd().makeDir(output_path) catch {};
    var writer = try flat_writer.FlatStoreWriter.open(output_path);

    // Sequential bulk read: 4 MB readahead, skip block cache and checksums
    const read_opts = c.rocksdb_readoptions_create();
    c.rocksdb_readoptions_set_readahead_size(read_opts, 4 * 1024 * 1024);
    c.rocksdb_readoptions_set_fill_cache(read_opts, 0);
    c.rocksdb_readoptions_set_verify_checksums(read_opts, 0);

    const iter = c.rocksdb_create_iterator_cf(db, read_opts, blocks_cf);
    if (iter == null) return error.RocksDBError;

    const slots = try allocator.create([SLOT_COUNT]Slot);
    for (slots) |*s| s.* = Slot{};

    const t_start = std.time.nanoTimestamp();
    var blocks_processed: u64 = 0;
    var blocks_with_logs: u64 = 0;
    var total_logs: u64 = 0;
    var decode_errors: u64 = 0;

    std.debug.print("Importing receipts ({} workers) from {s} → {s}\n", .{
        parallel.MAX_WORKERS, std.mem.span(receipts_path), output_path,
    });

    // Resume from flat store checkpoint if present
    if (writer.meta.last_finalized_block > 0) {
        var resume_key: [8]u8 = undefined;
        std.mem.writeInt(u64, &resume_key, writer.meta.last_finalized_block + 1, .big);
        c.rocksdb_iter_seek(iter, @ptrCast(&resume_key), resume_key.len);
        std.debug.print("Resuming from block {} (flat store size: {} bytes)\n", .{
            writer.meta.last_finalized_block + 1, writer.meta.blocks_dat_size,
        });
    } else {
        c.rocksdb_iter_seek_to_first(iter);
    }

    var worker_args: [parallel.MAX_WORKERS]WorkerArgs = undefined;
    var workers: [parallel.MAX_WORKERS]std.Thread = undefined;
    for (0..parallel.MAX_WORKERS) |i| {
        worker_args[i] = .{ .slots = slots, .worker_id = i, .allocator = allocator };
        workers[i] = try std.Thread.spawn(.{ .stack_size = parallel.WORKER_STACK_SIZE }, workerFn, .{&worker_args[i]});
    }

    // Single-threaded reader + writer loop. Reads ahead into empty slots,
    // then drains the next completed slot in order (preserves block ordering).
    var read_cursor: usize = 0;
    var write_cursor: usize = 0;

    while (write_cursor < read_cursor or iterValid(iter)) {
        // Fill empty slots from RocksDB iterator
        while (read_cursor - write_cursor < SLOT_COUNT and iterValid(iter)) {
            const next_slot = &slots[read_cursor % SLOT_COUNT];
            if (next_slot.state.load(.acquire) != Slot.EMPTY) break;
            if (fillSlot(next_slot, iter)) read_cursor += 1;
            c.rocksdb_iter_next(iter);
        }

        if (write_cursor >= read_cursor) break;
        const slot = &slots[write_cursor % SLOT_COUNT];
        while (slot.state.load(.acquire) != Slot.DONE) std.atomic.spinLoopHint();

        if (slot.has_error) {
            decode_errors += 1;
        } else if (slot.log_count > 0) {
            try writer.appendBlock(slot.block_number, slot.entry[0..slot.entry_len], &slot.topic_bloom, &slot.addr_bloom);
            blocks_with_logs += 1;
            total_logs += slot.log_count;
        }

        blocks_processed += 1;
        slot.state.store(Slot.EMPTY, .release);
        write_cursor += 1;

        if (blocks_processed % 100_000 == 0) {
            const elapsed_s = @as(f64, @floatFromInt(std.time.nanoTimestamp() - t_start)) / 1e9;
            std.debug.print("  {d:>10} blocks | {d:>12} logs | {d:.1}s | {d:.0} blk/s | {d:.0} logs/s\n", .{
                blocks_processed, total_logs, elapsed_s,
                @as(f64, @floatFromInt(blocks_processed)) / elapsed_s,
                @as(f64, @floatFromInt(total_logs)) / elapsed_s,
            });
        }
    }

    // Signal workers to exit and wait
    slots[0].block_number = std.math.maxInt(u64);
    for (&workers) |*w| w.join();

    // Cleanup RocksDB resources
    c.rocksdb_iter_destroy(iter);
    c.rocksdb_readoptions_destroy(read_opts);
    for (&cf_handles) |h| if (h) |handle| c.rocksdb_column_family_handle_destroy(handle);
    c.rocksdb_close(db);
    c.rocksdb_options_destroy(opts);

    try writer.finalize();
    writer.close();
    allocator.destroy(slots);

    const elapsed_s = @as(f64, @floatFromInt(std.time.nanoTimestamp() - t_start)) / 1e9;
    std.debug.print(
        \\
        \\=== Import Complete ===
        \\Blocks processed:  {d}
        \\Blocks with logs:  {d}
        \\Total logs:        {d}
        \\Decode errors:     {d}
        \\Elapsed:           {d:.1}s
        \\
    , .{ blocks_processed, blocks_with_logs, total_logs, decode_errors, elapsed_s });
    if (elapsed_s > 0) {
        std.debug.print("Blocks/sec:        {d:.0}\nLogs/sec:          {d:.0}\n", .{
            @as(f64, @floatFromInt(blocks_processed)) / elapsed_s,
            @as(f64, @floatFromInt(total_logs)) / elapsed_s,
        });
    }
}
