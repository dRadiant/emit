/// Bulk importer for Nethermind's receipts RocksDB into the flat log store.
/// Reads the CompactReceiptStore column family directly, no JSON-RPC.
///
/// Single reader thread fills a 512-slot ring from the RocksDB iterator.
/// N worker threads (one per slot modulo) decode RLP, build blooms,
/// serialize, LZ4 compress. The reader thread drains completed slots into
/// the flat store writer in order. Lock-free via atomic slot states.
///
/// Only finalized blocks land in the flat store, stopping at `head - 64`.
/// Nethermind retains reorg-history receipt rows in the Blocks CF for
/// not-yet-finalized blocks (multiple entries per block_number with a
/// discriminator suffix). First-8-bytes-as-block_number key parsing would
/// collapse those onto one block_number and write whichever version came
/// last. The pending ring in head_follower fills the trailing edge canonically.
///
/// Only compiled when the rocksdb lazy dependency is available (`zig build import`).
/// Decode logic lives in receipt_decoder.zig (testable without rocksdb).
const std = @import("std");

const c = @import("rocksdb");
const core = @import("core");
const eth = @import("eth");

const flat_writer = @import("flat_writer.zig");
const receipt_decoder = @import("receipt_decoder.zig");
const rlp = @import("rlp.zig");

const types = core.types;
const log_serial = core.log_serial;
const bloom = core.bloom;
const parallel = core.parallel;

const FINALITY_DEPTH = types.FINALITY_DEPTH;

const Iterator = ?*c.rocksdb_iterator_t;

// ── Parallel pipeline ────────────────────────────────────────────────────

/// Ring buffer of decode slots shared between reader and workers.
/// 512 slots keeps the pipeline full when block sizes vary.
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
};

/// Fill `slot` with a canonical zero-log entry and empty blooms. Used for
/// no-log blocks and, on the error path, for blocks that failed to decode.
/// Every block in range MUST land in the flat store so the reader's dense
/// index (`idx = block_number - first_block`) stays aligned. Skipping a block
/// shifts every later block's logs onto the wrong block number.
fn writeEmptyEntry(slot: *Slot, serialize_buf: []u8) void {
    const n = log_serial.serializeLogs(&[_]types.RawLog{}, serialize_buf);
    slot.entry_len = log_serial.compressEntry(serialize_buf[0..n], &slot.entry) catch 0;
    slot.topic_bloom = std.mem.zeroes([bloom.BLOOM_SIZE]u8);
    slot.addr_bloom = std.mem.zeroes([bloom.ADDR_BLOOM_SIZE]u8);
    slot.log_count = 0;
}

/// Worker: decode RLP receipts, build blooms, serialize, LZ4 compress.
/// Stack scratch is safe under the `core.parallel` buffer rule, spawned
/// with `WORKER_STACK_SIZE`.
fn workerFn(args: *WorkerArgs) void {
    var log_buf: [types.MAX_LOGS_PER_BLOCK]types.RawLog = undefined;
    var data_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var serialize_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;

    while (true) {
        for (0..SLOT_COUNT) |i| {
            if (i % parallel.MAX_WORKERS != args.worker_id) continue;
            const slot = &args.slots[i];
            if (slot.state.load(.acquire) != Slot.FILLED) continue;

            const log_count = receipt_decoder.decodeReceipts(
                slot.block_number, slot.raw_value[0..slot.raw_len], &log_buf, &data_buf,
            ) catch |e| {
                std.debug.print("decode error block {d}: {}\n", .{ slot.block_number, e });
                writeEmptyEntry(slot, &serialize_buf);
                slot.has_error = true;
                slot.state.store(Slot.DONE, .release);
                continue;
            };

            // No-log blocks still flow through here. serializeLogs writes a
            // zero-count entry, blooms come out empty, the writer appends it so
            // the dense block index stays contiguous. Skipping them corrupts
            // every later block's number.
            const topic_bloom = log_serial.buildTopicBloom(log_buf[0..log_count]);
            const addr_bloom = log_serial.buildAddrBloom(log_buf[0..log_count]);
            const serialized_len = log_serial.serializeLogs(log_buf[0..log_count], &serialize_buf);

            slot.entry_len = log_serial.compressEntry(serialize_buf[0..serialized_len], &slot.entry) catch {
                std.debug.print("compress error block {d}\n", .{slot.block_number});
                writeEmptyEntry(slot, &serialize_buf);
                slot.has_error = true;
                slot.state.store(Slot.DONE, .release);
                continue;
            };

            slot.topic_bloom = topic_bloom.bits;
            slot.addr_bloom = addr_bloom.bits;
            slot.log_count = log_count;
            slot.state.store(Slot.DONE, .release);
        }

        // Shutdown signal: reader sets slot 0's block_number to max once iterator exhausted.
        if (args.slots[0].block_number == std.math.maxInt(u64)) break;
        std.atomic.spinLoopHint();
    }
}

const FillStatus = enum { filled, skipped };

/// Fill `slot` with the canonical receipt row for the block number at the
/// current iterator position, then advance the iterator past every row of that
/// block number (leaving it at the next block, or invalid).
///
/// Nethermind keys receipts by block_number(8 BE) ++ block_hash(32) and retains
/// reorg-orphan rows even for finalized blocks. A block number with more than
/// one row keeps the one whose hash matches the canonical chain. `provider`
/// (eth_getBlockByNumber) supplies that hash. Single-row blocks, the
/// overwhelming majority, take the provider-free fast path. A duplicate with no
/// `provider` fails loud rather than guessing.
fn fillSlotCanonical(slot: *Slot, iter: Iterator, provider: ?*eth.provider.Provider, dup_blocks: *u64) !FillStatus {
    var klen: usize = 0;
    var vlen: usize = 0;
    const kp: [*]const u8 = @ptrCast(c.rocksdb_iter_key(iter, &klen) orelse return error.RocksDBError);
    const vp: [*]const u8 = @ptrCast(c.rocksdb_iter_value(iter, &vlen) orelse return error.RocksDBError);
    if (klen < 8) {
        c.rocksdb_iter_next(iter);
        return .skipped;
    }
    const bn = std.mem.readInt(u64, kp[0..8], .big);

    // Provisionally take the first row. It wins outright unless a duplicate
    // group turns up and a sibling matches the canonical hash.
    const have_row = vlen <= MAX_RAW_VALUE;
    if (have_row) {
        @memcpy(slot.raw_value[0..vlen], vp[0..vlen]);
        slot.raw_len = vlen;
    }
    var first_hash: [32]u8 = undefined;
    const first_has_hash = klen >= 40;
    if (first_has_hash) @memcpy(&first_hash, kp[8..40]);

    c.rocksdb_iter_next(iter);

    // Fast path: next row is a different block (or EOF), single-row block.
    if (!iterValid(iter) or (iterKeyBlock(iter) orelse (bn +% 1)) != bn) {
        if (!have_row) return .skipped;
        finalizeFilled(slot, bn);
        return .filled;
    }

    // Duplicate group: keep the row whose hash is canonical for this number.
    dup_blocks.* += 1;
    const p = provider orelse {
        std.debug.print("block {d} has reorg-duplicate receipt rows; rerun with --rpc <url> to resolve canonical.\n", .{bn});
        return error.DuplicateNeedsRpc;
    };
    const canon = (try p.getBlock(bn)) orelse return error.CanonicalBlockNotFound;

    var found = have_row and first_has_hash and std.mem.eql(u8, &first_hash, &canon.hash);
    while (iterValid(iter)) {
        var k2: usize = 0;
        const kp2: [*]const u8 = @ptrCast(c.rocksdb_iter_key(iter, &k2) orelse break);
        if (k2 < 8 or std.mem.readInt(u64, kp2[0..8], .big) != bn) break;
        if (!found and k2 >= 40 and std.mem.eql(u8, kp2[8..40], &canon.hash)) {
            var v2: usize = 0;
            const vp2: [*]const u8 = @ptrCast(c.rocksdb_iter_value(iter, &v2) orelse break);
            if (v2 <= MAX_RAW_VALUE) {
                @memcpy(slot.raw_value[0..v2], vp2[0..v2]);
                slot.raw_len = v2;
                found = true;
            }
        }
        c.rocksdb_iter_next(iter);
    }
    if (!found) {
        std.debug.print("block {d}: no receipt row matched the canonical hash among duplicates.\n", .{bn});
        return error.CanonicalRowMissing;
    }
    finalizeFilled(slot, bn);
    return .filled;
}

fn finalizeFilled(slot: *Slot, bn: u64) void {
    slot.block_number = bn;
    slot.has_error = false;
    slot.log_count = 0;
    slot.entry_len = 0;
    slot.state.store(Slot.FILLED, .release);
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

/// Peek the current iter key as a u64 BE block number. Null if the iter
/// is invalid or the key is shorter than 8 bytes.
fn iterKeyBlock(iter: Iterator) ?u64 {
    var klen: usize = 0;
    const key_ptr: [*]const u8 = @ptrCast(c.rocksdb_iter_key(iter, &klen) orelse return null);
    if (klen < 8) return null;
    return std.mem.readInt(u64, key_ptr[0..8], .big);
}

// ── Timestamp pass (sibling headers DB) ──────────────────────────────────

/// Thread entry: best-effort timestamp pass. Any failure (missing headers DB,
/// FD pressure, decode error) is logged and swallowed. Block timestamps then
/// fall back to `humanize.blockTimestamp`, so the log import is never blocked.
fn importHeaderTimestamps(
    headers_path: [*:0]const u8,
    output_path: []const u8,
    first_block: u64,
    cutoff: u64,
) void {
    const n = runHeaderTimestamps(headers_path, output_path, first_block, cutoff) catch |err| {
        std.debug.print("Timestamp pass skipped: {s} (timestamps fall back to the formula)\n", .{@errorName(err)});
        return;
    };
    if (n > 0) std.debug.print("Imported {d} block timestamps from headers\n", .{n});
}

/// Iterate Nethermind's `headers` DB over [start_block, cutoff], decode each
/// header's `timestamp` (RLP field 11) and write timestamps.bin. The headers
/// key is `block_number(8 BE) ++ block_hash(32)`, so iteration is in block
/// order like the receipts CF. Duplicate rows for one number (reorg orphans
/// near the unfinalized tip) collapse to the first seen.
fn runHeaderTimestamps(
    headers_path: [*:0]const u8,
    output_path: []const u8,
    first_block: u64,
    cutoff: u64,
) !u64 {
    var err: ?[*:0]u8 = null;
    const opts = c.rocksdb_options_create();
    defer c.rocksdb_options_destroy(opts);

    // headers has a single "default" CF, open read-only like the receipts DB.
    const cf_names = [_][*c]const u8{@ptrCast("default")};
    const cf_opts = [1]?*const c.rocksdb_options_t{opts};
    var cf_handles: [1]?*c.rocksdb_column_family_handle_t = .{null};
    const db = c.rocksdb_open_for_read_only_column_families(
        opts, headers_path, 1, &cf_names, &cf_opts, &cf_handles, 0, @ptrCast(&err),
    );
    try rocksErr(&err);
    if (db == null) return error.RocksDBError;
    defer c.rocksdb_close(db);
    defer for (&cf_handles) |h| if (h) |handle| c.rocksdb_column_family_handle_destroy(handle);
    const default_cf = cf_handles[0] orelse return error.RocksDBError;

    var dir = try std.fs.cwd().openDir(output_path, .{});
    defer dir.close();
    var ts_writer = try core.timestamps.TimestampWriter.open(dir, first_block);
    defer ts_writer.deinit();

    const read_opts = c.rocksdb_readoptions_create();
    defer c.rocksdb_readoptions_destroy(read_opts);
    c.rocksdb_readoptions_set_readahead_size(read_opts, 4 * 1024 * 1024);
    c.rocksdb_readoptions_set_fill_cache(read_opts, 0);
    c.rocksdb_readoptions_set_verify_checksums(read_opts, 0);

    const iter = c.rocksdb_create_iterator_cf(db, read_opts, default_cf);
    if (iter == null) return error.RocksDBError;
    defer c.rocksdb_iter_destroy(iter);

    // Resume from the first un-backfilled block so re-runs only fill the gap.
    const start_block = first_block + ts_writer.count;
    if (start_block > cutoff) return 0;
    var seek_key: [8]u8 = undefined;
    std.mem.writeInt(u64, &seek_key, start_block, .big);
    c.rocksdb_iter_seek(iter, @ptrCast(&seek_key), seek_key.len);

    var written: u64 = 0;
    var last: ?u64 = null;
    while (iterValid(iter)) : (c.rocksdb_iter_next(iter)) {
        const bn = iterKeyBlock(iter) orelse continue;
        if (bn > cutoff) break;
        if (bn < start_block) continue;
        if (last) |l| if (l == bn) continue;
        var vlen: usize = 0;
        const vp: [*]const u8 = @ptrCast(c.rocksdb_iter_value(iter, &vlen) orelse continue);
        const ts = rlp.headerTimestamp(vp[0..vlen]) catch continue;
        const ts32 = std.math.cast(u32, ts) orelse continue; // valid until 2106
        ts_writer.set(bn, ts32) catch continue;
        last = bn;
        written += 1;
    }
    try ts_writer.sync();
    return written;
}

// ── Entry points ─────────────────────────────────────────────────────────

pub fn main() !void {
    const alloc = std.heap.page_allocator;
    const args = try std.process.argsAlloc(alloc);
    if (args.len < 3) {
        std.debug.print(
            "Usage: rocksdb-import <receipts_db_path> <data_dir> [--start N] [--end N] [--rpc URL]\n",
            .{},
        );
        std.process.exit(1);
    }
    // Flags after the two required positionals. --start/--end bound the import
    // (debug/isolation). --rpc resolves the canonical row for blocks carrying
    // reorg-history receipt duplicates.
    var start_override: ?u64 = null;
    var end_override: ?u64 = null;
    var rpc_url: ?[:0]const u8 = null;
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--start") and i + 1 < args.len) {
            start_override = try std.fmt.parseInt(u64, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--end") and i + 1 < args.len) {
            end_override = try std.fmt.parseInt(u64, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--rpc") and i + 1 < args.len) {
            rpc_url = args[i + 1];
            i += 1;
        }
    }
    try run(args[1], args[2], start_override, end_override, rpc_url);
}

/// Wire up an optional RPC provider (kept alive for the whole import) and run.
pub fn run(
    receipts_path: [*:0]const u8,
    output_path: []const u8,
    start_override: ?u64,
    end_override: ?u64,
    rpc_url: ?[:0]const u8,
) !void {
    const allocator = std.heap.page_allocator;
    if (rpc_url) |url| {
        var http = eth.http_transport.HttpTransport.init(allocator, url);
        var provider = eth.provider.Provider.init(allocator, &http);
        try runInner(receipts_path, output_path, start_override, end_override, &provider);
    } else {
        try runInner(receipts_path, output_path, start_override, end_override, null);
    }
}

fn runInner(
    receipts_path: [*:0]const u8,
    output_path: []const u8,
    start_override: ?u64,
    end_override: ?u64,
    provider: ?*eth.provider.Provider,
) !void {
    const allocator = std.heap.page_allocator;

    // Open Nethermind's receipts DB read-only with column families.
    // "Blocks" CF holds receipts keyed by block number (u64 BE).
    var err: ?[*:0]u8 = null;
    const opts = c.rocksdb_options_create();
    defer c.rocksdb_options_destroy(opts);

    const cf_names = [_][*c]const u8{ @ptrCast("default"), @ptrCast("Transactions"), @ptrCast("Blocks") };
    const cf_opts = [3]?*const c.rocksdb_options_t{ opts, opts, opts };
    var cf_handles: [3]?*c.rocksdb_column_family_handle_t = .{ null, null, null };

    const db = c.rocksdb_open_for_read_only_column_families(
        opts, receipts_path, 3, &cf_names, &cf_opts, &cf_handles, 0, @ptrCast(&err),
    );
    try rocksErr(&err);
    if (db == null) return error.RocksDBError;
    defer c.rocksdb_close(db);
    defer for (&cf_handles) |h| if (h) |handle| c.rocksdb_column_family_handle_destroy(handle);

    const blocks_cf = cf_handles[2] orelse return error.RocksDBError;

    std.fs.cwd().makeDir(output_path) catch {};
    var writer = try flat_writer.FlatStoreWriter.open(output_path);
    defer writer.deinit();

    // Sequential bulk read: 4 MB readahead, skip block cache and checksums
    const read_opts = c.rocksdb_readoptions_create();
    defer c.rocksdb_readoptions_destroy(read_opts);
    c.rocksdb_readoptions_set_readahead_size(read_opts, 4 * 1024 * 1024);
    c.rocksdb_readoptions_set_fill_cache(read_opts, 0);
    c.rocksdb_readoptions_set_verify_checksums(read_opts, 0);

    const iter = c.rocksdb_create_iterator_cf(db, read_opts, blocks_cf);
    if (iter == null) return error.RocksDBError;
    defer c.rocksdb_iter_destroy(iter);

    // Cap import at head - FINALITY_DEPTH so the flat store holds only
    // finalized blocks. The pending ring (head_follower) fills the trailing
    // edge canonically.
    c.rocksdb_iter_seek_to_last(iter);
    if (!iterValid(iter)) {
        std.debug.print("Receipts DB is empty; nothing to import.\n", .{});
        return;
    }
    const head = iterKeyBlock(iter) orelse return error.InvalidKey;
    const head_cutoff: u64 = if (head > FINALITY_DEPTH) head - FINALITY_DEPTH else 0;
    // An explicit end caps the import below finality for bounded/isolation runs.
    const finality_cutoff: u64 = if (end_override) |e| @min(head_cutoff, e) else head_cutoff;
    std.debug.print("Chain head: {d}; finality cutoff: {d} (head - {d}).\n", .{ head, finality_cutoff, FINALITY_DEPTH });

    // Backfill timestamps.bin from the sibling `headers` DB over the store's
    // full finalized range, concurrent with the receipts decode (different DB,
    // different output file). Runs even when the log import is caught up, so an
    // existing store still gets its historical timestamps. Resumes from the
    // first un-backfilled block. Best-effort: any failure leaves block
    // timestamps on the formula fallback.
    const ts_first_block: u64 = if (writer.meta.blocks_idx_count > 0)
        writer.first_block
    else
        (start_override orelse core.types.MERGE_BLOCK);
    const receipts_span = std.mem.span(receipts_path);
    const headers_path: ?[:0]u8 = if (std.fs.path.dirname(receipts_span)) |parent|
        std.mem.concatWithSentinel(allocator, u8, &.{ parent, "/headers" }, 0) catch null
    else
        null;
    defer if (headers_path) |hp| allocator.free(hp);
    var ts_thread: ?std.Thread = null;
    if (headers_path) |hp| {
        ts_thread = std.Thread.spawn(.{}, importHeaderTimestamps, .{
            @as([*:0]const u8, hp.ptr), output_path, ts_first_block, finality_cutoff,
        }) catch null;
    }

    // Resume from where we left off, else start at the merge (or an explicit
    // override for bounded runs). Pre-merge events are rarely an indexing
    // target. Users who need them should pre-seed the data dir.
    const start_block: u64 = start_override orelse if (writer.meta.last_finalized_block > 0)
        writer.meta.last_finalized_block + 1
    else
        core.types.MERGE_BLOCK;
    if (start_block > finality_cutoff) {
        std.debug.print(
            "Flat store already covers the finalized prefix (last_finalized={d}); only timestamps backfilled.\n",
            .{writer.meta.last_finalized_block},
        );
        if (ts_thread) |t| t.join();
        return;
    }

    if (start_block > 0) {
        var resume_key: [8]u8 = undefined;
        std.mem.writeInt(u64, &resume_key, start_block, .big);
        c.rocksdb_iter_seek(iter, @ptrCast(&resume_key), resume_key.len);
        std.debug.print("Resuming from block {} (flat store size: {} bytes)\n", .{ start_block, writer.meta.blocks_dat_size });
    } else {
        c.rocksdb_iter_seek_to_first(iter);
    }

    const slots = try allocator.create([SLOT_COUNT]Slot);
    defer allocator.destroy(slots);
    for (slots) |*s| s.* = Slot{};

    const t_start = std.time.nanoTimestamp();
    var blocks_processed: u64 = 0;
    var blocks_with_logs: u64 = 0;
    var total_logs: u64 = 0;
    var decode_errors: u64 = 0;
    var skipped_raw: u64 = 0;
    var dup_blocks: u64 = 0;

    std.debug.print("Importing receipts ({} workers) from {s} → {s}\n", .{
        parallel.MAX_WORKERS, std.mem.span(receipts_path), output_path,
    });

    var worker_args: [parallel.MAX_WORKERS]WorkerArgs = undefined;
    var workers: [parallel.MAX_WORKERS]std.Thread = undefined;
    for (0..parallel.MAX_WORKERS) |i| {
        worker_args[i] = .{ .slots = slots, .worker_id = i };
        workers[i] = try std.Thread.spawn(.{ .stack_size = parallel.WORKER_STACK_SIZE }, workerFn, .{&worker_args[i]});
    }

    // Reader + writer loop. `read_done` flips once consumed past
    // `finality_cutoff` or the iter is exhausted. The drain phase then runs
    // until in-flight slots are flushed.
    var read_cursor: usize = 0;
    var write_cursor: usize = 0;
    var read_done = false;

    while (write_cursor < read_cursor or (!read_done and iterValid(iter))) {
        if (!read_done) {
            while (read_cursor - write_cursor < SLOT_COUNT and iterValid(iter)) {
                if (iterKeyBlock(iter)) |bn| {
                    if (bn > finality_cutoff) {
                        read_done = true;
                        break;
                    }
                }
                const next_slot = &slots[read_cursor % SLOT_COUNT];
                if (next_slot.state.load(.acquire) != Slot.EMPTY) break;
                // fillSlotCanonical advances the iterator past the block it
                // consumes (collapsing reorg-duplicate rows to the canonical one).
                switch (try fillSlotCanonical(next_slot, iter, provider, &dup_blocks)) {
                    .filled => read_cursor += 1,
                    .skipped => skipped_raw += 1,
                }
            }
            if (!iterValid(iter)) read_done = true;
        }

        if (write_cursor >= read_cursor) {
            if (read_done) break;
            std.atomic.spinLoopHint();
            continue;
        }

        const slot = &slots[write_cursor % SLOT_COUNT];
        while (slot.state.load(.acquire) != Slot.DONE) std.atomic.spinLoopHint();

        if (slot.has_error) {
            decode_errors += 1;
        } else if (slot.log_count > 0) {
            blocks_with_logs += 1;
            total_logs += slot.log_count;
        }
        // Append every block, including no-log and (post-loop-fatal) errored
        // ones, so the flat store's dense index never skips a block number.
        try writer.appendBlock(slot.block_number, slot.entry[0..slot.entry_len], &slot.topic_bloom, &slot.addr_bloom);

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

    // Signal workers to exit (max block_number sentinel), then join.
    slots[0].block_number = std.math.maxInt(u64);
    for (&workers) |*w| w.join();
    if (ts_thread) |t| t.join();

    try writer.finalize();

    const elapsed_s = @as(f64, @floatFromInt(std.time.nanoTimestamp() - t_start)) / 1e9;
    std.debug.print(
        \\
        \\=== Import Complete ===
        \\Blocks processed:  {d}
        \\Blocks with logs:  {d}
        \\Total logs:        {d}
        \\Decode errors:     {d}
        \\Reorg dups solved: {d}
        \\Elapsed:           {d:.1}s
        \\
    , .{ blocks_processed, blocks_with_logs, total_logs, decode_errors, dup_blocks, elapsed_s });
    if (elapsed_s > 0) {
        std.debug.print("Blocks/sec:        {d:.0}\nLogs/sec:          {d:.0}\n", .{
            @as(f64, @floatFromInt(blocks_processed)) / elapsed_s,
            @as(f64, @floatFromInt(total_logs)) / elapsed_s,
        });
    }

    if (skipped_raw > 0) {
        std.debug.print(
            "\nERROR: skipped {d} RocksDB entries (short key or oversized value > {d}B). " ++
                "Flat store is incomplete.\n",
            .{ skipped_raw, MAX_RAW_VALUE },
        );
        return error.ImportSkippedEntries;
    }

    if (decode_errors > 0) {
        std.debug.print(
            "\nERROR: {d} blocks failed to decode — flat store is incomplete. " ++
                "Likely MAX_LOGS_PER_BLOCK in core/src/types.zig.\n",
            .{decode_errors},
        );
        return error.ImportHadDecodeErrors;
    }
}
