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
const tx_decode = @import("tx_decode.zig");

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

/// Mark `slot` failed: write a canonical empty entry (so the dense index stays
/// aligned), flag the error, and publish DONE. Both worker error paths (decode,
/// compress) share this so the failure protocol has a single definition.
fn failSlot(slot: *Slot, serialize_buf: []u8) void {
    writeEmptyEntry(slot, serialize_buf);
    slot.has_error = true;
    slot.state.store(Slot.DONE, .release);
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
                slot.block_number,
                slot.raw_value[0..slot.raw_len],
                &log_buf,
                &data_buf,
            ) catch |e| {
                core.log.err("decode error block {d}: {}\n", .{ slot.block_number, e });
                failSlot(slot, &serialize_buf);
                continue;
            };

            // No-log blocks still flow through here. packBlock writes a
            // zero-count entry, blooms come out empty, the writer appends it so
            // the dense block index stays contiguous. Skipping them corrupts
            // every later block's number.
            const pack = log_serial.packBlock(log_buf[0..log_count], &serialize_buf, &slot.entry) catch {
                core.log.err("compress error block {d}\n", .{slot.block_number});
                failSlot(slot, &serialize_buf);
                continue;
            };

            slot.entry_len = pack.entry_len;
            slot.topic_bloom = pack.topic_bloom.bits;
            slot.addr_bloom = pack.addr_bloom.bits;
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
        // An oversized row must fail loud here. Skipping leaves a hole in the
        // dense range and the import dies blocks later as a misleading
        // NonDenseAppend with the wrong block number.
        if (!have_row) {
            core.log.err("block {d}: receipt row is {d} bytes, over the {d} byte buffer. Bump MAX_RAW_VALUE.\n", .{ bn, vlen, MAX_RAW_VALUE });
            return error.ReceiptValueTooLarge;
        }
        finalizeFilled(slot, bn);
        return .filled;
    }

    // Duplicate group: keep the row whose hash is canonical for this number.
    dup_blocks.* += 1;
    const p = provider orelse {
        core.log.err("block {d} has reorg-duplicate receipt rows; rerun with --rpc <url> to resolve canonical.\n", .{bn});
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
        core.log.err("block {d}: no receipt row matched the canonical hash among duplicates.\n", .{bn});
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

/// Read-only RocksDB handle plus a bulk-read iterator over one column family.
/// Shared by the receipts pass and the headers timestamp pass. Bulk-read
/// options: 4 MB readahead, no block cache, no checksum verification.
/// `target` picks the CF the iterator walks. `deinit` releases everything.
fn ReadOnlyCf(comptime N: usize) type {
    return struct {
        opts: ?*c.rocksdb_options_t,
        read_opts: ?*c.rocksdb_readoptions_t,
        db: ?*c.rocksdb_t,
        cf_handles: [N]?*c.rocksdb_column_family_handle_t,
        iter: Iterator,

        /// `verify_checksums`: off for the sequential bulk imports (speed,
        /// downstream parity-validated), on for the tx pass's point reads
        /// where corruption would decode silently wrong.
        fn open(path: [*:0]const u8, cf_names: [N][*c]const u8, target: usize, verify_checksums: bool) !@This() {
            var err: ?[*:0]u8 = null;
            const opts = c.rocksdb_options_create();
            errdefer c.rocksdb_options_destroy(opts);

            var cf_opts: [N]?*const c.rocksdb_options_t = undefined;
            for (&cf_opts) |*o| o.* = opts;
            var cf_handles: [N]?*c.rocksdb_column_family_handle_t = .{null} ** N;
            const db = c.rocksdb_open_for_read_only_column_families(
                opts,
                path,
                N,
                &cf_names,
                &cf_opts,
                &cf_handles,
                0,
                @ptrCast(&err),
            );
            try rocksErr(&err);
            if (db == null) return error.RocksDBError;
            errdefer c.rocksdb_close(db);
            errdefer for (&cf_handles) |h| if (h) |handle| c.rocksdb_column_family_handle_destroy(handle);
            const cf = cf_handles[target] orelse return error.RocksDBError;

            const read_opts = c.rocksdb_readoptions_create();
            errdefer c.rocksdb_readoptions_destroy(read_opts);
            c.rocksdb_readoptions_set_readahead_size(read_opts, 4 * 1024 * 1024);
            c.rocksdb_readoptions_set_fill_cache(read_opts, 0);
            c.rocksdb_readoptions_set_verify_checksums(read_opts, @intFromBool(verify_checksums));

            const iter = c.rocksdb_create_iterator_cf(db, read_opts, cf);
            if (iter == null) return error.RocksDBError;

            return .{ .opts = opts, .read_opts = read_opts, .db = db, .cf_handles = cf_handles, .iter = iter };
        }

        fn deinit(self: *@This()) void {
            c.rocksdb_iter_destroy(self.iter);
            for (&self.cf_handles) |h| if (h) |handle| c.rocksdb_column_family_handle_destroy(handle);
            c.rocksdb_close(self.db);
            c.rocksdb_readoptions_destroy(self.read_opts);
            c.rocksdb_options_destroy(self.opts);
        }
    };
}

/// Convert a RocksDB error pointer to a Zig error. Frees the C string.
fn rocksErr(err_ptr: *?[*:0]u8) !void {
    if (err_ptr.*) |e| {
        core.log.err("RocksDB error: {s}\n", .{std.mem.span(e)});
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
        core.log.info("Timestamp pass skipped: {s} (timestamps fall back to the formula)\n", .{@errorName(err)});
        return;
    };
    if (n > 0) core.log.info("Imported {d} block timestamps from headers\n", .{n});
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
    // headers has a single "default" CF, open read-only like the receipts DB.
    var rdb = try ReadOnlyCf(1).open(headers_path, .{@ptrCast("default")}, 0, false);
    defer rdb.deinit();
    const iter = rdb.iter;

    var dir = try std.fs.cwd().openDir(output_path, .{});
    defer dir.close();
    var ts_writer = try core.timestamps.TimestampWriter.open(dir, first_block);
    defer ts_writer.deinit();

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

// ── Transaction-fields pass (receipts senders + sibling blocks DB) ───────

/// Structural ceiling, not an assumption: tx_index is u16 in every format
/// (RawLog, TxRecord), so 65,536 is the domain itself. Buffers size to it
/// (~21 MB once, import binary only) so no plausible block can overflow.
/// `decodeSenders` still fails loud at the boundary rather than dropping.
const MAX_TXS_PER_BLOCK = 65_536;

/// Main-thread scratch for the tx pass, heap-allocated once.
const TxPassBufs = struct {
    senders: []receipt_decoder.TxSenderInfo,
    serialize_buf: []u8,
    compress_buf: []u8,
    // Dup-group resolution: candidate receipt logs vs our store's logs.
    cand_logs: []core.RawLog,
    cand_data: []u8,
    store_entry: []u8,
    store_decomp: []u8,
    store_logs: []core.RawLog,

    fn init(a: std.mem.Allocator) !TxPassBufs {
        const table_max = 4 + MAX_TXS_PER_BLOCK * core.txs.RECORD_SIZE;
        return .{
            .senders = try a.alloc(receipt_decoder.TxSenderInfo, MAX_TXS_PER_BLOCK),
            .serialize_buf = try a.alloc(u8, table_max),
            .compress_buf = try a.alloc(u8, table_max + table_max / 128 + 64),
            .cand_logs = try a.alloc(core.RawLog, core.types.MAX_LOGS_PER_BLOCK),
            .cand_data = try a.alloc(u8, core.types.BLOCK_BUF_SIZE),
            .store_entry = try a.alloc(u8, core.types.BLOCK_BUF_SIZE),
            .store_decomp = try a.alloc(u8, core.types.BLOCK_BUF_SIZE),
            .store_logs = try a.alloc(core.RawLog, core.types.MAX_LOGS_PER_BLOCK),
        };
    }
};

/// Per-block alloc/free at pipeline rate (slot senders + records).
const tx_alloc = std.heap.smp_allocator;

/// Recovery worker count. Wider than the I/O-tuned `parallel.MAX_WORKERS`:
/// the pass is ecrecover-bound pure compute, so use the logical cores minus
/// the reader/drain thread.
const TX_WORKERS = 10;

/// Recovery pipeline slot. EMPTY → FILLED (main: receipts walk + bodies get)
/// → DONE (owner worker: decode + recover) → EMPTY (main: ordered drain into
/// TxsWriter). Worker `slot_index % TX_WORKERS` owns the FILLED→DONE edge,
/// so slots are single-writer at every transition. Ring bound: ≤ 128 bodies
/// (~25 MB typical) in flight.
const TX_SLOT_COUNT = 128;

const TxSlot = struct {
    state: std.atomic.Value(u8) = .init(EMPTY),
    block_number: u64 = 0,
    /// RocksDB-owned body bytes, freed at drain. Empty for no-log blocks
    /// (those skip the workers entirely).
    body: []const u8 = &.{},
    senders: []receipt_decoder.TxSenderInfo = &.{},
    records: []core.txs.TxRecord = &.{},
    rec_count: usize = 0,
    unrecovered: u32 = 0,
    err: ?anyerror = null,

    const EMPTY: u8 = 0;
    const FILLED: u8 = 1;
    const DONE: u8 = 2;
};

const TxWorkerArgs = struct {
    slots: []TxSlot,
    worker_id: usize,
    shutdown: *const std.atomic.Value(bool),
};

fn txWorker(args: *TxWorkerArgs) void {
    // 4 MB stack scratch, safe under the core.parallel worker-stack rule.
    // 8-aligned for the XKCP keccak's u64 lane loads.
    var scratch: [core.types.BLOCK_BUF_SIZE]u8 align(8) = undefined;
    while (true) {
        var idle = true;
        var i: usize = args.worker_id;
        while (i < args.slots.len) : (i += TX_WORKERS) {
            const slot = &args.slots[i];
            if (slot.state.load(.acquire) != TxSlot.FILLED) continue;
            processTxSlot(slot, &scratch) catch |e| {
                slot.err = e;
            };
            slot.state.store(TxSlot.DONE, .release);
            idle = false;
        }
        if (idle) {
            if (args.shutdown.load(.acquire)) return;
            std.atomic.spinLoopHint();
        }
    }
}

/// Decode + recover every log-producing tx in the slot's body. Walks every
/// body tx so a count mismatch in either direction fails loud (the
/// no-silent-caps rule).
fn processTxSlot(slot: *TxSlot, scratch: []align(8) u8) !void {
    var it = try tx_decode.BodyTxs.init(slot.body);
    var tx_index: usize = 0;
    var rec_count: usize = 0;
    while (try it.next()) |raw| : (tx_index += 1) {
        if (tx_index >= slot.senders.len) continue; // counted, checked below
        const info = slot.senders[tx_index];
        if (!info.has_logs) continue;

        var rec = core.txs.TxRecord{
            .tx_index = @intCast(tx_index),
            .tx_type = undefined,
            .flags = 0,
            .from = info.sender,
            .to = undefined,
            .value = undefined,
        };
        var f: tx_decode.TxFields = undefined;
        if (info.has_sender) {
            // Stored sender (non-compact receipt formats). Fields only.
            f = try tx_decode.decode(raw);
        } else {
            // Compact rows leave the sender slot empty: splice + recover.
            const st = try tx_decode.decodeSigned(raw, scratch);
            f = st.fields;
            if (tx_decode.recoverSender(st.sig, st.sighash)) |sender| {
                rec.from = sender;
            } else |_| {
                // On-chain txs always recover. Flag rather than abort so one
                // anomalous row cannot stall the backfill.
                rec.flags |= core.txs.FLAG_FROM_UNRECOVERED;
                slot.unrecovered += 1;
            }
        }
        rec.tx_type = f.tx_type;
        rec.to = f.to orelse std.mem.zeroes([20]u8);
        if (f.to == null) rec.flags |= core.txs.FLAG_TO_ABSENT;
        std.mem.writeInt(u256, &rec.value, f.value, .little);
        slot.records[rec_count] = rec;
        rec_count += 1;
    }
    if (tx_index != slot.senders.len) return error.TxCountMismatch;
    slot.rec_count = rec_count;
}

/// In-order drain cursor + counters for the tx pipeline.
const TxDrain = struct {
    next: u64,
    start: u64,
    total: u64,
    t_start: i128,
    unrecovered: u64 = 0,
};

/// Append every consecutively-DONE slot from the drain cursor into the
/// writer, releasing slots back to EMPTY. Returns without blocking when the
/// next block is still in flight.
fn drainTxSlots(d: *TxDrain, slots: []TxSlot, txw: *core.txs.TxsWriter, bufs: *const TxPassBufs) !void {
    while (true) {
        const slot = &slots[@intCast((d.next - d.start) % TX_SLOT_COUNT)];
        if (slot.state.load(.acquire) != TxSlot.DONE or slot.block_number != d.next) return;
        if (slot.err) |e| {
            core.log.err("Tx fields: block {d}: {s}\n", .{ slot.block_number, @errorName(e) });
            return e;
        }
        try txw.append(slot.block_number, slot.records[0..slot.rec_count], bufs.serialize_buf, bufs.compress_buf);
        d.unrecovered += slot.unrecovered;
        releaseTxSlot(slot);
        d.next += 1;

        const done = d.next - d.start;
        if ((done % 10_000) == 0) try txw.sync();
        if ((done % 100_000) == 0) {
            const elapsed_s = @as(f64, @floatFromInt(std.time.nanoTimestamp() - d.t_start)) / 1e9;
            core.log.info("  tx fields {d}/{d} blocks | {d:.0} blk/s\n", .{ done, d.total, @as(f64, @floatFromInt(done)) / elapsed_s });
        }
    }
}

fn releaseTxSlot(slot: *TxSlot) void {
    if (slot.body.len > 0) c.rocksdb_free(@constCast(@ptrCast(slot.body.ptr)));
    if (slot.senders.len > 0) tx_alloc.free(slot.senders);
    if (slot.records.len > 0) tx_alloc.free(slot.records);
    slot.body = &.{};
    slot.senders = &.{};
    slot.records = &.{};
    slot.rec_count = 0;
    slot.unrecovered = 0;
    slot.err = null;
    slot.state.store(TxSlot.EMPTY, .release);
}

/// Best-effort wrapper, the timestamps-pass contract: the artifact is
/// advisory, a failed pass logs loud and leaves the resume cursor where it
/// stopped. A re-run picks up the gap. Manifests requiring tx fields fail
/// loud SDK-side on incomplete coverage.
fn importTxFields(receipts_path: [*:0]const u8, blocks_path: [*:0]const u8, output_path: []const u8) void {
    const n = runTxFields(receipts_path, blocks_path, output_path) catch |err| {
        core.log.err("Tx-fields pass failed: {s}. Re-run import to resume (tx fields unavailable until covered).\n", .{@errorName(err)});
        return;
    };
    if (n > 0) core.log.info("Tx fields: {d} blocks covered\n", .{n});
}

/// Walk the receipts CF over the store's range, pairing each canonical row
/// with a bodies point-get (same 40-byte `bn ‖ hash` key), and emit per-block
/// `TxRecord` tables into txs.{dat,idx}. Senders come from the receipt rows,
/// `to`/`value` from the body RLP. Resumes from the txs.idx cursor. Single
/// threaded, I/O-bound on the bodies blob reads.
fn runTxFields(receipts_path: [*:0]const u8, blocks_path: [*:0]const u8, output_path: []const u8) !u64 {
    var store = try core.FlatStoreReader.open(output_path);
    defer store.deinit();
    if (store.index_count == 0) return 0;
    const store_end = store.first_block + store.index_count - 1;

    var dir = try std.fs.cwd().openDir(output_path, .{});
    defer dir.close();
    var txw = try core.txs.TxsWriter.open(dir, store.first_block);
    defer txw.deinit();

    const start_block = store.first_block + txw.count;
    if (start_block > store_end) return 0;

    var receipts = try ReadOnlyCf(3).open(
        receipts_path,
        .{ @ptrCast("default"), @ptrCast("Transactions"), @ptrCast("Blocks") },
        2,
        true,
    );
    defer receipts.deinit();
    const iter = receipts.iter;

    var bodies = try ReadOnlyCf(1).open(blocks_path, .{@ptrCast("default")}, 0, true);
    defer bodies.deinit();
    const bodies_cf = bodies.cf_handles[0];

    const bufs = try TxPassBufs.init(std.heap.page_allocator);

    const slots = try std.heap.page_allocator.alloc(TxSlot, TX_SLOT_COUNT);
    defer std.heap.page_allocator.free(slots);
    for (slots) |*s| s.* = .{};

    var shutdown = std.atomic.Value(bool).init(false);
    var worker_args: [TX_WORKERS]TxWorkerArgs = undefined;
    var workers: [TX_WORKERS]std.Thread = undefined;
    var spawned: usize = 0;
    // Any error below must stop + join before the defers free slot memory
    // under spinning workers. LIFO order: this runs before the slot free.
    var joined = false;
    defer if (!joined) {
        shutdown.store(true, .release);
        for (workers[0..spawned]) |w| w.join();
    };
    for (0..TX_WORKERS) |w| {
        worker_args[w] = .{ .slots = slots, .worker_id = w, .shutdown = &shutdown };
        workers[w] = try std.Thread.spawn(.{ .stack_size = parallel.WORKER_STACK_SIZE }, txWorker, .{&worker_args[w]});
        spawned += 1;
    }

    var seek_key: [8]u8 = undefined;
    std.mem.writeInt(u64, &seek_key, start_block, .big);
    c.rocksdb_iter_seek(iter, @ptrCast(&seek_key), seek_key.len);

    core.log.info("Tx fields: covering blocks {d} → {d}\n", .{ start_block, store_end });
    var d = TxDrain{
        .next = start_block,
        .start = start_block,
        .total = store_end - start_block + 1,
        .t_start = std.time.nanoTimestamp(),
    };
    var expected = start_block;
    var dup_groups: u64 = 0;

    while (iterValid(iter)) {
        const bn = iterKeyBlock(iter) orelse {
            c.rocksdb_iter_next(iter);
            continue;
        };
        if (bn > store_end) break;
        if (bn < expected) {
            // Trailing duplicate rows of an already-written block.
            c.rocksdb_iter_next(iter);
            continue;
        }
        if (bn > expected) {
            core.log.err("Tx fields: receipts gap at block {d} (next row {d})\n", .{ expected, bn });
            return error.NonDenseReceipts;
        }

        var hash = iterKeyHash(iter) orelse return error.ReceiptKeyMissingHash;
        var vlen: usize = 0;
        const vp: [*]const u8 = @ptrCast(c.rocksdb_iter_value(iter, &vlen) orelse return error.RocksDBError);
        var n_tx = try receipt_decoder.decodeSenders(vp[0..vlen], bufs.senders);
        c.rocksdb_iter_next(iter);

        // Reorg-history duplicate rows: pick the row whose logs match our
        // store (resolved canonical at import), no RPC needed.
        if (iterValid(iter) and (iterKeyBlock(iter) orelse 0) == bn) {
            dup_groups += 1;
            const canon = try resolveDupGroup(iter, bn, &store, &bufs, bufs.senders);
            n_tx = canon.n_tx;
            hash = canon.hash;
        }

        var n_logtx: usize = 0;
        for (bufs.senders[0..n_tx]) |s| {
            if (s.has_logs) n_logtx += 1;
        }

        // Claim the block's ring slot, draining completed blocks while the
        // ring is full.
        const slot = &slots[@intCast((expected - start_block) % TX_SLOT_COUNT)];
        while (slot.state.load(.acquire) != TxSlot.EMPTY) {
            try drainTxSlots(&d, slots, &txw, &bufs);
        }
        slot.block_number = expected;
        if (n_logtx == 0) {
            // Nothing to decode or recover: skip the bodies get and the
            // workers, the drain writes the empty table.
            slot.state.store(TxSlot.DONE, .release);
        } else {
            slot.body = try getBody(bodies.db, bodies.read_opts, bodies_cf, bn, hash);
            slot.senders = try tx_alloc.dupe(receipt_decoder.TxSenderInfo, bufs.senders[0..n_tx]);
            slot.records = try tx_alloc.alloc(core.txs.TxRecord, n_logtx);
            slot.state.store(TxSlot.FILLED, .release);
        }
        expected += 1;
        try drainTxSlots(&d, slots, &txw, &bufs);
    }

    // Drain the in-flight tail, then stop the workers.
    while (d.next < expected) {
        try drainTxSlots(&d, slots, &txw, &bufs);
        std.atomic.spinLoopHint();
    }
    shutdown.store(true, .release);
    for (workers[0..spawned]) |w| w.join();
    joined = true;

    if (dup_groups > 0) core.log.info("Tx fields: {d} duplicate receipt groups resolved against the store\n", .{dup_groups});
    if (d.unrecovered > 0) core.log.err("Tx fields: {d} senders failed recovery (flagged FROM_UNRECOVERED)\n", .{d.unrecovered});
    if (expected <= store_end)
        core.log.info("Tx fields: receipts ended at block {d}, store covers {d}. Re-run after the next import.\n", .{ expected - 1, store_end });
    try txw.sync();
    return expected - start_block;
}

const CanonicalRow = struct { n_tx: usize, hash: [32]u8 };

/// Iter positioned at the second row of `bn`'s duplicate group. Re-seek to
/// the group start, pick the row whose decoded logs match our store's block
/// entry, and leave the iter on the first row past the group.
fn resolveDupGroup(
    iter: Iterator,
    bn: u64,
    store: *const core.FlatStoreReader,
    bufs: *const TxPassBufs,
    senders_out: []receipt_decoder.TxSenderInfo,
) !CanonicalRow {
    var seek_key: [8]u8 = undefined;
    std.mem.writeInt(u64, &seek_key, bn, .big);
    c.rocksdb_iter_seek(iter, @ptrCast(&seek_key), seek_key.len);

    var result: ?CanonicalRow = null;
    while (iterValid(iter)) : (c.rocksdb_iter_next(iter)) {
        if ((iterKeyBlock(iter) orelse 0) != bn) break;
        if (result != null) continue; // drain the rest of the group
        const hash = iterKeyHash(iter) orelse continue;
        var vlen: usize = 0;
        const vp: [*]const u8 = @ptrCast(c.rocksdb_iter_value(iter, &vlen) orelse continue);
        const row = vp[0..vlen];
        if (try rowMatchesStore(bn, row, store, bufs)) {
            result = .{ .n_tx = try receipt_decoder.decodeSenders(row, senders_out), .hash = hash };
        }
    }
    if (result) |r| return r;
    core.log.err("Tx fields: block {d}: no duplicate receipt row matches the store\n", .{bn});
    return error.CanonicalRowMissing;
}

/// A candidate receipt row matches when its log shape equals our store's
/// entry for the block: same count, same (tx_index, log_index, address) per
/// log. The store was canonical-resolved at import, so a match is canonical.
fn rowMatchesStore(
    bn: u64,
    row: []const u8,
    store: *const core.FlatStoreReader,
    bufs: *const TxPassBufs,
) !bool {
    const cand_n = receipt_decoder.decodeReceipts(bn, row, bufs.cand_logs, bufs.cand_data) catch return false;
    const entry = store.readBlock(bn, bufs.store_entry) catch return false;
    const decoded = core.log_serial.decompressEntry(entry, bufs.store_decomp) catch return false;
    const store_n = core.log_serial.deserializeLogs(decoded, bufs.store_logs);
    if (cand_n != store_n) return false;
    for (bufs.cand_logs[0..cand_n], bufs.store_logs[0..store_n]) |a, b| {
        if (a.tx_index != b.tx_index or a.log_index != b.log_index) return false;
        if (!std.mem.eql(u8, &a.address, &b.address)) return false;
    }
    return true;
}

/// Point-get one block body by the shared `bn(8 BE) ‖ hash(32)` key. Caller
/// frees via `rocksdb_free`.
fn getBody(
    db: ?*c.rocksdb_t,
    read_opts: ?*c.rocksdb_readoptions_t,
    cf: ?*c.rocksdb_column_family_handle_t,
    bn: u64,
    hash: [32]u8,
) ![]const u8 {
    var key: [40]u8 = undefined;
    std.mem.writeInt(u64, key[0..8], bn, .big);
    @memcpy(key[8..40], &hash);
    var err: ?[*:0]u8 = null;
    var vlen: usize = 0;
    const vp = c.rocksdb_get_cf(db, read_opts, cf, @ptrCast(&key), key.len, &vlen, @ptrCast(&err));
    try rocksErr(&err);
    if (vp == null) {
        core.log.err("Tx fields: block {d} body missing from the blocks DB\n", .{bn});
        return error.BodyMissing;
    }
    return @as([*]const u8, @ptrCast(vp))[0..vlen];
}

/// Current iter key's block hash (bytes 8..40 of the 40-byte key).
fn iterKeyHash(iter: Iterator) ?[32]u8 {
    var klen: usize = 0;
    const kp: [*]const u8 = @ptrCast(c.rocksdb_iter_key(iter, &klen) orelse return null);
    if (klen < 40) return null;
    return kp[8..40].*;
}

/// Best-effort RLIMIT_NOFILE raise to the hard cap. The BlobDB open touches
/// every blob file. A failure leaves the prior limit, surfacing later as a
/// loud open error.
fn raiseFdLimit() void {
    const lim = std.posix.getrlimit(.NOFILE) catch return;
    if (lim.cur >= lim.max) return;
    std.posix.setrlimit(.NOFILE, .{ .cur = lim.max, .max = lim.max }) catch {};
}

// ── Entry points ─────────────────────────────────────────────────────────

pub fn main() !void {
    // Every Nethermind DB this binary opens (receipts, headers, the BlobDB
    // blocks dir at ~4,600 files) counts against NOFILE. Non-interactive
    // shells default to 1024, far too low.
    raiseFdLimit();

    const alloc = std.heap.page_allocator;
    const args = try std.process.argsAlloc(alloc);
    if (args.len < 3) {
        core.log.err(
            "Usage: rocksdb-import <receipts_db_path> <data_dir> [--start N] [--end N] [--rpc URL] [--txs-db PATH] [--no-tx-fields]\n",
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
    var txs_db: ?[:0]const u8 = null;
    var no_tx_fields = false;
    var silent = false;
    var verbose = false;
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
        } else if (std.mem.eql(u8, args[i], "--txs-db") and i + 1 < args.len) {
            txs_db = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--no-tx-fields")) {
            no_tx_fields = true;
        } else if (std.mem.eql(u8, args[i], "--silent")) {
            silent = true;
        } else if (std.mem.eql(u8, args[i], "--verbose")) {
            verbose = true;
        }
    }
    core.log.setLevel(core.log.levelFromFlags(silent, verbose));
    try run(args[1], args[2], start_override, end_override, rpc_url);

    // Tx-fields pass after the logs import so dup resolution can match
    // candidate receipt rows against the freshly-written canonical store.
    if (!no_tx_fields) {
        const blocks_path = txs_db orelse blk: {
            // Default: the receipts DB's sibling `blocks` dir.
            const parent = std.fs.path.dirname(args[1]) orelse break :blk null;
            break :blk try std.fmt.allocPrintSentinel(alloc, "{s}/blocks", .{parent}, 0);
        };
        if (blocks_path) |bp| {
            importTxFields(args[1], bp, args[2]);
        } else {
            core.log.err("Tx fields skipped: cannot derive the blocks DB path, pass --txs-db\n", .{});
        }
    }
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
    var rdb = try ReadOnlyCf(3).open(
        receipts_path,
        .{ @ptrCast("default"), @ptrCast("Transactions"), @ptrCast("Blocks") },
        2,
        false,
    );
    defer rdb.deinit();
    const iter = rdb.iter;

    std.fs.cwd().makeDir(output_path) catch {};
    var writer = try flat_writer.FlatStoreWriter.open(output_path);
    defer writer.deinit();

    // Cap import at head - FINALITY_DEPTH so the flat store holds only
    // finalized blocks. The pending ring (head_follower) fills the trailing
    // edge canonically.
    c.rocksdb_iter_seek_to_last(iter);
    if (!iterValid(iter)) {
        core.log.info("Receipts DB is empty; nothing to import.\n", .{});
        return;
    }
    const head = iterKeyBlock(iter) orelse return error.InvalidKey;
    const head_cutoff: u64 = if (head > FINALITY_DEPTH) head - FINALITY_DEPTH else 0;
    // An explicit end caps the import below finality for bounded/isolation runs.
    const finality_cutoff: u64 = if (end_override) |e| @min(head_cutoff, e) else head_cutoff;
    core.log.info("Chain head: {d}; finality cutoff: {d} (head - {d}).\n", .{ head, finality_cutoff, FINALITY_DEPTH });

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
        core.log.info(
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
        core.log.info("Resuming from block {} (flat store size: {} bytes)\n", .{ start_block, writer.meta.blocks_dat_size });
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

    core.log.info("Importing receipts ({} workers) from {s} → {s}\n", .{
        parallel.MAX_WORKERS, std.mem.span(receipts_path), output_path,
    });

    var worker_args: [parallel.MAX_WORKERS]WorkerArgs = undefined;
    var workers: [parallel.MAX_WORKERS]std.Thread = undefined;
    var spawned: usize = 0;
    // Workers spin on `slots`. A post-spawn error returns through the
    // `defer allocator.destroy(slots)` above, freeing the ring out from under
    // them (use-after-free that corrupts the heap and masks the real error).
    // Stop and join first. errdefer runs before the destroy by LIFO order and
    // joins only the workers actually spawned.
    errdefer {
        slots[0].block_number = std.math.maxInt(u64);
        for (workers[0..spawned]) |*w| w.join();
    }
    for (0..parallel.MAX_WORKERS) |i| {
        worker_args[i] = .{ .slots = slots, .worker_id = i };
        workers[i] = try std.Thread.spawn(.{ .stack_size = parallel.WORKER_STACK_SIZE }, workerFn, .{&worker_args[i]});
        spawned += 1;
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
            core.log.info("  {d:>10} blocks | {d:>12} logs | {d:.1}s | {d:.0} blk/s | {d:.0} logs/s\n", .{
                blocks_processed,                                      total_logs,                                      elapsed_s,
                @as(f64, @floatFromInt(blocks_processed)) / elapsed_s, @as(f64, @floatFromInt(total_logs)) / elapsed_s,
            });
        }
    }

    // Signal workers to exit (max block_number sentinel), then join. Disarm the
    // errdefer (spawned = 0) so a later error does not double-join these threads.
    slots[0].block_number = std.math.maxInt(u64);
    for (workers[0..spawned]) |*w| w.join();
    spawned = 0;
    if (ts_thread) |t| t.join();

    try writer.finalize();

    const elapsed_s = @as(f64, @floatFromInt(std.time.nanoTimestamp() - t_start)) / 1e9;
    core.log.info(
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
        core.log.info("Blocks/sec:        {d:.0}\nLogs/sec:          {d:.0}\n", .{
            @as(f64, @floatFromInt(blocks_processed)) / elapsed_s,
            @as(f64, @floatFromInt(total_logs)) / elapsed_s,
        });
    }

    if (skipped_raw > 0) {
        core.log.info(
            "\nERROR: skipped {d} RocksDB entries (short key or oversized value > {d}B). " ++
                "Flat store is incomplete.\n",
            .{ skipped_raw, MAX_RAW_VALUE },
        );
        return error.ImportSkippedEntries;
    }

    if (decode_errors > 0) {
        core.log.info(
            "\nERROR: {d} blocks failed to decode — flat store is incomplete. " ++
                "Likely MAX_LOGS_PER_BLOCK in core/src/types.zig.\n",
            .{decode_errors},
        );
        return error.ImportHadDecodeErrors;
    }
}
