/// Follows the chain head, appending new blocks to the pending ring and
/// finalizing to the flat store. Prefers WebSocket (push via eth_subscribe
/// newHeads, <1s latency). Falls back to HTTP polling (~1s interval) when
/// --ws absent. Both use eth.zig for transport and JSON-RPC parsing.
const std = @import("std");

const core = @import("core");
const eth = @import("eth");

const FlatStoreWriter = @import("flat_writer.zig").FlatStoreWriter;
const pending_ring = @import("pending_ring.zig");
const rpc_import = @import("rpc_import.zig");
const tx_json = @import("tx_json.zig");

const PendingRing = pending_ring.PendingRing;
const log_serial = core.log_serial;
const types = core.types;

pub const FollowConfig = struct {
    rpc_url: []const u8,
    ws_url: ?[]const u8 = null,
    data_dir: []const u8,
    poll_interval_ms: u64 = 1000,
    /// Accept a multi-hour RPC catch-up when baseline is more than
    /// `GAP_REFUSE_THRESHOLD` blocks behind tip. Default refuses loud and
    /// points the operator at `import --rocksdb`.
    allow_rpc_catchup: bool = false,
    /// Build per-block tx tables (ADR-006): one full-block fetch per new
    /// block, mirrored into txs.dat on finalization. `--no-tx-fields` off.
    tx_fields: bool = true,
};

/// Live tx-table support: the full-block fetch transport plus the txs.dat
/// mirror target. Null end-to-end under --no-tx-fields.
const TxTables = struct {
    http: *eth.http_transport.HttpTransport,
    /// Null when the store is fresh (first_block unknown), mirror skipped.
    writer: ?*core.txs.TxsWriter,
};

/// Block count above which `follow` start refuses an RPC-only catch-up.
/// 1000 blocks ≈ 3.3 hours via serial `eth_getLogs`. Past that,
/// `rocksdb-import` is the right tool (~30 s at this scale).
const GAP_REFUSE_THRESHOLD: u64 = 1000;

/// WebSocket read timeout. newHeads arrive about every 12s, so 60s of silence
/// (roughly five missed blocks) means the stream is dead. Bounds `sub.next()`
/// so a half-open connection reconnects instead of hanging forever.
const WS_READ_TIMEOUT_S: u32 = 60;

/// Liveness heartbeat cadence. Finalization advances about one block per 12s,
/// so a log per 300 finalized blocks is roughly hourly
const HEARTBEAT_BLOCKS: u64 = 300;

/// Best-effort SO_RCVTIMEO so a blocking read returns instead of hanging on a
/// silent socket. A failure leaves the prior blocking behavior.
fn setReadTimeout(handle: std.posix.socket_t, seconds: u32) void {
    const tv = std.posix.timeval{ .sec = @intCast(seconds), .usec = 0 };
    std.posix.setsockopt(handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
}

pub fn run(config: FollowConfig) !void {
    const alloc = std.heap.page_allocator;

    var http = eth.http_transport.HttpTransport.init(alloc, config.rpc_url);
    var provider = eth.provider.Provider.init(alloc, &http);

    var writer = try FlatStoreWriter.open(config.data_dir);
    const dir = try std.fs.cwd().openDir(config.data_dir, .{});
    var ring = try PendingRing.open(dir, alloc);
    defer {
        writer.commitMeta() catch {};
        ring.flush() catch {};
        ring.deinit();
    }

    // Resume the importer's timestamps.bin so blocks finalized post-import keep
    // exact times for cold re-backfills (a live SDK already gets them via
    // pending.bin). Keyed off first_block to align with the importer's dense
    // indexing. Fresh follow-only store (first_block == 0) skips this and falls
    // back to the formula until imported.
    var ts_writer: ?core.timestamps.TimestampWriter =
        if (writer.first_block != 0)
            core.timestamps.TimestampWriter.open(dir, writer.first_block) catch null
        else
            null;
    defer if (ts_writer) |*w| w.deinit();

    // Tx tables ride their own transport (the provider owns `http`) and
    // mirror into txs.dat with the same advisory semantics as timestamps.
    var txs_http = eth.http_transport.HttpTransport.init(alloc, config.rpc_url);
    var txs_writer: ?core.txs.TxsWriter =
        if (config.tx_fields and writer.first_block != 0)
            core.txs.TxsWriter.open(dir, writer.first_block) catch null
        else
            null;
    defer if (txs_writer) |*w| w.deinit();
    var tx_tables: ?TxTables = if (config.tx_fields)
        .{ .http = &txs_http, .writer = if (txs_writer) |*w| w else null }
    else
        null;
    const txs: ?*TxTables = if (tx_tables) |*t| t else null;

    // Refuse follow against a stale baseline. Baseline is the highest known
    // block (pending tip if any, else last finalized), compared to chain tip.
    // Skipped when the dir is fresh (baseline = 0) or the operator opts into
    // RPC catch-up.
    const baseline: u64 = if (ring.latestBlock()) |t| t else writer.meta.last_finalized_block;
    if (baseline > 0 and !config.allow_rpc_catchup) {
        const tip = try provider.getBlockNumber();
        const gap: u64 = if (tip > baseline) tip - baseline else 0;
        if (gap > GAP_REFUSE_THRESHOLD) {
            core.log.err(
                \\
                \\Refusing follow: flat store ends at block {d}, chain is at {d}
                \\(gap of {d} blocks, ~{d}h via RPC). The fast path is:
                \\
                \\  emit-engine import --rocksdb <node_db_path> --data-dir {s}
                \\
                \\Then re-run follow. Or pass --catch-up-rpc to accept the wait.
                \\
            ,
                .{ baseline, tip, gap, gap * 12 / 3600, config.data_dir },
            );
            return error.StaleBaselineGap;
        }
    }

    if (config.ws_url == null)
        core.log.info("Polling {s} every {d}ms\n", .{ config.rpc_url, config.poll_interval_ms });

    // Prefer WebSocket. On connect failure or a dropped stream, fall back to one
    // poll cycle and reconnect, rather than degrading to HTTP polling forever.
    var poll_fails: u32 = 0;
    while (true) {
        if (config.ws_url) |ws_url| ws: {
            core.log.info("Connecting to {s}...\n", .{ws_url});
            var ws = eth.ws_transport.WsTransport.connect(alloc, ws_url) catch |err| {
                core.log.info("WS connect failed ({s}), polling then reconnecting\n", .{@errorName(err)});
                break :ws;
            };
            defer ws.close();
            // Bound sub.next() so a half-open stream (TCP up, no frames) surfaces
            // a read error and reconnects, instead of blocking forever.
            setReadTimeout(ws.stream.handle, WS_READ_TIMEOUT_S);
            followWs(&ws, &provider, &writer, &ring, alloc, if (ts_writer) |*w| w else null, txs) catch |err| {
                core.log.info("WS error ({s}), polling then reconnecting\n", .{@errorName(err)});
            };
        }

        // One poll cycle. The only mode when --ws is absent, a stopgap between
        // WS reconnects otherwise. A healthy WS never returns, so this is skipped.
        var poll_ok = true;
        followPoll(&provider, &writer, &ring, alloc, if (ts_writer) |*w| w else null, txs) catch |err| {
            poll_ok = false;
            poll_fails += 1;
            // Visible at the default level on the first failure and periodically
            // after, so a node that went away is not silently invisible.
            if (poll_fails == 1 or poll_fails % 60 == 0)
                core.log.info("Poll failing: {s} ({d} consecutive)\n", .{ @errorName(err), poll_fails });
        };
        if (poll_ok and poll_fails > 0) {
            core.log.info("Poll recovered after {d} failures\n", .{poll_fails});
            poll_fails = 0;
        }
        std.Thread.sleep(config.poll_interval_ms * std.time.ns_per_ms);
    }
}

// ── WebSocket ────────────────────────────────────────────────────────────

fn followWs(
    ws: *eth.ws_transport.WsTransport,
    provider: *eth.provider.Provider,
    writer: *FlatStoreWriter,
    ring: *PendingRing,
    alloc: std.mem.Allocator,
    ts_writer: ?*core.timestamps.TimestampWriter,
    txs: ?*TxTables,
) !void {
    var sub = try eth.subscription.Subscription.subscribe(alloc, ws, .{ .new_heads = {} });
    defer sub.deinit();
    core.log.info("Subscribed to newHeads\n", .{});

    while (true) {
        const msg = try sub.next();
        defer alloc.free(msg);
        const block_number = parseBlockNumber(msg) orelse continue;

        // Cold-start fallback matches followPoll so an empty ring after import
        // or pending-wipe doesn't skip last_finalized..block_number-1.
        const tip = ring.latestBlock() orelse blk: {
            if (writer.meta.last_finalized_block > 0) break :blk writer.meta.last_finalized_block;
            break :blk block_number -| 1;
        };

        // Providers occasionally re-broadcast. Re-appending breaks the dense-ring invariant.
        if (block_number <= tip) continue;

        // Finalize each block as it is ingested so pending.bin never persists
        // more than FINALITY_DEPTH entries. Finalizing only after the whole gap
        // would grow the ring past the cap, write an oversized file that
        // parseValidated rejects on the next open (bricking follow), and rewrite
        // the full file O(gap²) times. Finalize before each insert so the
        // on-disk count never overshoots the cap, even by one. On any failure
        // stop. The next notification retries from the new tip, ring stays dense.
        var bn = tip + 1;
        while (bn <= block_number) : (bn += 1) {
            finalizeReady(ring, writer, bn, alloc, ts_writer, txs);
            ingestBlock(bn, provider, ring, alloc, txs) catch |err| {
                core.log.info("Block {d}: {s}\n", .{ bn, @errorName(err) });
                break;
            };
        }
    }
}

// ── HTTP polling ─────────────────────────────────────────────────────────

fn followPoll(
    provider: *eth.provider.Provider,
    writer: *FlatStoreWriter,
    ring: *PendingRing,
    alloc: std.mem.Allocator,
    ts_writer: ?*core.timestamps.TimestampWriter,
    txs: ?*TxTables,
) !void {
    const latest = try provider.getBlockNumber();
    const tip = ring.latestBlock() orelse
        if (writer.meta.last_finalized_block > 0) writer.meta.last_finalized_block else latest -| 1;
    if (latest <= tip) return;

    // Finalize before each insert so the ring stays within FINALITY_DEPTH on
    // disk even across a large catch-up. See followWs for the full rationale.
    for (tip + 1..latest + 1) |bn| {
        finalizeReady(ring, writer, @intCast(bn), alloc, ts_writer, txs);
        ingestBlock(@intCast(bn), provider, ring, alloc, txs) catch |err| {
            core.log.info("Block {d}: {s}\n", .{ bn, @errorName(err) });
            break;
        };
    }
}

// ── Shared ───────────────────────────────────────────────────────────────

/// Fetch block hash + logs, serialize, compress, insert into pending ring.
/// On reorg, truncate divergent entries and re-ingest the canonical chain from
/// the fork point up to `block_number`. Replay is required because WS newHeads
/// will only push `block_number + 1` next, never re-emitting the fork-point span.
fn ingestBlock(
    block_number: u64,
    provider: *eth.provider.Provider,
    ring: *PendingRing,
    parent_alloc: std.mem.Allocator,
    txs: ?*TxTables,
) !void {
    var arena = std.heap.ArenaAllocator.init(parent_alloc);
    defer arena.deinit();
    const alloc = arena.allocator();

    const header = try provider.getBlock(block_number) orelse return error.BlockNotFound;

    // Reorg check: this block's parent hash vs the stored hash.
    if (ring.getHash(block_number - 1)) |stored_hash| {
        if (!std.mem.eql(u8, &stored_hash, &header.parent_hash)) {
            core.log.info("Reorg detected at block {d}\n", .{block_number});
            const fork = try resolveReorg(ring, block_number, provider);
            var bn = fork;
            while (bn <= block_number) : (bn += 1) {
                try ingestBlockNoReorgCheck(bn, provider, ring, parent_alloc, txs);
            }
            return;
        }
    }

    try ingestBlockCore(block_number, header, provider, ring, alloc, txs);
}

/// Recovery-path ingest: skips the reorg check. Restoring canonical state, so
/// the parent-hash comparison against an already-truncated ring is meaningless.
fn ingestBlockNoReorgCheck(
    block_number: u64,
    provider: *eth.provider.Provider,
    ring: *PendingRing,
    parent_alloc: std.mem.Allocator,
    txs: ?*TxTables,
) !void {
    var arena = std.heap.ArenaAllocator.init(parent_alloc);
    defer arena.deinit();
    const alloc = arena.allocator();

    const header = try provider.getBlock(block_number) orelse return error.BlockNotFound;
    try ingestBlockCore(block_number, header, provider, ring, alloc, txs);
}

/// Fetch logs for `block_number`, build blooms, compress, insert.
fn ingestBlockCore(
    block_number: u64,
    header: anytype,
    provider: *eth.provider.Provider,
    ring: *PendingRing,
    alloc: std.mem.Allocator,
    txs: ?*TxTables,
) !void {
    // Pin the query to the header's hash, not the number. A reorg between
    // `getBlock` and `getLogs` would otherwise store the new chain's logs
    // under the old hash. The node rejects an unknown hash, so the entry can
    // never mix two chains.
    var hash_buf: [66]u8 = undefined;
    const hash_hex = try std.fmt.bufPrint(&hash_buf, "0x{x}", .{&header.hash});
    const eth_logs = try provider.getLogs(.{ .blockHash = hash_hex });

    // Fail loud per the contract in core/src/types.zig. A silent truncate
    // would land an incomplete block in pending + flat store.
    if (eth_logs.len > types.MAX_LOGS_PER_BLOCK) {
        core.log.err(
            "Block {d}: {d} logs exceeds MAX_LOGS_PER_BLOCK ({d}). Bump the constant in core/src/types.zig.\n",
            .{ block_number, eth_logs.len, types.MAX_LOGS_PER_BLOCK },
        );
        return error.TooManyLogsInBlock;
    }
    const raw_logs = try alloc.alloc(types.RawLog, eth_logs.len);
    for (eth_logs, 0..) |log, i| {
        raw_logs[i] = try toRawLog(log, block_number, alloc);
    }

    const serialize_buf = try alloc.alloc(u8, types.BLOCK_BUF_SIZE);
    const compress_buf = try alloc.alloc(u8, types.BLOCK_BUF_SIZE);
    const pack = try log_serial.packBlock(raw_logs, serialize_buf, compress_buf);

    // Tx table for the block's log-producing txs. Strict like getLogs: a
    // fetch failure fails the ingest and the next notification retries.
    var tx_table: []const u8 = &.{};
    if (txs) |t| {
        const mask_buf = try alloc.alloc(u16, raw_logs.len);
        const mask = tx_json.logMask(raw_logs, mask_buf);
        tx_table = try fetchTxTable(t.http, block_number, mask, alloc);
    }

    const ts: u32 = std.math.cast(u32, header.timestamp) orelse 0; // exact block time, valid until 2106
    try ring.insert(block_number, ts, header.hash, &pack.topic_bloom.bits, &pack.addr_bloom.bits, compress_buf[0..pack.entry_len], tx_table);
    core.log.debug("Block {d}: {d} logs\n", .{ block_number, raw_logs.len });
}

/// Fetch the block's full tx objects and serialize the log-producing
/// records (the node's senders are pre-recovered, no crypto). Returns the
/// table payload (count ‖ records), arena-owned. An empty mask serializes a
/// count=0 table without touching the network, keeping "built" (len ≥ 4)
/// distinguishable from "disabled" (len 0) at the txs.dat mirror.
fn fetchTxTable(http: *eth.http_transport.HttpTransport, bn: u64, mask: []const u16, alloc: std.mem.Allocator) ![]const u8 {
    if (mask.len == 0) {
        const buf = try alloc.alloc(u8, 4);
        return buf[0..core.txs.serializeRecords(&.{}, buf)];
    }

    var body_buf: [128]u8 = undefined;
    const body = try std.fmt.bufPrint(&body_buf, "{{\"jsonrpc\":\"2.0\",\"id\":0,\"method\":\"eth_getBlockByNumber\",\"params\":[\"0x{x}\",true]}}", .{bn});
    const raw = try rpc_import.httpPost(http, body, alloc);

    const Resp = struct { result: ?struct { transactions: []tx_json.JsonTx } = null };
    const parsed = try std.json.parseFromSlice(Resp, alloc, raw, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const result = parsed.value.result orelse return error.BlockUnavailable;

    const records = try alloc.alloc(core.txs.TxRecord, mask.len);
    const filled = try tx_json.buildRecords(result.transactions, mask, records);
    const buf = try alloc.alloc(u8, 4 + filled.len * core.txs.RECORD_SIZE);
    return buf[0..core.txs.serializeRecords(filled, buf)];
}

/// Mirror a finalized block's table into txs.dat. Dense-append only: a
/// cursor gap means the import owns the backfill (advisory artifact), and an
/// unbuilt table (len 0, pre-enable history) must not write a false empty.
fn mirrorTxTable(tw: *core.txs.TxsWriter, block: u64, table: []const u8, alloc: std.mem.Allocator) void {
    if (table.len == 0) return;
    if (block != tw.first_block + tw.count) return;
    const cbuf = alloc.alloc(u8, table.len + table.len / 128 + 64) catch return;
    defer alloc.free(cbuf);
    tw.appendSerialized(block, table, cbuf) catch |e| {
        core.log.debug("txs.dat mirror skipped at block {d}: {s}\n", .{ block, @errorName(e) });
    };
}

/// Truncate divergent pending entries down to the fork point and return it,
/// so the caller can re-ingest the canonical chain from `fork..from`.
/// `provider` is any type with `getBlock(u64) !?Header` where the header
/// carries `hash`. Comptime generic so tests inject a mock chain.
fn resolveReorg(ring: *PendingRing, from: u64, provider: anytype) !u64 {
    // Walk backwards. Most reorgs are 1-2 blocks.
    var canonical: [pending_ring.FINALITY_DEPTH][32]u8 = undefined;
    const oldest = ring.oldestBlock() orelse return from;
    const depth = @min(from - oldest, pending_ring.FINALITY_DEPTH);
    for (0..depth) |i| {
        // Propagate fetch failures. Swallowing one shortens the canonical window
        // and pushes the fork point above the true fork, graduating orphan
        // blocks into the never-mutated flat store. The caller's gap-fill abort
        // retries the whole reorg on a transient error.
        const hdr = (try provider.getBlock(from - 1 - i)) orelse return error.ReorgBlockUnavailable;
        canonical[i] = hdr.hash;
    }

    // Null means the divergence runs deeper than the ring holds. Refuse rather
    // than guess a fork above the true one and orphan-finalize.
    const fork = ring.findForkPoint(from, canonical[0..depth]) orelse return error.ReorgExceedsRing;
    const removed = try ring.truncateFrom(fork);
    try ring.flush();
    core.log.info("Reorg: fork at {d}, removed {d} blocks\n", .{ fork, removed });
    return fork;
}

/// Move blocks with 64+ confirmations from pending ring to flat store.
/// Peek-then-pop: appendBlock failure must leave the entry on the ring so the
/// next finalize attempt retries. Popping first would orphan the block (pending
/// forgets it, flat never recorded it).
fn finalizeReady(
    ring: *PendingRing,
    writer: *FlatStoreWriter,
    head: u64,
    alloc: std.mem.Allocator,
    ts_writer: ?*core.timestamps.TimestampWriter,
    txs: ?*TxTables,
) void {
    var popped_count: u32 = 0;
    var finalized: u32 = 0;
    const start_finalized = writer.meta.last_finalized_block;
    while (ring.canFinalize(head)) {
        const oldest = ring.peekOldest() orelse break;
        const pre = writer.meta.last_finalized_block;
        writer.appendBlock(oldest.block_number, oldest.lz4_entry, &oldest.topic_bloom, &oldest.addr_bloom) catch |err| {
            core.log.info("Finalize block {d}: {s}\n", .{ oldest.block_number, @errorName(err) });
            break;
        };
        // Mirror the flat-store append into timestamps.bin so cold re-backfills
        // over post-import blocks stay exact. Advisory: a write error degrades to
        // the formula via the reader's zero-is-unknown rule, never blocks
        // finalization. count is published after the batch via `sync`.
        if (ts_writer) |w| {
            if (oldest.timestamp != 0) w.set(oldest.block_number, oldest.timestamp) catch {};
        }
        const popped = ring.popOldest().?;
        if (txs) |t| if (t.writer) |tw| mirrorTxTable(tw, popped.block_number, popped.tx_table, alloc);
        alloc.free(popped.lz4_entry);
        alloc.free(popped.tx_table);
        popped_count += 1;
        if (writer.meta.last_finalized_block > pre) finalized += 1;
    }
    if (finalized > 0) {
        writer.commitMeta() catch {};
        if (ts_writer) |w| w.sync() catch {};
    }
    // Flush ring on any pop, including idempotent skips, so crash recovery
    // doesn't keep re-presenting the same finalized blocks.
    if (popped_count > 0) ring.flush() catch {};
    if (finalized > 0) core.log.debug("Finalized {d} blocks\n", .{finalized});

    // Fire once per HEARTBEAT_BLOCKS of finalization, on the boundary crossing so jumps never skip it.
    const tip = writer.meta.last_finalized_block;
    if (start_finalized > 0 and tip / HEARTBEAT_BLOCKS != start_finalized / HEARTBEAT_BLOCKS)
        core.log.info("Following: finalized through block {d}\n", .{tip});
}

/// Convert an eth.zig log to a `RawLog`. With `alloc`, `data` is duped so the
/// RawLog outlives the RPC response. Null borrows `log.data`, valid only until
/// the response frees (rpc_import consumes it before the batch arena resets).
/// Shared with rpc_import, the one conversion for both ingestion paths.
pub fn toRawLog(log: eth.receipt.Log, block_number: u64, alloc: ?std.mem.Allocator) !types.RawLog {
    var topics: [types.MAX_TOPICS][32]u8 = std.mem.zeroes([types.MAX_TOPICS][32]u8);
    var topic_count: u8 = 0;
    for (log.topics) |t| {
        if (topic_count >= types.MAX_TOPICS) break;
        topics[topic_count] = t;
        topic_count += 1;
    }

    const data: []const u8 = if (alloc) |a|
        (if (log.data.len > 0) try a.dupe(u8, log.data) else &.{})
    else
        log.data;

    return .{
        .block_number = block_number,
        .log_index = @intCast(log.log_index orelse 0),
        .tx_index = @intCast(log.transaction_index orelse 0),
        .address = log.address,
        .topic_count = topic_count,
        .topics = topics,
        .data = data,
        .tx_hash = log.transaction_hash orelse std.mem.zeroes([32]u8),
    };
}

/// Extract block number from a newHeads notification ("number":"0x...").
fn parseBlockNumber(msg: []const u8) ?u64 {
    const marker = "\"number\":\"0x";
    const start = std.mem.indexOf(u8, msg, marker) orelse return null;
    const hex_start = start + marker.len;
    const hex_end = std.mem.indexOfPos(u8, msg, hex_start, "\"") orelse return null;
    return std.fmt.parseInt(u64, msg[hex_start..hex_end], 16) catch null;
}

// ── Tests ────────────────────────────────────────────────────────────────

test "parseBlockNumber from newHeads notification" {
    const valid =
        \\{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x1","result":{"number":"0x134b6a1"}}}
    ;
    try std.testing.expectEqual(@as(u64, 0x134b6a1), parseBlockNumber(valid).?);
    try std.testing.expect(parseBlockNumber("{\"id\":1,\"result\":true}") == null);
}

test "finalizeReady mirrors finalized timestamps into timestamps.bin" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const alloc = std.testing.allocator;

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);

    var writer = try FlatStoreWriter.open(path);
    defer writer.deinit();
    var ring = try PendingRing.open(tmp.dir, alloc);
    defer ring.deinit();

    const topic = [_]u8{0} ** core.bloom.BLOOM_SIZE;
    const addr = [_]u8{0} ** core.bloom.ADDR_BLOOM_SIZE;
    const entry = [_]u8{ 0, 0, 0, 0 }; // packed payload, log_count = 0
    const hash = [_]u8{0xAB} ** 32;

    // Two pending blocks carrying exact header timestamps.
    try ring.insert(100, 1_700_000_000, hash, &topic, &addr, &entry, &.{});
    try ring.insert(101, 1_700_000_012, hash, &topic, &addr, &entry, &.{});

    // Opened at the known first block, as run() does post-import.
    var ts_writer = try core.timestamps.TimestampWriter.open(tmp.dir, 100);
    defer ts_writer.deinit();

    // head = 100 + FINALITY_DEPTH finalizes only block 100. 101 stays pending.
    finalizeReady(&ring, &writer, 100 + pending_ring.FINALITY_DEPTH, alloc, &ts_writer, null);

    var reader = (try core.timestamps.TimestampReader.open(tmp.dir)).?;
    defer reader.deinit();
    try std.testing.expectEqual(@as(?u64, 1_700_000_000), reader.get(100));
    try std.testing.expectEqual(@as(?u64, null), reader.get(101)); // not yet finalized
}

test "catch-up finalizes as it ingests so the ring stays within FINALITY_DEPTH" {
    // Regression for the catch-up brick: ingesting a gap larger than the ring
    // while finalizing only afterward grows pending.bin past FINALITY_DEPTH,
    // which parseValidated rejects on the next open. Mirror the gap-fill cadence
    // (finalize before each insert) and assert the persisted ring never exceeds
    // the cap and reopens cleanly.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const alloc = std.testing.allocator;

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);

    const topic = [_]u8{0} ** core.bloom.BLOOM_SIZE;
    const addr = [_]u8{0} ** core.bloom.ADDR_BLOOM_SIZE;
    const entry = [_]u8{ 0, 0, 0, 0 }; // packed payload, log_count = 0
    const hash = [_]u8{0xAB} ** 32;

    const last: u64 = pending_ring.FINALITY_DEPTH * 2;
    {
        var writer = try FlatStoreWriter.open(path);
        defer writer.deinit();
        var ring = try PendingRing.open(tmp.dir, alloc);
        defer ring.deinit();

        var bn: u64 = 1;
        while (bn <= last) : (bn += 1) {
            finalizeReady(&ring, &writer, bn, alloc, null, null);
            try ring.insert(bn, 0, hash, &topic, &addr, &entry, &.{});
            try std.testing.expect(ring.count() <= pending_ring.FINALITY_DEPTH);
        }
    }

    // The persisted ring must reopen: it never wrote an oversized count.
    var ring2 = try PendingRing.open(tmp.dir, alloc);
    defer ring2.deinit();
    try std.testing.expect(ring2.count() <= pending_ring.FINALITY_DEPTH);
    try std.testing.expectEqual(last, ring2.latestBlock().?);
}

// Mock chain for resolveReorg tests: canonical hashes indexed by block number.
// Satisfies the `getBlock(u64) !?Header` shape resolveReorg requires.
const MockChain = struct {
    base: u64,
    hashes: []const [32]u8,
    fail_at: ?u64 = null,

    pub fn getBlock(self: *MockChain, n: u64) !?struct { hash: [32]u8 } {
        if (self.fail_at) |f| if (n == f) return error.RpcUnavailable;
        return .{ .hash = self.hashes[@intCast(n - self.base)] };
    }
};

const reorg_test = struct {
    const topic = [_]u8{0} ** core.bloom.BLOOM_SIZE;
    const addr = [_]u8{0} ** core.bloom.ADDR_BLOOM_SIZE;
    const entry = [_]u8{ 0, 0, 0, 0 }; // packed payload, log_count = 0
    const old_hash = [_]u8{0xAA} ** 32;
    const new_hash = [_]u8{0xBB} ** 32;

    fn ringOf(dir: std.fs.Dir, alloc: std.mem.Allocator) !PendingRing {
        var ring = try PendingRing.open(dir, alloc);
        errdefer ring.deinit();
        for (100..110) |bn| try ring.insert(bn, 0, old_hash, &topic, &addr, &entry, &.{});
        return ring;
    }
};

test "resolveReorg truncates to the verified fork point" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try reorg_test.ringOf(tmp.dir, std.testing.allocator);
    defer ring.deinit();

    // Canonical chain: 100..105 unchanged, 106..109 replaced.
    var hashes: [10][32]u8 = undefined;
    for (&hashes, 0..) |*h, i| h.* = if (i <= 5) reorg_test.old_hash else reorg_test.new_hash;
    var chain = MockChain{ .base = 100, .hashes = &hashes };

    const fork = try resolveReorg(&ring, 109, &chain);
    try std.testing.expectEqual(@as(u64, 106), fork);
    try std.testing.expectEqual(@as(u64, 105), ring.latestBlock().?);
}

test "resolveReorg propagates a canonical fetch failure without truncating" {
    // Regression: a swallowed fetch error used to shorten the canonical
    // window, pick a too-high fork, and orphan-finalize blocks below it.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try reorg_test.ringOf(tmp.dir, std.testing.allocator);
    defer ring.deinit();

    var hashes: [10][32]u8 = undefined;
    for (&hashes) |*h| h.* = reorg_test.new_hash;
    var chain = MockChain{ .base = 100, .hashes = &hashes, .fail_at = 107 };

    try std.testing.expectError(error.RpcUnavailable, resolveReorg(&ring, 109, &chain));
    try std.testing.expectEqual(@as(u64, 109), ring.latestBlock().?);
}

test "resolveReorg refuses a divergence deeper than the ring" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try reorg_test.ringOf(tmp.dir, std.testing.allocator);
    defer ring.deinit();

    // Every fetched canonical hash mismatches: the true fork is below the
    // ring's oldest entry. Guessing would orphan-finalize, so it must error.
    var hashes: [10][32]u8 = undefined;
    for (&hashes) |*h| h.* = reorg_test.new_hash;
    var chain = MockChain{ .base = 100, .hashes = &hashes };

    try std.testing.expectError(error.ReorgExceedsRing, resolveReorg(&ring, 109, &chain));
    try std.testing.expectEqual(@as(u64, 109), ring.latestBlock().?);
}
