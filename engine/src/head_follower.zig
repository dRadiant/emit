/// Follows the chain head, appending new blocks to the pending ring
/// and finalizing to the flat store.
///
/// Prefers WebSocket (push via eth_subscribe newHeads, <1s latency).
/// Falls back to HTTP polling (~1s interval) if --ws not provided.
/// Both use eth.zig for transport and JSON-RPC parsing.
const std = @import("std");

const core = @import("core");
const eth = @import("eth");

const FlatStoreWriter = @import("flat_writer.zig").FlatStoreWriter;
const pending_ring = @import("pending_ring.zig");

const PendingRing = pending_ring.PendingRing;
const log_serial = core.log_serial;
const types = core.types;

pub const FollowConfig = struct {
    rpc_url: []const u8,
    ws_url: ?[]const u8 = null,
    data_dir: []const u8,
    poll_interval_ms: u64 = 1000,
    /// Accept a multi-hour RPC catch-up if the baseline is more than
    /// `GAP_REFUSE_THRESHOLD` blocks behind chain tip. Default refuses
    /// loud and points the operator at `import --rocksdb`.
    allow_rpc_catchup: bool = false,
};

/// Block count above which the engine refuses an RPC-only catch-up on
/// `follow` start. 1000 blocks ≈ 3.3 hours via serial `eth_getLogs`;
/// past that, `rocksdb-import` is the right tool (~30 s at this scale).
const GAP_REFUSE_THRESHOLD: u64 = 1000;

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

    // Refuse follow against a stale baseline. The check uses the highest
    // known block — pending tip if any, else the last finalized — and
    // compares to current chain tip. Skipped when the dir is fresh
    // (baseline = 0) or when the operator opts in to RPC catch-up.
    const baseline: u64 = if (ring.latestBlock()) |t| t else writer.meta.last_finalized_block;
    if (baseline > 0 and !config.allow_rpc_catchup) {
        const tip = try provider.getBlockNumber();
        const gap: u64 = if (tip > baseline) tip - baseline else 0;
        if (gap > GAP_REFUSE_THRESHOLD) {
            std.debug.print(
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

    if (config.ws_url) |ws_url| ws: {
        std.debug.print("Connecting to {s}...\n", .{ws_url});
        var ws = eth.ws_transport.WsTransport.connect(alloc, ws_url) catch |err| {
            std.debug.print("WS failed ({s}), falling back to HTTP\n", .{@errorName(err)});
            break :ws;
        };
        defer ws.close();
        followWs(&ws, &provider, &writer, &ring, alloc) catch |err| {
            std.debug.print("WS error ({s}), falling back to HTTP\n", .{@errorName(err)});
        };
    }

    std.debug.print("Polling {s} every {d}ms\n", .{ config.rpc_url, config.poll_interval_ms });
    while (true) {
        followPoll(&provider, &writer, &ring, alloc) catch |err| {
            std.debug.print("Poll error: {s}\n", .{@errorName(err)});
        };
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
) !void {
    var sub = try eth.subscription.Subscription.subscribe(alloc, ws, .{ .new_heads = {} });
    defer sub.deinit();
    std.debug.print("Subscribed to newHeads\n", .{});

    while (true) {
        const msg = try sub.next();
        defer alloc.free(msg);
        const block_number = parseBlockNumber(msg) orelse continue;

        // Cold-start fallback matches followPoll so an empty ring after
        // import or pending-wipe doesn't skip last_finalized..block_number-1.
        const tip = ring.latestBlock() orelse blk: {
            if (writer.meta.last_finalized_block > 0) break :blk writer.meta.last_finalized_block;
            break :blk block_number -| 1;
        };

        // Providers occasionally re-broadcast. Dense-ring invariant breaks if re-appended.
        if (block_number <= tip) continue;

        // Partial gap-fill would leave the ring non-dense and break getHash —
        // abort on any failure and let the next notification retry from the tip.
        var bn = tip + 1;
        var gap_ok = true;
        while (bn < block_number) : (bn += 1) {
            ingestBlock(bn, provider, ring, alloc) catch |err| {
                std.debug.print("Gap-fill block {d}: {s}\n", .{ bn, @errorName(err) });
                gap_ok = false;
                break;
            };
        }
        if (!gap_ok) continue;

        ingestBlock(block_number, provider, ring, alloc) catch |err| {
            std.debug.print("Block {d}: {s}\n", .{ block_number, @errorName(err) });
            continue;
        };
        finalizeReady(ring, writer, block_number, alloc);
    }
}

// ── HTTP polling ─────────────────────────────────────────────────────────

fn followPoll(
    provider: *eth.provider.Provider,
    writer: *FlatStoreWriter,
    ring: *PendingRing,
    alloc: std.mem.Allocator,
) !void {
    const latest = try provider.getBlockNumber();
    const tip = ring.latestBlock() orelse
        if (writer.meta.last_finalized_block > 0) writer.meta.last_finalized_block else latest -| 1;
    if (latest <= tip) return;

    for (tip + 1..latest + 1) |bn| {
        ingestBlock(@intCast(bn), provider, ring, alloc) catch |err| {
            std.debug.print("Block {d}: {s}\n", .{ bn, @errorName(err) });
            break;
        };
    }
    finalizeReady(ring, writer, latest, alloc);
}

// ── Shared ───────────────────────────────────────────────────────────────

/// Fetch block hash + logs from node, serialize, compress, insert into pending ring.
/// On reorg detection, truncate divergent entries and re-ingest the canonical
/// chain from the fork point up to `block_number`. The replay is required
/// because WS newHeads will only push `block_number + 1` next, never re-emitting
/// the fork-point span.
fn ingestBlock(
    block_number: u64,
    provider: *eth.provider.Provider,
    ring: *PendingRing,
    parent_alloc: std.mem.Allocator,
) !void {
    var arena = std.heap.ArenaAllocator.init(parent_alloc);
    defer arena.deinit();
    const alloc = arena.allocator();

    const header = try provider.getBlock(block_number) orelse return error.BlockNotFound;

    // Reorg check: does this block's parent hash match what we stored?
    if (ring.getHash(block_number - 1)) |stored_hash| {
        if (!std.mem.eql(u8, &stored_hash, &header.parent_hash)) {
            std.debug.print("Reorg detected at block {d}\n", .{block_number});
            const fork = try resolveReorg(ring, block_number, provider);
            var bn = fork;
            while (bn <= block_number) : (bn += 1) {
                try ingestBlockNoReorgCheck(bn, provider, ring, parent_alloc);
            }
            return;
        }
    }

    try ingestBlockCore(block_number, header, provider, ring, alloc);
}

/// Recovery-path ingest: skip the reorg check (we're restoring canonical state,
/// the parent-hash comparison against an already-truncated ring is meaningless).
fn ingestBlockNoReorgCheck(
    block_number: u64,
    provider: *eth.provider.Provider,
    ring: *PendingRing,
    parent_alloc: std.mem.Allocator,
) !void {
    var arena = std.heap.ArenaAllocator.init(parent_alloc);
    defer arena.deinit();
    const alloc = arena.allocator();

    const header = try provider.getBlock(block_number) orelse return error.BlockNotFound;
    try ingestBlockCore(block_number, header, provider, ring, alloc);
}

/// Shared core: fetch logs for `block_number`, build blooms, compress, insert.
fn ingestBlockCore(
    block_number: u64,
    header: anytype,
    provider: *eth.provider.Provider,
    ring: *PendingRing,
    alloc: std.mem.Allocator,
) !void {
    var num_buf: [20]u8 = undefined;
    const hex = try std.fmt.bufPrint(&num_buf, "0x{x}", .{block_number});
    const eth_logs = try provider.getLogs(.{ .fromBlock = hex, .toBlock = hex });

    // Fail loud per the contract in core/src/types.zig — a silent truncate
    // would land an incomplete block in pending + flat store.
    if (eth_logs.len > types.MAX_LOGS_PER_BLOCK) {
        std.debug.print(
            "Block {d}: {d} logs exceeds MAX_LOGS_PER_BLOCK ({d}). Bump the constant in core/src/types.zig.\n",
            .{ block_number, eth_logs.len, types.MAX_LOGS_PER_BLOCK },
        );
        return error.TooManyLogsInBlock;
    }
    const raw_logs = try alloc.alloc(types.RawLog, eth_logs.len);
    for (eth_logs, 0..) |log, i| {
        raw_logs[i] = try toRawLog(log, block_number, alloc);
    }

    const topic_bloom = log_serial.buildTopicBloom(raw_logs);
    const addr_bloom = log_serial.buildAddrBloom(raw_logs);

    const serialize_buf = try alloc.alloc(u8, types.BLOCK_BUF_SIZE);
    const serialized_len = log_serial.serializeLogs(raw_logs, serialize_buf);

    const compress_buf = try alloc.alloc(u8, types.BLOCK_BUF_SIZE);
    const entry_len = try log_serial.compressEntry(serialize_buf[0..serialized_len], compress_buf);

    try ring.insert(block_number, header.hash, &topic_bloom.bits, &addr_bloom.bits, compress_buf[0..entry_len]);
    std.debug.print("Block {d}: {d} logs\n", .{ block_number, raw_logs.len });
}

/// Truncate divergent pending entries down to the fork point and return it,
/// so the caller can re-ingest the canonical chain from `fork..from`.
fn resolveReorg(ring: *PendingRing, from: u64, provider: *eth.provider.Provider) !u64 {
    // Walk backwards; most reorgs are 1-2 blocks.
    var canonical: [pending_ring.FINALITY_DEPTH][32]u8 = undefined;
    const oldest = ring.oldestBlock() orelse return from;
    const depth = @min(from - oldest, pending_ring.FINALITY_DEPTH);
    var filled: usize = 0;
    for (0..depth) |i| {
        const hdr = (provider.getBlock(from - 1 - i) catch break) orelse break;
        canonical[i] = hdr.hash;
        filled += 1;
    }

    // Pass only the filled prefix; uninitialized slots would corrupt the comparison.
    const fork = ring.findForkPoint(from, canonical[0..filled]);
    const removed = try ring.truncateFrom(fork);
    try ring.flush();
    std.debug.print("Reorg: fork at {d}, removed {d} blocks\n", .{ fork, removed });
    return fork;
}

/// Move blocks with 64+ confirmations from pending ring to flat store.
/// Peek-then-pop: appendBlock failure must leave the entry on the ring so
/// the next finalize attempt retries — popping first would orphan the
/// block (pending forgets it, flat never recorded it).
fn finalizeReady(ring: *PendingRing, writer: *FlatStoreWriter, head: u64, alloc: std.mem.Allocator) void {
    var popped_count: u32 = 0;
    var finalized: u32 = 0;
    while (ring.canFinalize(head)) {
        const oldest = ring.peekOldest() orelse break;
        const pre = writer.meta.last_finalized_block;
        writer.appendBlock(oldest.block_number, oldest.lz4_entry, &oldest.topic_bloom, &oldest.addr_bloom) catch |err| {
            std.debug.print("Finalize block {d}: {s}\n", .{ oldest.block_number, @errorName(err) });
            break;
        };
        const popped = ring.popOldest().?;
        alloc.free(popped.lz4_entry);
        popped_count += 1;
        if (writer.meta.last_finalized_block > pre) finalized += 1;
    }
    if (finalized > 0) writer.commitMeta() catch {};
    // Flush ring on any pop, including idempotent skips, so a crash-recovery
    // pass doesn't keep re-presenting the same finalized blocks.
    if (popped_count > 0) ring.flush() catch {};
    if (finalized > 0) std.debug.print("Finalized {d} blocks\n", .{finalized});
}

fn toRawLog(log: eth.receipt.Log, block_number: u64, alloc: std.mem.Allocator) !types.RawLog {
    var topics: [types.MAX_TOPICS][32]u8 = std.mem.zeroes([types.MAX_TOPICS][32]u8);
    var topic_count: u8 = 0;
    for (log.topics) |t| {
        if (topic_count >= types.MAX_TOPICS) break;
        topics[topic_count] = t;
        topic_count += 1;
    }

    const data: []const u8 = if (log.data.len > 0) try alloc.dupe(u8, log.data) else &.{};

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

/// Extract block number from a newHeads notification: "number":"0x..."
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
