/// Follows the chain head, appending new blocks to the pending ring
/// and finalizing to the flat store.
///
/// Prefers WebSocket (push via eth_subscribe newHeads, <1s latency).
/// Falls back to HTTP polling (~1s interval) if --ws not provided.
/// Both use eth.zig for transport and JSON-RPC parsing.
const std = @import("std");
const eth = @import("eth");
const core = @import("core");

const FlatStoreWriter = @import("flat_writer.zig").FlatStoreWriter;
const PendingRing = @import("pending_ring.zig").PendingRing;
const log_serial = core.log_serial;
const types = core.types;

pub const FollowConfig = struct {
    rpc_url: []const u8,
    ws_url: ?[]const u8 = null,
    data_dir: []const u8,
    poll_interval_ms: u64 = 1000,
};

pub fn run(config: FollowConfig) !void {
    const alloc = std.heap.page_allocator;

    var http = eth.http_transport.HttpTransport.init(alloc, config.rpc_url);
    var provider = eth.provider.Provider.init(alloc, &http);

    var writer = try FlatStoreWriter.open(config.data_dir);
    const dir = try std.fs.cwd().openDir(config.data_dir, .{});
    var ring = try PendingRing.open(dir, alloc);
    defer ring.deinit();

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
        followPoll(&provider, &writer, &ring, alloc) catch {};
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

    var num_buf: [20]u8 = undefined;
    const hex = try std.fmt.bufPrint(&num_buf, "0x{x}", .{block_number});
    const eth_logs = try provider.getLogs(.{ .fromBlock = hex, .toBlock = hex });

    const count = @min(eth_logs.len, types.MAX_LOGS_PER_BLOCK);
    const raw_logs = try alloc.alloc(types.RawLog, count);
    for (eth_logs[0..count], 0..) |log, i| {
        raw_logs[i] = toRawLog(log, block_number, alloc);
    }

    const topic_bloom = log_serial.buildTopicBloom(raw_logs);
    const addr_bloom = log_serial.buildAddrBloom(raw_logs);

    const serialize_buf = try alloc.alloc(u8, types.BLOCK_BUF_SIZE);
    const serialized_len = log_serial.serializeLogs(raw_logs, serialize_buf);

    const compress_buf = try alloc.alloc(u8, types.BLOCK_BUF_SIZE);
    const entry_len = try log_serial.compressEntry(serialize_buf[0..serialized_len], compress_buf);

    try ring.insert(block_number, header.hash, &topic_bloom.bits, &addr_bloom.bits, compress_buf[0..entry_len]);
    std.debug.print("Block {d}: {d} logs\n", .{ block_number, count });
}

/// Move blocks with 64+ confirmations from pending ring to flat store.
fn finalizeReady(ring: *PendingRing, writer: *FlatStoreWriter, head: u64, alloc: std.mem.Allocator) void {
    while (ring.canFinalize(head)) {
        const oldest = (ring.popOldest() catch break) orelse break;
        defer alloc.free(oldest.lz4_entry);
        writer.appendBlock(oldest.block_number, oldest.lz4_entry, &oldest.topic_bloom, &oldest.addr_bloom) catch break;
        std.debug.print("Finalized block {d}\n", .{oldest.block_number});
    }
}

fn toRawLog(log: eth.receipt.Log, block_number: u64, alloc: std.mem.Allocator) types.RawLog {
    var topics: [types.MAX_TOPICS][32]u8 = std.mem.zeroes([types.MAX_TOPICS][32]u8);
    var topic_count: u8 = 0;
    for (log.topics) |t| {
        if (topic_count >= types.MAX_TOPICS) break;
        topics[topic_count] = t;
        topic_count += 1;
    }

    const data: []const u8 = if (log.data.len > 0)
        alloc.dupe(u8, log.data) catch &.{}
    else
        &.{};

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
