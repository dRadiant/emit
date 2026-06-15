/// Remote-engine TCP client. Connects to an engine `serve` listener, registers
/// a filter, and streams the server-side-filtered backfill into a local
/// `FilteredStore`. The engine produces entries with `core.filter`, the same
/// primitive the local builder uses, so the streamed store is byte-identical to
/// a locally-built one and `scanner.replay` runs against it unchanged.
///
/// `backfill` is the one-shot path (REGISTER, stream, GOAWAY). `follow` keeps
/// the connection open after backfill and dispatches live blocks through the
/// per-block overlay, rolling back the reorged tail on REORG. Factory
/// ADD_ADDRESS is separate.
const std = @import("std");

const core = @import("core");

const tcp_frame = core.tcp_frame;
const filtered_store_mod = @import("filtered_store.zig");
const live = @import("live.zig");
const handler_mod = @import("handler.zig");
const sdk_manifest = @import("manifest.zig");
const humanize = @import("humanize.zig");

const FilteredStore = filtered_store_mod.FilteredStore;

/// REGISTER inputs. `cursor` is the client's last fully-dispatched block, read
/// from `state.snap`. The engine streams `(cursor, tip]`. `addresses`/`topics`
/// are the manifest's positive match sets.
pub const Filter = struct {
    cursor: u64,
    addresses: []const [20]u8,
    topics: []const [32]u8,
    /// Negative filter. The children pass excludes static∪factory so an address
    /// that is both a discovered child and a declared contract is not streamed twice.
    exclude_addresses: []const [20]u8 = &.{},
    /// Ask the engine to trail each PUSH entry with its tx subtable. The
    /// engine refuses (GOAWAY) when its store cannot cover the range.
    tx_fields: bool = false,
};

pub const BackfillResult = struct {
    blocks_received: u64 = 0,
    /// Highest block written. Advances the caller's cursor for reconnect.
    last_block: u64,
    goaway: tcp_frame.GoawayCode,
};

pub const Error = error{ UnexpectedFrame, UnexpectedReorg, ConnectionClosed };

/// Connect to `host:port`, REGISTER `filter`, and stream PUSH frames into
/// `store` until GOAWAY. Returns the received count, the final cursor, and the
/// GOAWAY code. Caller owns `store` and decides whether to reconnect.
pub fn backfill(
    host: []const u8,
    port: u16,
    filter: Filter,
    store: *FilteredStore,
    allocator: std.mem.Allocator,
) !BackfillResult {
    const stream = try std.net.tcpConnectToHost(allocator, host, port);
    defer stream.close();

    const reg_payload = try (tcp_frame.Register{
        .cursor = filter.cursor,
        .addresses = filter.addresses,
        .topics = filter.topics,
        .exclude_addresses = filter.exclude_addresses,
        .tx_fields = filter.tx_fields,
    }).encode(allocator);
    defer allocator.free(reg_payload);
    try writeFrame(stream, .register, reg_payload);

    var result = BackfillResult{ .last_block = filter.cursor, .goaway = .shutdown };

    // Grown on demand. A PUSH is one block's filtered entry, bounded by
    // MAX_PAYLOAD at the header check.
    var payload_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(payload_buf);

    var hdr: [tcp_frame.HEADER_SIZE]u8 = undefined;
    while (true) {
        try readExact(stream, &hdr);
        const h = try tcp_frame.parseHeader(&hdr);
        if (h.len > payload_buf.len) payload_buf = try allocator.realloc(payload_buf, h.len);
        try readExact(stream, payload_buf[0..h.len]);
        switch (h.type) {
            .push => {
                const p = try tcp_frame.Push.decode(payload_buf[0..h.len]);
                // appendEntry copies the bytes, so the next read may reuse the buffer.
                try store.appendEntry(p.block_number, p.timestamp, p.lz4_entry);
                result.blocks_received += 1;
                result.last_block = p.block_number;
            },
            .goaway => {
                result.goaway = (try tcp_frame.Goaway.decode(payload_buf[0..h.len])).code;
                break;
            },
            // Liveness only. A backfill carries no timing obligation.
            .heartbeat => {},
            // The backfill range is finalized, so the engine never reorgs it.
            .reorg => return Error.UnexpectedReorg,
            // register and add_address are client-to-engine only.
            else => return Error.UnexpectedFrame,
        }
    }
    try store.syncAll();
    return result;
}

// ── Live following ─────────────────────────────────────────────────────────

/// Backoff between a dropped connection and the reconnect attempt.
const RECONNECT_BACKOFF_NS: u64 = 500 * std.time.ns_per_ms;

/// Recv timeout on the follow socket. The read loop wakes this often to re-check
/// the stop flag, so `deinit` interrupts a follow idling on a quiet socket
/// within one period rather than hanging until the next frame.
const FOLLOW_RECV_TIMEOUT_MS: i64 = 250;

/// Follow the live head over a persistent connection. After the in-band
/// backfill, the engine streams PUSH (a new block), HEARTBEAT (finalization
/// progress), and REORG. Each PUSH dispatches into the per-block overlay. Each
/// HEARTBEAT commits the overlay blocks at or below `last_finalized`. A dropped
/// connection rebuilds the overlay and re-registers from the committed cursor,
/// which the engine re-streams. Blocks until `ctx` signals stop.
pub fn follow(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    host: []const u8,
    port: u16,
    addresses: []const [20]u8,
    topics: []const [32]u8,
    allocator: std.mem.Allocator,
) !void {
    live.enter(ctx);

    const decompress_buf = try allocator.alloc(u8, core.types.BLOCK_BUF_SIZE);
    defer allocator.free(decompress_buf);
    const log_buf = try allocator.alloc(core.RawLog, core.types.MAX_LOGS_PER_BLOCK);
    defer allocator.free(log_buf);
    var payload_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(payload_buf);
    // Tx subtable scratch, sized to the structural u16 ceiling.
    const tx_buf = if (comptime sdk_manifest.wantsTxFields(m)) try allocator.alloc(core.txs.TxRecord, core.txs.MAX_RECORDS) else &[_]core.txs.TxRecord{};
    defer if (comptime sdk_manifest.wantsTxFields(m)) allocator.free(tx_buf);

    // Blocks dispatched into the overlay but not yet finalized. Ascending by
    // arrival, so a HEARTBEAT finalizes a prefix.
    var live_blocks: std.ArrayListUnmanaged(u64) = .{};
    defer live_blocks.deinit(allocator);

    while (!live.stopRequested(ctx)) {
        followOnce(m, Handler, ctx, host, port, addresses, topics, decompress_buf, log_buf, tx_buf, &payload_buf, &live_blocks, allocator) catch |e| {
            // A fork below finality can't be recovered by reconnecting. Surface
            // it. Everything else is a dropped connection: back off and retry,
            // visibly. A silent retry loop reads as a healthy indexer while the
            // engine is unreachable.
            if (e == error.ReorgExceedsFinalityDepth) return e;
            core.log.info("remote follow: {s}, reconnecting\n", .{@errorName(e)});
            std.Thread.sleep(RECONNECT_BACKOFF_NS);
        };
    }
}

/// One connection's lifetime. Registers from the committed cursor with a fresh
/// overlay, then dispatches frames until GOAWAY, stop, or a read error. The
/// overlay reset makes a reconnect idempotent: the engine re-streams every
/// uncommitted block, so discarding and replaying yields the same state.
fn followOnce(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    host: []const u8,
    port: u16,
    addresses: []const [20]u8,
    topics: []const [32]u8,
    decompress_buf: []u8,
    log_buf: []core.RawLog,
    tx_buf: []core.txs.TxRecord,
    payload_buf: *[]u8,
    live_blocks: *std.ArrayListUnmanaged(u64),
    allocator: std.mem.Allocator,
) !void {
    const stream = try std.net.tcpConnectToHost(allocator, host, port);
    defer stream.close();
    try setRecvTimeout(stream, FOLLOW_RECV_TIMEOUT_MS);
    setNoDelay(stream);

    live.lockCtx(ctx);
    live.discardAllOverlays(ctx);
    live.unlockCtx(ctx);
    live_blocks.clearRetainingCapacity();

    // Register the static match set plus every child discovered so far, so a
    // reconnect keeps following children found during backfill or a prior
    // session. Children found live are added mid-stream via ADD_ADDRESS.
    var reg_addrs: std.ArrayListUnmanaged([20]u8) = .{};
    defer reg_addrs.deinit(allocator);
    try reg_addrs.appendSlice(allocator, addresses);
    try appendChildAddresses(ctx, &reg_addrs, allocator);

    const reg_payload = try (tcp_frame.Register{
        .cursor = ctx._last_dispatched_block,
        .addresses = reg_addrs.items,
        .topics = topics,
        .follow = true,
        .tx_fields = comptime sdk_manifest.wantsTxFields(m),
    }).encode(allocator);
    defer allocator.free(reg_payload);
    try writeFrame(stream, .register, reg_payload);

    var new_children: std.ArrayListUnmanaged([20]u8) = .{};
    defer new_children.deinit(allocator);

    var hdr: [tcp_frame.HEADER_SIZE]u8 = undefined;
    while (!live.stopRequested(ctx)) {
        // The header read is the idle wait point. A timeout there re-checks stop.
        switch (try fillOrTimeout(stream, &hdr)) {
            .timed_out => continue,
            .ok => {},
        }
        const h = try tcp_frame.parseHeader(&hdr);
        if (h.len > payload_buf.*.len) payload_buf.* = try allocator.realloc(payload_buf.*, h.len);
        // The payload trails its header immediately. Ride idle timeouts until full.
        while ((try fillOrTimeout(stream, payload_buf.*[0..h.len])) == .timed_out) {}
        switch (h.type) {
            .push => {
                const p = try tcp_frame.Push.decode(payload_buf.*[0..h.len]);
                live.lockCtx(ctx);
                defer live.unlockCtx(ctx);
                new_children.clearRetainingCapacity();
                try dispatchPush(m, Handler, ctx, p, decompress_buf, log_buf, tx_buf, &new_children, allocator);
                try live_blocks.append(allocator, p.block_number);
                // Register children spawned this block. The engine mini-backfills
                // [block, tip] for each, catching their same-block events.
                for (new_children.items) |child| try sendAddAddress(stream, child, p.block_number);
            },
            .heartbeat => {
                const hb = try tcp_frame.Heartbeat.decode(payload_buf.*[0..h.len]);
                live.lockCtx(ctx);
                defer live.unlockCtx(ctx);
                try promoteUpTo(ctx, live_blocks, hb.last_finalized);
            },
            .reorg => {
                const r = try tcp_frame.Reorg.decode(payload_buf.*[0..h.len]);
                live.lockCtx(ctx);
                defer live.unlockCtx(ctx);
                // A fork at or below the committed cursor would rewrite finalized
                // state. Beyond the recoverable window, so fail fatally.
                if (r.fork_point <= ctx._last_dispatched_block) return error.ReorgExceedsFinalityDepth;
                // Roll back the reorged tail. The engine re-streams the canonical
                // blocks at/above the fork as PUSH frames, which re-dispatch.
                live.discardOverlaysFrom(ctx, r.fork_point);
                var keep: usize = 0;
                for (live_blocks.items) |b| {
                    if (b < r.fork_point) {
                        live_blocks.items[keep] = b;
                        keep += 1;
                    }
                }
                live_blocks.items.len = keep;
            },
            // The engine is closing this connection. Reconnect from the cursor.
            .goaway => return,
            else => return Error.UnexpectedFrame,
        }
    }
}

/// Dispatch one streamed block. The engine already filtered it to the
/// registered addresses, so every log dispatches. The PUSH carries the exact
/// header timestamp (0 = unknown, derive it).
fn dispatchPush(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    p: tcp_frame.Push,
    decompress_buf: []u8,
    log_buf: []core.RawLog,
    tx_buf: []core.txs.TxRecord,
    new_children: *std.ArrayListUnmanaged([20]u8),
    allocator: std.mem.Allocator,
) !void {
    const decoded = try core.log_serial.decompressEntry(p.lz4_entry, decompress_buf);
    // Untrusted network input. The bounds-checked parse rejects a forged frame
    // (error propagates to follow's reconnect) instead of an OOB in ReleaseFast.
    const log_count = try core.log_serial.deserializeLogsChecked(decoded, log_buf);
    for (log_buf[0..log_count]) |*log| log.block_number = p.block_number;

    // The registered tx_fields entitles every PUSH to a trailing subtable.
    // An absent tail parses as Truncated, fail loud over a silent null tx.
    var tx_records: []const core.txs.TxRecord = &.{};
    if (comptime sdk_manifest.wantsTxFields(m)) {
        const lz4_len = std.mem.readInt(u32, p.lz4_entry[0..4], .little);
        tx_records = try core.txs.deserializeRecords(p.lz4_entry[4 + lz4_len ..], tx_buf);
    }

    // Factory children spawned in this block. Their same-block events were
    // filtered out (the child wasn't registered yet), so the caller mini-
    // backfills via ADD_ADDRESS.
    if (comptime m.factories.len > 0) try discoverNewChildren(m, ctx, log_buf[0..log_count], new_children, allocator);

    live.setLiveBlock(ctx, p.block_number);
    ctx.block_number = p.block_number;
    ctx.timestamp = if (p.timestamp != 0) @as(u64, p.timestamp) else humanize.timestampOf(ctx, p.block_number);

    for (log_buf[0..log_count]) |log| {
        const tx: ?*const core.txs.TxRecord = if (comptime sdk_manifest.wantsTxFields(m))
            core.txs.find(tx_records, log.tx_index) orelse return error.MissingTxRecord
        else
            null;
        try handler_mod.dispatchLog(m, Handler, ctx, log, tx);
    }
}

/// Append every runtime-discovered child address to `out`. No-op for a ctx
/// with no child set (non-factory manifests, counter-shaped tests).
fn appendChildAddresses(ctx: anytype, out: *std.ArrayListUnmanaged([20]u8), allocator: std.mem.Allocator) !void {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime !@hasField(T, "_child_addresses")) return;
    const set = ctx._child_addresses orelse return;
    var it = set.keyIterator();
    while (it.next()) |a| try out.append(allocator, a.*);
}

/// Add children spawned by create-events in `logs` to the runtime set,
/// reporting the ones not seen before. Mirrors the local loop's discovery, but
/// surfaces new entries so the caller can ADD_ADDRESS them mid-stream.
fn discoverNewChildren(
    comptime m: sdk_manifest.Manifest,
    ctx: anytype,
    logs: []const core.RawLog,
    new_children: *std.ArrayListUnmanaged([20]u8),
    allocator: std.mem.Allocator,
) !void {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime !@hasField(T, "_child_addresses")) return;
    const set = ctx._child_addresses orelse return;
    for (logs) |log| {
        if (log.topic_count == 0) continue;
        inline for (m.factories) |f| {
            const create_topic = comptime sdk_manifest.eventTopic0(f.create_event);
            if (std.mem.eql(u8, &log.address, &f.address) and std.mem.eql(u8, &log.topics[0], &create_topic)) {
                const addr = sdk_manifest.extractFactoryAddress(f, &log.topics, log.data);
                const gop = try set.getOrPut(addr);
                if (!gop.found_existing) try new_children.append(allocator, addr);
            }
        }
    }
}

fn sendAddAddress(stream: std.net.Stream, address: [20]u8, from_block: u64) !void {
    const payload = (tcp_frame.AddAddress{ .address = address, .from_block = from_block }).encode();
    try writeFrame(stream, .add_address, &payload);
}

/// Commit the overlay blocks at or below `last_finalized`, then drop them from
/// the pending list. `live_blocks` is ascending, so the finalized set is a
/// leading prefix.
fn promoteUpTo(ctx: anytype, live_blocks: *std.ArrayListUnmanaged(u64), last_finalized: u64) !void {
    var count: usize = 0;
    while (count < live_blocks.items.len and live_blocks.items[count] <= last_finalized) : (count += 1) {}
    if (count == 0) return;
    try live.promoteFinalized(ctx, live_blocks.items[0..count]);
    const remaining = live_blocks.items.len - count;
    std.mem.copyForwards(u64, live_blocks.items[0..remaining], live_blocks.items[count..]);
    live_blocks.items.len = remaining;
}

// ── Wire I/O ───────────────────────────────────────────────────────────────
// Duplicated with the engine server. Shared with it once a third caller lands.

/// Write a framed message. 5-byte header then payload.
fn writeFrame(stream: std.net.Stream, t: tcp_frame.FrameType, payload: []const u8) !void {
    const h = tcp_frame.header(t, @intCast(payload.len));
    try stream.writeAll(&h);
    if (payload.len > 0) try stream.writeAll(payload);
}

/// Fill `buf` exactly, or fail. A short read means the peer closed mid-frame.
fn readExact(stream: std.net.Stream, buf: []u8) !void {
    var n: usize = 0;
    while (n < buf.len) {
        const r = try stream.read(buf[n..]);
        if (r == 0) return Error.ConnectionClosed;
        n += r;
    }
}

/// Bound blocking reads on the follow socket, so the read loop can periodically
/// re-check stop. A fired timeout surfaces as `error.WouldBlock` on `read`.
fn setRecvTimeout(stream: std.net.Stream, ms: i64) !void {
    const tv = std.posix.timeval{ .sec = @intCast(@divTrunc(ms, 1000)), .usec = @intCast(@mod(ms, 1000) * 1000) };
    try std.posix.setsockopt(stream.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv));
}

/// Disable Nagle so an ADD_ADDRESS flushes without coalescing delay. Best-effort.
fn setNoDelay(stream: std.net.Stream) void {
    const one: c_int = 1;
    std.posix.setsockopt(stream.handle, std.posix.IPPROTO.TCP, std.os.linux.TCP.NODELAY, std.mem.asBytes(&one)) catch {};
}

/// Fill `buf`, distinguishing an idle timeout from a closed peer. `.timed_out`
/// is returned only when the recv timeout fires before any byte arrives, the
/// caller's cue to re-check stop. Once a frame starts, idle timeouts are ridden
/// through so framing stays intact.
fn fillOrTimeout(stream: std.net.Stream, buf: []u8) !enum { ok, timed_out } {
    var n: usize = 0;
    while (n < buf.len) {
        const r = stream.read(buf[n..]) catch |e| switch (e) {
            error.WouldBlock => {
                if (n == 0) return .timed_out;
                continue;
            },
            else => return e,
        };
        if (r == 0) return Error.ConnectionClosed;
        n += r;
    }
    return .ok;
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

const PushFixture = struct { block: u64, ts: u32, entry: []const u8 };

const MockServer = struct {
    server: *std.net.Server,
    pushes: []const PushFixture,
    allocator: std.mem.Allocator,
    // When set, a REORG with this fork point is sent after the pushes.
    reorg_fork: ?u64 = null,
    // When set, a HEARTBEAT with this `last_finalized` follows the pushes.
    heartbeat_last_finalized: ?u64 = null,
    // Filled from the REGISTER the client sends, for assertions.
    got_cursor: u64 = 0,
    got_addrs: usize = 0,
    got_topics: usize = 0,
    got_follow: bool = false,

    /// Accept one client, decode its REGISTER, stream the fixtures, GOAWAY.
    fn serve(self: *MockServer) void {
        const conn = self.server.accept() catch return;
        defer conn.stream.close();

        var hdr: [tcp_frame.HEADER_SIZE]u8 = undefined;
        readExact(conn.stream, &hdr) catch return;
        const h = tcp_frame.parseHeader(&hdr) catch return;
        const reg_buf = self.allocator.alloc(u8, h.len) catch return;
        defer self.allocator.free(reg_buf);
        readExact(conn.stream, reg_buf) catch return;
        const reg = tcp_frame.Register.decode(reg_buf) catch return;
        self.got_cursor = reg.cursor;
        self.got_addrs = reg.addresses.len;
        self.got_topics = reg.topics.len;
        self.got_follow = reg.follow;

        for (self.pushes) |p| {
            const payload = (tcp_frame.Push{ .block_number = p.block, .timestamp = p.ts, .lz4_entry = p.entry }).encode(self.allocator) catch return;
            defer self.allocator.free(payload);
            writeFrame(conn.stream, .push, payload) catch return;
        }
        if (self.reorg_fork) |fork| {
            const payload = (tcp_frame.Reorg{ .fork_point = fork }).encode();
            writeFrame(conn.stream, .reorg, &payload) catch return;
        }
        if (self.heartbeat_last_finalized) |lf| {
            const hb = (tcp_frame.Heartbeat{ .cursor = lf, .tip = lf, .last_finalized = lf }).encode();
            writeFrame(conn.stream, .heartbeat, &hb) catch return;
        }
        const goaway = (tcp_frame.Goaway{ .code = .shutdown }).encode(self.allocator) catch return;
        defer self.allocator.free(goaway);
        writeFrame(conn.stream, .goaway, goaway) catch return;
    }
};

test "backfill registers a filter and writes streamed PUSH entries to the store" {
    const allocator = testing.allocator;

    const ADDR: [20]u8 = [_]u8{0xAB} ** 20;
    const TOPIC: [32]u8 = [_]u8{0xCD} ** 32;
    const entry_a = [_]u8{ 0xAA, 0xBB };
    const entry_b = [_]u8{ 0x11, 0x22, 0x33 };
    const pushes = [_]PushFixture{
        .{ .block = 100, .ts = 1_700_000_000, .entry = &entry_a },
        .{ .block = 102, .ts = 1_700_000_024, .entry = &entry_b },
    };

    var server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();

    var mock = MockServer{ .server = &server, .pushes = &pushes, .allocator = allocator };
    const th = try std.Thread.spawn(.{}, MockServer.serve, .{&mock});

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try FilteredStore.open(allocator, tmp.dir, "primary");
    defer store.deinit();

    const addrs = [_][20]u8{ADDR};
    const topics = [_][32]u8{TOPIC};
    const result = try backfill("127.0.0.1", port, .{ .cursor = 18_500_000, .addresses = &addrs, .topics = &topics }, &store, allocator);
    th.join();

    // Client sent the right REGISTER.
    try testing.expectEqual(@as(u64, 18_500_000), mock.got_cursor);
    try testing.expectEqual(@as(usize, 1), mock.got_addrs);
    try testing.expectEqual(@as(usize, 1), mock.got_topics);

    // Result reflects the stream.
    try testing.expectEqual(@as(u64, 2), result.blocks_received);
    try testing.expectEqual(@as(u64, 102), result.last_block);
    try testing.expectEqual(tcp_frame.GoawayCode.shutdown, result.goaway);

    // Entries landed in the store with block, timestamp, and payload intact.
    try testing.expectEqual(@as(u64, 2), store.count());
    const e0 = try store.readEntry(0);
    try testing.expectEqual(@as(u64, 100), e0.block_number);
    try testing.expectEqual(@as(u32, 1_700_000_000), e0.timestamp);
    var buf: [16]u8 = undefined;
    try testing.expectEqualSlices(u8, &entry_a, try store.readPayload(0, &buf));
    const e1 = try store.readEntry(1);
    try testing.expectEqual(@as(u64, 102), e1.block_number);
    try testing.expectEqualSlices(u8, &entry_b, try store.readPayload(1, &buf));
}

const FollowTransfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
};
const FOLLOW_CONTRACT: [20]u8 = [_]u8{0xAB} ** 20;
const FollowManifest: sdk_manifest.Manifest = .{
    .name = "follow-test",
    .chain_id = 1,
    .start_block = 0,
    .contracts = &.{.{ .name = "T", .address = FOLLOW_CONTRACT, .events = &.{FollowTransfer} }},
};

// Counter-shaped ctx. No `stores`, so promoteFinalized is a no-op on the entity
// side. The test asserts the loop's dispatch and the prefix bookkeeping. The
// end-to-end commit (cursor advance) is covered by the integration suite.
const FollowRunner = struct {
    _allocator: std.mem.Allocator,
    _lock: std.Thread.Mutex = .{},
    _last_dispatched_block: u64 = 0,
    block_number: u64 = 0,
    timestamp: u64 = 0,
    transfers: u32 = 0,
    pub fn handleTransfer(_: handler_mod.Log(FollowTransfer), self: *FollowRunner) !void {
        self.transfers += 1;
    }
};

test "follow dispatches streamed live blocks and finalizes a heartbeat prefix" {
    const allocator = testing.allocator;

    // One Transfer from the registered contract, compressed as the engine would.
    const log: core.RawLog = .{
        .block_number = 0,
        .tx_index = 0,
        .log_index = 0,
        .address = FOLLOW_CONTRACT,
        .topic_count = 1,
        .topics = .{ sdk_manifest.eventTopic0(FollowTransfer), [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };
    var ser_buf: [4096]u8 = undefined;
    var comp_buf: [4096]u8 = undefined;
    const sn = core.log_serial.serializeLogs(&.{log}, &ser_buf);
    const entry = comp_buf[0..try core.log_serial.compressEntry(ser_buf[0..sn], &comp_buf)];

    const pushes = [_]PushFixture{
        .{ .block = 101, .ts = 1_700_000_012, .entry = entry },
        .{ .block = 102, .ts = 1_700_000_024, .entry = entry },
    };

    var server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();

    // Heartbeat finalizes 101 but not 102.
    var mock = MockServer{ .server = &server, .pushes = &pushes, .allocator = allocator, .heartbeat_last_finalized = 101 };
    const th = try std.Thread.spawn(.{}, MockServer.serve, .{&mock});

    var runner = FollowRunner{ ._allocator = allocator };
    const addrs = [_][20]u8{FOLLOW_CONTRACT};
    const topics = [_][32]u8{sdk_manifest.eventTopic0(FollowTransfer)};

    const decompress_buf = try allocator.alloc(u8, core.types.BLOCK_BUF_SIZE);
    defer allocator.free(decompress_buf);
    const log_buf = try allocator.alloc(core.RawLog, core.types.MAX_LOGS_PER_BLOCK);
    defer allocator.free(log_buf);
    var payload_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(payload_buf);
    var live_blocks: std.ArrayListUnmanaged(u64) = .{};
    defer live_blocks.deinit(allocator);

    try followOnce(FollowManifest, FollowRunner, &runner, "127.0.0.1", port, &addrs, &topics, decompress_buf, log_buf, &.{}, &payload_buf, &live_blocks, allocator);
    th.join();

    // The client registered to follow from its committed cursor.
    try testing.expect(mock.got_follow);
    try testing.expectEqual(@as(u64, 0), mock.got_cursor);

    // Both blocks dispatched, last block recorded.
    try testing.expectEqual(@as(u32, 2), runner.transfers);
    try testing.expectEqual(@as(u64, 102), runner.block_number);

    // Heartbeat finalized 101: it drains, 102 stays pending in the overlay.
    try testing.expectEqual(@as(usize, 1), live_blocks.items.len);
    try testing.expectEqual(@as(u64, 102), live_blocks.items[0]);
}

test "follow treats a reorg below the committed cursor as fatal" {
    const allocator = testing.allocator;

    var server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();

    // Fork at 50 while the client has committed through 100: a finality
    // violation no reconnect can fix.
    var mock = MockServer{ .server = &server, .pushes = &.{}, .allocator = allocator, .reorg_fork = 50 };
    const th = try std.Thread.spawn(.{}, MockServer.serve, .{&mock});

    var runner = FollowRunner{ ._allocator = allocator, ._last_dispatched_block = 100 };
    const addrs = [_][20]u8{FOLLOW_CONTRACT};
    const topics = [_][32]u8{sdk_manifest.eventTopic0(FollowTransfer)};

    const decompress_buf = try allocator.alloc(u8, core.types.BLOCK_BUF_SIZE);
    defer allocator.free(decompress_buf);
    const log_buf = try allocator.alloc(core.RawLog, core.types.MAX_LOGS_PER_BLOCK);
    defer allocator.free(log_buf);
    var payload_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(payload_buf);
    var live_blocks: std.ArrayListUnmanaged(u64) = .{};
    defer live_blocks.deinit(allocator);

    const r = followOnce(FollowManifest, FollowRunner, &runner, "127.0.0.1", port, &addrs, &topics, decompress_buf, log_buf, &.{}, &payload_buf, &live_blocks, allocator);
    th.join();
    try testing.expectError(error.ReorgExceedsFinalityDepth, r);
}
