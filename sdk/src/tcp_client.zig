/// Remote-engine TCP client. Connects to an engine `serve` listener, registers
/// a filter, and streams the server-side-filtered backfill into a local
/// `FilteredStore`. The engine produces entries with `core.filter`, the same
/// primitive the local builder uses, so the streamed store is byte-identical to
/// a locally-built one and `scanner.replay` runs against it unchanged.
///
/// Backfill only. Live following, reorg handling, and factory ADD_ADDRESS are
/// separate from this entry point.
const std = @import("std");

const core = @import("core");

const tcp_frame = core.tcp_frame;
const filtered_store_mod = @import("filtered_store.zig");

const FilteredStore = filtered_store_mod.FilteredStore;

/// REGISTER inputs. `cursor` is the client's last fully-dispatched block, read
/// from `state.snap`. The engine streams `(cursor, tip]`. `addresses`/`topics`
/// are the manifest's positive match sets.
pub const Filter = struct {
    cursor: u64,
    addresses: []const [20]u8,
    topics: []const [32]u8,
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

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

const PushFixture = struct { block: u64, ts: u32, entry: []const u8 };

const MockServer = struct {
    server: *std.net.Server,
    pushes: []const PushFixture,
    allocator: std.mem.Allocator,
    // Filled from the REGISTER the client sends, for assertions.
    got_cursor: u64 = 0,
    got_addrs: usize = 0,
    got_topics: usize = 0,

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

        for (self.pushes) |p| {
            const payload = (tcp_frame.Push{ .block_number = p.block, .timestamp = p.ts, .lz4_entry = p.entry }).encode(self.allocator) catch return;
            defer self.allocator.free(payload);
            writeFrame(conn.stream, .push, payload) catch return;
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
