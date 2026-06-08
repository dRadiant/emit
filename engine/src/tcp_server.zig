/// Remote-engine TCP server. Streams server-side-filtered blocks to off-host
/// indexers, removing the collocation requirement.
///
/// Backfill only. A client sends one REGISTER (filter + cursor), the server
/// replays every block in `(cursor, tip]` whose logs match as PUSH frames, then
/// closes with GOAWAY. Accept loop serves one connection at a time.
///
/// Stateless across connections: all subscription state rides in REGISTER, so a
/// reconnect re-streams from the client's cursor. Filtered-entry bytes come from
/// `core.filter.filterBlockEntry`, the same primitive the sdk builder uses, so a
/// streamed FilteredStore is byte-identical to a locally-built one.
const std = @import("std");

const core = @import("core");

const tcp_frame = core.tcp_frame;
const FlatStoreReader = core.FlatStoreReader;
const TimestampReader = core.TimestampReader;
const log_serial = core.log_serial;
const block_filter = core.block_filter;
const types = core.types;

pub const Options = struct {
    data_dir: []const u8,
    /// Bind host. Defaults to localhost. Engine is never directly exposed.
    /// Remote clients reach it through an SSH tunnel. `0.0.0.0` is opt-in.
    host: []const u8 = "127.0.0.1",
    port: u16,
};

/// Open the flat store once (read-only, mmap'd, shared across connections) and
/// serve forever. Accept loop is single-threaded.
pub fn run(opts: Options) !void {
    const alloc = std.heap.page_allocator;

    var reader = try FlatStoreReader.open(opts.data_dir);
    defer reader.deinit();

    var dir = try std.fs.cwd().openDir(opts.data_dir, .{});
    defer dir.close();
    var ts_reader = try TimestampReader.open(dir);
    defer if (ts_reader) |*r| r.deinit();
    const ts_ptr: ?*const TimestampReader = if (ts_reader) |*r| r else null;

    const address = try std.net.Address.parseIp(opts.host, opts.port);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();
    std.debug.print(
        "emit-engine serve: listening on {s}:{d} (data-dir {s}, tip {d})\n",
        .{ opts.host, server.listen_address.getPort(), opts.data_dir, tipOf(&reader) },
    );

    while (true) {
        const conn = server.accept() catch |e| {
            std.debug.print("serve: accept failed: {s}\n", .{@errorName(e)});
            continue;
        };
        defer conn.stream.close();
        serveConnection(conn.stream, &reader, ts_ptr, alloc) catch |e| {
            std.debug.print("serve: connection ended: {s}\n", .{@errorName(e)});
        };
    }
}

/// Serve one client to completion: read REGISTER, stream the backfill, GOAWAY.
/// `reader` is shared read-only. `ts_reader` supplies exact per-block times.
/// Null means the client falls back to its own formula.
fn serveConnection(
    stream: std.net.Stream,
    reader: *const FlatStoreReader,
    ts_reader: ?*const TimestampReader,
    allocator: std.mem.Allocator,
) !void {
    const reg_payload = try readFrame(stream, .register, allocator);
    defer allocator.free(reg_payload);
    const reg = try tcp_frame.Register.decode(reg_payload);

    if (reg.version != tcp_frame.PROTOCOL_VERSION) {
        return sendGoaway(stream, .version_mismatch, "unsupported protocol version", allocator);
    }
    // Bloom scan needs at least one positive set. Empty filter is a client bug,
    // not a "match everything" request.
    if (reg.addresses.len == 0 and reg.topics.len == 0) {
        return sendGoaway(stream, .shutdown, "empty filter", allocator);
    }

    try streamBackfill(stream, reader, ts_reader, reg, allocator);
    return sendGoaway(stream, .shutdown, "backfill complete", allocator);
}

/// Bloom-scan `(cursor, tip]`, then PUSH every block whose logs match the
/// precise filter. A bloom false positive (block hit the bloom but no log
/// survives `filterBlockEntry`) is skipped silently. The expected 8% the dual
/// bloom lets through. A store read/decompress failure on a matched block is
/// fatal. Refuse to hand a client an index with a hole.
fn streamBackfill(
    stream: std.net.Stream,
    reader: *const FlatStoreReader,
    ts_reader: ?*const TimestampReader,
    reg: tcp_frame.Register,
    allocator: std.mem.Allocator,
) !void {
    if (reader.index_count == 0) return;
    const tip = tipOf(reader);
    const start = reg.cursor + 1; // cursor = last block the client already has
    if (start > tip) return; // already caught up

    var matching = std.ArrayListUnmanaged(u64){};
    defer matching.deinit(allocator);
    var scanned: u64 = 0;
    var dropped: u64 = 0;
    try block_filter.scanBloomsParallel(reader, reg.addresses, reg.topics, start, tip, &matching, &scanned, &dropped, allocator);
    if (dropped > 0) return error.BloomScanDropped;

    dedupAdjacent(&matching); // blooms.bin can carry duplicate block rows (reorg collapse)

    const filter: core.filter.Filter = .{
        .match_addrs = reg.addresses,
        .match_topics = reg.topics,
        .exclude_addrs = &.{},
    };

    // One block's worth of scratch each, heap not stack (BLOCK_BUF_SIZE is 4 MB).
    // Sequential pread per block. Network write dominates a remote client, so
    // io_uring batching like the sdk builder buys little here.
    const read_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(read_buf);
    const decompress_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(decompress_buf);
    const serialize_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(serialize_buf);
    const compress_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(compress_buf);

    for (matching.items) |bn| {
        const entry_data = try reader.readBlock(bn, read_buf);
        const decompressed = try log_serial.decompressEntry(entry_data, decompress_buf);
        const maybe = try core.filter.filterBlockEntry(decompressed, filter, serialize_buf, compress_buf);
        const filtered = maybe orelse continue; // bloom false positive, no log matched
        const ts: u32 = if (ts_reader) |r| @intCast(r.get(bn) orelse 0) else 0;
        try sendPush(stream, bn, ts, filtered.entry);
    }
}

/// Highest block number stored (the last dense index slot).
fn tipOf(reader: *const FlatStoreReader) u64 {
    return reader.first_block + reader.index_count - 1;
}

/// Collapse runs of equal block numbers in a sorted list, in place. Mirrors the
/// sdk builder's dedup so a streamed index matches a locally-built one when
/// `blooms.bin` holds duplicate rows for a reorged block.
fn dedupAdjacent(list: *std.ArrayListUnmanaged(u64)) void {
    if (list.items.len < 2) return;
    var w: usize = 1;
    for (1..list.items.len) |r| {
        if (list.items[r] == list.items[r - 1]) continue;
        list.items[w] = list.items[r];
        w += 1;
    }
    list.items.len = w;
}

// ── Wire I/O ──────────────────────────────────────────────────────────────

/// PUSH a block: `header ‖ prefix ‖ entry`. The 17-byte head goes in one write.
/// The lz4 entry follows straight from the block buffer. No payload copy, no
/// per-block allocation.
fn sendPush(stream: std.net.Stream, block_number: u64, timestamp: u32, entry: []const u8) !void {
    const h = tcp_frame.header(.push, @intCast(tcp_frame.Push.PREFIX + entry.len));
    const pfx = tcp_frame.Push.prefix(block_number, timestamp);
    var head: [tcp_frame.HEADER_SIZE + tcp_frame.Push.PREFIX]u8 = undefined;
    @memcpy(head[0..tcp_frame.HEADER_SIZE], &h);
    @memcpy(head[tcp_frame.HEADER_SIZE..], &pfx);
    try stream.writeAll(&head);
    try stream.writeAll(entry);
}

fn sendGoaway(stream: std.net.Stream, code: tcp_frame.GoawayCode, reason: []const u8, allocator: std.mem.Allocator) !void {
    const payload = try (tcp_frame.Goaway{ .code = code, .reason = reason }).encode(allocator);
    defer allocator.free(payload);
    try writeFrame(stream, .goaway, payload);
}

/// Write a framed message: 5-byte header then payload.
fn writeFrame(stream: std.net.Stream, t: tcp_frame.FrameType, payload: []const u8) !void {
    const h = tcp_frame.header(t, @intCast(payload.len));
    try stream.writeAll(&h);
    if (payload.len > 0) try stream.writeAll(payload);
}

/// Read one frame whose type must be `expect`. Returns the owned payload.
fn readFrame(stream: std.net.Stream, expect: tcp_frame.FrameType, allocator: std.mem.Allocator) ![]u8 {
    var hdr: [tcp_frame.HEADER_SIZE]u8 = undefined;
    try readExact(stream, &hdr);
    const h = try tcp_frame.parseHeader(&hdr);
    if (h.type != expect) return error.UnexpectedFrame;
    const payload = try allocator.alloc(u8, h.len);
    errdefer allocator.free(payload);
    try readExact(stream, payload);
    return payload;
}

/// Fill `buf` exactly, or fail. A short read means the peer closed mid-frame.
fn readExact(stream: std.net.Stream, buf: []u8) !void {
    var n: usize = 0;
    while (n < buf.len) {
        const r = try stream.read(buf[n..]);
        if (r == 0) return error.ConnectionClosed;
        n += r;
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;
const flat_reader = core.flat_reader;
const RawLog = core.RawLog;
const TestBlock = flat_reader.TestBlock;

const ADDR_A: [20]u8 = [_]u8{0xAA} ** 20;
const ADDR_B: [20]u8 = [_]u8{0xBB} ** 20;
const TOPIC_T: [32]u8 = [_]u8{0x77} ** 32;
const TOPIC_U: [32]u8 = [_]u8{0x88} ** 32;

/// One received frame: type + owned payload.
const RecvFrame = struct { type: tcp_frame.FrameType, payload: []u8 };

fn recvFrame(stream: std.net.Stream, allocator: std.mem.Allocator) !RecvFrame {
    var hdr: [tcp_frame.HEADER_SIZE]u8 = undefined;
    try readExact(stream, &hdr);
    const h = try tcp_frame.parseHeader(&hdr);
    const payload = try allocator.alloc(u8, h.len);
    errdefer allocator.free(payload);
    try readExact(stream, payload);
    return .{ .type = h.type, .payload = payload };
}

const ServeCtx = struct {
    server: *std.net.Server,
    reader: *const FlatStoreReader,
    ts: ?*const TimestampReader,
    allocator: std.mem.Allocator,

    fn accept(self: *ServeCtx) void {
        const conn = self.server.accept() catch return;
        defer conn.stream.close();
        serveConnection(conn.stream, self.reader, self.ts, self.allocator) catch {};
    }
};

test "serve streams the matching backfill blocks as PUSH, then GOAWAY" {
    const allocator = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    // Blocks 100 (A/T) and 102 (A/T) match. 101 (B/T) and 103 (A/U) do not.
    const blocks = [_]TestBlock{
        .{ .block_number = 100, .timestamp = 1_700_000_000, .logs = &.{.{ .address = ADDR_A, .topic0 = TOPIC_T }} },
        .{ .block_number = 101, .timestamp = 1_700_000_012, .logs = &.{.{ .address = ADDR_B, .topic0 = TOPIC_T }} },
        .{ .block_number = 102, .timestamp = 1_700_000_024, .logs = &.{.{ .address = ADDR_A, .topic0 = TOPIC_T }} },
        .{ .block_number = 103, .timestamp = 1_700_000_036, .logs = &.{.{ .address = ADDR_A, .topic0 = TOPIC_U }} },
    };
    try flat_reader.writeTestStore(tmp.dir, &blocks, allocator);

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);
    var reader = try FlatStoreReader.open(path);
    defer reader.deinit();
    var ts_reader = try TimestampReader.open(tmp.dir);
    defer if (ts_reader) |*r| r.deinit();
    const ts_ptr: ?*const TimestampReader = if (ts_reader) |*r| r else null;

    var server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();

    var ctx = ServeCtx{ .server = &server, .reader = &reader, .ts = ts_ptr, .allocator = allocator };
    const th = try std.Thread.spawn(.{}, ServeCtx.accept, .{&ctx});

    const client = try std.net.tcpConnectToAddress(try std.net.Address.parseIp("127.0.0.1", port));
    defer client.close();

    // REGISTER for address A + topic T from genesis (cursor 0).
    const reg = tcp_frame.Register{ .cursor = 0, .addresses = &.{ADDR_A}, .topics = &.{TOPIC_T} };
    const reg_payload = try reg.encode(allocator);
    defer allocator.free(reg_payload);
    try writeFrame(client, .register, reg_payload);

    // Collect PUSH frames until GOAWAY.
    var pushed = std.ArrayListUnmanaged(tcp_frame.Push){};
    defer pushed.deinit(allocator);
    var entries = std.ArrayListUnmanaged([]u8){};
    defer {
        for (entries.items) |e| allocator.free(e);
        entries.deinit(allocator);
    }
    var goaway: ?tcp_frame.GoawayCode = null;
    while (goaway == null) {
        const f = try recvFrame(client, allocator);
        switch (f.type) {
            .push => {
                const p = try tcp_frame.Push.decode(f.payload);
                // Keep the entry bytes alive past freeing the frame payload.
                const owned = try allocator.dupe(u8, p.lz4_entry);
                try entries.append(allocator, owned);
                try pushed.append(allocator, .{ .block_number = p.block_number, .timestamp = p.timestamp, .lz4_entry = owned });
                allocator.free(f.payload);
            },
            .goaway => {
                goaway = (try tcp_frame.Goaway.decode(f.payload)).code;
                allocator.free(f.payload);
            },
            else => {
                allocator.free(f.payload);
                return error.UnexpectedFrame;
            },
        }
    }
    th.join();

    try testing.expectEqual(tcp_frame.GoawayCode.shutdown, goaway.?);
    try testing.expectEqual(@as(usize, 2), pushed.items.len);

    try testing.expectEqual(@as(u64, 100), pushed.items[0].block_number);
    try testing.expectEqual(@as(u32, 1_700_000_000), pushed.items[0].timestamp);
    try testing.expectEqual(@as(u64, 102), pushed.items[1].block_number);
    try testing.expectEqual(@as(u32, 1_700_000_024), pushed.items[1].timestamp);

    // Each PUSH entry decodes back to exactly the planted matching log.
    var decompress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var log_buf: [4]RawLog = undefined;
    for (pushed.items, [_][20]u8{ ADDR_A, ADDR_A }) |push, want_addr| {
        const decoded = try log_serial.decompressEntry(push.lz4_entry, &decompress_buf);
        const n = log_serial.deserializeLogs(decoded, &log_buf);
        try testing.expectEqual(@as(usize, 1), n);
        try testing.expectEqualSlices(u8, &want_addr, &log_buf[0].address);
        try testing.expectEqualSlices(u8, &TOPIC_T, &log_buf[0].topics[0]);
    }
}

test "serve rejects an empty filter with GOAWAY" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const blocks = [_]TestBlock{.{ .block_number = 100, .timestamp = 1, .logs = &.{.{ .address = ADDR_A, .topic0 = TOPIC_T }} }};
    try flat_reader.writeTestStore(tmp.dir, &blocks, allocator);

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);
    var reader = try FlatStoreReader.open(path);
    defer reader.deinit();

    var server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();

    var ctx = ServeCtx{ .server = &server, .reader = &reader, .ts = null, .allocator = allocator };
    const th = try std.Thread.spawn(.{}, ServeCtx.accept, .{&ctx});

    const client = try std.net.tcpConnectToAddress(try std.net.Address.parseIp("127.0.0.1", port));
    defer client.close();

    const reg_payload = try (tcp_frame.Register{ .cursor = 0 }).encode(allocator);
    defer allocator.free(reg_payload);
    try writeFrame(client, .register, reg_payload);

    const f = try recvFrame(client, allocator);
    defer allocator.free(f.payload);
    th.join();
    try testing.expectEqual(tcp_frame.FrameType.goaway, f.type);
    try testing.expectEqual(tcp_frame.GoawayCode.shutdown, (try tcp_frame.Goaway.decode(f.payload)).code);
}
