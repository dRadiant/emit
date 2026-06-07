/// Remote-engine TCP server
///
/// Streams server-side-filtered blocks to off-host indexers so an indexer
/// no longer has to be collocated with the engine.
///
/// **backfill only**. A client connects, sends one REGISTER
/// (its filter + cursor), and the server replays every block in `(cursor, tip]`
/// whose logs match — as PUSH frames — then closes with GOAWAY. Live streaming,
/// reorg signalling, and factory `ADD_ADDRESS` arrive in later steps, as does
/// concurrency: this accept loop serves one connection at a time.
///
/// Stateless across connections: all subscription state rides in
/// REGISTER, so a reconnect just re-streams from the client's cursor. The
/// engine never imports the sdk — the filtered-entry bytes come from
/// `core.filter.filterBlockEntry`, the same primitive the sdk builder uses, so
/// a streamed FilteredStore is byte-identical to a locally-built one.
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
    /// Bind host. Defaults to localhost — the engine is never directly exposed;
    /// remote clients reach it through an SSH tunnel. `0.0.0.0` is opt-in.
    host: []const u8 = "127.0.0.1",
    port: u16,
};

/// Open the flat store once (read-only, shared across connections) and serve
/// forever. The flat store is mmap'd; the accept loop is single-threaded for
/// now (a bounded per-client worker pool is a later step).
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
/// `reader` is shared read-only; `ts_reader` supplies exact per-block times
/// (null ⇒ the client falls back to its own formula).
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
    // The bloom scan needs at least one positive set; an empty filter is a
    // client bug, not a "match everything" request.
    if (reg.addresses.len == 0 and reg.topics.len == 0) {
        return sendGoaway(stream, .shutdown, "empty filter", allocator);
    }

    try streamBackfill(stream, reader, ts_reader, reg, allocator);
    return sendGoaway(stream, .shutdown, "backfill complete", allocator);
}

/// Bloom-scan `(cursor, tip]`, then PUSH every block whose logs actually match
/// the precise filter. A bloom false positive (block hit the bloom but no log
/// survives `filterBlockEntry`) is skipped silently — that is the expected
/// 8% the dual bloom lets through. A store read/decompress failure on a matched
/// block is **fatal**: we refuse to hand a client an index with a hole.
fn streamBackfill(
    stream: std.net.Stream,
    reader: *const FlatStoreReader,
    ts_reader: ?*const TimestampReader,
    reg: tcp_frame.Register,
    allocator: std.mem.Allocator,
) !void {
    if (reader.index_count == 0) return;
    const tip = tipOf(reader);
    const start = reg.cursor + 1; // cursor = the last block the client already has
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

    // One block's worth of scratch each — heap, not stack (BLOCK_BUF_SIZE is 4 MB).
    // Sequential pread per block; io_uring batching like the sdk builder is a
    // later optimization (the network write dominates a remote client anyway).
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
        const filtered = maybe orelse continue; // bloom false positive — no log matched
        const ts: u32 = if (ts_reader) |r| @intCast(r.get(bn) orelse 0) else 0;
        try sendPush(stream, bn, ts, filtered.entry);
    }
}

fn tipOf(reader: *const FlatStoreReader) u64 {
    return reader.first_block + reader.index_count - 1;
}

/// Collapse runs of equal block numbers in a sorted list, in place. Mirrors the
/// sdk builder's dedup so a streamed index matches a locally-built one even when
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

/// PUSH a block: `header ‖ prefix ‖ entry`. The 17-byte head goes in one write
/// and the lz4 entry straight from the block buffer in the next — no payload
/// copy, no per-block allocation.
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

/// Read one frame whose type must be `expect`; returns the owned payload.
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

/// Fill `buf` exactly, or fail — a short read means the peer closed mid-frame.
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
const bloom = core.bloom;
const flat_reader = core.flat_reader;
const RawLog = core.RawLog;

const ADDR_A: [20]u8 = [_]u8{0xAA} ** 20;
const ADDR_B: [20]u8 = [_]u8{0xBB} ** 20;
const TOPIC_T: [32]u8 = [_]u8{0x77} ** 32;
const TOPIC_U: [32]u8 = [_]u8{0x88} ** 32;

const PlantLog = struct { address: [20]u8, topic0: [32]u8 };
const PlantBlock = struct { block_number: u64, ts: u32, log: PlantLog };

/// Write a minimal flat store (one log per block) into `dir`, plus a
/// timestamps.bin. Mirrors the on-disk format `FlatStoreReader` reads.
fn plantStore(dir: std.fs.Dir, blocks: []const PlantBlock, allocator: std.mem.Allocator) !void {
    var blocks_file = try dir.createFile("blocks.dat", .{});
    defer blocks_file.close();
    var idx_file = try dir.createFile("blocks.idx", .{});
    defer idx_file.close();
    var blooms_file = try dir.createFile("blooms.bin", .{});
    defer blooms_file.close();

    var idx_hdr: [flat_reader.INDEX_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, idx_hdr[0..8], blocks[0].block_number, .little);
    std.mem.writeInt(u64, idx_hdr[8..16], blocks.len, .little);
    try idx_file.writeAll(&idx_hdr);

    var blooms_hdr: [flat_reader.BLOOM_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, &blooms_hdr, blocks.len, .little);
    try blooms_file.writeAll(&blooms_hdr);

    const serialize_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(serialize_buf);
    const compress_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(compress_buf);

    var ts_writer = try core.timestamps.TimestampWriter.open(dir, blocks[0].block_number);
    defer ts_writer.deinit();

    var offset: u64 = 0;
    for (blocks) |blk| {
        const log: RawLog = .{
            .block_number = blk.block_number,
            .tx_index = 0,
            .log_index = 0,
            .address = blk.log.address,
            .topic_count = 1,
            .topics = .{ blk.log.topic0, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
            .data = &.{},
            .tx_hash = [_]u8{0xFE} ** 32,
        };
        const written = log_serial.serializeLogs(&[_]RawLog{log}, serialize_buf);
        const entry_len = try log_serial.compressEntry(serialize_buf[0..written], compress_buf);
        try blocks_file.writeAll(compress_buf[0..entry_len]);

        var idx_entry: [flat_reader.INDEX_ENTRY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, idx_entry[0..8], offset, .little);
        std.mem.writeInt(u32, idx_entry[8..12], @intCast(entry_len), .little);
        try idx_file.writeAll(&idx_entry);

        const tb = log_serial.buildTopicBloom(&[_]RawLog{log});
        const ab = log_serial.buildAddrBloom(&[_]RawLog{log});
        var bloom_entry: [flat_reader.BLOOM_ENTRY_SIZE]u8 = std.mem.zeroes([flat_reader.BLOOM_ENTRY_SIZE]u8);
        std.mem.writeInt(u64, bloom_entry[0..8], blk.block_number, .big);
        @memcpy(bloom_entry[flat_reader.TOPIC_BLOOM_OFFSET..][0..bloom.BLOOM_SIZE], &tb.bits);
        @memcpy(bloom_entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE], &ab.bits);
        try blooms_file.writeAll(&bloom_entry);

        try ts_writer.set(blk.block_number, blk.ts);
        offset += entry_len;
    }
}

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

    // Block 100 (A/T) and 102 (A/T) match; 101 (B/T) and 103 (A/U) do not.
    const blocks = [_]PlantBlock{
        .{ .block_number = 100, .ts = 1_700_000_000, .log = .{ .address = ADDR_A, .topic0 = TOPIC_T } },
        .{ .block_number = 101, .ts = 1_700_000_012, .log = .{ .address = ADDR_B, .topic0 = TOPIC_T } },
        .{ .block_number = 102, .ts = 1_700_000_024, .log = .{ .address = ADDR_A, .topic0 = TOPIC_T } },
        .{ .block_number = 103, .ts = 1_700_000_036, .log = .{ .address = ADDR_A, .topic0 = TOPIC_U } },
    };
    try plantStore(tmp.dir, &blocks, allocator);

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
    const blocks = [_]PlantBlock{.{ .block_number = 100, .ts = 1, .log = .{ .address = ADDR_A, .topic0 = TOPIC_T } }};
    try plantStore(tmp.dir, &blocks, allocator);

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
