/// Remote-engine TCP server. Streams server-side-filtered blocks to off-host
/// indexers, removing the collocation requirement.
///
/// A client sends one REGISTER (filter, cursor, follow). The server replays
/// every matching block in `(cursor, tip]` as PUSH frames. With follow=false it
/// then closes with GOAWAY. With follow=true it stays open, watching pending.bin
/// and streaming live blocks (plus REORG and HEARTBEAT) until the client
/// disconnects. A bounded worker pool serves connections concurrently.
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
const types = core.types;
const head_watch = core.head_watch;
const Entry = core.pending_format.Entry;

pub const Options = struct {
    data_dir: []const u8,
    /// Bind host. Defaults to localhost. Engine is never directly exposed.
    /// Remote clients reach it through an SSH tunnel. `0.0.0.0` is opt-in.
    host: []const u8 = "127.0.0.1",
    port: u16,
    /// Worker threads, each serving one connection at a time. A follow holds
    /// its worker until the client disconnects, so this caps concurrent
    /// indexers. Further clients wait in the listen backlog.
    max_connections: u32 = 16,
};

/// Per-connection serving config. `data_dir` lets the live phase watch
/// pending.bin. `heartbeat_ms` is the live-loop wait and heartbeat cadence.
pub const ServeConfig = struct {
    data_dir: []const u8,
    heartbeat_ms: u32 = 30_000,
};

/// Accept timeout. Bounds how long a worker blocks in `accept` before it
/// re-checks the stop flag, so the pool shuts down promptly. The daemon never
/// stops, so this just wakes idle workers harmlessly.
const ACCEPT_POLL_MS: i64 = 250;

/// Shared, read-only context every worker serves from. The reader and timestamp
/// store are mmaps, safe for concurrent preads. `stop` ends the pool.
const WorkerCtx = struct {
    server: *std.net.Server,
    reader: *const FlatStoreReader,
    ts: ?*const TimestampReader,
    cfg: ServeConfig,
    alloc: std.mem.Allocator,
    stop: *std.atomic.Value(bool),
};

/// Open the flat store once (read-only, mmap'd, shared across connections) and
/// serve forever from a bounded pool of `max_connections` worker threads.
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
    try setAcceptTimeout(server, ACCEPT_POLL_MS);
    core.log.info(
        "emit-engine serve: listening on {s}:{d} ({d} workers, data-dir {s}, tip {d})\n",
        .{ opts.host, server.listen_address.getPort(), opts.max_connections, opts.data_dir, tipOf(&reader) },
    );

    var stop = std.atomic.Value(bool).init(false);
    const ctx = WorkerCtx{ .server = &server, .reader = &reader, .ts = ts_ptr, .cfg = .{ .data_dir = opts.data_dir }, .alloc = alloc, .stop = &stop };

    const workers = try alloc.alloc(std.Thread, opts.max_connections);
    defer alloc.free(workers);
    var spawned: usize = 0;
    errdefer {
        stop.store(true, .release);
        for (workers[0..spawned]) |w| w.join();
    }
    while (spawned < workers.len) : (spawned += 1) {
        workers[spawned] = try std.Thread.spawn(.{}, acceptLoop, .{ctx});
    }
    for (workers) |w| w.join();
}

/// One worker: accept and fully serve connections until `stop` is set. The
/// accept timeout surfaces as `error.WouldBlock`, the cue to re-check stop.
fn acceptLoop(ctx: WorkerCtx) void {
    while (!ctx.stop.load(.acquire)) {
        const conn = ctx.server.accept() catch |e| {
            if (e == error.WouldBlock) continue;
            core.log.info("serve: accept failed: {s}\n", .{@errorName(e)});
            continue;
        };
        defer conn.stream.close();
        serveConnection(conn.stream, ctx.reader, ctx.ts, ctx.cfg, ctx.alloc) catch |e| {
            core.log.debug("serve: connection ended: {s}\n", .{@errorName(e)});
        };
    }
}

/// Bound `accept` so workers periodically re-check the stop flag.
fn setAcceptTimeout(server: std.net.Server, ms: i64) !void {
    const tv = std.posix.timeval{ .sec = @intCast(@divTrunc(ms, 1000)), .usec = @intCast(@mod(ms, 1000) * 1000) };
    try std.posix.setsockopt(server.stream.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv));
}

/// Serve one client: read REGISTER, stream the backfill, then either GOAWAY
/// (follow=false) or keep the connection open streaming live blocks
/// (follow=true). `reader` is shared read-only. `ts_reader` supplies exact
/// per-block times (null means the client falls back to its own formula).
pub fn serveConnection(
    stream: std.net.Stream,
    reader: *const FlatStoreReader,
    ts_reader: ?*const TimestampReader,
    cfg: ServeConfig,
    allocator: std.mem.Allocator,
) !void {
    setNoDelay(stream);
    clearRecvTimeout(stream);

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
    if (!reg.follow) return sendGoaway(stream, .shutdown, "backfill complete", allocator);
    try streamLive(stream, reg, tipOf(reader), cfg, allocator);
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

    const filter: core.filter.Filter = .{
        .match_addrs = reg.addresses,
        .match_topics = reg.topics,
        .exclude_addrs = reg.exclude_addresses,
    };

    // Shared with the SDK builder: a parallel io_uring read + filter pipeline,
    // chunked so each chunk's survivors PUSH before the next reads.
    var sink = PushSink{ .stream = stream, .ts = ts_reader };
    const r = try core.parallel_filter.run(reader, reg.addresses, filter, start, tip, PushSink, &sink, allocator);
    // A dropped block (read/decompress failure) means a hole. Refuse to hand the
    // client an incomplete index.
    if (r.dropped_blocks > 0) return error.BloomScanDropped;
}

/// PUSHes each filtered survivor with its exact timestamp. The chunked pipeline
/// emits in ascending block order, satisfying the client store's monotonic key.
const PushSink = struct {
    stream: std.net.Stream,
    ts: ?*const TimestampReader,
    pub fn emit(self: *PushSink, block_number: u64, entry: []const u8, _: u32) !void {
        const ts: u32 = if (self.ts) |r| @intCast(r.get(block_number) orelse 0) else 0;
        try sendPush(self.stream, block_number, ts, entry);
    }
};

/// Highest block number stored (the last dense index slot).
fn tipOf(reader: *const FlatStoreReader) u64 {
    return reader.first_block + reader.index_count - 1;
}

// ── Live streaming ─────────────────────────────────────────────────────────

/// After backfill, watch pending.bin and stream live blocks as they arrive.
/// Each ring update is diffed: new blocks are filtered and PUSHed, a reorg
/// emits REORG then re-streams the canonical tail, and every wake sends a
/// HEARTBEAT carrying the tip and finality boundary. Returns when the client
/// disconnects (a write fails) or the ring read errors.
fn streamLive(
    stream: std.net.Stream,
    reg: tcp_frame.Register,
    flat_tip: u64,
    cfg: ServeConfig,
    allocator: std.mem.Allocator,
) !void {
    var watcher = try head_watch.Watcher.init(cfg.data_dir);
    defer watcher.deinit();
    var prev = try head_watch.readPending(allocator, cfg.data_dir);
    defer prev.deinit(allocator);

    // Match set grows as the client registers live-discovered factory children
    // via ADD_ADDRESS. Seeded from the REGISTER's static + factory addresses.
    var match_addrs: std.ArrayListUnmanaged([20]u8) = .{};
    defer match_addrs.deinit(allocator);
    try match_addrs.appendSlice(allocator, reg.addresses);

    const decompress_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(decompress_buf);
    const serialize_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(serialize_buf);
    const compress_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(compress_buf);

    // Pending blocks above the backfill coverage (and the client's cursor)
    // bridge the gap between the finalized tip and the live head. They may
    // already be in the ring before the first wake, so stream them up front.
    const live_start = @max(reg.cursor, flat_tip);
    for (prev.entries) |e| {
        if (e.block_number <= live_start) continue;
        try filterAndPush(stream, e, filterFor(match_addrs.items, reg), decompress_buf, serialize_buf, compress_buf);
    }

    while (true) {
        const ready = watcher.waitWith(stream.handle, cfg.heartbeat_ms);

        // A live-discovered factory child. Extend the match set and re-stream
        // [from_block, tip] under the new filter so the client catches the
        // child's same-block-as-creation events.
        var add_floor: ?u64 = null;
        if (ready.extra) {
            const aa = try recvAddAddress(stream, allocator);
            try match_addrs.append(allocator, aa.address);
            add_floor = aa.from_block;
        }

        var curr = try head_watch.readPending(allocator, cfg.data_dir);
        var keep_curr = false;
        defer if (!keep_curr) curr.deinit(allocator);

        const last_finalized = try head_watch.readMeta(cfg.data_dir);
        var classification = try head_watch.classifyChanges(allocator, prev.entries, curr.entries, last_finalized);
        defer classification.deinit(allocator);

        const filter = filterFor(match_addrs.items, reg);

        // A reorg (same-block hash change or truncated tail) and an ADD_ADDRESS
        // floor both roll the client back and re-stream. The lower fork wins.
        if (minOpt(reorgFork(classification), add_floor)) |fork| {
            try sendReorg(stream, fork);
            for (curr.entries) |e| {
                if (e.block_number < fork) continue;
                try filterAndPush(stream, e, filter, decompress_buf, serialize_buf, compress_buf);
            }
        } else {
            for (classification.new_blocks) |e| {
                try filterAndPush(stream, e, filter, decompress_buf, serialize_buf, compress_buf);
            }
        }

        const tip = if (curr.entries.len > 0) curr.entries[curr.entries.len - 1].block_number else last_finalized;
        try sendHeartbeat(stream, tip, tip, last_finalized);

        prev.deinit(allocator);
        prev = curr;
        keep_curr = true;
    }
}

/// Decompress a pending block, filter to matching logs, PUSH when non-empty.
/// A block with no matching log produces no frame.
fn filterAndPush(
    stream: std.net.Stream,
    entry: Entry,
    filter: core.filter.Filter,
    decompress_buf: []u8,
    serialize_buf: []u8,
    compress_buf: []u8,
) !void {
    const decompressed = try log_serial.decompressEntry(entry.lz4_entry, decompress_buf);
    const maybe = try core.filter.filterBlockEntry(decompressed, filter, serialize_buf, compress_buf);
    const filtered = maybe orelse return;
    try sendPush(stream, entry.block_number, entry.timestamp, filtered.entry);
}

/// Lowest divergent block across both reorg signals, or null when the head
/// only grew. `reorg_from` is a same-block hash change, `reorged_out` a
/// disappeared tail. The fork is the minimum of either.
fn reorgFork(c: head_watch.Classification) ?u64 {
    var fork = c.reorg_from;
    for (c.reorged_out) |b| {
        fork = if (fork) |f| @min(f, b) else b;
    }
    return fork;
}

fn minOpt(a: ?u64, b: ?u64) ?u64 {
    if (a) |x| return if (b) |y| @min(x, y) else x;
    return b;
}

fn filterFor(addrs: []const [20]u8, reg: tcp_frame.Register) core.filter.Filter {
    return .{ .match_addrs = addrs, .match_topics = reg.topics, .exclude_addrs = reg.exclude_addresses };
}

/// Read one ADD_ADDRESS frame. The only frame valid client to engine while
/// following. Any other type aborts the connection.
fn recvAddAddress(stream: std.net.Stream, allocator: std.mem.Allocator) !tcp_frame.AddAddress {
    const payload = try readFrame(stream, .add_address, allocator);
    defer allocator.free(payload);
    return tcp_frame.AddAddress.decode(payload);
}

fn sendReorg(stream: std.net.Stream, fork_point: u64) !void {
    const payload = (tcp_frame.Reorg{ .fork_point = fork_point }).encode();
    try writeFrame(stream, .reorg, &payload);
}

fn sendHeartbeat(stream: std.net.Stream, cursor: u64, tip: u64, last_finalized: u64) !void {
    const payload = (tcp_frame.Heartbeat{ .cursor = cursor, .tip = tip, .last_finalized = last_finalized }).encode();
    try writeFrame(stream, .heartbeat, &payload);
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
    // One writev so the header and entry leave in a single syscall, and under
    // NODELAY a single segment when they fit.
    var iov = [_]std.posix.iovec_const{
        .{ .base = &head, .len = head.len },
        .{ .base = entry.ptr, .len = entry.len },
    };
    try stream.writevAll(&iov);
}

fn sendGoaway(stream: std.net.Stream, code: tcp_frame.GoawayCode, reason: []const u8, allocator: std.mem.Allocator) !void {
    const payload = try (tcp_frame.Goaway{ .code = code, .reason = reason }).encode(allocator);
    defer allocator.free(payload);
    try writeFrame(stream, .goaway, payload);
}

/// Write a framed message: 5-byte header then payload, in one writev.
fn writeFrame(stream: std.net.Stream, t: tcp_frame.FrameType, payload: []const u8) !void {
    const h = tcp_frame.header(t, @intCast(payload.len));
    if (payload.len == 0) return stream.writeAll(&h);
    var iov = [_]std.posix.iovec_const{
        .{ .base = &h, .len = h.len },
        .{ .base = payload.ptr, .len = payload.len },
    };
    try stream.writevAll(&iov);
}

/// Disable Nagle so small frames (PUSH, HEARTBEAT) flush immediately rather than
/// coalescing under a delay. Best-effort, a missing option is not fatal.
fn setNoDelay(stream: std.net.Stream) void {
    const one: c_int = 1;
    std.posix.setsockopt(stream.handle, std.posix.IPPROTO.TCP, std.os.linux.TCP.NODELAY, std.mem.asBytes(&one)) catch {};
}

/// Clear the recv timeout an accepted socket inherits from the listener (set by
/// `setAcceptTimeout` for the accept poll). Connection reads must block, not
/// time out at 250 ms, which would spuriously fail REGISTER over a latent link.
fn clearRecvTimeout(stream: std.net.Stream) void {
    const tv = std.posix.timeval{ .sec = 0, .usec = 0 };
    std.posix.setsockopt(stream.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
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
const ADDR_C: [20]u8 = [_]u8{0xCC} ** 20;
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
    data_dir: []const u8 = "",
    heartbeat_ms: u32 = 30_000,

    fn accept(self: *ServeCtx) void {
        const conn = self.server.accept() catch return;
        defer conn.stream.close();
        serveConnection(conn.stream, self.reader, self.ts, .{ .data_dir = self.data_dir, .heartbeat_ms = self.heartbeat_ms }, self.allocator) catch {};
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

/// Single-topic RawLog for pending fixtures.
fn mkLog(block: u64, log_index: u16, addr: [20]u8, topic: [32]u8) RawLog {
    return .{
        .block_number = block,
        .tx_index = 0,
        .log_index = log_index,
        .address = addr,
        .topic_count = 1,
        .topics = .{ topic, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };
}

/// Atomic-write a single-block, single-log pending.bin.
fn writePendingBlock(dir: std.fs.Dir, block: u64, ts: u32, addr: [20]u8, topic: [32]u8, hash: [32]u8, allocator: std.mem.Allocator) !void {
    try writePendingLogs(dir, block, ts, &.{mkLog(block, 0, addr, topic)}, hash, allocator);
}

/// Atomic-write a single-block pending.bin from `logs` (tmp + rename triggers
/// the Watcher).
fn writePendingLogs(dir: std.fs.Dir, block: u64, ts: u32, logs: []const RawLog, hash: [32]u8, allocator: std.mem.Allocator) !void {
    var serialize_buf: [4096]u8 = undefined;
    var compress_buf: [4096]u8 = undefined;
    const written = log_serial.serializeLogs(logs, &serialize_buf);
    const entry_len = try log_serial.compressEntry(serialize_buf[0..written], &compress_buf);
    const entry = core.pending_format.Entry{
        .block_number = block,
        .timestamp = ts,
        .hash = hash,
        .topic_bloom = log_serial.buildTopicBloom(logs).bits,
        .addr_bloom = log_serial.buildAddrBloom(logs).bits,
        .lz4_entry = compress_buf[0..entry_len],
    };
    const buf = try core.pending_format.serialize(allocator, &[_]core.pending_format.Entry{entry});
    defer allocator.free(buf);
    const tmpf = try dir.createFile("pending.bin.tmp", .{});
    try tmpf.writeAll(buf);
    tmpf.close();
    try dir.rename("pending.bin.tmp", "pending.bin");
}

/// Read frames, skipping heartbeats, until a PUSH for `block`. Asserts it
/// decodes to `want_logs` logs.
fn expectLivePush(client: std.net.Stream, allocator: std.mem.Allocator, block: u64, want_logs: usize) !void {
    var beats: u32 = 0;
    while (true) {
        const f = try recvFrame(client, allocator);
        defer allocator.free(f.payload);
        if (f.type == .heartbeat) {
            beats += 1;
            if (beats > 30) return error.NoLivePush;
            continue;
        }
        try testing.expectEqual(tcp_frame.FrameType.push, f.type);
        const p = try tcp_frame.Push.decode(f.payload);
        try testing.expectEqual(block, p.block_number);
        var decompress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
        var log_buf: [8]RawLog = undefined;
        const decoded = try log_serial.decompressEntry(p.lz4_entry, &decompress_buf);
        try testing.expectEqual(want_logs, log_serial.deserializeLogs(decoded, &log_buf));
        return;
    }
}

test "serve follow streams a live block injected into pending.bin" {
    const allocator = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    // Flat store with one matching block. Backfill streams it, then live.
    const blocks = [_]TestBlock{
        .{ .block_number = 100, .timestamp = 1_700_000_000, .logs = &.{.{ .address = ADDR_A, .topic0 = TOPIC_T }} },
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

    var ctx = ServeCtx{ .server = &server, .reader = &reader, .ts = ts_ptr, .allocator = allocator, .data_dir = path, .heartbeat_ms = 100 };
    const th = try std.Thread.spawn(.{}, ServeCtx.accept, .{&ctx});

    const client = try std.net.tcpConnectToAddress(try std.net.Address.parseIp("127.0.0.1", port));
    // Closing the client ends the server's live loop. Close before join.
    defer th.join();
    defer client.close();

    const reg = tcp_frame.Register{ .cursor = 0, .addresses = &.{ADDR_A}, .topics = &.{TOPIC_T}, .follow = true };
    const reg_payload = try reg.encode(allocator);
    defer allocator.free(reg_payload);
    try writeFrame(client, .register, reg_payload);

    // First PUSH is the backfilled block 100.
    const f0 = try recvFrame(client, allocator);
    defer allocator.free(f0.payload);
    try testing.expectEqual(tcp_frame.FrameType.push, f0.type);
    try testing.expectEqual(@as(u64, 100), (try tcp_frame.Push.decode(f0.payload)).block_number);

    // Inject a live block. The Watcher wakes and the engine streams it.
    try writePendingBlock(tmp.dir, 101, 1_700_000_012, ADDR_A, TOPIC_T, [_]u8{0xBB} ** 32, allocator);

    // Read until the live PUSH for block 101 arrives (HEARTBEATs ignored).
    var beats: u32 = 0;
    while (true) {
        const f = try recvFrame(client, allocator);
        defer allocator.free(f.payload);
        if (f.type == .heartbeat) {
            beats += 1;
            if (beats > 30) return error.NoLivePush;
            continue;
        }
        try testing.expectEqual(tcp_frame.FrameType.push, f.type);
        const p = try tcp_frame.Push.decode(f.payload);
        try testing.expectEqual(@as(u64, 101), p.block_number);
        try testing.expectEqual(@as(u32, 1_700_000_012), p.timestamp);
        break;
    }
}

test "serve follow adds a child via ADD_ADDRESS and re-streams its block" {
    const allocator = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const blocks = [_]TestBlock{
        .{ .block_number = 100, .timestamp = 1_700_000_000, .logs = &.{.{ .address = ADDR_A, .topic0 = TOPIC_T }} },
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

    var ctx = ServeCtx{ .server = &server, .reader = &reader, .ts = ts_ptr, .allocator = allocator, .data_dir = path, .heartbeat_ms = 100 };
    const th = try std.Thread.spawn(.{}, ServeCtx.accept, .{&ctx});

    const client = try std.net.tcpConnectToAddress(try std.net.Address.parseIp("127.0.0.1", port));
    defer th.join();
    defer client.close();

    // Register A with both topics. The child C/U is unmatched until ADD_ADDRESS.
    const reg = tcp_frame.Register{ .cursor = 0, .addresses = &.{ADDR_A}, .topics = &.{ TOPIC_T, TOPIC_U }, .follow = true };
    const reg_payload = try reg.encode(allocator);
    defer allocator.free(reg_payload);
    try writeFrame(client, .register, reg_payload);

    try expectLivePush(client, allocator, 100, 1);

    // Block 101 carries a factory log (A/T) and a child log (C/U). Only A/T
    // matches the initial filter.
    try writePendingLogs(tmp.dir, 101, 1_700_000_012, &.{ mkLog(101, 0, ADDR_A, TOPIC_T), mkLog(101, 1, ADDR_C, TOPIC_U) }, [_]u8{0xBB} ** 32, allocator);

    // The first live PUSH(101) carries only the factory log.
    try expectLivePush(client, allocator, 101, 1);

    // Register the child. The engine re-streams 101 under the extended filter.
    const aa = (tcp_frame.AddAddress{ .address = ADDR_C, .from_block = 101 }).encode();
    try writeFrame(client, .add_address, &aa);

    // A REORG(101) precedes the re-streamed PUSH(101), now holding both logs.
    var saw_reorg = false;
    var beats: u32 = 0;
    while (true) {
        const f = try recvFrame(client, allocator);
        defer allocator.free(f.payload);
        switch (f.type) {
            .heartbeat => {
                beats += 1;
                if (beats > 30) return error.NoRestream;
            },
            .reorg => {
                try testing.expectEqual(@as(u64, 101), (try tcp_frame.Reorg.decode(f.payload)).fork_point);
                saw_reorg = true;
            },
            .push => {
                try testing.expect(saw_reorg);
                const p = try tcp_frame.Push.decode(f.payload);
                try testing.expectEqual(@as(u64, 101), p.block_number);
                var decompress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
                var log_buf: [8]RawLog = undefined;
                const decoded = try log_serial.decompressEntry(p.lz4_entry, &decompress_buf);
                try testing.expectEqual(@as(usize, 2), log_serial.deserializeLogs(decoded, &log_buf));
                break;
            },
            else => return error.UnexpectedFrame,
        }
    }
}

test "the worker pool serves concurrent follow connections" {
    const allocator = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const blocks = [_]TestBlock{
        .{ .block_number = 100, .timestamp = 1_700_000_000, .logs = &.{.{ .address = ADDR_A, .topic0 = TOPIC_T }} },
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
    try setAcceptTimeout(server, ACCEPT_POLL_MS);

    var stop = std.atomic.Value(bool).init(false);
    const wctx = WorkerCtx{ .server = &server, .reader = &reader, .ts = ts_ptr, .cfg = .{ .data_dir = path, .heartbeat_ms = 100 }, .alloc = allocator, .stop = &stop };

    // Two workers. A single-worker pool would block the second follow forever.
    var w0 = try std.Thread.spawn(.{}, acceptLoop, .{wctx});
    var w1 = try std.Thread.spawn(.{}, acceptLoop, .{wctx});
    defer {
        w0.join();
        w1.join();
    }
    defer stop.store(true, .release);

    const addr = try std.net.Address.parseIp("127.0.0.1", port);
    const c0 = try std.net.tcpConnectToAddress(addr);
    defer c0.close();
    const c1 = try std.net.tcpConnectToAddress(addr);
    defer c1.close();

    const reg = tcp_frame.Register{ .cursor = 0, .addresses = &.{ADDR_A}, .topics = &.{TOPIC_T}, .follow = true };
    const reg_payload = try reg.encode(allocator);
    defer allocator.free(reg_payload);
    try writeFrame(c0, .register, reg_payload);
    try writeFrame(c1, .register, reg_payload);

    // Both follows are live at once, so both receive their backfill PUSH(100).
    try expectLivePush(c0, allocator, 100, 1);
    try expectLivePush(c1, allocator, 100, 1);
}

test "an accepted connection does not inherit the listener's recv timeout" {
    // Regression: setAcceptTimeout sets SO_RCVTIMEO on the listener, which Linux
    // copies onto accepted sockets. Without clearing it, a REGISTER that arrives
    // after the timeout fails with WouldBlock. Loopback hides this (reads are
    // instant); a latent link, or this deliberate stall, exposes it.
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
    try setAcceptTimeout(server, 50);

    var stop = std.atomic.Value(bool).init(false);
    const wctx = WorkerCtx{ .server = &server, .reader = &reader, .ts = null, .cfg = .{ .data_dir = path }, .alloc = allocator, .stop = &stop };
    var w = try std.Thread.spawn(.{}, acceptLoop, .{wctx});
    defer w.join();
    defer stop.store(true, .release);

    const client = try std.net.tcpConnectToAddress(try std.net.Address.parseIp("127.0.0.1", port));
    defer client.close();

    // Stall past the 50 ms accept timeout before sending REGISTER.
    std.Thread.sleep(150 * std.time.ns_per_ms);

    const reg = tcp_frame.Register{ .cursor = 0, .addresses = &.{ADDR_A}, .topics = &.{TOPIC_T} };
    const reg_payload = try reg.encode(allocator);
    defer allocator.free(reg_payload);
    try writeFrame(client, .register, reg_payload);

    // The backfill PUSH(100) still arrives: the inherited timeout was cleared.
    try expectLivePush(client, allocator, 100, 1);
}
