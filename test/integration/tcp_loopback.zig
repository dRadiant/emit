//! Engine `serve` path and sdk client in one process over a real socket. A
//! streamed FilteredStore must hold the same filtered logs as a locally-built
//! one, and additionally carry the exact per-block timestamps a local build
//! leaves at 0.
const std = @import("std");

const core = @import("core");
const engine = @import("engine");
const sdk = @import("sdk_internal");

const tcp_server = engine.tcp_server;
const tcp_client = sdk.tcp_client;
const filter_builder = sdk.filter_builder;
const filtered_store = sdk.filtered_store;
const sdk_manifest = sdk.manifest;

const FlatStoreReader = core.FlatStoreReader;
const TimestampReader = core.TimestampReader;
const FilteredStore = filtered_store.FilteredStore;
const TestBlock = core.flat_reader.TestBlock;
const writeTestStore = core.flat_reader.writeTestStore;

const Transfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
};
const Approval = struct {
    pub const signature = "Approval(address,address,uint256)";
};

const ADDR_TOKEN: [20]u8 = [_]u8{0x42} ** 20;
const ADDR_OTHER: [20]u8 = [_]u8{0x99} ** 20;

const TokenManifest: sdk_manifest.Manifest = .{
    .name = "loopback",
    .chain_id = 1,
    .start_block = 0,
    .contracts = &.{.{ .name = "Token", .address = ADDR_TOKEN, .events = &.{Transfer} }},
};

const ServeCtx = struct {
    server: *std.net.Server,
    reader: *const FlatStoreReader,
    ts: ?*const TimestampReader,
    allocator: std.mem.Allocator,
    // Connections to serve before the thread returns. A factory backfill opens
    // two (primary REGISTER, then children REGISTER).
    conns: usize = 1,
    data_dir: []const u8 = "",
    heartbeat_ms: u32 = 30_000,

    fn run(self: *ServeCtx) void {
        var n: usize = 0;
        while (n < self.conns) : (n += 1) {
            const conn = self.server.accept() catch return;
            defer conn.stream.close();
            tcp_server.serveConnection(conn.stream, self.reader, self.ts, .{ .data_dir = self.data_dir, .heartbeat_ms = self.heartbeat_ms }, self.allocator) catch {};
        }
    }
};

test "streamed FilteredStore matches a local build, with exact timestamps added" {
    const allocator = std.testing.allocator;

    const tt = sdk_manifest.eventTopic0(Transfer);
    const other = sdk_manifest.eventTopic0(Approval);

    // Blocks 100 and 102 match (token address, Transfer topic). 101 (other
    // address) and 103 (token address, non-Transfer topic) do not.
    const blocks = [_]TestBlock{
        .{ .block_number = 100, .timestamp = 1_700_000_000, .logs = &.{.{ .address = ADDR_TOKEN, .topic0 = tt }} },
        .{ .block_number = 101, .timestamp = 1_700_000_012, .logs = &.{.{ .address = ADDR_OTHER, .topic0 = tt }} },
        .{ .block_number = 102, .timestamp = 1_700_000_024, .logs = &.{.{ .address = ADDR_TOKEN, .topic0 = tt }} },
        .{ .block_number = 103, .timestamp = 1_700_000_036, .logs = &.{.{ .address = ADDR_TOKEN, .topic0 = other }} },
    };

    var src = std.testing.tmpDir(.{});
    defer src.cleanup();
    try writeTestStore(src.dir, &blocks, allocator);

    var src_path: [std.fs.max_path_bytes]u8 = undefined;
    const path = try src.dir.realpath(".", &src_path);
    var reader = try FlatStoreReader.open(path);
    defer reader.deinit();
    var ts_reader = try TimestampReader.open(src.dir);
    defer if (ts_reader) |*r| r.deinit();
    const ts_ptr: ?*const TimestampReader = if (ts_reader) |*r| r else null;

    // Local build for comparison.
    var local = std.testing.tmpDir(.{});
    defer local.cleanup();
    _ = try filter_builder.build(&reader, TokenManifest, null, local.dir, allocator);

    // Remote stream into a separate store.
    var server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();
    var ctx = ServeCtx{ .server = &server, .reader = &reader, .ts = ts_ptr, .allocator = allocator };
    const th = try std.Thread.spawn(.{}, ServeCtx.run, .{&ctx});

    var remote = std.testing.tmpDir(.{});
    defer remote.cleanup();
    var remote_store = try FilteredStore.open(allocator, remote.dir, "primary");
    defer remote_store.deinit();

    const addrs = comptime filter_builder.collectKnownAddresses(TokenManifest);
    const topics = comptime filter_builder.collectAllTopics(TokenManifest);
    const result = try tcp_client.backfill("127.0.0.1", port, .{ .cursor = 0, .addresses = addrs, .topics = topics }, &remote_store, allocator);
    th.join();

    try std.testing.expectEqual(@as(u64, 2), result.blocks_received);
    try std.testing.expectEqual(@as(u64, 102), result.last_block);

    var local_store = try FilteredStore.open(allocator, local.dir, "primary");
    defer local_store.deinit();
    try std.testing.expectEqual(@as(u64, 2), local_store.count());
    try std.testing.expectEqual(local_store.count(), remote_store.count());

    // Per entry: same block and same filtered payload bytes (identical
    // filtering). Local timestamp is 0, remote carries the exact value.
    var lbuf: [4096]u8 = undefined;
    var rbuf: [4096]u8 = undefined;
    const want_blocks = [_]u64{ 100, 102 };
    const want_ts = [_]u32{ 1_700_000_000, 1_700_000_024 };
    for (want_blocks, want_ts, 0..) |bn, ts, i| {
        const le = try local_store.readEntry(i);
        const re = try remote_store.readEntry(i);
        try std.testing.expectEqual(bn, le.block_number);
        try std.testing.expectEqual(bn, re.block_number);
        try std.testing.expectEqualSlices(u8, try local_store.readPayload(i, &lbuf), try remote_store.readPayload(i, &rbuf));
        try std.testing.expectEqual(@as(u32, 0), le.timestamp);
        try std.testing.expectEqual(ts, re.timestamp);
    }
}

// One immutable record per dispatched Transfer, capturing the block and its
// timestamp. Lets the end-to-end test compare a remote init against a local one.
const Hit = struct {
    pub const storage: sdk.StorageMode = .immutable;
    id: [16]u8,
    block: u64,
    ts: u64,
};

const HitHandler = struct {
    pub fn handleTransfer(log: sdk.handler.Log(Transfer), ctx: anytype) !void {
        try ctx.stores.hits.save(.{ .id = log.eventId(), .block = ctx.block_number, .ts = ctx.timestamp });
    }
};

test "remote init produces the same entity state as a local init" {
    const allocator = std.testing.allocator;
    const tt = sdk_manifest.eventTopic0(Transfer);

    const blocks = [_]TestBlock{
        .{ .block_number = 100, .timestamp = 1_700_000_000, .logs = &.{.{ .address = ADDR_TOKEN, .topic0 = tt }} },
        .{ .block_number = 101, .timestamp = 1_700_000_012, .logs = &.{.{ .address = ADDR_OTHER, .topic0 = tt }} },
        .{ .block_number = 102, .timestamp = 1_700_000_024, .logs = &.{.{ .address = ADDR_TOKEN, .topic0 = tt }} },
    };

    var src = std.testing.tmpDir(.{});
    defer src.cleanup();
    try writeTestStore(src.dir, &blocks, allocator);
    var src_path: [std.fs.max_path_bytes]u8 = undefined;
    const path = try src.dir.realpath(".", &src_path);

    // Local init reads the flat store directly.
    var local_data = std.testing.tmpDir(.{});
    defer local_data.cleanup();
    var ld: [std.fs.max_path_bytes]u8 = undefined;
    const local_ctx = try sdk.entry.init(TokenManifest, HitHandler, .{Hit}, .{
        .engine_data_dir = path,
        .data_dir = try local_data.dir.realpath(".", &ld),
    }, allocator);
    defer local_ctx.deinit();

    // Remote init streams the backfill from the engine instead.
    var reader = try FlatStoreReader.open(path);
    defer reader.deinit();
    var ts_reader = try TimestampReader.open(src.dir);
    defer if (ts_reader) |*r| r.deinit();
    const ts_ptr: ?*const TimestampReader = if (ts_reader) |*r| r else null;

    var server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();
    var ctx = ServeCtx{ .server = &server, .reader = &reader, .ts = ts_ptr, .allocator = allocator };
    const th = try std.Thread.spawn(.{}, ServeCtx.run, .{&ctx});

    var remote_data = std.testing.tmpDir(.{});
    defer remote_data.cleanup();
    var rd: [std.fs.max_path_bytes]u8 = undefined;
    const remote_ctx = try sdk.entry.init(TokenManifest, HitHandler, .{Hit}, .{
        .engine_data_dir = path, // unused in remote mode
        .data_dir = try remote_data.dir.realpath(".", &rd),
        .remote_engine = .{ .host = "127.0.0.1", .port = port },
    }, allocator);
    defer remote_ctx.deinit();
    th.join();

    try std.testing.expectEqual(@as(u64, 2), remote_ctx.stats.blocks_dispatched);
    try std.testing.expectEqual(local_ctx.stats.blocks_dispatched, remote_ctx.stats.blocks_dispatched);
    try std.testing.expectEqual(local_ctx.count(Hit), remote_ctx.count(Hit));

    // Indexed span is reported on both paths, not left at 0 on remote. Local
    // reads it from the flat reader, remote from the streamed store. Endpoints
    // coincide here since blocks 100 and 102 both match.
    try std.testing.expectEqual(@as(u64, 100), remote_ctx.stats.start_block);
    try std.testing.expectEqual(@as(u64, 102), remote_ctx.stats.end_block);
    try std.testing.expectEqual(local_ctx.stats.start_block, remote_ctx.stats.start_block);
    try std.testing.expectEqual(local_ctx.stats.end_block, remote_ctx.stats.end_block);

    // Record by record: same block, and the same exact timestamp (local reads
    // it from timestamps.bin, remote from the streamed FilteredStore entry).
    var lrec: [4]Hit = undefined;
    var rrec: [4]Hit = undefined;
    const ls = try local_ctx.range(Hit, 0, &lrec);
    const rs = try remote_ctx.range(Hit, 0, &rrec);
    try std.testing.expectEqual(ls.len, rs.len);
    for (ls, rs) |l, r| {
        try std.testing.expectEqual(l.block, r.block);
        try std.testing.expectEqual(l.ts, r.ts);
    }
}

// ── Factory streaming ──────────────────────────────────────────────────────

const Create = struct {
    pub const signature = "Create(address child)";
};
const Ping = struct {
    pub const signature = "Ping(uint256)";
};
const FACTORY_ADDR: [20]u8 = [_]u8{0xF0} ** 20;
const CHILD_ADDR: [20]u8 = [_]u8{0xC1} ** 20;

const FactoryManifest: sdk_manifest.Manifest = .{
    .name = "factory",
    .chain_id = 1,
    .start_block = 0,
    .factories = &.{.{
        .name = "F",
        .address = FACTORY_ADDR,
        .create_event = Create,
        .spawn_param = "child",
        .child_events = &.{Ping},
    }},
};

// One immutable record per child Ping, to compare local vs remote factory backfill.
const PingHit = struct {
    pub const storage: sdk.StorageMode = .immutable;
    id: [16]u8,
    block: u64,
};

const FacHandler = struct {
    pub fn handleCreate(log: sdk.handler.Log(Create), ctx: anytype) !void {
        _ = log;
        _ = ctx;
    }
    pub fn handlePing(log: sdk.handler.Log(Ping), ctx: anytype) !void {
        try ctx.stores.pingHits.save(.{ .id = log.eventId(), .block = ctx.block_number });
    }
};

test "remote factory backfill discovers children and matches a local build" {
    const allocator = std.testing.allocator;

    // Block 200: factory creation, child address in the non-indexed data word.
    // Block 201: the created child emits Ping. Only the child pass should catch it.
    var child_word: [32]u8 = [_]u8{0} ** 32;
    @memcpy(child_word[12..], &CHILD_ADDR);
    const blocks = [_]TestBlock{
        .{ .block_number = 200, .timestamp = 1_700_000_000, .logs = &.{.{ .address = FACTORY_ADDR, .topic0 = sdk_manifest.eventTopic0(Create), .data = &child_word }} },
        .{ .block_number = 201, .timestamp = 1_700_000_012, .logs = &.{.{ .address = CHILD_ADDR, .topic0 = sdk_manifest.eventTopic0(Ping) }} },
    };

    var src = std.testing.tmpDir(.{});
    defer src.cleanup();
    try writeTestStore(src.dir, &blocks, allocator);
    var src_path: [std.fs.max_path_bytes]u8 = undefined;
    const path = try src.dir.realpath(".", &src_path);

    // Local factory build (build + scanCreations + appendChildren + replay).
    var local_data = std.testing.tmpDir(.{});
    defer local_data.cleanup();
    var ld: [std.fs.max_path_bytes]u8 = undefined;
    const local_ctx = try sdk.entry.init(FactoryManifest, FacHandler, .{PingHit}, .{
        .engine_data_dir = path,
        .data_dir = try local_data.dir.realpath(".", &ld),
    }, allocator);
    defer local_ctx.deinit();

    // Remote: two REGISTERs (primary, then the discovered children).
    var reader = try FlatStoreReader.open(path);
    defer reader.deinit();
    var ts_reader = try TimestampReader.open(src.dir);
    defer if (ts_reader) |*r| r.deinit();
    const ts_ptr: ?*const TimestampReader = if (ts_reader) |*r| r else null;

    var server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();
    var sctx = ServeCtx{ .server = &server, .reader = &reader, .ts = ts_ptr, .allocator = allocator, .conns = 2 };
    const th = try std.Thread.spawn(.{}, ServeCtx.run, .{&sctx});

    var remote_data = std.testing.tmpDir(.{});
    defer remote_data.cleanup();
    var rd: [std.fs.max_path_bytes]u8 = undefined;
    const remote_ctx = try sdk.entry.init(FactoryManifest, FacHandler, .{PingHit}, .{
        .engine_data_dir = path,
        .data_dir = try remote_data.dir.realpath(".", &rd),
        .remote_engine = .{ .host = "127.0.0.1", .port = port },
    }, allocator);
    defer remote_ctx.deinit();
    th.join();

    // The child was discovered remotely and its Ping dispatched, same as local.
    try std.testing.expectEqual(@as(u32, 1), remote_ctx.stats.discovered_children);
    try std.testing.expectEqual(local_ctx.count(PingHit), remote_ctx.count(PingHit));
    try std.testing.expectEqual(@as(u64, 1), remote_ctx.count(PingHit));

    var lrec: [4]PingHit = undefined;
    var rrec: [4]PingHit = undefined;
    const ls = try local_ctx.range(PingHit, 0, &lrec);
    const rs = try remote_ctx.range(PingHit, 0, &rrec);
    try std.testing.expectEqual(ls.len, rs.len);
    for (ls, rs) |l, r| try std.testing.expectEqual(l.block, r.block);
    try std.testing.expectEqual(@as(u64, 201), rs[0].block);
}

// ── Live following ──────────────────────────────────────────────────────────

const fake_engine = sdk.fake_engine;

// A single fixed-key tally bumped per Transfer. A tip-inclusive read reflects
// the backfilled block and any live-dispatched block in the overlay.
const Counter = struct {
    pub const storage: sdk.StorageMode = .mutable;
    id: u64,
    count: u64,
};

const CountHandler = struct {
    pub fn handleTransfer(log: sdk.handler.Log(Transfer), ctx: anytype) !void {
        _ = log;
        var c = try ctx.stores.counters.loadOrInit(@as(u64, 0));
        c.count += 1;
        try ctx.stores.counters.save(c);
    }
};

// Poll the tip-inclusive counter until it reaches `target`, bounded by a
// timeout. The follow loop dispatches on a background thread, so reads race it.
fn waitForCount(ctx: anytype, target: u64) !bool {
    var waited: u64 = 0;
    while (waited < 3000) : (waited += 20) {
        const c = try ctx.read(Counter, @as(u64, 0));
        if (c) |v| if (v.count == target) return true;
        std.Thread.sleep(20 * std.time.ns_per_ms);
    }
    return false;
}

fn transferLog(block: u64, log_index: u16) core.RawLog {
    return .{
        .block_number = block,
        .tx_index = 0,
        .log_index = log_index,
        .address = ADDR_TOKEN,
        .topic_count = 1,
        .topics = .{ sdk_manifest.eventTopic0(Transfer), [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };
}

test "remote follow dispatches a live block injected after backfill" {
    const allocator = std.testing.allocator;
    const tt = sdk_manifest.eventTopic0(Transfer);

    // One finalized block to backfill. The engine serves it from the flat store
    // and live blocks from the pending ring, both in this dir.
    const blocks = [_]TestBlock{
        .{ .block_number = 100, .timestamp = 1_700_000_000, .logs = &.{.{ .address = ADDR_TOKEN, .topic0 = tt }} },
    };
    var src = std.testing.tmpDir(.{});
    defer src.cleanup();
    try writeTestStore(src.dir, &blocks, allocator);
    var src_path: [std.fs.max_path_bytes]u8 = undefined;
    const path = try src.dir.realpath(".", &src_path);

    var fake = fake_engine.FakeEngine.init(src.dir, allocator);
    defer fake.deinit();

    var reader = try FlatStoreReader.open(path);
    defer reader.deinit();
    var ts_reader = try TimestampReader.open(src.dir);
    defer if (ts_reader) |*r| r.deinit();
    const ts_ptr: ?*const TimestampReader = if (ts_reader) |*r| r else null;

    var server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();
    // One-shot backfill connection, then the persistent follow. A short
    // heartbeat lets the engine notice the client's disconnect promptly.
    var sctx = ServeCtx{ .server = &server, .reader = &reader, .ts = ts_ptr, .allocator = allocator, .conns = 2, .data_dir = path, .heartbeat_ms = 100 };
    const th = try std.Thread.spawn(.{}, ServeCtx.run, .{&sctx});
    defer th.join();

    var remote_data = std.testing.tmpDir(.{});
    defer remote_data.cleanup();
    var rd: [std.fs.max_path_bytes]u8 = undefined;
    const ctx = try sdk.entry.spawn(TokenManifest, CountHandler, .{Counter}, .{
        .engine_data_dir = path,
        .data_dir = try remote_data.dir.realpath(".", &rd),
        .remote_engine = .{ .host = "127.0.0.1", .port = port },
    }, allocator);
    // Runs first at scope exit: stops the follow thread and closes its socket,
    // which unblocks the serve thread joined just after.
    defer ctx.deinit();

    // Backfill committed block 100.
    {
        const c = try ctx.read(Counter, @as(u64, 0));
        try std.testing.expect(c != null);
        try std.testing.expectEqual(@as(u64, 1), c.?.count);
    }

    // Inject a live block. The engine streams it, the follow loop dispatches it
    // into the overlay, and a tip-inclusive read sees the second Transfer.
    try fake.ingest(101, [_]u8{0xBB} ** 32, &.{transferLog(101, 0)});
    try std.testing.expect(try waitForCount(ctx, 2));
}

test "remote follow rolls back a reorged live block and applies the canonical one" {
    const allocator = std.testing.allocator;
    const tt = sdk_manifest.eventTopic0(Transfer);

    const blocks = [_]TestBlock{
        .{ .block_number = 100, .timestamp = 1_700_000_000, .logs = &.{.{ .address = ADDR_TOKEN, .topic0 = tt }} },
    };
    var src = std.testing.tmpDir(.{});
    defer src.cleanup();
    try writeTestStore(src.dir, &blocks, allocator);
    var src_path: [std.fs.max_path_bytes]u8 = undefined;
    const path = try src.dir.realpath(".", &src_path);

    var fake = fake_engine.FakeEngine.init(src.dir, allocator);
    defer fake.deinit();

    var reader = try FlatStoreReader.open(path);
    defer reader.deinit();
    var ts_reader = try TimestampReader.open(src.dir);
    defer if (ts_reader) |*r| r.deinit();
    const ts_ptr: ?*const TimestampReader = if (ts_reader) |*r| r else null;

    var server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();
    var sctx = ServeCtx{ .server = &server, .reader = &reader, .ts = ts_ptr, .allocator = allocator, .conns = 2, .data_dir = path, .heartbeat_ms = 100 };
    const th = try std.Thread.spawn(.{}, ServeCtx.run, .{&sctx});
    defer th.join();

    var remote_data = std.testing.tmpDir(.{});
    defer remote_data.cleanup();
    var rd: [std.fs.max_path_bytes]u8 = undefined;
    const ctx = try sdk.entry.spawn(TokenManifest, CountHandler, .{Counter}, .{
        .engine_data_dir = path,
        .data_dir = try remote_data.dir.realpath(".", &rd),
        .remote_engine = .{ .host = "127.0.0.1", .port = port },
    }, allocator);
    defer ctx.deinit();

    // Backfill committed block 100, then a live block with one Transfer.
    try std.testing.expect(try waitForCount(ctx, 1));
    try fake.ingest(101, [_]u8{0xAA} ** 32, &.{transferLog(101, 0)});
    try std.testing.expect(try waitForCount(ctx, 2));

    // Reorg block 101: the canonical replacement carries two Transfers. The
    // engine emits REORG then re-streams it. The client must drop the reorged
    // overlay (back to 1) before applying the canonical block (to 3), never 4.
    try fake.reorg(101);
    try fake.ingest(101, [_]u8{0xBB} ** 32, &.{ transferLog(101, 0), transferLog(101, 1) });
    try std.testing.expect(try waitForCount(ctx, 3));
}

// ── Tx-fields streaming ─────────────────────────────────────────────────────

// Transfer variant opting into `log.tx`. A distinct type keeps the other
// manifests in this file tx-blind.
const TxTransfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
    pub const tx_fields = true;
};

const TxTokenManifest: sdk_manifest.Manifest = .{
    .name = "loopback-tx",
    .chain_id = 1,
    .start_block = 0,
    .contracts = &.{.{ .name = "Token", .address = ADDR_TOKEN, .events = &.{TxTransfer} }},
};

// Per-dispatch tally over the decoded tx fields. Sums make a wrong or stale
// record visible, not just a missing one.
const TxTally = struct {
    pub const storage: sdk.StorageMode = .mutable;
    id: u64,
    count: u64,
    from_sum: u64,
    val_sum: u64,
};

const TxTallyHandler = struct {
    pub fn handleTransfer(log: sdk.handler.Log(TxTransfer), ctx: anytype) !void {
        var c = try ctx.stores.txTallys.loadOrInit(@as(u64, 0));
        c.count += 1;
        c.from_sum += log.tx.from[0];
        c.val_sum += @as(u64, @intCast(log.tx.value));
        try ctx.stores.txTallys.save(c);
    }
};

fn waitForTxCount(ctx: anytype, target: u64) !bool {
    var waited: u64 = 0;
    while (waited < 3000) : (waited += 20) {
        const c = try ctx.read(TxTally, @as(u64, 0));
        if (c) |v| if (v.count == target) return true;
        std.Thread.sleep(20 * std.time.ns_per_ms);
    }
    return false;
}

fn txRec(tx_index: u16, seed: u8) core.txs.TxRecord {
    return .{
        .tx_index = tx_index,
        .tx_type = 2,
        .flags = 0,
        .from = [_]u8{seed} ** 20,
        .to = [_]u8{seed +% 1} ** 20,
        .value = [_]u8{seed} ++ [_]u8{0} ** 31,
    };
}

fn transferLogAt(block: u64, tx_index: u16, log_index: u16) core.RawLog {
    var log = transferLog(block, log_index);
    log.tx_index = tx_index;
    return log;
}

test "local init with tx_fields fails loud without txs.dat, then resolves with it" {
    const allocator = std.testing.allocator;
    const tt = sdk_manifest.eventTopic0(Transfer);

    const blocks = [_]TestBlock{
        .{ .block_number = 100, .timestamp = 1_700_000_000, .logs = &.{.{ .address = ADDR_TOKEN, .topic0 = tt, .tx_index = 4 }} },
    };
    var src = std.testing.tmpDir(.{});
    defer src.cleanup();
    try writeTestStore(src.dir, &blocks, allocator);
    var src_path: [std.fs.max_path_bytes]u8 = undefined;
    const path = try src.dir.realpath(".", &src_path);

    var data = std.testing.tmpDir(.{});
    defer data.cleanup();
    var dd: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data.dir.realpath(".", &dd);

    // No txs.{dat,idx}: a tx-enabled manifest must refuse at init.
    try std.testing.expectError(error.TxFieldsUnavailable, sdk.entry.init(TxTokenManifest, TxTallyHandler, .{TxTally}, .{
        .engine_data_dir = path,
        .data_dir = data_path,
    }, allocator));

    {
        var sbuf: [4096]u8 = undefined;
        var cbuf: [4096]u8 = undefined;
        var tw = try core.txs.TxsWriter.open(src.dir, 100);
        defer tw.deinit();
        try tw.append(100, &.{txRec(4, 0x44)}, &sbuf, &cbuf);
    }

    const ctx = try sdk.entry.init(TxTokenManifest, TxTallyHandler, .{TxTally}, .{
        .engine_data_dir = path,
        .data_dir = data_path,
    }, allocator);
    defer ctx.deinit();

    const c = (try ctx.read(TxTally, @as(u64, 0))).?;
    try std.testing.expectEqual(@as(u64, 1), c.count);
    try std.testing.expectEqual(@as(u64, 0x44), c.from_sum);
}

test "remote tx_fields: backfill and live PUSH both carry resolvable TxRecords" {
    const allocator = std.testing.allocator;
    const tt = sdk_manifest.eventTopic0(Transfer);

    // Backfill: block 100 with a Transfer in tx 1. The table also holds tx 0
    // (a log-producing tx the filter drops) to prove the carry selects.
    const blocks = [_]TestBlock{
        .{ .block_number = 100, .timestamp = 1_700_000_000, .logs = &.{
            .{ .address = ADDR_OTHER, .topic0 = tt, .tx_index = 0 },
            .{ .address = ADDR_TOKEN, .topic0 = tt, .tx_index = 1, .log_index = 1 },
        } },
    };
    var src = std.testing.tmpDir(.{});
    defer src.cleanup();
    try writeTestStore(src.dir, &blocks, allocator);
    {
        var sbuf: [4096]u8 = undefined;
        var cbuf: [4096]u8 = undefined;
        var tw = try core.txs.TxsWriter.open(src.dir, 100);
        defer tw.deinit();
        try tw.append(100, &.{ txRec(0, 0xAA), txRec(1, 0x11) }, &sbuf, &cbuf);
    }
    var src_path: [std.fs.max_path_bytes]u8 = undefined;
    const path = try src.dir.realpath(".", &src_path);

    var fake = fake_engine.FakeEngine.init(src.dir, allocator);
    defer fake.deinit();

    var reader = try FlatStoreReader.open(path);
    defer reader.deinit();
    var ts_reader = try TimestampReader.open(src.dir);
    defer if (ts_reader) |*r| r.deinit();
    const ts_ptr: ?*const TimestampReader = if (ts_reader) |*r| r else null;

    var server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();
    var sctx = ServeCtx{ .server = &server, .reader = &reader, .ts = ts_ptr, .allocator = allocator, .conns = 2, .data_dir = path, .heartbeat_ms = 100 };
    const th = try std.Thread.spawn(.{}, ServeCtx.run, .{&sctx});
    defer th.join();

    var remote_data = std.testing.tmpDir(.{});
    defer remote_data.cleanup();
    var rd: [std.fs.max_path_bytes]u8 = undefined;
    const ctx = try sdk.entry.spawn(TxTokenManifest, TxTallyHandler, .{TxTally}, .{
        .engine_data_dir = path,
        .data_dir = try remote_data.dir.realpath(".", &rd),
        .remote_engine = .{ .host = "127.0.0.1", .port = port },
    }, allocator);
    defer ctx.deinit();

    // Backfill dispatched the kept log with tx 1's record.
    {
        const c = try ctx.read(TxTally, @as(u64, 0));
        try std.testing.expect(c != null);
        try std.testing.expectEqual(@as(u64, 1), c.?.count);
        try std.testing.expectEqual(@as(u64, 0x11), c.?.from_sum);
        try std.testing.expectEqual(@as(u64, 0x11), c.?.val_sum);
    }

    // Live: the pending entry's table feeds the PUSH subtable.
    try fake.ingestWithTxs(101, [_]u8{0xBB} ** 32, &.{transferLogAt(101, 2, 0)}, &.{txRec(2, 0x22)});
    try std.testing.expect(try waitForTxCount(ctx, 2));
    const c = (try ctx.read(TxTally, @as(u64, 0))).?;
    try std.testing.expectEqual(@as(u64, 0x11 + 0x22), c.from_sum);
    try std.testing.expectEqual(@as(u64, 0x11 + 0x22), c.val_sum);
}

const CHILD2_ADDR: [20]u8 = [_]u8{0xC2} ** 20;

const FacCountHandler = struct {
    pub fn handleCreate(log: sdk.handler.Log(Create), ctx: anytype) !void {
        _ = log;
        _ = ctx;
    }
    pub fn handlePing(log: sdk.handler.Log(Ping), ctx: anytype) !void {
        _ = log;
        var c = try ctx.stores.counters.loadOrInit(@as(u64, 0));
        c.count += 1;
        try ctx.stores.counters.save(c);
    }
};

fn createLog(block: u64, child: [20]u8, word: *[32]u8) core.RawLog {
    @memset(word, 0);
    @memcpy(word[12..], &child);
    return .{
        .block_number = block,
        .tx_index = 0,
        .log_index = 0,
        .address = FACTORY_ADDR,
        .topic_count = 1,
        .topics = .{ sdk_manifest.eventTopic0(Create), [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = word,
        .tx_hash = [_]u8{0xFE} ** 32,
    };
}

fn pingLog(block: u64, log_index: u16, emitter: [20]u8) core.RawLog {
    return .{
        .block_number = block,
        .tx_index = 0,
        .log_index = log_index,
        .address = emitter,
        .topic_count = 1,
        .topics = .{ sdk_manifest.eventTopic0(Ping), [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };
}

test "remote follow tracks a backfilled child and registers a live-spawned one" {
    const allocator = std.testing.allocator;

    // Block 100 spawns CHILD via the factory, so backfill discovers it.
    var child_word: [32]u8 = undefined;
    @memset(&child_word, 0);
    @memcpy(child_word[12..], &CHILD_ADDR);
    const blocks = [_]TestBlock{
        .{ .block_number = 100, .timestamp = 1_700_000_000, .logs = &.{.{ .address = FACTORY_ADDR, .topic0 = sdk_manifest.eventTopic0(Create), .data = &child_word }} },
    };
    var src = std.testing.tmpDir(.{});
    defer src.cleanup();
    try writeTestStore(src.dir, &blocks, allocator);
    var src_path: [std.fs.max_path_bytes]u8 = undefined;
    const path = try src.dir.realpath(".", &src_path);

    var fake = fake_engine.FakeEngine.init(src.dir, allocator);
    defer fake.deinit();

    var reader = try FlatStoreReader.open(path);
    defer reader.deinit();
    var ts_reader = try TimestampReader.open(src.dir);
    defer if (ts_reader) |*r| r.deinit();
    const ts_ptr: ?*const TimestampReader = if (ts_reader) |*r| r else null;

    var server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();
    // Backfill: primary + children REGISTERs. Then the persistent follow.
    var sctx = ServeCtx{ .server = &server, .reader = &reader, .ts = ts_ptr, .allocator = allocator, .conns = 3, .data_dir = path, .heartbeat_ms = 100 };
    const th = try std.Thread.spawn(.{}, ServeCtx.run, .{&sctx});
    defer th.join();

    var remote_data = std.testing.tmpDir(.{});
    defer remote_data.cleanup();
    var rd: [std.fs.max_path_bytes]u8 = undefined;
    const ctx = try sdk.entry.spawn(FactoryManifest, FacCountHandler, .{Counter}, .{
        .engine_data_dir = path,
        .data_dir = try remote_data.dir.realpath(".", &rd),
        .remote_engine = .{ .host = "127.0.0.1", .port = port },
    }, allocator);
    defer ctx.deinit();

    // The backfilled child CHILD emits live. It is in the follow REGISTER, so
    // its Ping streams without an ADD_ADDRESS.
    try fake.ingest(101, [_]u8{0xAA} ** 32, &.{pingLog(101, 0, CHILD_ADDR)});
    try std.testing.expect(try waitForCount(ctx, 1));

    // A new child CHILD2 is spawned live and emits in the same block. The client
    // discovers it from the create-event, ADD_ADDRESSes it, and the engine
    // mini-backfills the block so the same-block Ping is caught.
    var word2: [32]u8 = undefined;
    try fake.ingest(102, [_]u8{0xBB} ** 32, &.{ createLog(102, CHILD2_ADDR, &word2), pingLog(102, 1, CHILD2_ADDR) });
    try std.testing.expect(try waitForCount(ctx, 2));
}
