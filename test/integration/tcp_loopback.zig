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

    fn run(self: *ServeCtx) void {
        const conn = self.server.accept() catch return;
        defer conn.stream.close();
        tcp_server.serveConnection(conn.stream, self.reader, self.ts, self.allocator) catch {};
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
    _ = try filter_builder.build(&reader, TokenManifest, local.dir, allocator);

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
