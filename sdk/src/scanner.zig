/// Filtered-index scanner. Two entry points:
///
/// - `scanCreations(env, manifest, allocator)` walks `BLOCKS_PRIMARY` and
///   dispatches only factory creation events to a comptime-restricted
///   collector that returns the discovered child addresses. No user
///   handlers run. Phase 2 of the four-phase pipeline.
///
/// - `replay(env, manifest, Handler, ctx, options)` walks `BLOCKS_PRIMARY`
///   (always) and `BLOCKS_CHILDREN` (when present), k-way-merges logs by
///   `(block_number, tx_index, log_index)`, and dispatches each to the
///   user's handler via `handler.dispatcherFor(manifest).dispatch`. Phase 4.
///
/// Both functions assume the env was produced by `filter_builder` and
/// therefore that DBI names match `filter_builder.DBI_PRIMARY` and
/// `filter_builder.DBI_CHILDREN`.
const std = @import("std");
const lmdbx = @import("lmdbx");
const core = @import("core");

const sdk_manifest = @import("manifest.zig");
const filter_builder = @import("filter_builder.zig");
const handler_mod = @import("handler.zig");
const humanize = @import("humanize.zig");

const RawLog = core.RawLog;
const log_serial = core.log_serial;
const types = core.types;

pub const ReplayOptions = struct {
    commit_interval: u32 = 100_000,
};

pub const ReplayResult = struct {
    logs_dispatched: u64 = 0,
    blocks_dispatched: u64 = 0,
    elapsed_ns: u64 = 0,
};

// ── Phase 2: scanCreations ───────────────────────────────────────────────

/// Walk `BLOCKS_PRIMARY` and collect spawned-contract addresses from every
/// log whose `topic0` matches a factory's `create_event`. Caller owns the
/// returned hashmap. Returns an empty map for factory-free manifests, for
/// envs missing `BLOCKS_PRIMARY`, and for empty filtered indexes.
pub fn scanCreations(
    env: lmdbx.Environment,
    comptime m: sdk_manifest.Manifest,
    allocator: std.mem.Allocator,
) !std.AutoHashMap([20]u8, void) {
    var discovered = std.AutoHashMap([20]u8, void).init(allocator);
    if (comptime m.factories.len == 0) return discovered;

    const txn = try env.transaction(.{ .mode = .ReadOnly });
    defer txn.abort() catch {};

    const db = lmdbx.Database.open(txn, filter_builder.DBI_PRIMARY, .{}) catch return discovered;
    var cursor = try db.cursor();
    defer cursor.deinit();

    var decompress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var log_buf: [types.MAX_LOGS_PER_BLOCK]RawLog = undefined;

    var key_opt = cursor.goToFirst() catch return discovered;
    while (key_opt) |k| : (key_opt = try cursor.goToNext()) {
        const block_number = filter_builder.blockFromKey(k);
        const value = try cursor.getCurrentValue();
        const decoded = log_serial.decompressEntry(value, &decompress_buf) catch continue;
        const log_count = log_serial.deserializeLogs(decoded, &log_buf);

        for (log_buf[0..log_count]) |*log| {
            log.block_number = block_number;
            if (log.topic_count == 0) continue;
            inline for (m.factories) |f| {
                const create_topic = comptime sdk_manifest.eventTopic0(f.create_event);
                if (std.mem.eql(u8, &log.topics[0], &create_topic)) {
                    const addr = sdk_manifest.extractAddress(&log.topics, log.data, f.address_param);
                    try discovered.put(addr, {});
                }
            }
        }
    }
    return discovered;
}

// ── Phase 4: replay ──────────────────────────────────────────────────────

/// Walk the filtered index in canonical `(block_number, tx_index, log_index)`
/// order, dispatching each log via the comptime topic0 dispatcher. Logs
/// with no matching event in the manifest (bloom false positives that
/// slipped through) are silently skipped by the dispatcher, not by us.
pub fn replay(
    env: lmdbx.Environment,
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    options: ReplayOptions,
) !ReplayResult {
    _ = options; // commit cadence is wired in once entity stores land here.
    var result = ReplayResult{};
    var timer = try std.time.Timer.start();

    const Dispatcher = handler_mod.dispatcherFor(m);
    comptime Dispatcher.validateHandler(Handler);

    const txn = try env.transaction(.{ .mode = .ReadOnly });
    defer txn.abort() catch {};

    var primary = try CursorWalker.open(txn, filter_builder.DBI_PRIMARY);
    defer primary.deinit();

    const has_children = comptime m.factories.len > 0;
    var children_storage: CursorWalker = undefined;
    var children: ?*CursorWalker = null;
    if (has_children) {
        if (CursorWalker.open(txn, filter_builder.DBI_CHILDREN)) |w| {
            children_storage = w;
            children = &children_storage;
        } else |_| {
            // BLOCKS_CHILDREN may not exist (no factory discoveries) — fine.
        }
    }
    defer if (children) |c| c.deinit();

    var decompress_primary: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var decompress_children: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var log_buf_primary: [types.MAX_LOGS_PER_BLOCK]RawLog = undefined;
    var log_buf_children: [types.MAX_LOGS_PER_BLOCK]RawLog = undefined;
    var merge_buf: [2 * types.MAX_LOGS_PER_BLOCK]RawLog = undefined;

    while (true) {
        const next_p: ?u64 = primary.peek();
        const next_c: ?u64 = if (children) |c| c.peek() else null;
        if (next_p == null and next_c == null) break;

        const block_number = pickMin(next_p, next_c);

        var merge_count: usize = 0;
        if (next_p) |bn| if (bn == block_number) {
            const logs = try primary.consume(block_number, &decompress_primary, &log_buf_primary);
            for (logs) |log| {
                merge_buf[merge_count] = log;
                merge_count += 1;
            }
        };
        if (children) |c| {
            if (next_c) |bn| if (bn == block_number) {
                const logs = try c.consume(block_number, &decompress_children, &log_buf_children);
                for (logs) |log| {
                    merge_buf[merge_count] = log;
                    merge_count += 1;
                }
            };
        }

        if (merge_count == 0) continue;
        std.mem.sort(RawLog, merge_buf[0..merge_count], {}, lessByTxLog);

        // Update ctx for this block. Handlers that want timestamp can read
        // it from ctx; we set it once per block to avoid recomputing.
        ctx.block_number = block_number;
        ctx.timestamp = humanize.blockTimestamp(block_number);
        result.blocks_dispatched += 1;

        for (merge_buf[0..merge_count]) |log| {
            const decoded = handler_mod.DecodedLog.fromRawLog(log);
            try Dispatcher.dispatch(Handler, decoded, ctx);
            result.logs_dispatched += 1;
        }
    }

    result.elapsed_ns = timer.read();
    return result;
}

fn pickMin(a: ?u64, b: ?u64) u64 {
    if (a) |va| if (b) |vb| return @min(va, vb) else return va;
    if (b) |vb| return vb;
    unreachable;
}

fn lessByTxLog(_: void, a: RawLog, b: RawLog) bool {
    if (a.tx_index != b.tx_index) return a.tx_index < b.tx_index;
    return a.log_index < b.log_index;
}

/// Wraps a cursor over a single DBI, providing peek/consume so the merge
/// loop in replay can advance one DBI at a time without juggling cursor
/// state across two streams.
const CursorWalker = struct {
    cursor: lmdbx.Cursor,
    next_key: ?[]const u8,

    fn open(txn: lmdbx.Transaction, dbi_name: [*:0]const u8) !CursorWalker {
        const db = try lmdbx.Database.open(txn, dbi_name, .{});
        var cursor = try db.cursor();
        const first = cursor.goToFirst() catch null;
        return .{ .cursor = cursor, .next_key = first };
    }

    fn deinit(self: *CursorWalker) void {
        self.cursor.deinit();
    }

    fn peek(self: *const CursorWalker) ?u64 {
        if (self.next_key) |k| return filter_builder.blockFromKey(k);
        return null;
    }

    fn consume(
        self: *CursorWalker,
        block_number: u64,
        decompress_buf: []u8,
        log_buf: []RawLog,
    ) ![]RawLog {
        const value = try self.cursor.getCurrentValue();
        const decoded = try log_serial.decompressEntry(value, decompress_buf);
        const log_count = log_serial.deserializeLogs(decoded, log_buf);
        for (log_buf[0..log_count]) |*log| log.block_number = block_number;
        self.next_key = self.cursor.goToNext() catch null;
        return log_buf[0..log_count];
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;
const block_context = @import("block_context.zig");
const FlatStoreReader = core.FlatStoreReader;

const TestLog = struct {
    tx_index: u16 = 0,
    log_index: u16 = 0,
    address: [20]u8,
    topic0: [32]u8,
};

const TestBlock = struct {
    block_number: u64,
    logs: []const TestLog,
};

const Transfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
};
const Approval = struct {
    pub const signature = "Approval(address,address,uint256)";
};
const PairCreated = struct {
    pub const signature = "PairCreated(address,address,address,uint256)";
};
const Sync = struct {
    pub const signature = "Sync(uint112,uint112)";
};

const ADDR_TOKEN: [20]u8 = [_]u8{0xAE} ** 20;
const FACTORY_ADDR: [20]u8 = [_]u8{0xF0} ** 20;
const CHILD_1: [20]u8 = [_]u8{0xC1} ** 20;
const CHILD_2: [20]u8 = [_]u8{0xC2} ** 20;

fn topicOf(comptime E: type) [32]u8 {
    return sdk_manifest.eventTopic0(E);
}

fn writeFlatStore(dir: std.fs.Dir, blocks: []const TestBlock, allocator: std.mem.Allocator) !void {
    var blocks_file = try dir.createFile("blocks.dat", .{});
    defer blocks_file.close();
    var idx_file = try dir.createFile("blocks.idx", .{});
    defer idx_file.close();
    var blooms_file = try dir.createFile("blooms.bin", .{});
    defer blooms_file.close();

    const flat_reader = core.flat_reader;
    const bloom = core.bloom;

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

    var offset: u64 = 0;
    for (blocks) |blk| {
        var raw_logs: [types.MAX_LOGS_PER_BLOCK]RawLog = undefined;
        for (blk.logs, 0..) |tl, i| {
            raw_logs[i] = .{
                .block_number = blk.block_number,
                .tx_index = tl.tx_index,
                .log_index = tl.log_index,
                .address = tl.address,
                .topic_count = 1,
                .topics = .{ tl.topic0, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
                .data = &.{},
                .tx_hash = [_]u8{0xFE} ** 32,
            };
        }
        const written = log_serial.serializeLogs(raw_logs[0..blk.logs.len], serialize_buf);
        const entry_len = try log_serial.compressEntry(serialize_buf[0..written], compress_buf);
        try blocks_file.writeAll(compress_buf[0..entry_len]);

        var idx_entry: [flat_reader.INDEX_ENTRY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, idx_entry[0..8], offset, .little);
        std.mem.writeInt(u32, idx_entry[8..12], @intCast(entry_len), .little);
        try idx_file.writeAll(&idx_entry);

        const tb = log_serial.buildTopicBloom(raw_logs[0..blk.logs.len]);
        const ab = log_serial.buildAddrBloom(raw_logs[0..blk.logs.len]);
        var bloom_entry: [flat_reader.BLOOM_ENTRY_SIZE]u8 = std.mem.zeroes([flat_reader.BLOOM_ENTRY_SIZE]u8);
        std.mem.writeInt(u64, bloom_entry[0..8], blk.block_number, .big);
        @memcpy(bloom_entry[flat_reader.TOPIC_BLOOM_OFFSET..][0..bloom.BLOOM_SIZE], &tb.bits);
        @memcpy(bloom_entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE], &ab.bits);
        try blooms_file.writeAll(&bloom_entry);

        offset += entry_len;
    }
}

fn realpathZ(dir: *std.testing.TmpDir, out_z: *[std.fs.max_path_bytes:0]u8) ![*:0]const u8 {
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try dir.dir.realpath(".", &buf);
    @memcpy(out_z[0..path.len], path);
    out_z[path.len] = 0;
    return @ptrCast(out_z);
}

const Counter = struct {
    transfers: u32 = 0,
    approvals: u32 = 0,
    syncs: u32 = 0,
    pair_creations: u32 = 0,
    dispatched: std.ArrayListUnmanaged(DispatchedLog) = .{},
    block_number: u64 = 0,
    timestamp: u64 = 0,
    allocator: std.mem.Allocator,

    const DispatchedLog = struct {
        block_number: u64,
        tx_index: u16,
        log_index: u16,
        topic0_first_byte: u8,
    };

    fn deinit(self: *Counter) void {
        self.dispatched.deinit(self.allocator);
    }

    fn record(self: *Counter, log: handler_mod.DecodedLog) !void {
        try self.dispatched.append(self.allocator, .{
            .block_number = log.block_number,
            .tx_index = log.tx_index,
            .log_index = log.log_index,
            .topic0_first_byte = log.topics[0][0],
        });
    }

    pub fn handleTransfer(log: handler_mod.DecodedLog, self: *Counter) !void {
        self.transfers += 1;
        try self.record(log);
    }
    pub fn handleApproval(log: handler_mod.DecodedLog, self: *Counter) !void {
        self.approvals += 1;
        try self.record(log);
    }
    pub fn handleSync(log: handler_mod.DecodedLog, self: *Counter) !void {
        self.syncs += 1;
        try self.record(log);
    }
    pub fn handlePairCreated(log: handler_mod.DecodedLog, self: *Counter) !void {
        self.pair_creations += 1;
        try self.record(log);
    }
};

test "scanCreations: extracts spawned addresses from factory creation events" {
    const allocator = testing.allocator;

    // Plant a flat store containing a PairCreated log at block 100 whose
    // `pair` address (the address_param target) sits at data[0..32].
    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();

    const create_topic = topicOf(PairCreated);

    // Use writeFlatStore for simplicity. Note that writeFlatStore stores an
    // empty `data` field, so we instead skip the helper and craft a single
    // block manually with the spawned address embedded in topic[1] (indexed
    // path) — that exercises the indexed AddressParam.
    const flat_reader = core.flat_reader;
    const bloom = core.bloom;

    var blocks_file = try src_tmp.dir.createFile("blocks.dat", .{});
    defer blocks_file.close();
    var idx_file = try src_tmp.dir.createFile("blocks.idx", .{});
    defer idx_file.close();
    var blooms_file = try src_tmp.dir.createFile("blooms.bin", .{});
    defer blooms_file.close();

    const FACTORY_BLOCK: u64 = 100;
    var idx_hdr: [flat_reader.INDEX_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, idx_hdr[0..8], FACTORY_BLOCK, .little);
    std.mem.writeInt(u64, idx_hdr[8..16], 1, .little);
    try idx_file.writeAll(&idx_hdr);

    var blooms_hdr: [flat_reader.BLOOM_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, &blooms_hdr, 1, .little);
    try blooms_file.writeAll(&blooms_hdr);

    var spawned_topic1: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(spawned_topic1[12..32], &CHILD_1);

    const log: RawLog = .{
        .block_number = FACTORY_BLOCK,
        .tx_index = 0,
        .log_index = 0,
        .address = FACTORY_ADDR,
        .topic_count = 2,
        .topics = .{ create_topic, spawned_topic1, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };

    var serialize_buf: [4096]u8 = undefined;
    var compress_buf: [4096]u8 = undefined;
    const written = log_serial.serializeLogs(&[_]RawLog{log}, &serialize_buf);
    const entry_len = try log_serial.compressEntry(serialize_buf[0..written], &compress_buf);
    try blocks_file.writeAll(compress_buf[0..entry_len]);

    var idx_entry: [flat_reader.INDEX_ENTRY_SIZE]u8 = undefined;
    std.mem.writeInt(u64, idx_entry[0..8], 0, .little);
    std.mem.writeInt(u32, idx_entry[8..12], @intCast(entry_len), .little);
    try idx_file.writeAll(&idx_entry);

    const tb = log_serial.buildTopicBloom(&[_]RawLog{log});
    const ab = log_serial.buildAddrBloom(&[_]RawLog{log});
    var bloom_entry: [flat_reader.BLOOM_ENTRY_SIZE]u8 = std.mem.zeroes([flat_reader.BLOOM_ENTRY_SIZE]u8);
    std.mem.writeInt(u64, bloom_entry[0..8], FACTORY_BLOCK, .big);
    @memcpy(bloom_entry[flat_reader.TOPIC_BLOOM_OFFSET..][0..bloom.BLOOM_SIZE], &tb.bits);
    @memcpy(bloom_entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE], &ab.bits);
    try blooms_file.writeAll(&bloom_entry);

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.close();

    const FactoryManifest: sdk_manifest.Manifest = .{
        .name = "factory",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FACTORY_ADDR,
            .create_event = PairCreated,
            .address_param = .{ .indexed = 0 },
            .child_events = &.{Sync},
        }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();
    var dst_path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    const dst_path = try realpathZ(&dst_tmp, &dst_path_z);

    _ = try filter_builder.build(&reader, FactoryManifest, dst_path, allocator);

    const env = try lmdbx.Environment.init(dst_path, .{ .max_dbs = 2 });
    defer env.deinit() catch {};

    var discovered = try scanCreations(env, FactoryManifest, allocator);
    defer discovered.deinit();

    try testing.expectEqual(@as(u32, 1), discovered.count());
    try testing.expect(discovered.contains(CHILD_1));
}

test "replay: dispatches logs in canonical (block, tx, log_index) order across one DBI" {
    const allocator = testing.allocator;

    // Three blocks. Block 100 has logs at (tx=0, log=0) and (tx=0, log=1).
    // writeFlatStore preserves the order as given in `logs`, but we plant
    // them out of order to verify the scanner sorts within a block.
    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();

    const tt = topicOf(Transfer);
    const blocks = [_]TestBlock{
        .{ .block_number = 100, .logs = &.{
            .{ .tx_index = 0, .log_index = 1, .address = ADDR_TOKEN, .topic0 = tt },
            .{ .tx_index = 0, .log_index = 0, .address = ADDR_TOKEN, .topic0 = tt },
        } },
        .{ .block_number = 101, .logs = &.{
            .{ .tx_index = 0, .log_index = 0, .address = ADDR_TOKEN, .topic0 = tt },
        } },
    };
    try writeFlatStore(src_tmp.dir, &blocks, allocator);

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.close();

    const Manifest: sdk_manifest.Manifest = .{
        .name = "test",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();
    var dst_path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    const dst_path = try realpathZ(&dst_tmp, &dst_path_z);

    _ = try filter_builder.build(&reader, Manifest, dst_path, allocator);

    const env = try lmdbx.Environment.init(dst_path, .{ .max_dbs = 2 });
    defer env.deinit() catch {};

    var counter = Counter{ .allocator = allocator };
    defer counter.deinit();

    const result = try replay(env, Manifest, Counter, &counter, .{});
    try testing.expectEqual(@as(u64, 3), result.logs_dispatched);
    try testing.expectEqual(@as(u64, 2), result.blocks_dispatched);
    try testing.expectEqual(@as(u32, 3), counter.transfers);

    // Canonical order: (100,0,0), (100,0,1), (101,0,0).
    try testing.expectEqual(@as(usize, 3), counter.dispatched.items.len);
    try testing.expectEqual(@as(u64, 100), counter.dispatched.items[0].block_number);
    try testing.expectEqual(@as(u16, 0), counter.dispatched.items[0].log_index);
    try testing.expectEqual(@as(u64, 100), counter.dispatched.items[1].block_number);
    try testing.expectEqual(@as(u16, 1), counter.dispatched.items[1].log_index);
    try testing.expectEqual(@as(u64, 101), counter.dispatched.items[2].block_number);
}

test "replay: k-way merge across BLOCKS_PRIMARY and BLOCKS_CHILDREN preserves block order" {
    const allocator = testing.allocator;

    // Plant a flat store with:
    //   block 100: factory creation (PrimaryAddr emits PairCreated)
    //   block 101: child 1 emits Sync
    //   block 102: factory address emits another PairCreated → primary
    //   block 103: child 2 emits Sync
    // After build + appendChildren, PRIMARY has {100, 102} and CHILDREN has
    // {101, 103}. replay's k-way merge must dispatch in (100, 101, 102, 103).
    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();

    const create_topic = topicOf(PairCreated);
    const sync_topic = topicOf(Sync);

    const blocks = [_]TestBlock{
        .{ .block_number = 100, .logs = &.{.{ .address = FACTORY_ADDR, .topic0 = create_topic }} },
        .{ .block_number = 101, .logs = &.{.{ .address = CHILD_1, .topic0 = sync_topic }} },
        .{ .block_number = 102, .logs = &.{.{ .address = FACTORY_ADDR, .topic0 = create_topic }} },
        .{ .block_number = 103, .logs = &.{.{ .address = CHILD_2, .topic0 = sync_topic }} },
    };
    try writeFlatStore(src_tmp.dir, &blocks, allocator);

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.close();

    const FactoryManifest: sdk_manifest.Manifest = .{
        .name = "factory",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FACTORY_ADDR,
            .create_event = PairCreated,
            .address_param = .{ .data = 0 },
            .child_events = &.{Sync},
        }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();
    var dst_path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    const dst_path = try realpathZ(&dst_tmp, &dst_path_z);

    _ = try filter_builder.build(&reader, FactoryManifest, dst_path, allocator);
    const child_addrs = [_][20]u8{ CHILD_1, CHILD_2 };
    _ = try filter_builder.appendChildren(&reader, FactoryManifest, &child_addrs, dst_path, allocator);

    const env = try lmdbx.Environment.init(dst_path, .{ .max_dbs = 2 });
    defer env.deinit() catch {};

    var counter = Counter{ .allocator = allocator };
    defer counter.deinit();

    const result = try replay(env, FactoryManifest, Counter, &counter, .{});
    try testing.expectEqual(@as(u64, 4), result.logs_dispatched);
    try testing.expectEqual(@as(u64, 4), result.blocks_dispatched);
    try testing.expectEqual(@as(u32, 2), counter.pair_creations);
    try testing.expectEqual(@as(u32, 2), counter.syncs);

    // (100, 101, 102, 103) in dispatch order.
    try testing.expectEqual(@as(usize, 4), counter.dispatched.items.len);
    try testing.expectEqual(@as(u64, 100), counter.dispatched.items[0].block_number);
    try testing.expectEqual(@as(u64, 101), counter.dispatched.items[1].block_number);
    try testing.expectEqual(@as(u64, 102), counter.dispatched.items[2].block_number);
    try testing.expectEqual(@as(u64, 103), counter.dispatched.items[3].block_number);
}

test "factory orchestration: build → scanCreations → appendChildren → replay" {
    const allocator = testing.allocator;

    // Block 100: factory creates child_1.
    // Block 101: child_1 emits Sync (child event).
    // Block 102: noise — unrelated address with unrelated topic.
    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();

    const create_topic = topicOf(PairCreated);
    const sync_topic = topicOf(Sync);

    // For the factory log we need data containing the spawned address at offset 0.
    // writeFlatStore plants empty data; build a custom log with non-empty data.
    const flat_reader = core.flat_reader;
    const bloom = core.bloom;

    var blocks_file = try src_tmp.dir.createFile("blocks.dat", .{});
    defer blocks_file.close();
    var idx_file = try src_tmp.dir.createFile("blocks.idx", .{});
    defer idx_file.close();
    var blooms_file = try src_tmp.dir.createFile("blooms.bin", .{});
    defer blooms_file.close();

    var idx_hdr: [flat_reader.INDEX_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, idx_hdr[0..8], 100, .little);
    std.mem.writeInt(u64, idx_hdr[8..16], 3, .little);
    try idx_file.writeAll(&idx_hdr);

    var blooms_hdr: [flat_reader.BLOOM_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, &blooms_hdr, 3, .little);
    try blooms_file.writeAll(&blooms_hdr);

    var spawned_data: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(spawned_data[12..32], &CHILD_1);

    const factory_log: RawLog = .{
        .block_number = 100,
        .tx_index = 0,
        .log_index = 0,
        .address = FACTORY_ADDR,
        .topic_count = 1,
        .topics = .{ create_topic, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &spawned_data,
        .tx_hash = [_]u8{0xFE} ** 32,
    };
    const child_log: RawLog = .{
        .block_number = 101,
        .tx_index = 0,
        .log_index = 0,
        .address = CHILD_1,
        .topic_count = 1,
        .topics = .{ sync_topic, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };
    const noise_topic = topicOf(Approval);
    const noise_log: RawLog = .{
        .block_number = 102,
        .tx_index = 0,
        .log_index = 0,
        .address = [_]u8{0x99} ** 20,
        .topic_count = 1,
        .topics = .{ noise_topic, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };

    const all_logs = [_][]const RawLog{ &.{factory_log}, &.{child_log}, &.{noise_log} };
    var serialize_buf: [4096]u8 = undefined;
    var compress_buf: [4096]u8 = undefined;
    var offset: u64 = 0;
    for (all_logs, 0..) |logs, i| {
        const written = log_serial.serializeLogs(logs, &serialize_buf);
        const entry_len = try log_serial.compressEntry(serialize_buf[0..written], &compress_buf);
        try blocks_file.writeAll(compress_buf[0..entry_len]);

        var idx_entry: [flat_reader.INDEX_ENTRY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, idx_entry[0..8], offset, .little);
        std.mem.writeInt(u32, idx_entry[8..12], @intCast(entry_len), .little);
        try idx_file.writeAll(&idx_entry);
        offset += entry_len;

        const tb = log_serial.buildTopicBloom(logs);
        const ab = log_serial.buildAddrBloom(logs);
        var bloom_entry: [flat_reader.BLOOM_ENTRY_SIZE]u8 = std.mem.zeroes([flat_reader.BLOOM_ENTRY_SIZE]u8);
        std.mem.writeInt(u64, bloom_entry[0..8], 100 + @as(u64, i), .big);
        @memcpy(bloom_entry[flat_reader.TOPIC_BLOOM_OFFSET..][0..bloom.BLOOM_SIZE], &tb.bits);
        @memcpy(bloom_entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE], &ab.bits);
        try blooms_file.writeAll(&bloom_entry);
    }

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.close();

    const FactoryManifest: sdk_manifest.Manifest = .{
        .name = "factory",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FACTORY_ADDR,
            .create_event = PairCreated,
            .address_param = .{ .data = 0 },
            .child_events = &.{Sync},
        }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();
    var dst_path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    const dst_path = try realpathZ(&dst_tmp, &dst_path_z);

    _ = try filter_builder.build(&reader, FactoryManifest, dst_path, allocator);

    {
        const env = try lmdbx.Environment.init(dst_path, .{ .max_dbs = 2 });
        defer env.deinit() catch {};
        var discovered = try scanCreations(env, FactoryManifest, allocator);
        defer discovered.deinit();
        try testing.expectEqual(@as(u32, 1), discovered.count());
        try testing.expect(discovered.contains(CHILD_1));
    }

    const child_addrs = [_][20]u8{CHILD_1};
    _ = try filter_builder.appendChildren(&reader, FactoryManifest, &child_addrs, dst_path, allocator);

    const env = try lmdbx.Environment.init(dst_path, .{ .max_dbs = 2 });
    defer env.deinit() catch {};

    var counter = Counter{ .allocator = allocator };
    defer counter.deinit();

    const result = try replay(env, FactoryManifest, Counter, &counter, .{});
    try testing.expectEqual(@as(u64, 2), result.logs_dispatched);
    try testing.expectEqual(@as(u32, 1), counter.pair_creations);
    try testing.expectEqual(@as(u32, 1), counter.syncs);
}
