/// Filtered-index builder. Reads the engine's flat log store via `core`,
/// keeps logs matching the manifest, writes them into a flat-file pair
/// (`<base>.dat` + `<base>.idx`) via `sdk.filtered_store`. Per ADR-002.
///
/// Two entry points:
///   - `build`: phase 1, writes the `primary` pair for static + factory addresses.
///   - `appendChildren`: phase 3, writes the `children` pair for addresses
///     discovered by the scanner's factory pre-pass. No-op for an empty set.
///
/// Per-log keep rule (uniform across both phases via `Filter`):
///   keep = (address ∈ filter.match_addrs)
///       AND (topic0 ∈ filter.match_topics)
///       AND (address ∉ filter.exclude_addrs)
/// Phase 1 leaves exclude_addrs empty. Phase 3 sets it to static∪factory so a
/// static contract that's also a factory child does not appear in both pairs.
///
/// The rule matches manifest-level sets, not per-contract pairs. A declared
/// address emitting another contract's topic is kept (cross product). Dispatch
/// owns per-contract scoping via the comptime emitter gate in `handler.zig`,
/// so the over-inclusion costs index bytes, never a mis-dispatched handler.
const std = @import("std");

const core = @import("core");

const filtered_store_mod = @import("filtered_store.zig");
const sdk_manifest = @import("manifest.zig");

const FilteredStore = filtered_store_mod.FilteredStore;
const RawLog = core.RawLog;
const FlatStoreReader = core.FlatStoreReader;
const log_serial = core.log_serial;
const types = core.types;

pub const BASE_PRIMARY: []const u8 = "primary";
pub const BASE_CHILDREN: []const u8 = "children";

pub const BuildResult = struct {
    blocks_scanned: u64 = 0,
    blocks_matched: u64 = 0,
    total_logs: u64 = 0,
    /// Bloom-matched blocks the worker pipeline failed to materialize. Any
    /// non-zero value is a hard failure (incomplete index).
    dropped_blocks: u64 = 0,
    elapsed_ns: u64 = 0,
};

/// Per-log keep predicate, shared with the engine via `core.filter`. Phase 1
/// (`build`) leaves `exclude_addrs` empty. Phase 3 (`appendChildren`) sets it
/// to static∪factory.
const Filter = core.filter.Filter;

/// Phase 1: build the `primary` filtered-store pair under `dir` from the
/// manifest's static and factory addresses. Caller owns `reader` and `dir`.
/// Appending to a pre-existing pair extends it. Block numbers must be
/// strictly greater than the last recorded block.
pub fn build(
    reader: *const FlatStoreReader,
    comptime m: sdk_manifest.Manifest,
    txs: ?*const core.txs.TxsReader,
    dir: std.fs.Dir,
    allocator: std.mem.Allocator,
) !BuildResult {
    return appendBlocks(reader, m, txs, m.start_block, m.end_block orelse std.math.maxInt(u64), dir, allocator);
}

/// Extend the primary filtered-store pair over `from_block..=to_block`.
/// `build` is the special case `from_block = manifest.start_block`.
/// Drives the follow-mode gap fill in `entry.init`: when the engine advances
/// during backfill, the SDK re-scans the new range and appends matching blocks
/// without rebuilding from scratch.
pub fn appendBlocks(
    reader: *const FlatStoreReader,
    comptime m: sdk_manifest.Manifest,
    txs: ?*const core.txs.TxsReader,
    from_block: u64,
    to_block: u64,
    dir: std.fs.Dir,
    allocator: std.mem.Allocator,
) !BuildResult {
    const known_addresses = comptime collectKnownAddresses(m);
    const all_topics = comptime collectAllTopics(m);
    return runPhase(
        reader,
        known_addresses,
        txs,
        from_block,
        to_block,
        .{
            .match_addrs = known_addresses,
            .match_topics = all_topics,
            .exclude_addrs = &.{},
        },
        dir,
        BASE_PRIMARY,
        allocator,
    );
}

/// Phase 3: walk the engine's flat store filtered by scanner-discovered child
/// addresses, write matching child-event logs to the `children` pair under
/// `dir`. Spans the manifest's whole range. The follow-mode gap fill uses
/// `appendChildrenBlocks` for a sub-range instead.
pub fn appendChildren(
    reader: *const FlatStoreReader,
    comptime m: sdk_manifest.Manifest,
    txs: ?*const core.txs.TxsReader,
    child_addresses: []const [20]u8,
    dir: std.fs.Dir,
    allocator: std.mem.Allocator,
) !BuildResult {
    return appendChildrenBlocks(
        reader,
        m,
        txs,
        child_addresses,
        m.start_block,
        m.end_block orelse std.math.maxInt(u64),
        dir,
        allocator,
    );
}

/// Extend the children pair over `from_block..=to_block` for `child_addresses`.
/// Returns a zero BuildResult when there are no children or child events. The
/// per-log filter excludes addresses already in `static∪factory` so a static
/// contract that's also a factory child does not produce duplicate entries
/// across pairs. Block numbers must exceed the children store's current tail
/// (caller fills strictly-increasing ranges).
pub fn appendChildrenBlocks(
    reader: *const FlatStoreReader,
    comptime m: sdk_manifest.Manifest,
    txs: ?*const core.txs.TxsReader,
    child_addresses: []const [20]u8,
    from_block: u64,
    to_block: u64,
    dir: std.fs.Dir,
    allocator: std.mem.Allocator,
) !BuildResult {
    if (child_addresses.len == 0) return .{};

    const known_addresses = comptime collectKnownAddresses(m);
    const child_topics = comptime collectChildTopics(m);
    if (child_topics.len == 0) return .{};

    return runPhase(
        reader,
        child_addresses,
        txs,
        from_block,
        to_block,
        .{
            .match_addrs = child_addresses,
            .match_topics = child_topics,
            .exclude_addrs = known_addresses,
        },
        dir,
        BASE_CHILDREN,
        allocator,
    );
}

/// Shared phase runner. `bloom_addresses` feeds the bloom scan (block-level
/// prefilter). `filter` is the per-log keep predicate (post-decompression
/// precision filter). `base` selects the flat-store pair under `dir` to append
/// to (`BASE_PRIMARY` or `BASE_CHILDREN`).
fn runPhase(
    reader: *const FlatStoreReader,
    bloom_addresses: []const [20]u8,
    txs: ?*const core.txs.TxsReader,
    start_block: u64,
    end_block: u64,
    filter: Filter,
    dir: std.fs.Dir,
    comptime base: []const u8,
    allocator: std.mem.Allocator,
) !BuildResult {
    var store = try FilteredStore.open(allocator, dir, base);
    defer store.deinit();

    var sink = StoreSink{ .store = &store };
    const r = try core.parallel_filter.run(reader, bloom_addresses, filter, txs, start_block, end_block, StoreSink, &sink, allocator);
    try store.syncAll();

    return .{
        .blocks_scanned = r.blocks_scanned,
        .blocks_matched = r.blocks_matched,
        .total_logs = r.total_logs,
        .dropped_blocks = r.dropped_blocks,
        .elapsed_ns = r.elapsed_ns,
    };
}

/// Writes each filtered survivor to the FilteredStore pair. The chunked pipeline
/// emits in ascending block order, satisfying `appendEntry`'s monotonic key.
const StoreSink = struct {
    store: *FilteredStore,
    /// The local build leaves the timestamp 0. The scanner falls back to the
    /// engine's timestamps.bin via `timestampOf`; only the remote client fills
    /// it, from the PUSH frame.
    pub fn emit(self: *StoreSink, block_number: u64, entry: []const u8, _: u32) !void {
        try self.store.appendEntry(block_number, 0, entry);
    }
};

// ── Manifest projections ─────────────────────────────────────────────────
// Public so the remote client builds its REGISTER filter from the manifest.

pub fn collectKnownAddresses(comptime m: sdk_manifest.Manifest) []const [20]u8 {
    comptime {
        var out: []const [20]u8 = &.{};
        for (m.contracts) |c| out = out ++ &[_][20]u8{c.address};
        for (m.factories) |f| out = out ++ &[_][20]u8{f.address};
        return out;
    }
}

pub fn collectAllTopics(comptime m: sdk_manifest.Manifest) []const [32]u8 {
    comptime {
        var out: []const [32]u8 = &.{};
        for (m.contracts) |c| {
            for (c.events) |E| {
                const t = sdk_manifest.eventTopic0(E);
                if (core.filter.containsTopic(out, &t)) continue;
                out = out ++ &[_][32]u8{t};
            }
        }
        for (m.factories) |f| {
            const t = sdk_manifest.eventTopic0(f.create_event);
            if (!core.filter.containsTopic(out, &t)) out = out ++ &[_][32]u8{t};
        }
        return out;
    }
}

/// Every topic a live follow must match: contract events, factory create
/// events, and child events. `collectAllTopics` omits child topics (the
/// backfill streams children in a separate pass), but a single follow
/// connection carries both, so children registered via ADD_ADDRESS match.
pub fn collectFollowTopics(comptime m: sdk_manifest.Manifest) []const [32]u8 {
    comptime {
        var out: []const [32]u8 = collectAllTopics(m);
        for (collectChildTopics(m)) |t| {
            if (!core.filter.containsTopic(out, &t)) out = out ++ &[_][32]u8{t};
        }
        return out;
    }
}

pub fn collectChildTopics(comptime m: sdk_manifest.Manifest) []const [32]u8 {
    comptime {
        var out: []const [32]u8 = &.{};
        for (m.factories) |f| {
            for (f.child_events) |E| {
                const t = sdk_manifest.eventTopic0(E);
                if (!core.filter.containsTopic(out, &t)) out = out ++ &[_][32]u8{t};
            }
        }
        return out;
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;
const flat_reader = core.flat_reader;

const TestLog = flat_reader.TestLog;
const TestBlock = flat_reader.TestBlock;

const ContractA = struct {
    pub const signature = "EventA(uint256)";
};
const ContractB = struct {
    pub const signature = "EventB(uint256)";
};
const Other = struct {
    pub const signature = "Other(uint256)";
};

const ADDR_A: [20]u8 = [_]u8{0xAA} ** 20;
const ADDR_B: [20]u8 = [_]u8{0xBB} ** 20;
const ADDR_C: [20]u8 = [_]u8{0xCC} ** 20;

fn topicOf(comptime E: type) [32]u8 {
    return sdk_manifest.eventTopic0(E);
}

const SmallManifest: sdk_manifest.Manifest = .{
    .name = "test",
    .chain_id = 1,
    .start_block = 0,
    .contracts = &.{
        .{ .name = "A", .address = ADDR_A, .events = &.{ContractA} },
        .{ .name = "B", .address = ADDR_B, .events = &.{ContractB} },
    },
};

fn dumpDecodedBlocks(dir: std.fs.Dir, comptime base: []const u8, allocator: std.mem.Allocator) !std.ArrayListUnmanaged(DecodedBlock) {
    var store = FilteredStore.open(allocator, dir, base) catch {
        return std.ArrayListUnmanaged(DecodedBlock){};
    };
    defer store.deinit();

    var out: std.ArrayListUnmanaged(DecodedBlock) = .{};
    errdefer {
        for (out.items) |db_item| allocator.free(db_item.logs);
        out.deinit(allocator);
    }

    const decompress_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(decompress_buf);
    const payload_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(payload_buf);
    const log_scratch = try allocator.alloc(RawLog, types.MAX_LOGS_PER_BLOCK);
    defer allocator.free(log_scratch);

    var i: u64 = 0;
    while (i < store.count()) : (i += 1) {
        const entry = try store.readEntry(i);
        const payload = try store.readPayload(i, payload_buf);
        const decoded = try log_serial.decompressEntry(payload, decompress_buf);
        const n = log_serial.deserializeLogs(decoded, log_scratch);
        const owned = try allocator.alloc(LogSummary, n);
        for (log_scratch[0..n], 0..) |*l, j| {
            owned[j] = .{ .address = l.address, .topic0 = l.topics[0] };
        }
        try out.append(allocator, .{ .block_number = entry.block_number, .logs = owned });
    }
    return out;
}

const LogSummary = struct {
    address: [20]u8,
    topic0: [32]u8,
};

const DecodedBlock = struct {
    block_number: u64,
    logs: []LogSummary,
};

fn freeDecoded(decoded: *std.ArrayListUnmanaged(DecodedBlock), allocator: std.mem.Allocator) void {
    for (decoded.items) |db| allocator.free(db.logs);
    decoded.deinit(allocator);
}

test "build: filters multi-contract flat store, primary contains exactly the matches" {
    const allocator = testing.allocator;

    // Plant 1000 blocks. Even blocks: contract A+B logs (matching). Odd
    // blocks: contract C only (non-matching).
    const N: u64 = 1000;
    const a_topic = topicOf(ContractA);
    const b_topic = topicOf(ContractB);
    const other_topic = topicOf(Other);

    var blocks_list: std.ArrayListUnmanaged(TestBlock) = .{};
    defer blocks_list.deinit(allocator);
    var log_arena = std.heap.ArenaAllocator.init(allocator);
    defer log_arena.deinit();
    const arena = log_arena.allocator();

    var matching_count: u64 = 0;
    var matching_log_total: u64 = 0;

    for (0..N) |i| {
        const bn: u64 = 100 + i;
        const logs = if (bn % 2 == 0) blk: {
            const buf = try arena.alloc(TestLog, 2);
            buf[0] = .{ .address = ADDR_A, .topic0 = a_topic };
            buf[1] = .{ .address = ADDR_B, .topic0 = b_topic, .log_index = 1 };
            matching_count += 1;
            matching_log_total += 2;
            break :blk @as([]const TestLog, buf);
        } else blk: {
            const buf = try arena.alloc(TestLog, 1);
            buf[0] = .{ .address = ADDR_C, .topic0 = other_topic };
            break :blk @as([]const TestLog, buf);
        };
        try blocks_list.append(allocator, .{ .block_number = bn, .logs = logs });
    }

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try flat_reader.writeTestStore(src_tmp.dir, blocks_list.items, allocator);

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    const result = try build(&reader, SmallManifest, null, dst_tmp.dir, allocator);
    try testing.expectEqual(matching_count, result.blocks_matched);
    try testing.expectEqual(matching_log_total, result.total_logs);
    try testing.expectEqual(N, result.blocks_scanned);

    var decoded = try dumpDecodedBlocks(dst_tmp.dir, BASE_PRIMARY, allocator);
    defer freeDecoded(&decoded, allocator);

    try testing.expectEqual(matching_count, @as(u64, decoded.items.len));
    for (decoded.items) |entry| {
        try testing.expect(entry.block_number % 2 == 0);
        try testing.expectEqual(@as(usize, 2), entry.logs.len);
        try testing.expectEqualSlices(u8, &ADDR_A, &entry.logs[0].address);
        try testing.expectEqualSlices(u8, &a_topic, &entry.logs[0].topic0);
        try testing.expectEqualSlices(u8, &ADDR_B, &entry.logs[1].address);
        try testing.expectEqualSlices(u8, &b_topic, &entry.logs[1].topic0);
    }
}

test "build: rebuild produces decoded-identical output" {
    const allocator = testing.allocator;
    const a_topic = topicOf(ContractA);

    var blocks_list: std.ArrayListUnmanaged(TestBlock) = .{};
    defer blocks_list.deinit(allocator);
    var log_arena = std.heap.ArenaAllocator.init(allocator);
    defer log_arena.deinit();
    const arena = log_arena.allocator();

    for (0..200) |i| {
        const bn: u64 = 1000 + i;
        const buf = try arena.alloc(TestLog, 1);
        buf[0] = .{ .address = ADDR_A, .topic0 = a_topic };
        try blocks_list.append(allocator, .{ .block_number = bn, .logs = buf });
    }

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try flat_reader.writeTestStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    const RebuildManifest: sdk_manifest.Manifest = .{
        .name = "rebuild",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "A", .address = ADDR_A, .events = &.{ContractA} }},
    };

    var first = try buildIntoNewTmp(&reader, RebuildManifest, allocator);
    defer freeDecoded(&first.decoded, allocator);
    defer first.tmp.cleanup();

    var second = try buildIntoNewTmp(&reader, RebuildManifest, allocator);
    defer freeDecoded(&second.decoded, allocator);
    defer second.tmp.cleanup();

    try testing.expectEqual(first.decoded.items.len, second.decoded.items.len);
    for (first.decoded.items, second.decoded.items) |a, b| {
        try testing.expectEqual(a.block_number, b.block_number);
        try testing.expectEqual(a.logs.len, b.logs.len);
        for (a.logs, b.logs) |la, lb| {
            try testing.expectEqualSlices(u8, &la.address, &lb.address);
            try testing.expectEqualSlices(u8, &la.topic0, &lb.topic0);
        }
    }
}

const RebuildOutput = struct {
    tmp: std.testing.TmpDir,
    decoded: std.ArrayListUnmanaged(DecodedBlock),
};

fn buildIntoNewTmp(
    reader: *const FlatStoreReader,
    comptime m: sdk_manifest.Manifest,
    allocator: std.mem.Allocator,
) !RebuildOutput {
    const tmp = testing.tmpDir(.{});
    _ = try build(reader, m, null, tmp.dir, allocator);
    const decoded = try dumpDecodedBlocks(tmp.dir, BASE_PRIMARY, allocator);
    return .{ .tmp = tmp, .decoded = decoded };
}

test "build + appendChildren: primary holds creations, children holds child events, no duplication" {
    const allocator = testing.allocator;

    const FactoryAddr: [20]u8 = [_]u8{0xF0} ** 20;
    const ChildAddr1: [20]u8 = [_]u8{0xC1} ** 20;
    const ChildAddr2: [20]u8 = [_]u8{0xC2} ** 20;
    const ChildAddr3: [20]u8 = [_]u8{0xC3} ** 20;

    const Create = struct {
        pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair)";
    };
    const Sync = struct {
        pub const signature = "Sync(uint112,uint112)";
    };

    const FactoryManifest: sdk_manifest.Manifest = .{
        .name = "factory",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FactoryAddr,
            .create_event = Create,
            .spawn_param = "pair",
            .child_events = &.{Sync},
        }},
    };

    const create_topic = topicOf(Create);
    const sync_topic = topicOf(Sync);

    var blocks_list: std.ArrayListUnmanaged(TestBlock) = .{};
    defer blocks_list.deinit(allocator);
    var log_arena = std.heap.ArenaAllocator.init(allocator);
    defer log_arena.deinit();
    const arena = log_arena.allocator();

    // Block 100: factory emits a creation event.
    const create_logs = try arena.alloc(TestLog, 1);
    create_logs[0] = .{ .address = FactoryAddr, .topic0 = create_topic };
    try blocks_list.append(allocator, .{ .block_number = 100, .logs = create_logs });

    // Blocks 101..103: child contracts emit Sync.
    const sync1_logs = try arena.alloc(TestLog, 1);
    sync1_logs[0] = .{ .address = ChildAddr1, .topic0 = sync_topic };
    try blocks_list.append(allocator, .{ .block_number = 101, .logs = sync1_logs });
    const sync2_logs = try arena.alloc(TestLog, 1);
    sync2_logs[0] = .{ .address = ChildAddr2, .topic0 = sync_topic };
    try blocks_list.append(allocator, .{ .block_number = 102, .logs = sync2_logs });
    const sync3_logs = try arena.alloc(TestLog, 1);
    sync3_logs[0] = .{ .address = ChildAddr3, .topic0 = sync_topic };
    try blocks_list.append(allocator, .{ .block_number = 103, .logs = sync3_logs });

    // Block 104: unrelated address with unrelated topic. Must NOT appear in either pair.
    const noise_topic = topicOf(Other);
    const noise_logs = try arena.alloc(TestLog, 1);
    noise_logs[0] = .{ .address = ADDR_C, .topic0 = noise_topic };
    try blocks_list.append(allocator, .{ .block_number = 104, .logs = noise_logs });

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try flat_reader.writeTestStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    // Phase 1: build primary. Only the factory creation event qualifies, since
    // child addresses are not yet known.
    const primary = try build(&reader, FactoryManifest, null, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 1), primary.blocks_matched);
    try testing.expectEqual(@as(u64, 1), primary.total_logs);

    // Phase 3: append children for the addresses the pre-pass discovered.
    const discovered = [_][20]u8{ ChildAddr1, ChildAddr2, ChildAddr3 };
    const children = try appendChildren(&reader, FactoryManifest, null, &discovered, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 3), children.blocks_matched);
    try testing.expectEqual(@as(u64, 3), children.total_logs);

    var primary_blocks = try dumpDecodedBlocks(dst_tmp.dir, BASE_PRIMARY, allocator);
    defer freeDecoded(&primary_blocks, allocator);
    var child_blocks = try dumpDecodedBlocks(dst_tmp.dir, BASE_CHILDREN, allocator);
    defer freeDecoded(&child_blocks, allocator);

    try testing.expectEqual(@as(usize, 1), primary_blocks.items.len);
    try testing.expectEqual(@as(u64, 100), primary_blocks.items[0].block_number);
    try testing.expectEqualSlices(u8, &FactoryAddr, &primary_blocks.items[0].logs[0].address);
    try testing.expectEqualSlices(u8, &create_topic, &primary_blocks.items[0].logs[0].topic0);

    try testing.expectEqual(@as(usize, 3), child_blocks.items.len);
    try testing.expectEqual(@as(u64, 101), child_blocks.items[0].block_number);
    try testing.expectEqualSlices(u8, &ChildAddr1, &child_blocks.items[0].logs[0].address);
    try testing.expectEqualSlices(u8, &sync_topic, &child_blocks.items[0].logs[0].topic0);
    try testing.expectEqual(@as(u64, 102), child_blocks.items[1].block_number);
    try testing.expectEqualSlices(u8, &ChildAddr2, &child_blocks.items[1].logs[0].address);
    try testing.expectEqual(@as(u64, 103), child_blocks.items[2].block_number);
    try testing.expectEqualSlices(u8, &ChildAddr3, &child_blocks.items[2].logs[0].address);
}

test "appendChildren: returns zero-result for empty discovered set" {
    const allocator = testing.allocator;

    const FactoryAddr: [20]u8 = [_]u8{0xF0} ** 20;
    const Create = struct {
        pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair)";
    };
    const Sync = struct {
        pub const signature = "Sync(uint112,uint112)";
    };
    const FactoryManifest: sdk_manifest.Manifest = .{
        .name = "factory",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FactoryAddr,
            .create_event = Create,
            .spawn_param = "pair",
            .child_events = &.{Sync},
        }},
    };

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    var blocks: [1]TestBlock = .{.{ .block_number = 100, .logs = &.{} }};
    blocks[0].logs = &[_]TestLog{.{ .address = FactoryAddr, .topic0 = topicOf(Create) }};
    try flat_reader.writeTestStore(src_tmp.dir, &blocks, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    _ = try build(&reader, FactoryManifest, null, dst_tmp.dir, allocator);
    const result = try appendChildren(&reader, FactoryManifest, null, &.{}, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 0), result.blocks_scanned);
    try testing.expectEqual(@as(u64, 0), result.blocks_matched);
}

test "build: end_block clamps the scan range to a fixed window" {
    const allocator = testing.allocator;

    // Plant 50 contiguous blocks, each with a matching ContractA log. With
    // end_block = 119 (first block 100), the build matches exactly 20 blocks
    // (100..=119) and ignores 120..=149.
    const a_topic = topicOf(ContractA);
    var blocks_list: std.ArrayListUnmanaged(TestBlock) = .{};
    defer blocks_list.deinit(allocator);
    var log_arena = std.heap.ArenaAllocator.init(allocator);
    defer log_arena.deinit();
    const arena = log_arena.allocator();
    for (0..50) |i| {
        const buf = try arena.alloc(TestLog, 1);
        buf[0] = .{ .address = ADDR_A, .topic0 = a_topic };
        try blocks_list.append(allocator, .{ .block_number = 100 + i, .logs = buf });
    }

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try flat_reader.writeTestStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    const ClampedManifest: sdk_manifest.Manifest = .{
        .name = "clamped",
        .chain_id = 1,
        .start_block = 0,
        .end_block = 119,
        .contracts = &.{.{ .name = "A", .address = ADDR_A, .events = &.{ContractA} }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    const result = try build(&reader, ClampedManifest, null, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 20), result.blocks_matched);
    try testing.expectEqual(@as(u64, 20), result.total_logs);

    // On-disk contents: exactly blocks 100..=119, none past 119.
    var decoded = try dumpDecodedBlocks(dst_tmp.dir, BASE_PRIMARY, allocator);
    defer freeDecoded(&decoded, allocator);
    try testing.expectEqual(@as(usize, 20), decoded.items.len);
    try testing.expectEqual(@as(u64, 100), decoded.items[0].block_number);
    try testing.expectEqual(@as(u64, 119), decoded.items[19].block_number);
}

test "appendBlocks extends a primary filter env over the new range" {
    const allocator = testing.allocator;

    // 10 contiguous blocks, every one matches ContractA.
    const a_topic = topicOf(ContractA);
    var blocks_list: std.ArrayListUnmanaged(TestBlock) = .{};
    defer blocks_list.deinit(allocator);
    var log_arena = std.heap.ArenaAllocator.init(allocator);
    defer log_arena.deinit();
    const arena = log_arena.allocator();
    for (0..10) |i| {
        const buf = try arena.alloc(TestLog, 1);
        buf[0] = .{ .address = ADDR_A, .topic0 = a_topic };
        try blocks_list.append(allocator, .{ .block_number = 100 + i, .logs = buf });
    }

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try flat_reader.writeTestStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    const M: sdk_manifest.Manifest = .{
        .name = "ext",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "A", .address = ADDR_A, .events = &.{ContractA} }},
    };

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    // First pass: cover blocks 100..=104.
    const first = try appendBlocks(&reader, M, null, 100, 104, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 5), first.blocks_matched);

    // Second pass extends the pair over 105..=109, same files, appended.
    const second = try appendBlocks(&reader, M, null, 105, 109, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 5), second.blocks_matched);

    var decoded = try dumpDecodedBlocks(dst_tmp.dir, BASE_PRIMARY, allocator);
    defer freeDecoded(&decoded, allocator);
    try testing.expectEqual(@as(usize, 10), decoded.items.len);
    try testing.expectEqual(@as(u64, 100), decoded.items[0].block_number);
    try testing.expectEqual(@as(u64, 109), decoded.items[9].block_number);
}

test "appendChildrenBlocks extends the children pair over a sub-range" {
    const allocator = testing.allocator;

    const ChildAddr: [20]u8 = [_]u8{0xC1} ** 20;
    const FactoryAddr: [20]u8 = [_]u8{0xF0} ** 20;
    const Create = struct {
        pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair)";
    };
    const Sync = struct {
        pub const signature = "Sync(uint112,uint112)";
    };
    const M: sdk_manifest.Manifest = .{
        .name = "gapchild",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = FactoryAddr,
            .create_event = Create,
            .spawn_param = "pair",
            .child_events = &.{Sync},
        }},
    };

    const sync_topic = topicOf(Sync);
    var blocks_list: std.ArrayListUnmanaged(TestBlock) = .{};
    defer blocks_list.deinit(allocator);
    var log_arena = std.heap.ArenaAllocator.init(allocator);
    defer log_arena.deinit();
    const arena = log_arena.allocator();
    // The same child emits Sync in 10 contiguous blocks.
    for (0..10) |i| {
        const buf = try arena.alloc(TestLog, 1);
        buf[0] = .{ .address = ChildAddr, .topic0 = sync_topic };
        try blocks_list.append(allocator, .{ .block_number = 100 + i, .logs = buf });
    }

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    try flat_reader.writeTestStore(src_tmp.dir, blocks_list.items, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);
    var reader = try FlatStoreReader.open(src_path);
    defer reader.deinit();

    var dst_tmp = testing.tmpDir(.{});
    defer dst_tmp.cleanup();

    const children = [_][20]u8{ChildAddr};
    // Backfill covers 100..=104, then the follow gap extends 105..=109.
    const first = try appendChildrenBlocks(&reader, M, null, &children, 100, 104, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 5), first.blocks_matched);
    const second = try appendChildrenBlocks(&reader, M, null, &children, 105, 109, dst_tmp.dir, allocator);
    try testing.expectEqual(@as(u64, 5), second.blocks_matched);

    var decoded = try dumpDecodedBlocks(dst_tmp.dir, BASE_CHILDREN, allocator);
    defer freeDecoded(&decoded, allocator);
    try testing.expectEqual(@as(usize, 10), decoded.items.len);
    try testing.expectEqual(@as(u64, 100), decoded.items[0].block_number);
    try testing.expectEqual(@as(u64, 109), decoded.items[9].block_number);
}
