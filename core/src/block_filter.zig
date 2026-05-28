/// Parallel bloom scan over the flat store's blooms.bin.
///
/// A block matches when every non-empty bloom set hits: any `target_addresses`
/// against the addr bloom AND any `target_topics` against the topic bloom.
/// At least one set must be non-empty (asserted). The bloom scan is the
/// block-level prefilter; per-log filtering after decompression applies the
/// precise predicate.
const std = @import("std");

const bloom = @import("bloom.zig");
const flat_reader = @import("flat_reader.zig");
const parallel = @import("parallel.zig");

const AddrBloom = bloom.AddrBloom;
const TopicBloom = bloom.Bloom;
const FlatStoreReader = flat_reader.FlatStoreReader;

/// Single-threaded bloom scan. Used directly for small datasets and as the
/// inline body of `scanBloomsParallel` when only one worker is needed.
pub fn scanBlooms(
    reader: *const FlatStoreReader,
    target_addresses: []const [20]u8,
    target_topics: []const [32]u8,
    start_block: u64,
    end_block: u64,
    matching: *std.ArrayListUnmanaged(u64),
    blocks_scanned: *u64,
    blocks_dropped: *u64,
    allocator: std.mem.Allocator,
) !void {
    std.debug.assert(target_addresses.len > 0 or target_topics.len > 0);

    const addr_keys = try buildAddrKeys(target_addresses, allocator);
    defer allocator.free(addr_keys);

    var args = WorkerArgs{
        .reader = reader,
        .addr_keys = addr_keys,
        .topic_keys = target_topics,
        .start_idx = reader.findBloomStart(start_block),
        .end_idx = reader.blooms_count,
        .end_block = end_block,
        .result_matching = matching.*,
        .result_scanned = blocks_scanned.*,
        .result_dropped = blocks_dropped.*,
        .alloc = allocator,
    };
    scanRange(false, &args);
    matching.* = args.result_matching;
    blocks_scanned.* = args.result_scanned;
    blocks_dropped.* = args.result_dropped;
}

/// Parallel bloom scan: split blooms.bin across N threads, aggregate results
/// in block order. Issues `fadvise(WILLNEED)` on matching blocks (Linux) so
/// the kernel starts async NVMe DMA before io_uring workers begin.
pub fn scanBloomsParallel(
    reader: *const FlatStoreReader,
    target_addresses: []const [20]u8,
    target_topics: []const [32]u8,
    start_block: u64,
    end_block: u64,
    matching: *std.ArrayListUnmanaged(u64),
    blocks_scanned: *u64,
    blocks_dropped: *u64,
    allocator: std.mem.Allocator,
) !void {
    std.debug.assert(target_addresses.len > 0 or target_topics.len > 0);

    const num_workers = parallel.workerCount(reader.blooms_count, 100_000);
    if (num_workers <= 1) {
        return scanBlooms(reader, target_addresses, target_topics, start_block, end_block, matching, blocks_scanned, blocks_dropped, allocator);
    }

    const addr_keys = try buildAddrKeys(target_addresses, allocator);
    defer allocator.free(addr_keys);

    const scan_start = reader.findBloomStart(start_block);
    const scan_count = if (reader.blooms_count > scan_start) reader.blooms_count - scan_start else 0;
    const ranges = parallel.chunkRanges(scan_count, num_workers);

    var worker_args: [parallel.MAX_WORKERS]WorkerArgs = undefined;
    for (0..num_workers) |i| {
        worker_args[i] = .{
            .reader = reader,
            .addr_keys = addr_keys,
            .topic_keys = target_topics,
            .start_idx = scan_start + ranges[i][0],
            .end_idx = scan_start + ranges[i][1],
            .end_block = end_block,
            .result_matching = .{},
            .result_scanned = 0,
            .result_dropped = 0,
            .alloc = allocator,
        };
    }

    try parallel.run(WorkerArgs, worker_args[0..num_workers], num_workers, workerFn);

    // Each chunk is already in block order since chunkRanges hands out
    // contiguous slices and the bloom file is sorted by block number.
    for (0..num_workers) |i| {
        try matching.appendSlice(allocator, worker_args[i].result_matching.items);
        blocks_scanned.* += worker_args[i].result_scanned;
        blocks_dropped.* += worker_args[i].result_dropped;
        worker_args[i].result_matching.deinit(allocator);
    }

    // No `madvise(DONTNEED)` on blooms.bin: warm reruns avoid a full
    // re-read of the bloom file. On memory-pressured hosts the kernel
    // evicts naturally; the hint would only hurt the steady state.
}

// ── Internal ─────────────────────────────────────────────────────────────

fn buildAddrKeys(addresses: []const [20]u8, allocator: std.mem.Allocator) ![]const [32]u8 {
    const keys = try allocator.alloc([32]u8, addresses.len);
    for (addresses, 0..) |addr, i| keys[i] = AddrBloom.addrToBloomKey(addr);
    return keys;
}

const WorkerArgs = struct {
    reader: *const FlatStoreReader,
    addr_keys: []const [32]u8,
    topic_keys: []const [32]u8,
    start_idx: usize,
    end_idx: usize,
    end_block: u64,
    result_matching: std.ArrayListUnmanaged(u64),
    result_scanned: u64,
    result_dropped: u64,
    alloc: std.mem.Allocator,
};

fn scanRange(comptime prefetch: bool, args: *WorkerArgs) void {
    const base = args.reader.blooms_map[flat_reader.BLOOM_HEADER_SIZE..];
    const check_addr = args.addr_keys.len > 0;
    const check_topic = args.topic_keys.len > 0;
    for (args.start_idx..args.end_idx) |i| {
        const offset = i * flat_reader.BLOOM_ENTRY_SIZE;
        if (offset + flat_reader.BLOOM_ENTRY_SIZE > base.len) break;
        const entry = base[offset..][0..flat_reader.BLOOM_ENTRY_SIZE];
        const block_number = std.mem.readInt(u64, entry[0..8], .big);
        if (block_number > args.end_block) break;
        args.result_scanned += 1;

        if (check_addr) {
            const addr_bloom = entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE];
            if (!AddrBloom.bytesContainAny(addr_bloom, args.addr_keys)) continue;
        }
        if (check_topic) {
            const topic_bloom = entry[flat_reader.TOPIC_BLOOM_OFFSET..][0..bloom.BLOOM_SIZE];
            if (!TopicBloom.bytesContainAny(topic_bloom, args.topic_keys)) continue;
        }

        args.result_matching.append(args.alloc, block_number) catch {
            args.result_dropped += 1;
            continue;
        };

        if (comptime prefetch and @import("builtin").os.tag == .linux) {
            if (args.reader.getBlockLoc(block_number)) |loc| {
                _ = std.os.linux.fadvise(args.reader.blocks_file.handle, @intCast(loc.offset), @intCast(loc.length), std.os.linux.POSIX_FADV.WILLNEED);
            } else |_| {}
        }
    }
}

fn workerFn(args: *WorkerArgs) void {
    scanRange(true, args);
}

// ── Tests ────────────────────────────────────────────────────────────────

const page_align = std.heap.page_size_min;

test "scanBlooms: single address matches blocks via addr bloom" {
    const alloc = std.testing.allocator;
    const target_addr = [_]u8{0xAE} ** 20;
    const other_addr = [_]u8{0xFF} ** 20;

    var ab_target = AddrBloom.init();
    ab_target.insert(AddrBloom.addrToBloomKey(target_addr));
    var ab_other = AddrBloom.init();
    ab_other.insert(AddrBloom.addrToBloomKey(other_addr));
    const addr_blooms = [_][bloom.ADDR_BLOOM_SIZE]u8{ ab_target.bits, ab_other.bits, ab_target.bits };
    const block_numbers = [_]u64{ 100, 101, 102 };

    const blooms_buf = try flat_reader.buildTestBlooms(&block_numbers, &addr_blooms, alloc);
    defer alloc.free(blooms_buf);

    var idx: [flat_reader.INDEX_HEADER_SIZE]u8 align(page_align) = undefined;
    std.mem.writeInt(u64, idx[0..8], 100, .little);
    std.mem.writeInt(u64, idx[8..16], 0, .little);
    const reader = flat_reader.testReader(&idx, blooms_buf, undefined);

    var matching = std.ArrayListUnmanaged(u64){};
    defer matching.deinit(alloc);
    var scanned: u64 = 0;
    var dropped: u64 = 0;

    const targets = [_][20]u8{target_addr};
    try scanBlooms(&reader, &targets, &.{}, 0, 200, &matching, &scanned, &dropped, alloc);

    try std.testing.expectEqual(@as(u64, 3), scanned);
    try std.testing.expectEqual(@as(u64, 0), dropped);
    try std.testing.expectEqual(@as(usize, 2), matching.items.len);
    try std.testing.expectEqual(@as(u64, 100), matching.items[0]);
    try std.testing.expectEqual(@as(u64, 102), matching.items[1]);
}

test "scanBlooms: union of multiple addresses" {
    const alloc = std.testing.allocator;
    const addr_a = [_]u8{0xAA} ** 20;
    const addr_b = [_]u8{0xBB} ** 20;
    const addr_c = [_]u8{0xCC} ** 20;

    var ab_a = AddrBloom.init();
    ab_a.insert(AddrBloom.addrToBloomKey(addr_a));
    var ab_b = AddrBloom.init();
    ab_b.insert(AddrBloom.addrToBloomKey(addr_b));
    var ab_c = AddrBloom.init();
    ab_c.insert(AddrBloom.addrToBloomKey(addr_c));

    const addr_blooms = [_][bloom.ADDR_BLOOM_SIZE]u8{ ab_a.bits, ab_b.bits, ab_c.bits };
    const block_numbers = [_]u64{ 100, 101, 102 };

    const blooms_buf = try flat_reader.buildTestBlooms(&block_numbers, &addr_blooms, alloc);
    defer alloc.free(blooms_buf);

    var idx: [flat_reader.INDEX_HEADER_SIZE]u8 align(page_align) = undefined;
    std.mem.writeInt(u64, idx[0..8], 100, .little);
    std.mem.writeInt(u64, idx[8..16], 0, .little);
    const reader = flat_reader.testReader(&idx, blooms_buf, undefined);

    var matching = std.ArrayListUnmanaged(u64){};
    defer matching.deinit(alloc);
    var scanned: u64 = 0;
    var dropped: u64 = 0;

    const targets = [_][20]u8{ addr_a, addr_b };
    try scanBlooms(&reader, &targets, &.{}, 0, 200, &matching, &scanned, &dropped, alloc);

    try std.testing.expectEqual(@as(u64, 3), scanned);
    try std.testing.expectEqual(@as(usize, 2), matching.items.len);
    try std.testing.expectEqual(@as(u64, 100), matching.items[0]);
    try std.testing.expectEqual(@as(u64, 101), matching.items[1]);
}

test "scanBlooms: dual-bloom AND rejects address hits whose topic bloom doesn't match" {
    const alloc = std.testing.allocator;
    const target_addr = [_]u8{0xAE} ** 20;
    const want_topic = [_]u8{0xCA} ** 32;
    const noise_topic = [_]u8{0xFE} ** 32;

    const blooms_buf = blk: {
        const total = flat_reader.BLOOM_HEADER_SIZE + 3 * flat_reader.BLOOM_ENTRY_SIZE;
        const buf = try alloc.alignedAlloc(u8, .fromByteUnits(page_align), total);
        std.mem.writeInt(u64, buf[0..8], 3, .little);
        const block_numbers = [_]u64{ 100, 101, 102 };
        const topics = [_][32]u8{ want_topic, noise_topic, noise_topic };
        for (block_numbers, topics, 0..) |bn, t, i| {
            var ab = AddrBloom.init();
            ab.insert(AddrBloom.addrToBloomKey(target_addr));
            var tb = TopicBloom.init();
            tb.insert(t);

            const off = flat_reader.BLOOM_HEADER_SIZE + i * flat_reader.BLOOM_ENTRY_SIZE;
            var entry: [flat_reader.BLOOM_ENTRY_SIZE]u8 = std.mem.zeroes([flat_reader.BLOOM_ENTRY_SIZE]u8);
            std.mem.writeInt(u64, entry[0..8], bn, .big);
            @memcpy(entry[flat_reader.TOPIC_BLOOM_OFFSET..][0..bloom.BLOOM_SIZE], &tb.bits);
            @memcpy(entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE], &ab.bits);
            @memcpy(buf[off..][0..flat_reader.BLOOM_ENTRY_SIZE], &entry);
        }
        break :blk buf;
    };
    defer alloc.free(blooms_buf);

    var idx: [flat_reader.INDEX_HEADER_SIZE]u8 align(page_align) = undefined;
    std.mem.writeInt(u64, idx[0..8], 100, .little);
    std.mem.writeInt(u64, idx[8..16], 0, .little);
    const reader = flat_reader.testReader(&idx, blooms_buf, undefined);

    var matching = std.ArrayListUnmanaged(u64){};
    defer matching.deinit(alloc);
    var scanned: u64 = 0;
    var dropped: u64 = 0;

    const targets = [_][20]u8{target_addr};
    const topics = [_][32]u8{want_topic};
    try scanBlooms(&reader, &targets, &topics, 0, 200, &matching, &scanned, &dropped, alloc);

    try std.testing.expectEqual(@as(u64, 3), scanned);
    try std.testing.expectEqual(@as(usize, 1), matching.items.len);
    try std.testing.expectEqual(@as(u64, 100), matching.items[0]);
}
