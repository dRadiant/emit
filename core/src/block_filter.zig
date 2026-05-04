/// Parallel bloom scan over the flat store's blooms.bin.
/// Takes a target address, scans bloom entries, returns matching block numbers.
/// Used by sdk (filtered index build) and engine (v2 remote streaming).
const std = @import("std");
const bloom_mod = @import("bloom.zig");
const flat_reader = @import("flat_reader.zig");
const parallel = @import("parallel.zig");

const AddrBloom = bloom_mod.AddrBloom;
const FlatStoreReader = flat_reader.FlatStoreReader;

/// Single-threaded bloom scan. For small datasets or when called from parallel.
pub fn scanBlooms(
    reader: *const FlatStoreReader,
    target_address: [20]u8,
    start_block: u64,
    end_block: u64,
    matching: *std.ArrayListUnmanaged(u64),
    blocks_scanned: *u64,
    allocator: std.mem.Allocator,
) void {
    var args = BloomWorkerArgs{
        .reader = reader,
        .addr_bloom_key = AddrBloom.addrToBloomKey(target_address),
        .start_idx = reader.findBloomStart(start_block),
        .end_idx = reader.blooms_count,
        .end_block = end_block,
        .result_matching = matching.*,
        .result_scanned = blocks_scanned.*,
        .alloc = allocator,
    };
    scanBloomRange(false, &args);
    matching.* = args.result_matching;
    blocks_scanned.* = args.result_scanned;
}

/// Parallel bloom scan: split across N threads, aggregate results in block order.
/// Issues fadvise(WILLNEED) on matching blocks during scan (Linux only) so the
/// kernel starts async NVMe DMA before io_uring workers begin.
pub fn scanBloomsParallel(
    reader: *const FlatStoreReader,
    target_address: [20]u8,
    start_block: u64,
    end_block: u64,
    matching: *std.ArrayListUnmanaged(u64),
    blocks_scanned: *u64,
    allocator: std.mem.Allocator,
) !void {
    const num_workers = parallel.workerCount(reader.blooms_count, 100_000);

    if (num_workers <= 1) {
        scanBlooms(reader, target_address, start_block, end_block, matching, blocks_scanned, allocator);
        return;
    }

    // Phase 1: Binary search to skip entries before start_block (O(log N)).
    // Phase 2: Split the remaining range across N workers, each scans its chunk.
    // Phase 3: Aggregate results — each chunk is already sorted by block number.
    const scan_start = reader.findBloomStart(start_block);
    const scan_count = if (reader.blooms_count > scan_start) reader.blooms_count - scan_start else 0;
    const ranges = parallel.chunkRanges(scan_count, num_workers);
    const addr_bloom_key = AddrBloom.addrToBloomKey(target_address);

    var worker_args: [parallel.MAX_WORKERS]BloomWorkerArgs = undefined;
    for (0..num_workers) |i| {
        worker_args[i] = .{
            .reader = reader,
            .addr_bloom_key = addr_bloom_key,
            .start_idx = scan_start + ranges[i][0],
            .end_idx = scan_start + ranges[i][1],
            .end_block = end_block,
            .result_matching = .{},
            .result_scanned = 0,
            .alloc = allocator,
        };
    }

    try parallel.run(BloomWorkerArgs, worker_args[0..num_workers], num_workers, bloomWorkerFn);

    // Aggregate — each chunk is already in block order
    for (0..num_workers) |i| {
        try matching.appendSlice(allocator, worker_args[i].result_matching.items);
        blocks_scanned.* += worker_args[i].result_scanned;
        worker_args[i].result_matching.deinit(allocator);
    }

    // Release blooms.bin pages after aggregation — free page cache for blocks.dat reads
    std.posix.madvise(@constCast(reader.blooms_map.ptr), reader.blooms_map.len, std.posix.MADV.DONTNEED) catch {};
}

// ── Internal ─────────────────────────────────────────────────────────────

/// Per-thread state for parallel bloom scan. Each worker scans a contiguous
/// slice of bloom entries and collects matching block numbers into its own
/// list — no cross-thread contention. Results are concatenated after join.
const BloomWorkerArgs = struct {
    reader: *const FlatStoreReader,
    addr_bloom_key: [32]u8, // 20-byte address right-padded to 32 for bloom hashing
    start_idx: usize, // first bloom entry index (inclusive)
    end_idx: usize, // last bloom entry index (exclusive)
    end_block: u64,
    result_matching: std.ArrayListUnmanaged(u64),
    result_scanned: u64,
    alloc: std.mem.Allocator,
};

/// Scan a range of bloom entries, checking address bloom membership.
/// When `prefetch` is true (parallel path), issues fadvise(WILLNEED) on
/// matching blocks so the kernel starts async NVMe DMA during the scan.
fn scanBloomRange(comptime prefetch: bool, args: *BloomWorkerArgs) void {
    const base = args.reader.blooms_map[flat_reader.BLOOM_HEADER_SIZE..];
    for (args.start_idx..args.end_idx) |i| {
        const offset = i * flat_reader.BLOOM_ENTRY_SIZE;
        if (offset + flat_reader.BLOOM_ENTRY_SIZE > base.len) break;
        const entry = base[offset..][0..flat_reader.BLOOM_ENTRY_SIZE];
        const block_number = std.mem.readInt(u64, entry[0..8], .big);
        if (block_number > args.end_block) break;
        args.result_scanned += 1;

        const addr_bloom = entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom_mod.ADDR_BLOOM_SIZE];
        if (AddrBloom.bytesContain(addr_bloom, args.addr_bloom_key)) {
            args.result_matching.append(args.alloc, block_number) catch continue;

            if (comptime prefetch and @import("builtin").os.tag == .linux) {
                if (args.reader.getBlockLoc(block_number)) |loc| {
                    _ = std.os.linux.fadvise(args.reader.blocks_file.handle, @intCast(loc.offset), @intCast(loc.length), std.os.linux.POSIX_FADV.WILLNEED);
                } else |_| {}
            }
        }
    }
}

fn bloomWorkerFn(args: *BloomWorkerArgs) void {
    scanBloomRange(true, args);
}

// ── Tests ────────────────────────────────────────────────────────────────

const page_align = std.heap.page_size_min;

test "scanBlooms matches correct blocks" {
    const alloc = std.testing.allocator;

    const target_addr = [_]u8{0xAE} ** 20;
    const other_addr = [_]u8{0xFF} ** 20;

    // Build addr blooms: target in blocks 100/102, other in 101
    var ab_target = AddrBloom.init();
    ab_target.insert(AddrBloom.addrToBloomKey(target_addr));
    var ab_other = AddrBloom.init();
    ab_other.insert(AddrBloom.addrToBloomKey(other_addr));
    const addr_blooms = [_][bloom_mod.ADDR_BLOOM_SIZE]u8{ ab_target.bits, ab_other.bits, ab_target.bits };

    const block_numbers = [_]u64{ 100, 101, 102 };
    const blooms_buf = try flat_reader.buildTestBlooms(&block_numbers, &addr_blooms, alloc);
    defer alloc.free(blooms_buf);

    // Minimal index header (no blocks needed for bloom scan)
    var idx: [flat_reader.INDEX_HEADER_SIZE]u8 align(page_align) = undefined;
    std.mem.writeInt(u64, idx[0..8], 100, .little);
    std.mem.writeInt(u64, idx[8..16], 0, .little);

    const reader = flat_reader.testReader(&idx, blooms_buf, undefined);

    var matching = std.ArrayListUnmanaged(u64){};
    defer matching.deinit(alloc);
    var scanned: u64 = 0;

    scanBlooms(&reader, target_addr, 0, 200, &matching, &scanned, alloc);

    try std.testing.expectEqual(@as(u64, 3), scanned);
    try std.testing.expectEqual(@as(usize, 2), matching.items.len);
    try std.testing.expectEqual(@as(u64, 100), matching.items[0]);
    try std.testing.expectEqual(@as(u64, 102), matching.items[1]);
}
