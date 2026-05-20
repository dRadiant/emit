/// Read-only access to the flat log store (blocks.dat + blocks.idx + blooms.bin).
/// Thread-safe — multiple readers can operate concurrently via pread + mmap.
/// Writer lives in engine; this module is read-only infrastructure shared by
/// engine (status/validation) and sdk (filtered index builds).
const std = @import("std");

const bloom = @import("bloom.zig");

// ── Flat store format constants ──────────────────────────────────────────

pub const INDEX_ENTRY_SIZE = 12; // offset:u64 LE + length:u32 LE
pub const INDEX_HEADER_SIZE = 16; // first_block:u64 LE + entry_count:u64 LE

/// blooms.bin entry: block_number(8 BE) + topic_bloom(256) + addr_bloom(1024)
pub const BLOOM_ENTRY_SIZE = 8 + bloom.BLOOM_SIZE + bloom.ADDR_BLOOM_SIZE;
pub const TOPIC_BLOOM_OFFSET = 8;
pub const ADDR_BLOOM_OFFSET = 8 + bloom.BLOOM_SIZE;
pub const BLOOM_HEADER_SIZE = 8; // entry_count:u64 LE

pub const META_SIZE = 40;

// ── Meta ─────────────────────────────────────────────────────────────────

/// Flat store checkpoint persisted to meta.bin via atomic write (tmp + rename).
/// Enables crash recovery: on restart, resume from last_finalized_block + 1.
/// Checksum is XOR of all fields with a magic constant — detects partial writes.
pub const Meta = struct {
    last_finalized_block: u64,
    blocks_dat_size: u64,
    blocks_idx_count: u64,
    blooms_count: u64,
    checksum: u64,

    pub fn computeChecksum(self: Meta) u64 {
        return self.last_finalized_block ^ self.blocks_dat_size ^
            self.blocks_idx_count ^ self.blooms_count ^ 0xDEADBEEF_CAFEBABE;
    }

    pub fn serialize(self: Meta, buf: *[META_SIZE]u8) void {
        std.mem.writeInt(u64, buf[0..8], self.last_finalized_block, .little);
        std.mem.writeInt(u64, buf[8..16], self.blocks_dat_size, .little);
        std.mem.writeInt(u64, buf[16..24], self.blocks_idx_count, .little);
        std.mem.writeInt(u64, buf[24..32], self.blooms_count, .little);
        std.mem.writeInt(u64, buf[32..40], self.computeChecksum(), .little);
    }

    pub fn deserialize(buf: *const [META_SIZE]u8) ?Meta {
        const m = Meta{
            .last_finalized_block = std.mem.readInt(u64, buf[0..8], .little),
            .blocks_dat_size = std.mem.readInt(u64, buf[8..16], .little),
            .blocks_idx_count = std.mem.readInt(u64, buf[16..24], .little),
            .blooms_count = std.mem.readInt(u64, buf[24..32], .little),
            .checksum = std.mem.readInt(u64, buf[32..40], .little),
        };
        if (m.checksum != m.computeChecksum()) return null;
        return m;
    }
};

// ── FlatStoreReader ──────────────────────────────────────────────────────

const MmapSlice = []align(std.heap.page_size_min) const u8;

/// Position and size of a block's LZ4 entry within blocks.dat.
pub const BlockLoc = struct { offset: u64, length: u32 };

/// Read-only handle to the flat log store. blocks.idx and blooms.bin are
/// mmap'd for O(1) lookups; blocks.dat is read via pread (or io_uring in
/// block_filter). All fields are pub for in-memory test construction.
pub const FlatStoreReader = struct {
    blocks_file: std.fs.File, // pread target for block data
    index_map: MmapSlice, // mmap'd blocks.idx: header(16) + dense [offset,length] array
    blooms_map: MmapSlice, // mmap'd blooms.bin: header(8) + flat bloom entries
    first_block: u64, // block number of index entry 0
    index_count: u64, // number of entries in blocks.idx
    blooms_count: u64, // number of entries in blooms.bin

    pub fn open(dir_path: []const u8) !FlatStoreReader {
        var dir = try std.fs.cwd().openDir(dir_path, .{});
        defer dir.close();

        const blocks_file = try dir.openFile("blocks.dat", .{});

        // mmap blocks.idx
        const idx_file = try dir.openFile("blocks.idx", .{});
        defer idx_file.close();
        const idx_size = (try idx_file.stat()).size;
        if (idx_size < INDEX_HEADER_SIZE) return error.InvalidIndex;

        const index_map = try std.posix.mmap(
            null, idx_size, std.posix.PROT.READ,
            .{ .TYPE = .SHARED }, idx_file.handle, 0,
        );

        const first_block = std.mem.readInt(u64, index_map[0..8], .little);
        const index_count = std.mem.readInt(u64, index_map[8..16], .little);

        // mmap blooms.bin
        const blooms_file = try dir.openFile("blooms.bin", .{});
        defer blooms_file.close();
        const blooms_size = (try blooms_file.stat()).size;
        if (blooms_size < BLOOM_HEADER_SIZE) return error.InvalidBlooms;

        const blooms_map = try std.posix.mmap(
            null, blooms_size, std.posix.PROT.READ,
            .{ .TYPE = .SHARED }, blooms_file.handle, 0,
        );

        const blooms_count = std.mem.readInt(u64, blooms_map[0..8], .little);

        return .{
            .blocks_file = blocks_file,
            .index_map = index_map,
            .blooms_map = blooms_map,
            .first_block = first_block,
            .index_count = index_count,
            .blooms_count = blooms_count,
        };
    }

    /// Get (offset, length) for a block in blocks.dat. Used by io_uring pipeline.
    pub fn getBlockLoc(self: *const FlatStoreReader, block_number: u64) !BlockLoc {
        if (block_number < self.first_block) return error.BlockNotFound;
        const idx = block_number - self.first_block;
        if (idx >= self.index_count) return error.BlockNotFound;
        const entry_offset = INDEX_HEADER_SIZE + idx * INDEX_ENTRY_SIZE;
        if (entry_offset + INDEX_ENTRY_SIZE > self.index_map.len) return error.BlockNotFound;
        const entry = self.index_map[entry_offset..][0..INDEX_ENTRY_SIZE];
        return .{
            .offset = std.mem.readInt(u64, entry[0..8], .little),
            .length = std.mem.readInt(u32, entry[8..12], .little),
        };
    }

    /// Read a block's LZ4 entry from blocks.dat into buf.
    pub fn readBlock(self: *const FlatStoreReader, block_number: u64, buf: []u8) ![]const u8 {
        const loc = try self.getBlockLoc(block_number);
        if (loc.length == 0) return error.EmptyBlock;
        if (loc.length > buf.len) return error.BufferTooSmall;

        const n = try self.blocks_file.pread(buf[0..loc.length], loc.offset);
        if (n != loc.length) return error.ShortRead;

        return buf[0..loc.length];
    }

    /// Read meta.bin from the same directory. Returns null on missing/corrupt meta.
    pub fn readMeta(dir_path: []const u8) ?Meta {
        var dir = std.fs.cwd().openDir(dir_path, .{}) catch return null;
        defer dir.close();
        const file = dir.openFile("meta.bin", .{}) catch return null;
        defer file.close();
        var buf: [META_SIZE]u8 = undefined;
        const n = file.pread(&buf, 0) catch return null;
        if (n != META_SIZE) return null;
        return Meta.deserialize(&buf);
    }

    /// Binary search for the first bloom entry with block_number >= target.
    /// Bloom entries are sorted ascending by block number (big-endian).
    /// Skips ~33% of entries when start_block > 0. O(log N).
    pub fn findBloomStart(self: *const FlatStoreReader, target_block: u64) usize {
        const base = self.blooms_map[BLOOM_HEADER_SIZE..];
        var lo: usize = 0;
        var hi: usize = self.blooms_count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const offset = mid * BLOOM_ENTRY_SIZE;
            if (offset + BLOOM_ENTRY_SIZE > base.len) {
                hi = mid;
                continue;
            }
            const block_number = std.mem.readInt(u64, base[offset..][0..8], .big);
            if (block_number < target_block) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    pub fn close(self: *FlatStoreReader) void {
        self.blocks_file.close();
        std.posix.munmap(self.index_map);
        std.posix.munmap(self.blooms_map);
    }
};

// ── Test helpers ─────────────────────────────────────────────────────────

const page_align = std.heap.page_size_min;

/// Build an in-memory index buffer (header + entries) for testing.
fn buildTestIndex(comptime N: usize, first_block: u64, entries: [N][2]u64) [INDEX_HEADER_SIZE + N * INDEX_ENTRY_SIZE]u8 {
    var buf: [INDEX_HEADER_SIZE + N * INDEX_ENTRY_SIZE]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], first_block, .little);
    std.mem.writeInt(u64, buf[8..16], N, .little);
    for (entries, 0..) |e, i| {
        const off = INDEX_HEADER_SIZE + i * INDEX_ENTRY_SIZE;
        std.mem.writeInt(u64, buf[off..][0..8], e[0], .little); // offset
        std.mem.writeInt(u32, buf[off + 8 ..][0..4], @intCast(e[1]), .little); // length
    }
    return buf;
}

/// Build an in-memory blooms buffer (header + entries) for testing.
pub fn buildTestBlooms(block_numbers: []const u64, addr_blooms: ?[]const [bloom.ADDR_BLOOM_SIZE]u8, allocator: std.mem.Allocator) ![]align(page_align) u8 {
    const total = BLOOM_HEADER_SIZE + block_numbers.len * BLOOM_ENTRY_SIZE;
    const buf = try allocator.alignedAlloc(u8, .fromByteUnits(page_align), total);
    std.mem.writeInt(u64, buf[0..8], block_numbers.len, .little);
    for (block_numbers, 0..) |bn, i| {
        const off = BLOOM_HEADER_SIZE + i * BLOOM_ENTRY_SIZE;
        var entry: [BLOOM_ENTRY_SIZE]u8 = std.mem.zeroes([BLOOM_ENTRY_SIZE]u8);
        std.mem.writeInt(u64, entry[0..8], bn, .big);
        if (addr_blooms) |abs| {
            @memcpy(entry[ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE], &abs[i]);
        }
        @memcpy(buf[off..][0..BLOOM_ENTRY_SIZE], &entry);
    }
    return buf;
}

/// Construct a FlatStoreReader from in-memory buffers. Does not own the
/// buffers. Caller is responsible for freeing. Do not call close() on this.
pub fn testReader(
    index_buf: []align(page_align) const u8,
    blooms_buf: []align(page_align) const u8,
    blocks_file: std.fs.File,
) FlatStoreReader {
    return .{
        .blocks_file = blocks_file,
        .index_map = index_buf,
        .blooms_map = blooms_buf,
        .first_block = std.mem.readInt(u64, index_buf[0..8], .little),
        .index_count = std.mem.readInt(u64, index_buf[8..16], .little),
        .blooms_count = std.mem.readInt(u64, blooms_buf[0..8], .little),
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

test "Meta serialize/deserialize roundtrip" {
    var m = Meta{
        .last_finalized_block = 42,
        .blocks_dat_size = 1000,
        .blocks_idx_count = 5,
        .blooms_count = 5,
        .checksum = 0,
    };
    m.checksum = m.computeChecksum();
    var buf: [META_SIZE]u8 = undefined;
    m.serialize(&buf);
    const restored = Meta.deserialize(&buf).?;
    try std.testing.expectEqual(m.last_finalized_block, restored.last_finalized_block);
    try std.testing.expectEqual(m.blocks_dat_size, restored.blocks_dat_size);
    try std.testing.expectEqual(m.blocks_idx_count, restored.blocks_idx_count);
    try std.testing.expectEqual(m.blooms_count, restored.blooms_count);
}

test "Meta corrupt checksum returns null" {
    var buf: [META_SIZE]u8 = std.mem.zeroes([META_SIZE]u8);
    try std.testing.expect(Meta.deserialize(&buf) == null);
}

test "getBlockLoc from in-memory index" {
    // Two blocks starting at block 100: offsets 0/7, lengths 7/6
    const idx = buildTestIndex(2, 100, .{ .{ 0, 7 }, .{ 7, 6 } });
    var idx_buf: [idx.len]u8 align(std.heap.page_size_min) = idx;
    const idx_slice: MmapSlice = &idx_buf;

    // Minimal blooms (empty, just a valid header)
    var bhdr: [BLOOM_HEADER_SIZE]u8 align(page_align) = undefined;
    std.mem.writeInt(u64, &bhdr, 0, .little);
    const blooms_slice: []align(page_align) const u8 = &bhdr;

    const reader = testReader(idx_slice, blooms_slice, undefined);

    const loc0 = try reader.getBlockLoc(100);
    try std.testing.expectEqual(@as(u64, 0), loc0.offset);
    try std.testing.expectEqual(@as(u32, 7), loc0.length);

    const loc1 = try reader.getBlockLoc(101);
    try std.testing.expectEqual(@as(u64, 7), loc1.offset);
    try std.testing.expectEqual(@as(u32, 6), loc1.length);

    try std.testing.expectError(error.BlockNotFound, reader.getBlockLoc(999));
    // Below-first-block: would underflow `block_number - first_block` if
    // the guard were missing. Must surface as BlockNotFound, not panic.
    try std.testing.expectError(error.BlockNotFound, reader.getBlockLoc(99));
    try std.testing.expectError(error.BlockNotFound, reader.getBlockLoc(0));
}

test "readBlock via tmpfile" {
    // Write block data to a tmpfile (no directory needed)
    const entry1 = [_]u8{ 3, 0, 0, 0, 0xAA, 0xBB, 0xCC };
    const entry2 = [_]u8{ 2, 0, 0, 0, 0xDD, 0xEE };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const f = try tmp.dir.createFile("b", .{ .read = true });
    defer f.close();
    _ = try f.write(&entry1);
    _ = try f.write(&entry2);

    const idx = buildTestIndex(2, 100, .{ .{ 0, 7 }, .{ 7, 6 } });
    var idx_buf: [idx.len]u8 align(std.heap.page_size_min) = idx;
    const idx_slice: MmapSlice = &idx_buf;

    var bhdr: [BLOOM_HEADER_SIZE]u8 align(page_align) = undefined;
    std.mem.writeInt(u64, &bhdr, 0, .little);
    const blooms_slice: []align(page_align) const u8 = &bhdr;

    const reader = testReader(idx_slice, blooms_slice, f);

    var buf: [1024]u8 = undefined;
    const data1 = try reader.readBlock(100, &buf);
    try std.testing.expectEqual(@as(usize, 7), data1.len);
    try std.testing.expectEqual(@as(u8, 0xAA), data1[4]);

    const data2 = try reader.readBlock(101, &buf);
    try std.testing.expectEqual(@as(usize, 6), data2.len);
    try std.testing.expectEqual(@as(u8, 0xDD), data2[4]);

    try std.testing.expectError(error.BlockNotFound, reader.readBlock(999, &buf));
}

test "findBloomStart binary search" {
    const alloc = std.testing.allocator;
    const block_numbers = [_]u64{ 100, 200, 300 };
    const blooms_buf = try buildTestBlooms(&block_numbers, null, alloc);
    defer alloc.free(blooms_buf);

    var bhdr: [BLOOM_HEADER_SIZE]u8 align(page_align) = undefined;
    std.mem.writeInt(u64, &bhdr, 0, .little);
    var idx: [INDEX_HEADER_SIZE]u8 align(page_align) = undefined;
    std.mem.writeInt(u64, idx[0..8], 100, .little);
    std.mem.writeInt(u64, idx[8..16], 0, .little);

    const reader = testReader(&idx, blooms_buf, undefined);

    try std.testing.expectEqual(@as(usize, 0), reader.findBloomStart(0));
    try std.testing.expectEqual(@as(usize, 0), reader.findBloomStart(100));
    try std.testing.expectEqual(@as(usize, 1), reader.findBloomStart(101));
    try std.testing.expectEqual(@as(usize, 1), reader.findBloomStart(200));
    try std.testing.expectEqual(@as(usize, 2), reader.findBloomStart(201));
    try std.testing.expectEqual(@as(usize, 3), reader.findBloomStart(400));
}
