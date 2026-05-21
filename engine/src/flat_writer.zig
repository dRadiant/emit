/// Append-only writer for the flat log store (blocks.dat + blocks.idx + blooms.bin + meta.bin).
/// Single-threaded — the import pipeline feeds it from one writer thread.
/// Counterpart to core's FlatStoreReader.
const std = @import("std");

const core = @import("core");

const bloom = core.bloom;
const flat_reader = core.flat_reader;
const Meta = flat_reader.Meta;

pub const FlatStoreWriter = struct {
    blocks_file: std.fs.File,
    index_file: std.fs.File,
    blooms_file: std.fs.File,
    dir: std.fs.Dir,

    meta: Meta,
    first_block: u64,
    commit_interval: usize,
    blocks_since_commit: usize,

    pub fn open(dir_path: []const u8) !FlatStoreWriter {
        var dir = try std.fs.cwd().openDir(dir_path, .{});

        const blocks_file = try dir.createFile("blocks.dat", .{ .truncate = false, .read = true });
        const index_file = try dir.createFile("blocks.idx", .{ .truncate = false, .read = true });
        const blooms_file = try dir.createFile("blooms.bin", .{ .truncate = false, .read = true });

        var meta = Meta{
            .last_finalized_block = 0,
            .blocks_dat_size = 0,
            .blocks_idx_count = 0,
            .blooms_count = 0,
            .checksum = 0,
        };
        var first_block: u64 = 0;

        // Restore from existing meta if present
        if (dir.openFile("meta.bin", .{})) |meta_file| {
            defer meta_file.close();
            var buf: [flat_reader.META_SIZE]u8 = undefined;
            const n = meta_file.pread(&buf, 0) catch 0;
            if (n == flat_reader.META_SIZE) {
                if (Meta.deserialize(&buf)) |m| meta = m;
            }
        } else |_| {}

        // Read first_block from index header, or pre-initialize empty
        // headers so a `FlatStoreReader` can open this dir before the
        // first block is finalized. Without this the SDK's `--follow`
        // path fails with `error.InvalidIndex` when an engine has only
        // been writing pending.bin (no finalizations yet, ~13 min cold
        // start at 12 s/block × FINALITY_DEPTH = 64).
        {
            var hdr: [flat_reader.INDEX_HEADER_SIZE]u8 = undefined;
            const n = index_file.pread(&hdr, 0) catch 0;
            if (n == flat_reader.INDEX_HEADER_SIZE) {
                first_block = std.mem.readInt(u64, hdr[0..8], .little);
            } else if (n == 0) {
                // Zero-init header: first_block placeholder=0, count=0.
                // `appendBlock` rewrites the header on the first append.
                @memset(&hdr, 0);
                _ = try index_file.pwrite(&hdr, 0);
            }
        }
        {
            const blooms_size = (try blooms_file.stat()).size;
            if (blooms_size == 0) {
                var bhdr: [flat_reader.BLOOM_HEADER_SIZE]u8 = std.mem.zeroes([flat_reader.BLOOM_HEADER_SIZE]u8);
                _ = try blooms_file.pwrite(&bhdr, 0);
            }
        }

        return .{
            .blocks_file = blocks_file,
            .index_file = index_file,
            .blooms_file = blooms_file,
            .dir = dir,
            .meta = meta,
            .first_block = first_block,
            .commit_interval = core.types.COMMIT_INTERVAL,
            .blocks_since_commit = 0,
        };
    }

    /// Append a block's compressed log data + bloom filters.
    pub fn appendBlock(
        self: *FlatStoreWriter,
        block_number: u64,
        lz4_entry: []const u8,
        topic_bloom: *const [bloom.BLOOM_SIZE]u8,
        addr_bloom: *const [bloom.ADDR_BLOOM_SIZE]u8,
    ) !void {
        // Write index header on first block
        if (self.meta.blocks_idx_count == 0 and self.first_block == 0) {
            self.first_block = block_number;
            var hdr: [flat_reader.INDEX_HEADER_SIZE]u8 = undefined;
            std.mem.writeInt(u64, hdr[0..8], self.first_block, .little);
            std.mem.writeInt(u64, hdr[8..16], 0, .little);
            _ = try self.index_file.pwrite(&hdr, 0);
        }

        // Reject below-first-block appends — index uses block_number -
        // first_block as the dense slot; an unsigned underflow here would
        // silently scribble somewhere far past the file end.
        if (block_number < self.first_block) return error.BlockBeforeFirst;

        // Append to blocks.dat
        const offset = self.meta.blocks_dat_size;
        _ = try self.blocks_file.pwrite(lz4_entry, offset);

        // Write dense index entry: block_number maps to (offset, length)
        const idx = block_number - self.first_block;
        var idx_entry: [flat_reader.INDEX_ENTRY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, idx_entry[0..8], offset, .little);
        std.mem.writeInt(u32, idx_entry[8..12], @intCast(lz4_entry.len), .little);
        _ = try self.index_file.pwrite(&idx_entry, flat_reader.INDEX_HEADER_SIZE + idx * flat_reader.INDEX_ENTRY_SIZE);

        // Append bloom entry: [block_number:8 BE][topic_bloom][addr_bloom]
        var bloom_entry: [flat_reader.BLOOM_ENTRY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, bloom_entry[0..8], block_number, .big);
        @memcpy(bloom_entry[flat_reader.TOPIC_BLOOM_OFFSET..][0..bloom.BLOOM_SIZE], topic_bloom);
        @memcpy(bloom_entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE], addr_bloom);
        _ = try self.blooms_file.pwrite(&bloom_entry, flat_reader.BLOOM_HEADER_SIZE + self.meta.blooms_count * flat_reader.BLOOM_ENTRY_SIZE);

        // Update counters
        self.meta.blocks_dat_size += lz4_entry.len;
        if (idx + 1 > self.meta.blocks_idx_count) self.meta.blocks_idx_count = idx + 1;
        self.meta.blooms_count += 1;
        self.meta.last_finalized_block = block_number;

        self.blocks_since_commit += 1;
        if (self.blocks_since_commit >= self.commit_interval) {
            try self.commitMeta();
        }
    }

    /// Flush file headers and persist meta atomically (tmp + rename).
    pub fn commitMeta(self: *FlatStoreWriter) !void {
        // Update index header entry_count
        var count_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &count_buf, self.meta.blocks_idx_count, .little);
        _ = try self.index_file.pwrite(&count_buf, 8);

        // Update blooms header entry_count
        var bloom_count_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &bloom_count_buf, self.meta.blooms_count, .little);
        _ = try self.blooms_file.pwrite(&bloom_count_buf, 0);

        // Flush data files so meta.bin doesn't durably reference bytes that
        // are still in the kernel page cache. Without this a power-loss
        // window between the data writes and the meta rename can leave
        // meta pointing at non-existent offsets.
        try self.blocks_file.sync();
        try self.index_file.sync();
        try self.blooms_file.sync();

        self.meta.checksum = self.meta.computeChecksum();
        var meta_buf: [flat_reader.META_SIZE]u8 = undefined;
        self.meta.serialize(&meta_buf);
        try core.atomic_file.write(self.dir, "meta.bin.tmp", "meta.bin", &meta_buf);

        self.blocks_since_commit = 0;
    }

    pub fn finalize(self: *FlatStoreWriter) !void {
        try self.commitMeta();
    }

    pub fn close(self: *FlatStoreWriter) void {
        self.blocks_file.close();
        self.index_file.close();
        self.blooms_file.close();
        self.dir.close();
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

test "write blocks then read back via core reader" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var writer = openFromDir(tmp.dir);
        defer closeFilesOnly(&writer);

        const entry1 = [_]u8{ 3, 0, 0, 0, 0xAA, 0xBB, 0xCC };
        const entry2 = [_]u8{ 2, 0, 0, 0, 0xDD, 0xEE };
        var topic1 = [_]u8{0x11} ** bloom.BLOOM_SIZE;
        var addr1 = [_]u8{0x22} ** bloom.ADDR_BLOOM_SIZE;
        var topic2 = [_]u8{0x33} ** bloom.BLOOM_SIZE;
        var addr2 = [_]u8{0x44} ** bloom.ADDR_BLOOM_SIZE;

        try writer.appendBlock(100, &entry1, &topic1, &addr1);
        try writer.appendBlock(101, &entry2, &topic2, &addr2);
        try writer.finalize();
    }

    var reader = openReaderFromDir(tmp.dir);
    defer reader.close();

    try std.testing.expectEqual(@as(u64, 100), reader.first_block);
    try std.testing.expectEqual(@as(u64, 2), reader.index_count);
    try std.testing.expectEqual(@as(u64, 2), reader.blooms_count);

    var buf: [1024]u8 = undefined;
    const data1 = try reader.readBlock(100, &buf);
    try std.testing.expectEqual(@as(usize, 7), data1.len);
    try std.testing.expectEqual(@as(u8, 0xAA), data1[4]);

    const data2 = try reader.readBlock(101, &buf);
    try std.testing.expectEqual(@as(usize, 6), data2.len);
    try std.testing.expectEqual(@as(u8, 0xDD), data2[4]);
}

test "appendBlock rejects block_number below first_block" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var writer = openFromDir(tmp.dir);
    defer closeFilesOnly(&writer);

    const entry = [_]u8{ 1, 0, 0, 0, 0x42 };
    var topic = [_]u8{0} ** bloom.BLOOM_SIZE;
    var addr = [_]u8{0} ** bloom.ADDR_BLOOM_SIZE;

    // First write sets first_block = 100.
    try writer.appendBlock(100, &entry, &topic, &addr);
    // Below first_block: would underflow `block_number - first_block` and
    // scribble somewhere far past the file end without the guard.
    try std.testing.expectError(error.BlockBeforeFirst, writer.appendBlock(99, &entry, &topic, &addr));
}

test "meta persists across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var writer = openFromDir(tmp.dir);
        defer closeFilesOnly(&writer);
        const entry = [_]u8{ 1, 0, 0, 0, 0x42 };
        var topic = [_]u8{0} ** bloom.BLOOM_SIZE;
        var addr = [_]u8{0} ** bloom.ADDR_BLOOM_SIZE;
        try writer.appendBlock(50, &entry, &topic, &addr);
        try writer.appendBlock(51, &entry, &topic, &addr);
        try writer.finalize();
    }

    var writer = openFromDir(tmp.dir);
    defer closeFilesOnly(&writer);
    try std.testing.expectEqual(@as(u64, 51), writer.meta.last_finalized_block);
    try std.testing.expectEqual(@as(u64, 10), writer.meta.blocks_dat_size);
    try std.testing.expectEqual(@as(u64, 2), writer.meta.blocks_idx_count);
    try std.testing.expectEqual(@as(u64, 2), writer.meta.blooms_count);
}

test "resume appends after reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const entry = [_]u8{ 1, 0, 0, 0, 0x42 };
    var topic = [_]u8{0} ** bloom.BLOOM_SIZE;
    var addr = [_]u8{0} ** bloom.ADDR_BLOOM_SIZE;

    {
        var writer = openFromDir(tmp.dir);
        defer closeFilesOnly(&writer);
        try writer.appendBlock(100, &entry, &topic, &addr);
        try writer.finalize();
    }

    {
        var writer = openFromDir(tmp.dir);
        defer closeFilesOnly(&writer);
        try writer.appendBlock(101, &entry, &topic, &addr);
        try writer.finalize();
    }

    var reader = openReaderFromDir(tmp.dir);
    defer reader.close();
    try std.testing.expectEqual(@as(u64, 2), reader.index_count);
    var buf: [1024]u8 = undefined;
    _ = try reader.readBlock(100, &buf);
    _ = try reader.readBlock(101, &buf);
}

test "bloom scan finds written blocks" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const target_addr = [_]u8{0xAE} ** 20;
    const other_addr = [_]u8{0xFF} ** 20;
    const entry = [_]u8{ 0, 0, 0, 0 };
    var topic = [_]u8{0} ** bloom.BLOOM_SIZE;

    {
        var writer = openFromDir(tmp.dir);
        defer closeFilesOnly(&writer);

        var ab1 = core.AddrBloom.init();
        ab1.insert(core.AddrBloom.addrToBloomKey(target_addr));
        try writer.appendBlock(100, &entry, &topic, &ab1.bits);

        var ab2 = core.AddrBloom.init();
        ab2.insert(core.AddrBloom.addrToBloomKey(other_addr));
        try writer.appendBlock(101, &entry, &topic, &ab2.bits);

        var ab3 = core.AddrBloom.init();
        ab3.insert(core.AddrBloom.addrToBloomKey(target_addr));
        try writer.appendBlock(102, &entry, &topic, &ab3.bits);

        try writer.finalize();
    }

    var reader = openReaderFromDir(tmp.dir);
    defer reader.close();

    var matching = std.ArrayListUnmanaged(u64){};
    defer matching.deinit(std.testing.allocator);
    var scanned: u64 = 0;

    const targets = [_][20]u8{target_addr};
    try core.block_filter.scanBlooms(&reader, &targets, &.{}, 0, 200, &matching, &scanned, std.testing.allocator);

    try std.testing.expectEqual(@as(u64, 3), scanned);
    try std.testing.expectEqual(@as(usize, 2), matching.items.len);
    try std.testing.expectEqual(@as(u64, 100), matching.items[0]);
    try std.testing.expectEqual(@as(u64, 102), matching.items[1]);
}

// ── Test helpers ─────────────────────────────────────────────────────────

/// Open a FlatStoreWriter against a dir handle. Does NOT own the dir —
/// use closeFilesOnly() instead of close() to avoid invalidating the handle.
fn openFromDir(dir: std.fs.Dir) FlatStoreWriter {
    const blocks_file = dir.createFile("blocks.dat", .{ .truncate = false, .read = true }) catch unreachable;
    const index_file = dir.createFile("blocks.idx", .{ .truncate = false, .read = true }) catch unreachable;
    const blooms_file = dir.createFile("blooms.bin", .{ .truncate = false, .read = true }) catch unreachable;

    var meta = Meta{
        .last_finalized_block = 0,
        .blocks_dat_size = 0,
        .blocks_idx_count = 0,
        .blooms_count = 0,
        .checksum = 0,
    };
    var first_block: u64 = 0;

    if (dir.openFile("meta.bin", .{})) |meta_file| {
        defer meta_file.close();
        var buf: [flat_reader.META_SIZE]u8 = undefined;
        const n = meta_file.pread(&buf, 0) catch 0;
        if (n == flat_reader.META_SIZE) {
            if (Meta.deserialize(&buf)) |m| meta = m;
        }
    } else |_| {}

    {
        var hdr: [flat_reader.INDEX_HEADER_SIZE]u8 = undefined;
        const n = index_file.pread(&hdr, 0) catch 0;
        if (n == flat_reader.INDEX_HEADER_SIZE) {
            first_block = std.mem.readInt(u64, hdr[0..8], .little);
        }
    }

    return .{
        .blocks_file = blocks_file,
        .index_file = index_file,
        .blooms_file = blooms_file,
        .dir = dir,
        .meta = meta,
        .first_block = first_block,
        .commit_interval = core.types.COMMIT_INTERVAL,
        .blocks_since_commit = 0,
    };
}

/// Close file handles without closing the dir (owned by tmpDir).
fn closeFilesOnly(writer: *FlatStoreWriter) void {
    writer.blocks_file.close();
    writer.index_file.close();
    writer.blooms_file.close();
}

/// Open a core FlatStoreReader against a dir handle.
fn openReaderFromDir(dir: std.fs.Dir) core.FlatStoreReader {
    const blocks_file = dir.openFile("blocks.dat", .{}) catch unreachable;

    const idx_file = dir.openFile("blocks.idx", .{}) catch unreachable;
    defer idx_file.close();
    const idx_size = (idx_file.stat() catch unreachable).size;
    const index_map = std.posix.mmap(
        null, idx_size, std.posix.PROT.READ,
        .{ .TYPE = .SHARED }, idx_file.handle, 0,
    ) catch unreachable;

    const blooms_file = dir.openFile("blooms.bin", .{}) catch unreachable;
    defer blooms_file.close();
    const blooms_size = (blooms_file.stat() catch unreachable).size;
    const blooms_map = std.posix.mmap(
        null, blooms_size, std.posix.PROT.READ,
        .{ .TYPE = .SHARED }, blooms_file.handle, 0,
    ) catch unreachable;

    return .{
        .blocks_file = blocks_file,
        .index_map = index_map,
        .blooms_map = blooms_map,
        .first_block = std.mem.readInt(u64, index_map[0..8], .little),
        .index_count = std.mem.readInt(u64, index_map[8..16], .little),
        .blooms_count = std.mem.readInt(u64, blooms_map[0..8], .little),
    };
}
