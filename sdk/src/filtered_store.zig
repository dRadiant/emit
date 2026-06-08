/// Owns a (data, index) pair of flat files for one phase of the filtered
/// index. Mirrors the engine's `blocks.dat` + `blocks.idx` pattern.
///
/// Layout per pair (base = "primary" or "children", etc.):
///   <base>.dat    magic "EMITFDAT" + LZ4-compressed log entries appended sequentially
///   <base>.idx    magic "EMITFIDX" + dense [(block_number u64 BE, timestamp u32 LE, offset u64 LE, length u32 LE)]
///
/// Authoritative entry count is `(idx_size - MAGIC_SIZE) / ENTRY_SIZE`.
/// A crashed append may leave the dat file longer than the idx claims. The
/// next append seeks past the prior offset+length boundary and overwrites
/// orphan bytes. A partial trailing idx entry is truncated on open, its dat
/// bytes become orphan and get overwritten the same way.
///
/// No durability requirement (rebuildable from the engine's flat store), so
/// the open path may raise `error.IndexInconsistent` for any inconsistency.
/// Caller deletes both files and re-runs the build.
const std = @import("std");

const core = @import("core");

pub const ENTRY_SIZE: usize = 8 + 4 + 8 + 4; // block(BE) + timestamp(LE) + offset(LE) + length(LE)
const MAGIC_DAT: core.flat_format.Magic = "EMITFDAT".*;
const MAGIC_IDX: core.flat_format.Magic = "EMITFIDX".*;
const HEADER_SIZE: usize = core.flat_format.MAGIC_SIZE;

pub const Error = error{ OutOfOrder, IndexInconsistent, Truncated, BufferTooSmall };

pub const IndexEntry = struct {
    block_number: u64,
    /// Exact block time (u32 epoch-seconds). 0 = unknown. Local builds leave it
    /// 0 and the scanner falls back to the engine's `timestamps.bin`. The remote
    /// client fills it from the PUSH frame so an off-engine store is self-timed.
    timestamp: u32,
    offset: u64,
    length: u32,
};

pub const FilteredStore = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    dat_file: std.fs.File,
    idx_file: std.fs.File,
    /// Byte length of the dat file. Tracks where the next append goes.
    dat_size: u64,
    /// Number of (block, offset, length) entries in the idx file.
    entry_count: u64,

    /// Open or create the `<base>.dat`/`<base>.idx` pair under `dir`.
    /// A trailing partial idx entry is truncated. A last idx entry whose
    /// `offset + length` exceeds the dat file's size is dropped.
    pub fn open(
        allocator: std.mem.Allocator,
        dir: std.fs.Dir,
        comptime base: []const u8,
    ) !Self {
        const dat_name = base ++ ".dat";
        const idx_name = base ++ ".idx";

        var dat_file = try core.flat_format.openOrCreateWithMagic(dir, dat_name, MAGIC_DAT);
        errdefer dat_file.close();
        var idx_file = try core.flat_format.openOrCreateWithMagic(dir, idx_name, MAGIC_IDX);
        errdefer idx_file.close();

        const dat_size = try dat_file.getEndPos();
        const idx_size = try idx_file.getEndPos();
        const idx_body_size = idx_size - HEADER_SIZE;

        var entry_count = idx_body_size / ENTRY_SIZE;
        if (idx_body_size % ENTRY_SIZE != 0) {
            try idx_file.setEndPos(HEADER_SIZE + entry_count * ENTRY_SIZE);
        }

        if (entry_count > 0) {
            const last = try readEntryAt(idx_file, entry_count - 1);
            if (last.offset + last.length > dat_size) {
                // Last entry references bytes past the dat tail. Drop it.
                // The next append overwrites any remaining dat orphan bytes.
                entry_count -= 1;
                try idx_file.setEndPos(HEADER_SIZE + entry_count * ENTRY_SIZE);
            }
        }

        return .{
            .allocator = allocator,
            .dat_file = dat_file,
            .idx_file = idx_file,
            .dat_size = if (entry_count == 0)
                @as(u64, HEADER_SIZE) // dat magic occupies header bytes, appends start after
            else blk: {
                const last = try readEntryAt(idx_file, entry_count - 1);
                break :blk last.offset + last.length;
            },
            .entry_count = entry_count,
        };
    }

    pub fn deinit(self: *Self) void {
        self.dat_file.close();
        self.idx_file.close();
    }

    /// Append `(block_number, timestamp, lz4_entry)`. Block numbers MUST be
    /// strictly increasing (mirrors the engine's flat-store monotonic
    /// invariant). `timestamp` is the exact block time, or 0 when unknown.
    /// Local builds pass 0 and the scanner falls back to `timestamps.bin`. The
    /// remote client passes the PUSH timestamp so the store is self-timed.
    pub fn appendEntry(
        self: *Self,
        block_number: u64,
        timestamp: u32,
        lz4_entry: []const u8,
    ) !void {
        if (self.entry_count > 0) {
            const last = try readEntryAt(self.idx_file, self.entry_count - 1);
            if (block_number <= last.block_number) return error.OutOfOrder;
        }

        try self.dat_file.pwriteAll(lz4_entry, self.dat_size);

        var entry_bytes: [ENTRY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, entry_bytes[0..8], block_number, .big);
        std.mem.writeInt(u32, entry_bytes[8..12], timestamp, .little);
        std.mem.writeInt(u64, entry_bytes[12..20], self.dat_size, .little);
        std.mem.writeInt(u32, entry_bytes[20..24], @intCast(lz4_entry.len), .little);
        const idx_offset = HEADER_SIZE + self.entry_count * ENTRY_SIZE;
        try self.idx_file.pwriteAll(&entry_bytes, idx_offset);

        self.dat_size += lz4_entry.len;
        self.entry_count += 1;
    }

    pub fn syncAll(self: *Self) !void {
        try self.dat_file.sync();
        try self.idx_file.sync();
    }

    pub fn count(self: *const Self) u64 {
        return self.entry_count;
    }

    /// Read the i-th index entry. O(1).
    pub fn readEntry(self: *const Self, i: u64) !IndexEntry {
        return readEntryAt(self.idx_file, i);
    }

    /// Read the i-th entry's dat payload into `buf`. Returns the filled slice.
    pub fn readPayload(self: *const Self, i: u64, buf: []u8) ![]const u8 {
        return self.readPayloadFor(try self.readEntry(i), buf);
    }

    /// Read the payload for an already-known index entry. Skips the
    /// idx-file lookup. Use when peek + consume share the same entry.
    pub fn readPayloadFor(self: *const Self, entry: IndexEntry, buf: []u8) ![]const u8 {
        if (buf.len < entry.length) return error.BufferTooSmall;
        if ((try self.dat_file.pread(buf[0..entry.length], entry.offset)) != entry.length) return error.Truncated;
        return buf[0..entry.length];
    }

    /// Find the smallest index `i` with `entry(i).block_number > start_block`.
    /// `start_block == 0` returns 0 (scan from the oldest entry).
    pub fn seekPast(self: *const Self, start_block: u64) !u64 {
        if (self.entry_count == 0 or start_block == 0) return 0;
        var lo: u64 = 0;
        var hi: u64 = self.entry_count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const e = try self.readEntry(mid);
            if (e.block_number > start_block) hi = mid else lo = mid + 1;
        }
        return lo;
    }
};

fn readEntryAt(idx_file: std.fs.File, i: u64) !IndexEntry {
    var buf: [ENTRY_SIZE]u8 = undefined;
    const offset = HEADER_SIZE + i * ENTRY_SIZE;
    if ((try idx_file.pread(&buf, offset)) != ENTRY_SIZE) return error.Truncated;
    return .{
        .block_number = std.mem.readInt(u64, buf[0..8], .big),
        .timestamp = std.mem.readInt(u32, buf[8..12], .little),
        .offset = std.mem.readInt(u64, buf[12..20], .little),
        .length = std.mem.readInt(u32, buf[20..24], .little),
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

test "open on a fresh dir creates both files with magic headers" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var s = try FilteredStore.open(testing.allocator, tmp.dir, "primary");
    defer s.deinit();
    try testing.expectEqual(@as(u64, 0), s.count());
}

test "append then read round-trips entries" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var s = try FilteredStore.open(testing.allocator, tmp.dir, "primary");
    defer s.deinit();

    const payload_a = [_]u8{ 0xAA, 0xBB, 0xCC };
    const payload_b = [_]u8{ 0x11, 0x22, 0x33, 0x44 };
    try s.appendEntry(100, 0, &payload_a);
    try s.appendEntry(101, 1_700_000_000, &payload_b);

    try testing.expectEqual(@as(u64, 2), s.count());

    var read_buf: [16]u8 = undefined;
    try testing.expectEqualSlices(u8, &payload_a, try s.readPayload(0, &read_buf));
    try testing.expectEqualSlices(u8, &payload_b, try s.readPayload(1, &read_buf));

    const e0 = try s.readEntry(0);
    try testing.expectEqual(@as(u64, 100), e0.block_number);
    try testing.expectEqual(@as(u32, 0), e0.timestamp);
    try testing.expectEqual(@as(u32, payload_a.len), e0.length);

    const e1 = try s.readEntry(1);
    try testing.expectEqual(@as(u32, 1_700_000_000), e1.timestamp);
}

test "out-of-order append is rejected" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var s = try FilteredStore.open(testing.allocator, tmp.dir, "primary");
    defer s.deinit();

    const payload = [_]u8{0xAA};
    try s.appendEntry(100, 0, &payload);
    try testing.expectError(error.OutOfOrder, s.appendEntry(100, 0, &payload));
    try testing.expectError(error.OutOfOrder, s.appendEntry(99, 0, &payload));
}

test "seekPast finds the first entry past a cursor" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var s = try FilteredStore.open(testing.allocator, tmp.dir, "primary");
    defer s.deinit();

    const p = [_]u8{0};
    for ([_]u64{ 100, 200, 300, 400 }) |bn| try s.appendEntry(bn, 0, &p);

    try testing.expectEqual(@as(u64, 0), try s.seekPast(0));
    try testing.expectEqual(@as(u64, 1), try s.seekPast(100));
    try testing.expectEqual(@as(u64, 2), try s.seekPast(200));
    try testing.expectEqual(@as(u64, 2), try s.seekPast(250));
    try testing.expectEqual(@as(u64, 4), try s.seekPast(400));
    try testing.expectEqual(@as(u64, 4), try s.seekPast(999));
}

test "persistence across close and re-open" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const payload = [_]u8{ 0xDE, 0xAD };
    {
        var s = try FilteredStore.open(testing.allocator, tmp.dir, "primary");
        defer s.deinit();
        try s.appendEntry(42, 0, &payload);
        try s.syncAll();
    }

    var s = try FilteredStore.open(testing.allocator, tmp.dir, "primary");
    defer s.deinit();
    try testing.expectEqual(@as(u64, 1), s.count());
    var buf: [4]u8 = undefined;
    try testing.expectEqualSlices(u8, &payload, try s.readPayload(0, &buf));
}

test "trailing partial idx entry is truncated on open" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const payload = [_]u8{ 1, 2, 3 };
    {
        var s = try FilteredStore.open(testing.allocator, tmp.dir, "primary");
        defer s.deinit();
        try s.appendEntry(10, 0, &payload);
    }

    // Tack on a partial idx entry (fewer than 20 bytes).
    {
        const f = try tmp.dir.openFile("primary.idx", .{ .mode = .read_write });
        defer f.close();
        const end = try f.getEndPos();
        try f.pwriteAll(&[_]u8{ 0xFF, 0xFF, 0xFF }, end);
    }

    var s = try FilteredStore.open(testing.allocator, tmp.dir, "primary");
    defer s.deinit();
    try testing.expectEqual(@as(u64, 1), s.count());

    // Next append works at the right offset.
    try s.appendEntry(11, 0, &payload);
    try testing.expectEqual(@as(u64, 2), s.count());
}

test "last idx entry referencing past-dat-end is dropped on open" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const payload = [_]u8{ 1, 2 };
    {
        var s = try FilteredStore.open(testing.allocator, tmp.dir, "primary");
        defer s.deinit();
        try s.appendEntry(10, 0, &payload);
    }

    // Forge an idx entry whose offset+length exceeds the dat file.
    {
        const idx = try tmp.dir.openFile("primary.idx", .{ .mode = .read_write });
        defer idx.close();
        var forged: [ENTRY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, forged[0..8], 11, .big); // block_number
        std.mem.writeInt(u32, forged[8..12], 0, .little); // timestamp
        std.mem.writeInt(u64, forged[12..20], 9999, .little); // offset past dat end
        std.mem.writeInt(u32, forged[20..24], 100, .little); // length
        const end = try idx.getEndPos();
        try idx.pwriteAll(&forged, end);
    }

    var s = try FilteredStore.open(testing.allocator, tmp.dir, "primary");
    defer s.deinit();
    // Forged entry dropped. Only the real entry remains.
    try testing.expectEqual(@as(u64, 1), s.count());
}

test "bad magic on dat surfaces InvalidMagic" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("primary.dat", .{});
        defer f.close();
        try f.writeAll(&[_]u8{0xFF} ** 16);
    }

    try testing.expectError(error.InvalidMagic, FilteredStore.open(testing.allocator, tmp.dir, "primary"));
}
