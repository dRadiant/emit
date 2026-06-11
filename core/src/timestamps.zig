//! Per-block Unix timestamps. Dense `u32 LE` array indexed by
//! `block - first_block`, in `timestamps.bin`.
//!
//! `u32` epoch-seconds exact until 2106, halves the file versus `u64`
//! (~39 MB at the tip). Zero entry means unknown (gap not yet backfilled).
//! Callers fall back to the formula. Partial or absent file is always safe.
//!
//! Layout:
//!   0   8         magic "EMITTIME"
//!   8   8         first_block (u64 LE)
//!   16  8         count (u64 LE)
//!   24  count*4   timestamps (u32 LE)
const std = @import("std");

const flat_format = @import("flat_format.zig");

pub const MAGIC: flat_format.Magic = "EMITTIME".*;
pub const HEADER_SIZE: usize = 24; // magic(8) + first_block(8) + count(8)
pub const ENTRY_SIZE: usize = 4; // u32 LE epoch-seconds
pub const FILE_NAME = "timestamps.bin";

const MmapSlice = []align(std.heap.page_size_min) const u8;

/// Read-only, mmap-backed, O(1) lookup by block number. Thread-safe: the map
/// is immutable for a reader's lifetime. Returns null for any block outside
/// the covered range or whose entry is unknown. Caller keeps the formula
/// fallback, and old stores without the file just work.
pub const TimestampReader = struct {
    map: MmapSlice,
    first_block: u64,
    count: u64,

    /// Open `dir/timestamps.bin`. Returns null when the file is absent or too
    /// short to hold a header. `error.InvalidMagic` and `error.Truncated`
    /// surface a corrupt file loudly.
    pub fn open(dir: std.fs.Dir) !?TimestampReader {
        const file = dir.openFile(FILE_NAME, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer file.close();

        const size = (try file.stat()).size;
        if (size < HEADER_SIZE) return null;

        const map = try std.posix.mmap(null, size, std.posix.PROT.READ, .{ .TYPE = .SHARED }, file.handle, 0);
        errdefer std.posix.munmap(map);

        try flat_format.validateMagic(map, MAGIC);
        const first_block = std.mem.readInt(u64, map[8..16], .little);
        const count = std.mem.readInt(u64, map[16..24], .little);
        if (HEADER_SIZE + count * ENTRY_SIZE > size) return error.Truncated;

        return .{ .map = map, .first_block = first_block, .count = count };
    }

    pub fn deinit(self: *TimestampReader) void {
        std.posix.munmap(self.map);
    }

    /// Exact Unix timestamp for `block`, or null when out of range or unknown
    /// (zero entry). Caller falls back to the derivation formula.
    pub fn get(self: *const TimestampReader, block: u64) ?u64 {
        if (block < self.first_block) return null;
        const idx = block - self.first_block;
        if (idx >= self.count) return null;
        const off = HEADER_SIZE + idx * ENTRY_SIZE;
        const ts = std.mem.readInt(u32, self.map[off..][0..4], .little);
        if (ts == 0) return null;
        return ts;
    }
};

/// Writer for timestamps.bin. Decoupled from `FlatStoreWriter` so it can run as
/// a standalone pass without touching `appendBlock` or the receipts pipeline.
/// Fed by the importer's concurrent pass over Nethermind's `headers` DB
/// (historical) and by the follower (live). Both read the header `timestamp`
/// field, so no extra RPC is involved.
///
/// Advisory data. A torn or partial file degrades to the formula via the
/// reader's zero-is-unknown rule, so writes are plain positional `pwrite`
/// (no atomic rename). Header count is flushed on `sync`/`deinit`.
pub const TimestampWriter = struct {
    file: std.fs.File,
    first_block: u64,
    count: u64,

    /// Open `dir/timestamps.bin` for a store whose first block is
    /// `first_block`. Resumes an existing matching file or creates a fresh
    /// one (header only). A file with a different first_block (store rebuilt)
    /// is recreated so dense indexing stays aligned.
    pub fn open(dir: std.fs.Dir, first_block: u64) !TimestampWriter {
        if (dir.openFile(FILE_NAME, .{ .mode = .read_write })) |file| {
            var hdr: [HEADER_SIZE]u8 = undefined;
            const n = file.preadAll(&hdr, 0) catch 0;
            if (n == HEADER_SIZE and std.mem.eql(u8, hdr[0..8], &MAGIC) and
                std.mem.readInt(u64, hdr[8..16], .little) == first_block)
            {
                return .{ .file = file, .first_block = first_block, .count = std.mem.readInt(u64, hdr[16..24], .little) };
            }
            file.close();
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }

        const file = try dir.createFile(FILE_NAME, .{ .read = true, .truncate = true });
        errdefer file.close();
        var hdr: [HEADER_SIZE]u8 = undefined;
        @memcpy(hdr[0..8], &MAGIC);
        std.mem.writeInt(u64, hdr[8..16], first_block, .little);
        std.mem.writeInt(u64, hdr[16..24], 0, .little);
        try file.writeAll(&hdr);
        return .{ .file = file, .first_block = first_block, .count = 0 };
    }

    /// Record `ts` for `block`. Blocks below first_block are ignored. The u32
    /// lands at its dense slot. Any skipped slot stays zero (unknown). Call
    /// `sync` or `deinit` to publish the new count to readers.
    pub fn set(self: *TimestampWriter, block: u64, ts: u32) !void {
        if (block < self.first_block) return;
        const idx = block - self.first_block;
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, ts, .little);
        try self.file.pwriteAll(&buf, HEADER_SIZE + idx * ENTRY_SIZE);
        if (idx + 1 > self.count) self.count = idx + 1;
    }

    /// Publish the current count in the header so readers see the new range.
    pub fn sync(self: *TimestampWriter) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, self.count, .little);
        try self.file.pwriteAll(&buf, 16);
    }

    pub fn deinit(self: *TimestampWriter) void {
        self.sync() catch {};
        self.file.close();
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

/// Write a timestamps.bin with the given first_block and entries.
fn writeFixture(dir: std.fs.Dir, first_block: u64, entries: []const u32) !void {
    var file = try dir.createFile(FILE_NAME, .{});
    defer file.close();
    var hdr: [HEADER_SIZE]u8 = undefined;
    @memcpy(hdr[0..8], &MAGIC);
    std.mem.writeInt(u64, hdr[8..16], first_block, .little);
    std.mem.writeInt(u64, hdr[16..24], entries.len, .little);
    try file.writeAll(&hdr);
    for (entries) |e| {
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, e, .little);
        try file.writeAll(&b);
    }
}

test "reader returns exact timestamps and null outside the covered range" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    // blocks 100..103 with slot-aligned timestamps, block 102 unknown (0)
    try writeFixture(tmp.dir, 100, &.{ 1_700_000_000, 1_700_000_012, 0, 1_700_000_036 });

    var r = (try TimestampReader.open(tmp.dir)).?;
    defer r.deinit();

    try testing.expectEqual(@as(?u64, 1_700_000_000), r.get(100));
    try testing.expectEqual(@as(?u64, 1_700_000_012), r.get(101));
    try testing.expectEqual(@as(?u64, null), r.get(102)); // zero = unknown
    try testing.expectEqual(@as(?u64, 1_700_000_036), r.get(103));
    try testing.expectEqual(@as(?u64, null), r.get(99)); // before first_block
    try testing.expectEqual(@as(?u64, null), r.get(104)); // past count
}

test "absent file opens as null so callers fall back to the formula" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try testing.expectEqual(@as(?TimestampReader, null), try TimestampReader.open(tmp.dir));
}

test "wrong magic is rejected loudly" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var file = try tmp.dir.createFile(FILE_NAME, .{});
    try file.writeAll("NOTTHIS!" ++ ([_]u8{0} ** 16));
    file.close();
    try testing.expectError(error.InvalidMagic, TimestampReader.open(tmp.dir));
}

test "writer records timestamps the reader reads back, gaps stay unknown" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        var w = try TimestampWriter.open(tmp.dir, 100);
        defer w.deinit();
        try w.set(100, 1_700_000_000);
        try w.set(101, 1_700_000_012);
        try w.set(103, 1_700_000_036); // 102 skipped -> zero
        try w.set(50, 999); // below first_block -> ignored
    }
    var r = (try TimestampReader.open(tmp.dir)).?;
    defer r.deinit();
    try testing.expectEqual(@as(?u64, 1_700_000_000), r.get(100));
    try testing.expectEqual(@as(?u64, 1_700_000_012), r.get(101));
    try testing.expectEqual(@as(?u64, null), r.get(102));
    try testing.expectEqual(@as(?u64, 1_700_000_036), r.get(103));
}

test "writer resumes an existing file, preserving prior entries" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        var w = try TimestampWriter.open(tmp.dir, 100);
        defer w.deinit();
        try w.set(100, 111);
    }
    {
        var w = try TimestampWriter.open(tmp.dir, 100); // resume
        defer w.deinit();
        try w.set(101, 222);
    }
    var r = (try TimestampReader.open(tmp.dir)).?;
    defer r.deinit();
    try testing.expectEqual(@as(?u64, 111), r.get(100));
    try testing.expectEqual(@as(?u64, 222), r.get(101));
}
