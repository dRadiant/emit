/// Owns one `<entity>.blobs.dat` file for a blob-bearing entity type. Holds
/// the variable-length payloads a fixed record addresses by `BlobRef`
/// (ADR-005), the entity-layer analogue of the engine's `blocks.dat`.
/// Append-only, NEVER mutated in place. The authoritative valid length lives
/// in `state.snap.blob_bytes[slot]`, not this file's header. Bytes past it
/// are orphan tail from a crashed commit, invisible (no committed record
/// references them) and overwritten by the next flush.
///
/// Layout:
///   magic   [8]u8   "EMITBLOB"
///   body    [committed_len bytes]   concatenated payloads, element-aligned
///
/// One commit cycle: `stage` each dirty blob into an in-memory buffer (it
/// assigns the final element-aligned absolute offset and returns the
/// `BlobRef`), then `flush` writes + fsyncs the buffer and remaps. The order
/// `flush(blobs.dat)` → `state.snap` rename keeps a crash from referencing
/// undurable bytes. Reads (`slice`) resolve only committed refs against the
/// mmap, never staged bytes.
const std = @import("std");

const core = @import("core");
const entity_serial = @import("entity_serial.zig");

const BlobRef = entity_serial.BlobRef;

pub const MAGIC: core.flat_format.Magic = "EMITBLOB".*;
pub const HEADER_SIZE: usize = core.flat_format.MAGIC_SIZE;

const MmapSlice = []align(std.heap.page_size_min) const u8;

pub const BlobLog = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    /// Maps `[0, HEADER_SIZE + committed_len)`. Null until first mapped, and
    /// whenever `committed_len` is 0 (nothing to read). `BlobRef.offset` is an
    /// absolute file offset, so `slice` indexes this directly.
    map: ?MmapSlice = null,
    /// Authoritative payload byte length, from `state.snap.blob_bytes[slot]`.
    committed_len: u64,
    /// This cycle's appended bytes (with alignment padding), starting at
    /// absolute offset `HEADER_SIZE + committed_len`. Flushed and cleared per
    /// commit. Dropped (cleared) on a reorg that never finalizes.
    staging: std.ArrayListUnmanaged(u8) = .{},

    /// Open `<dir>/<name>`, creating it with a fresh magic header on first
    /// use, and map the committed region. `committed_len` comes from
    /// `state.snap.blob_bytes[slot]` (0 for a new or version-1 store). Wrong
    /// magic surfaces `error.InvalidMagic` so the caller fails loud.
    pub fn open(allocator: std.mem.Allocator, dir: std.fs.Dir, name: []const u8, committed_len: u64) !BlobLog {
        const file = try core.flat_format.openOrCreateWithMagic(dir, name, MAGIC);
        errdefer file.close();
        var self = BlobLog{ .allocator = allocator, .file = file, .committed_len = committed_len };
        try self.remap();
        return self;
    }

    pub fn deinit(self: *BlobLog) void {
        if (self.map) |m| std.posix.munmap(m);
        self.staging.deinit(self.allocator);
        self.file.close();
    }

    /// Stage `bytes` for this cycle, returning the `BlobRef` a record embeds.
    /// The absolute offset is padded up to `alignment` so `slice` can cast the
    /// payload to `[]const T` (the mmap base is page-aligned, so an
    /// `alignment`-aligned offset yields an `alignment`-aligned address).
    /// `alignment` is 1 for `[]const u8`. Empty input returns the empty ref
    /// with no staging.
    pub fn stage(self: *BlobLog, bytes: []const u8, alignment: usize) !BlobRef {
        if (bytes.len == 0) return BlobRef.empty;
        const abs_base = HEADER_SIZE + self.committed_len;
        const cur_abs = abs_base + self.staging.items.len;
        const aligned_abs = std.mem.alignForward(usize, cur_abs, alignment);
        try self.staging.appendNTimes(self.allocator, 0, aligned_abs - cur_abs);
        try self.staging.appendSlice(self.allocator, bytes);
        return .{ .offset = @intCast(aligned_abs), .len = @intCast(bytes.len) };
    }

    /// Append this cycle's staged bytes, fsync, and remap so the new offsets
    /// resolve. MUST complete before the `state.snap` rename that publishes
    /// the matching `blob_bytes`. No-op when nothing was staged.
    pub fn flush(self: *BlobLog) !void {
        if (self.staging.items.len == 0) return;
        try self.file.pwriteAll(self.staging.items, HEADER_SIZE + self.committed_len);
        try self.file.sync();
        self.committed_len += self.staging.items.len;
        self.staging.clearRetainingCapacity();
        try self.remap();
    }

    /// Discard staged bytes without writing. The reorg path, mirroring the
    /// entity overlay's drop. Staged bytes never reached disk, so nothing to
    /// undo.
    pub fn abort(self: *BlobLog) void {
        self.staging.clearRetainingCapacity();
    }

    /// Resolve a committed `BlobRef` to its bytes in the mmap. Empty ref or
    /// empty map yields an empty slice. The slice borrows the mapping, valid
    /// until the next `flush` remaps or the log is deinitialized.
    pub fn slice(self: *const BlobLog, ref: BlobRef) []const u8 {
        if (ref.len == 0) return &.{};
        const m = self.map orelse return &.{};
        return m[ref.offset..][0..ref.len];
    }

    /// The committed mmap, for the store's `deserializeWithBlobs` base. Empty
    /// when nothing is committed.
    pub fn map_bytes(self: *const BlobLog) []const u8 {
        return self.map orelse &.{};
    }

    fn remap(self: *BlobLog) !void {
        if (self.map) |m| {
            std.posix.munmap(m);
            self.map = null;
        }
        if (self.committed_len == 0) return;
        const len = HEADER_SIZE + self.committed_len;
        self.map = try std.posix.mmap(null, len, std.posix.PROT.READ, .{ .TYPE = .SHARED }, self.file.handle, 0);
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

test "open empty, stage + flush, slice resolves the payload" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try BlobLog.open(testing.allocator, tmp.dir, "x.blobs.dat", 0);
    defer log.deinit();

    const ref_a = try log.stage("vitalik.eth", 1);
    const ref_b = try log.stage("hello", 1);
    try log.flush();

    try testing.expectEqualSlices(u8, "vitalik.eth", log.slice(ref_a));
    try testing.expectEqualSlices(u8, "hello", log.slice(ref_b));
    // Distinct, non-overlapping, past the 8-byte header.
    try testing.expectEqual(@as(u40, HEADER_SIZE), ref_a.offset);
    try testing.expectEqual(@as(u40, HEADER_SIZE + "vitalik.eth".len), ref_b.offset);
}

test "empty payload stages nothing and resolves to an empty slice" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var log = try BlobLog.open(testing.allocator, tmp.dir, "x.blobs.dat", 0);
    defer log.deinit();

    const ref = try log.stage("", 1);
    try testing.expect(ref.isEmpty());
    try testing.expectEqual(@as(usize, 0), log.staging.items.len);
    try testing.expectEqual(@as(usize, 0), log.slice(ref).len);
}

test "stage aligns the absolute offset for wide elements" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var log = try BlobLog.open(testing.allocator, tmp.dir, "x.blobs.dat", 0);
    defer log.deinit();

    // One byte first so the next u64 blob does not start 8-aligned by luck.
    _ = try log.stage("z", 1);
    const ref = try log.stage(std.mem.sliceAsBytes(&[_]u64{ 1, 2 }), 8);
    try testing.expectEqual(@as(usize, 0), ref.offset % 8);
    try log.flush();

    const bytes = log.slice(ref);
    const ptr: [*]align(1) const u64 = @ptrCast(bytes.ptr);
    // The mmap base is page-aligned and offset is 8-aligned, so a real read
    // through deserializeWithBlobs is aligned. Here just confirm the values.
    try testing.expectEqual(@as(u64, 1), ptr[0]);
    try testing.expectEqual(@as(u64, 2), ptr[1]);
}

test "reopen sees committed bytes and ignores orphan tail" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var ref_a: BlobRef = undefined;
    {
        var log = try BlobLog.open(testing.allocator, tmp.dir, "x.blobs.dat", 0);
        defer log.deinit();
        ref_a = try log.stage("committed", 1);
        try log.flush();
        // A second cycle's bytes hit disk, but simulate a crash before the
        // state.snap rename: stage + flush advances the file, yet the caller's
        // persisted committed_len stays at the first cycle's length.
        _ = try log.stage("orphaned", 1);
        try log.flush();
    }

    // Reopen at the first cycle's committed length: the orphan tail is past
    // the map and invisible, the committed blob still resolves.
    var log = try BlobLog.open(testing.allocator, tmp.dir, "x.blobs.dat", "committed".len);
    defer log.deinit();
    try testing.expectEqualSlices(u8, "committed", log.slice(ref_a));

    // The next stage overwrites the orphan region.
    const ref_b = try log.stage("fresh", 1);
    try testing.expectEqual(ref_a.offset + @as(u40, "committed".len), ref_b.offset);
    try log.flush();
    try testing.expectEqualSlices(u8, "fresh", log.slice(ref_b));
}

test "abort discards staged bytes without writing" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var log = try BlobLog.open(testing.allocator, tmp.dir, "x.blobs.dat", 0);
    defer log.deinit();

    _ = try log.stage("doomed", 1);
    log.abort();
    try testing.expectEqual(@as(usize, 0), log.staging.items.len);
    try testing.expectEqual(@as(u64, 0), log.committed_len);

    // A post-abort cycle starts clean at the header.
    const ref = try log.stage("kept", 1);
    try testing.expectEqual(@as(u40, HEADER_SIZE), ref.offset);
    try log.flush();
    try testing.expectEqualSlices(u8, "kept", log.slice(ref));
}
