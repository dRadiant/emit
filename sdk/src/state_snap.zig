/// Owns `state.snap`. One file is the atomic commit unit for cursor +
/// every MutableStore slab + every ImmutableStore boundary + every blob
/// file's valid length. Its rename commits them all consistently.
///
/// Layout (ADR-003, +blob_bytes per ADR-005):
///   magic              [8]u8       "EMITSTAT"
///   version            u32 LE      1 with no blob stores, 2 with any
///   cursor             u64 LE      last fully-dispatched block
///   mutable_bytes      [mutable_count]u64 LE   slab byte length per MutableStore slot
///   immutable_counts   [immutable_count]u64 LE   record count per ImmutableStore slot
///   blob_bytes         [blob_count]u64 LE   committed payload length per `<entity>.blobs.dat`
///   [body: `mutable_count` MutableStore slabs concatenated in slot order]
///
/// `blob_bytes` are lengths into the separate `blobs.dat` files, so the body
/// size is unchanged. A schema with zero blob stores writes version 1 and a
/// byte-identical header to ADR-003, the numeric-only path is untouched.
/// Schema is comptime-known via the type parameters. No per-slot descriptors
/// on disk.
const std = @import("std");

const core = @import("core");

pub const MAGIC: core.flat_format.Magic = "EMITSTAT".*;
pub const VERSION_BASE: u32 = 1;
pub const VERSION_BLOBS: u32 = 2;

pub const Error = error{
    InvalidMagic,
    Truncated,
    VersionMismatch,
    SizeMismatch,
} || std.mem.Allocator.Error || std.fs.File.OpenError || std.fs.File.WriteError || std.fs.File.ReadError || std.fs.Dir.DeleteFileError;

/// `mutables` = MutableStore slot count, `immutables` = ImmutableStore slot
/// count, `blobs` = count of blob-bearing stores (either kind). All
/// zero-permitted. No-store case yields a 20-byte header file. `blobs == 0`
/// keeps version 1 and the ADR-003 layout exactly.
pub fn StateSnap(comptime mutables: usize, comptime immutables: usize, comptime blobs: usize) type {
    const HEADER_SIZE: usize = 8 + 4 + 8 + mutables * 8 + immutables * 8 + blobs * 8;
    const VERSION: u32 = if (blobs == 0) VERSION_BASE else VERSION_BLOBS;

    return struct {
        const Self = @This();

        pub const header_size = HEADER_SIZE;
        pub const mutable_count = mutables;
        pub const immutable_count = immutables;
        pub const blob_count = blobs;
        pub const version = VERSION;

        allocator: std.mem.Allocator,
        dir: std.fs.Dir,
        cursor: u64,
        mutable_bytes: [mutables]u64,
        immutable_counts: [immutables]u64,
        blob_bytes: [blobs]u64,
        /// Concatenated MutableStore slabs in slot order. A view into `backing`.
        body: []u8,
        /// Allocation `body` points into, owned by `allocator`. `open` reads
        /// the body alone, `commit` retains the full header + body file image,
        /// so the two differ only in a header-sized prefix.
        backing: []u8,

        /// Open `state.snap` from `dir`. Missing file returns an empty
        /// StateSnap with cursor=0. Unlinks any `state.snap.tmp` left by a
        /// crashed prior commit so the next commit isn't confused by it.
        pub fn open(allocator: std.mem.Allocator, dir: std.fs.Dir) !Self {
            dir.deleteFile("state.snap.tmp") catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };

            const file = dir.openFile("state.snap", .{}) catch |err| switch (err) {
                error.FileNotFound => return Self{
                    .allocator = allocator,
                    .dir = dir,
                    .cursor = 0,
                    .mutable_bytes = [_]u64{0} ** mutables,
                    .immutable_counts = [_]u64{0} ** immutables,
                    .blob_bytes = [_]u64{0} ** blobs,
                    .body = &.{},
                    .backing = &.{},
                },
                else => return err,
            };
            defer file.close();

            const stat = try file.stat();
            if (stat.size < HEADER_SIZE) return error.Truncated;

            var hdr: [HEADER_SIZE]u8 = undefined;
            if ((try file.readAll(&hdr)) < HEADER_SIZE) return error.Truncated;

            try core.flat_format.validateMagic(&hdr, MAGIC);

            const file_version = std.mem.readInt(u32, hdr[8..12], .little);
            if (file_version != VERSION) return error.VersionMismatch;

            var self = Self{
                .allocator = allocator,
                .dir = dir,
                .cursor = std.mem.readInt(u64, hdr[12..20], .little),
                .mutable_bytes = undefined,
                .immutable_counts = undefined,
                .blob_bytes = undefined,
                .body = &.{},
                .backing = &.{},
            };

            var pos: usize = 20;
            for (&self.mutable_bytes) |*b| {
                b.* = std.mem.readInt(u64, hdr[pos..][0..8], .little);
                pos += 8;
            }
            for (&self.immutable_counts) |*c| {
                c.* = std.mem.readInt(u64, hdr[pos..][0..8], .little);
                pos += 8;
            }
            for (&self.blob_bytes) |*b| {
                b.* = std.mem.readInt(u64, hdr[pos..][0..8], .little);
                pos += 8;
            }

            var total: u64 = 0;
            for (self.mutable_bytes) |b| total += b;
            if (stat.size != HEADER_SIZE + total) return error.SizeMismatch;

            if (total > 0) {
                self.body = try allocator.alloc(u8, @intCast(total));
                errdefer allocator.free(self.body);
                if ((try file.readAll(self.body)) != total) return error.Truncated;
                self.backing = self.body;
            }

            return self;
        }

        pub fn deinit(self: *Self) void {
            if (self.backing.len > 0) self.allocator.free(self.backing);
        }

        /// Borrow the slab bytes for MutableStore slot `i`. Returned slice
        /// points into `self.body`, valid until the next `commit` or `deinit`.
        pub fn mutableSlab(self: *const Self, i: usize) []const u8 {
            std.debug.assert(i < mutables);
            var offset: usize = 0;
            for (self.mutable_bytes[0..i]) |b| offset += @intCast(b);
            const len: usize = @intCast(self.mutable_bytes[i]);
            return self.body[offset .. offset + len];
        }

        pub fn immutableCount(self: *const Self, i: usize) u64 {
            std.debug.assert(i < immutables);
            return self.immutable_counts[i];
        }

        /// Committed payload length of blob store slot `i`, the `committed_len`
        /// its `BlobLog` reopens at.
        pub fn blobBytes(self: *const Self, i: usize) u64 {
            std.debug.assert(i < blobs);
            return self.blob_bytes[i];
        }

        /// Atomically commit a new cursor, new MutableStore slabs (slot
        /// order), and new ImmutableStore counts (slot order). The rename
        /// of `state.snap.tmp` makes all three visible together. See
        /// ADR-003 §"Cursor location and atomicity".
        /// Caller MUST flush + fsync every `<entity>.blobs.dat` to its
        /// `new_blob_bytes` length BEFORE this rename, so the committed snap
        /// never references undurable blob payload (ADR-005 commit order).
        pub fn commit(
            self: *Self,
            new_cursor: u64,
            new_slabs: *const [mutables][]const u8,
            new_counts: *const [immutables]u64,
            new_blob_bytes: *const [blobs]u64,
        ) !void {
            var total: usize = 0;
            for (new_slabs) |slab| total += slab.len;

            // One buffer is both the file image and the next in-memory
            // backing, so the slabs are copied once, not assembled and then
            // duplicated. Nothing allocates after the rename, disk and memory
            // cannot desync.
            const buf = try self.allocator.alloc(u8, HEADER_SIZE + total);
            errdefer self.allocator.free(buf);

            core.flat_format.writeMagic(buf, MAGIC);
            std.mem.writeInt(u32, buf[8..12], VERSION, .little);
            std.mem.writeInt(u64, buf[12..20], new_cursor, .little);

            var pos: usize = 20;
            for (new_slabs) |slab| {
                std.mem.writeInt(u64, buf[pos..][0..8], slab.len, .little);
                pos += 8;
            }
            for (new_counts) |c| {
                std.mem.writeInt(u64, buf[pos..][0..8], c, .little);
                pos += 8;
            }
            for (new_blob_bytes) |b| {
                std.mem.writeInt(u64, buf[pos..][0..8], b, .little);
                pos += 8;
            }
            for (new_slabs) |slab| {
                @memcpy(buf[pos..][0..slab.len], slab);
                pos += slab.len;
            }

            try core.atomic_file.write(self.dir, "state.snap.tmp", "state.snap", buf);

            if (self.backing.len > 0) self.allocator.free(self.backing);
            self.backing = buf;
            self.body = buf[HEADER_SIZE..];
            self.cursor = new_cursor;
            for (&self.mutable_bytes, new_slabs) |*b, slab| b.* = slab.len;
            for (&self.immutable_counts, new_counts) |*c, n| c.* = n;
            for (&self.blob_bytes, new_blob_bytes) |*b, n| b.* = n;
        }
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

test "open on a fresh directory returns cursor 0 and empty slabs" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var snap = try StateSnap(2, 1, 0).open(testing.allocator, tmp.dir);
    defer snap.deinit();

    try testing.expectEqual(@as(u64, 0), snap.cursor);
    try testing.expectEqual(@as(usize, 0), snap.mutableSlab(0).len);
    try testing.expectEqual(@as(usize, 0), snap.mutableSlab(1).len);
    try testing.expectEqual(@as(u64, 0), snap.immutableCount(0));
}

test "commit then reopen round-trips cursor, slabs, and counts" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const slab_a = [_]u8{ 0xAA, 0xBB, 0xCC };
    const slab_b = [_]u8{ 0x11, 0x22 };
    const new_slabs = [_][]const u8{ &slab_a, &slab_b };
    const new_counts = [_]u64{ 42, 17 };

    {
        var snap = try StateSnap(2, 2, 0).open(testing.allocator, tmp.dir);
        defer snap.deinit();
        try snap.commit(1234, &new_slabs, &new_counts, &.{});
    }

    var snap = try StateSnap(2, 2, 0).open(testing.allocator, tmp.dir);
    defer snap.deinit();

    try testing.expectEqual(@as(u64, 1234), snap.cursor);
    try testing.expectEqualSlices(u8, &slab_a, snap.mutableSlab(0));
    try testing.expectEqualSlices(u8, &slab_b, snap.mutableSlab(1));
    try testing.expectEqual(@as(u64, 42), snap.immutableCount(0));
    try testing.expectEqual(@as(u64, 17), snap.immutableCount(1));
}

test "invalid magic raises a loud error" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile("state.snap", .{});
    defer file.close();
    try file.writeAll(&[_]u8{0xFF} ** 20);

    try testing.expectError(error.InvalidMagic, StateSnap(0, 0, 0).open(testing.allocator, tmp.dir));
}

test "version mismatch raises a loud error" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var buf: [20]u8 = undefined;
    core.flat_format.writeMagic(&buf, MAGIC);
    std.mem.writeInt(u32, buf[8..12], 999, .little);
    std.mem.writeInt(u64, buf[12..20], 0, .little);

    const file = try tmp.dir.createFile("state.snap", .{});
    defer file.close();
    try file.writeAll(&buf);

    try testing.expectError(error.VersionMismatch, StateSnap(0, 0, 0).open(testing.allocator, tmp.dir));
}

test "stale state.snap.tmp from a crashed commit is removed on open" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const stale = try tmp.dir.createFile("state.snap.tmp", .{});
    try stale.writeAll(&[_]u8{0xDE} ** 32);
    stale.close();

    var snap = try StateSnap(0, 0, 0).open(testing.allocator, tmp.dir);
    defer snap.deinit();

    try testing.expectError(error.FileNotFound, tmp.dir.openFile("state.snap.tmp", .{}));
}

test "commit overwrites prior content cleanly" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var snap = try StateSnap(1, 0, 0).open(testing.allocator, tmp.dir);
    defer snap.deinit();

    const first = [_]u8{ 1, 2, 3, 4 };
    try snap.commit(100, &[_][]const u8{&first}, &[_]u64{}, &.{});
    try testing.expectEqual(@as(u64, 100), snap.cursor);
    try testing.expectEqualSlices(u8, &first, snap.mutableSlab(0));

    const second = [_]u8{ 9, 8 };
    try snap.commit(200, &[_][]const u8{&second}, &[_]u64{}, &.{});
    try testing.expectEqual(@as(u64, 200), snap.cursor);
    try testing.expectEqualSlices(u8, &second, snap.mutableSlab(0));
}

test "blob stores bump the version and round-trip blob_bytes" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const Snap = StateSnap(1, 0, 2);
    try testing.expectEqual(@as(u32, VERSION_BLOBS), Snap.version);

    const slab = [_]u8{ 0xAA, 0xBB };
    {
        var snap = try Snap.open(testing.allocator, tmp.dir);
        defer snap.deinit();
        try snap.commit(77, &[_][]const u8{&slab}, &[_]u64{}, &[_]u64{ 4096, 9000 });
    }

    var snap = try Snap.open(testing.allocator, tmp.dir);
    defer snap.deinit();
    try testing.expectEqual(@as(u64, 77), snap.cursor);
    try testing.expectEqualSlices(u8, &slab, snap.mutableSlab(0));
    try testing.expectEqual(@as(u64, 4096), snap.blobBytes(0));
    try testing.expectEqual(@as(u64, 9000), snap.blobBytes(1));

    // A version-1 reader of the same (version-2) file fails loud.
    try testing.expectError(error.VersionMismatch, StateSnap(1, 0, 0).open(testing.allocator, tmp.dir));
}

test "empty slabs and zero counts round-trip" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var snap = try StateSnap(2, 2, 0).open(testing.allocator, tmp.dir);
    defer snap.deinit();

    const empty_a: []const u8 = &.{};
    const empty_b: []const u8 = &.{};
    try snap.commit(5, &[_][]const u8{ empty_a, empty_b }, &[_]u64{ 0, 0 }, &.{});

    var reopened = try StateSnap(2, 2, 0).open(testing.allocator, tmp.dir);
    defer reopened.deinit();

    try testing.expectEqual(@as(u64, 5), reopened.cursor);
    try testing.expectEqual(@as(usize, 0), reopened.mutableSlab(0).len);
    try testing.expectEqual(@as(usize, 0), reopened.mutableSlab(1).len);
}
