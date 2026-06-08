/// Magic-header helpers for flat files. Every flat file in `core` and `sdk`
/// carries an 8-byte magic at offset 0 so a stray copy, truncated download,
/// or arbitrary bytes is rejected loud rather than misread as empty state.
const std = @import("std");

pub const MAGIC_SIZE: usize = 8;
pub const Magic = [MAGIC_SIZE]u8;

pub const Error = error{ InvalidMagic, Truncated };

pub fn writeMagic(buf: []u8, magic: Magic) void {
    @memcpy(buf[0..MAGIC_SIZE], &magic);
}

pub fn validateMagic(buf: []const u8, expected: Magic) Error!void {
    if (buf.len < MAGIC_SIZE) return error.Truncated;
    if (!std.mem.eql(u8, buf[0..MAGIC_SIZE], &expected)) return error.InvalidMagic;
}

/// Create `dir/name`, truncating any prior content, and write the magic
/// header. Recovery path when an existing file is corrupted past the magic.
pub fn createWithMagic(dir: std.fs.Dir, name: []const u8, magic: Magic) !std.fs.File {
    const file = try dir.createFile(name, .{ .read = true, .truncate = true });
    errdefer file.close();
    try file.writeAll(&magic);
    return file;
}

/// Open `dir/name` and validate its magic, or create a fresh file with
/// `magic` as its first bytes. Files shorter than `MAGIC_SIZE` are treated
/// as fresh (prior run crashed before any record was written). Wrong magic
/// surfaces `error.InvalidMagic` so callers pick their own recovery policy.
///
/// Caller owns the returned file: further header parsing and closing.
pub fn openOrCreateWithMagic(dir: std.fs.Dir, name: []const u8, magic: Magic) !std.fs.File {
    if (dir.openFile(name, .{ .mode = .read_write })) |file| {
        errdefer file.close();
        var buf: [MAGIC_SIZE]u8 = undefined;
        // Propagate real I/O errors. Swallowing them would silently truncate the file.
        const n = try file.pread(&buf, 0);
        if (n < MAGIC_SIZE) {
            file.close();
            return createWithMagic(dir, name, magic);
        }
        try validateMagic(&buf, magic);
        return file;
    } else |err| switch (err) {
        error.FileNotFound => return createWithMagic(dir, name, magic),
        else => return err,
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

test "writeMagic then validateMagic round-trips" {
    var buf: [16]u8 = undefined;
    const magic: Magic = "EMITTEST".*;
    writeMagic(&buf, magic);
    try validateMagic(&buf, magic);
}

test "validateMagic rejects a mismatched magic" {
    var buf: [16]u8 = [_]u8{0xFF} ** 16;
    writeMagic(&buf, "EMITTEST".*);
    try testing.expectError(error.InvalidMagic, validateMagic(&buf, "OTHERMAG".*));
}

test "validateMagic rejects a buffer shorter than MAGIC_SIZE" {
    const buf: [4]u8 = [_]u8{0xAA} ** 4;
    try testing.expectError(error.Truncated, validateMagic(&buf, "EMITTEST".*));
}

test "openOrCreateWithMagic creates the file when missing" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try openOrCreateWithMagic(tmp.dir, "f.dat", "EMITTEST".*);
    defer file.close();

    var buf: [MAGIC_SIZE]u8 = undefined;
    const n = try file.pread(&buf, 0);
    try testing.expectEqual(MAGIC_SIZE, n);
    try testing.expectEqualSlices(u8, "EMITTEST", &buf);
}

test "openOrCreateWithMagic opens an existing file with a valid magic" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var fresh = try createWithMagic(tmp.dir, "f.dat", "EMITTEST".*);
        defer fresh.close();
        try fresh.writeAll("payload");
    }

    var file = try openOrCreateWithMagic(tmp.dir, "f.dat", "EMITTEST".*);
    defer file.close();

    var buf: [MAGIC_SIZE + 7]u8 = undefined;
    const n = try file.pread(&buf, 0);
    try testing.expectEqual(@as(usize, MAGIC_SIZE + 7), n);
    try testing.expectEqualSlices(u8, "payload", buf[MAGIC_SIZE..]);
}

test "openOrCreateWithMagic returns InvalidMagic for a wrong magic" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("f.dat", .{});
        defer f.close();
        try f.writeAll(&[_]u8{0xFF} ** MAGIC_SIZE);
    }

    try testing.expectError(error.InvalidMagic, openOrCreateWithMagic(tmp.dir, "f.dat", "EMITTEST".*));
}

test "openOrCreateWithMagic treats a file shorter than the magic as fresh" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("f.dat", .{});
        defer f.close();
        try f.writeAll(&[_]u8{ 0x01, 0x02 });
    }

    var file = try openOrCreateWithMagic(tmp.dir, "f.dat", "EMITTEST".*);
    defer file.close();

    var buf: [MAGIC_SIZE]u8 = undefined;
    const n = try file.pread(&buf, 0);
    try testing.expectEqual(MAGIC_SIZE, n);
    try testing.expectEqualSlices(u8, "EMITTEST", &buf);
}
