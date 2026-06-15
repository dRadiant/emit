//! `tmp + fsync + rename` snapshot writer. Readers of `final_name` see
//! either the previous or new content, never torn.
//!
//! No directory fsync. Power loss between the rename hitting the inode
//! cache and directory metadata reaching disk can roll the rename back,
//! leaving the reader on prior (still-consistent) content. Recovery is
//! the caller's normal open-and-resume path (e.g. cursor at an earlier
//! block replays).
const std = @import("std");

pub fn write(
    dir: std.fs.Dir,
    tmp_name: []const u8,
    final_name: []const u8,
    bytes: []const u8,
) !void {
    {
        const tmp = try dir.createFile(tmp_name, .{});
        defer tmp.close();
        try tmp.writeAll(bytes);
        try tmp.sync();
    }
    try dir.rename(tmp_name, final_name);
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

test "atomic write places content under final name; tmp removed" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try write(tmp.dir, "foo.tmp", "foo", "hello");

    const file = try tmp.dir.openFile("foo", .{});
    defer file.close();
    var buf: [5]u8 = undefined;
    const n = try file.readAll(&buf);
    try testing.expectEqualSlices(u8, "hello", buf[0..n]);

    try testing.expectError(error.FileNotFound, tmp.dir.openFile("foo.tmp", .{}));
}

test "atomic write overwrites existing final" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try write(tmp.dir, "x.tmp", "x", "first");
    try write(tmp.dir, "x.tmp", "x", "second-overwrite");

    const file = try tmp.dir.openFile("x", .{});
    defer file.close();
    var buf: [32]u8 = undefined;
    const n = try file.readAll(&buf);
    try testing.expectEqualSlices(u8, "second-overwrite", buf[0..n]);
}
