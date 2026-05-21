/// Shared magic-header helpers for flat files across the project. Every
/// flat file in `core` and `sdk` carries an 8-byte magic at offset 0 so a
/// stray copy, truncated download, or arbitrary bytes is rejected loud
/// rather than misread as empty state.
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
