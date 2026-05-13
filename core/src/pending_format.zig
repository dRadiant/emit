/// Wire format for `pending.bin` — the engine's pre-finality block buffer.
///
/// Engine owns the write path (insert, persist, truncate). SDK owns the read
/// path. This module is the single source of truth on the byte layout so the
/// two never drift.
///
/// Layout:
///   count(u32 LE)
///   [count × Entry]:
///     block_number(u64 BE)
///     hash(32)
///     topic_bloom(BLOOM_SIZE = 256)
///     addr_bloom(ADDR_BLOOM_SIZE = 1024)
///     lz4_len(u32 LE)
///     lz4_data(lz4_len)
const std = @import("std");

const bloom = @import("bloom.zig");
const types = @import("types.zig");

pub const FINALITY_DEPTH = types.FINALITY_DEPTH;
pub const HASH_SIZE: usize = 32;
pub const FIXED_ENTRY_SIZE: usize =
    8 + HASH_SIZE + bloom.BLOOM_SIZE + bloom.ADDR_BLOOM_SIZE + 4;

/// A parsed entry. `lz4_entry` is a non-owning slice into the source buffer
/// passed to `parse` — callers must keep that buffer alive while reading.
pub const Entry = struct {
    block_number: u64,
    hash: [HASH_SIZE]u8,
    topic_bloom: [bloom.BLOOM_SIZE]u8,
    addr_bloom: [bloom.ADDR_BLOOM_SIZE]u8,
    lz4_entry: []const u8,
};

pub const ParseError = error{ Truncated, OutOfMemory };

/// Parse `pending.bin` contents into a slice of entries. Entries are
/// zero-copy views into `buf`; the returned slice itself is owned by
/// `allocator` and freed with `allocator.free`.
pub fn parse(allocator: std.mem.Allocator, buf: []const u8) ParseError![]Entry {
    if (buf.len < 4) return try allocator.alloc(Entry, 0);
    const entry_count: usize = std.mem.readInt(u32, buf[0..4], .little);
    if (entry_count == 0) return try allocator.alloc(Entry, 0);

    const entries = try allocator.alloc(Entry, entry_count);
    errdefer allocator.free(entries);

    var pos: usize = 4;
    for (entries) |*e| {
        if (pos + FIXED_ENTRY_SIZE > buf.len) return error.Truncated;
        e.block_number = std.mem.readInt(u64, buf[pos..][0..8], .big);
        pos += 8;
        e.hash = buf[pos..][0..HASH_SIZE].*;
        pos += HASH_SIZE;
        e.topic_bloom = buf[pos..][0..bloom.BLOOM_SIZE].*;
        pos += bloom.BLOOM_SIZE;
        e.addr_bloom = buf[pos..][0..bloom.ADDR_BLOOM_SIZE].*;
        pos += bloom.ADDR_BLOOM_SIZE;
        const lz4_len: usize = std.mem.readInt(u32, buf[pos..][0..4], .little);
        pos += 4;
        if (pos + lz4_len > buf.len) return error.Truncated;
        e.lz4_entry = buf[pos..][0..lz4_len];
        pos += lz4_len;
    }
    return entries;
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

/// Build a pending.bin byte buffer from one entry. Used by tests here and by
/// the engine's own round-trip checks.
fn writeOne(
    buf: []u8,
    block_number: u64,
    hash: [HASH_SIZE]u8,
    topic_bloom: *const [bloom.BLOOM_SIZE]u8,
    addr_bloom: *const [bloom.ADDR_BLOOM_SIZE]u8,
    lz4: []const u8,
) usize {
    std.mem.writeInt(u32, buf[0..4], 1, .little);
    var pos: usize = 4;
    std.mem.writeInt(u64, buf[pos..][0..8], block_number, .big);
    pos += 8;
    @memcpy(buf[pos..][0..HASH_SIZE], &hash);
    pos += HASH_SIZE;
    @memcpy(buf[pos..][0..bloom.BLOOM_SIZE], topic_bloom);
    pos += bloom.BLOOM_SIZE;
    @memcpy(buf[pos..][0..bloom.ADDR_BLOOM_SIZE], addr_bloom);
    pos += bloom.ADDR_BLOOM_SIZE;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(lz4.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..lz4.len], lz4);
    pos += lz4.len;
    return pos;
}

test "parse empty buffer returns empty slice" {
    const empty = try parse(testing.allocator, &.{});
    defer testing.allocator.free(empty);
    try testing.expectEqual(@as(usize, 0), empty.len);
}

test "parse count=0 returns empty slice" {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, 0, .little);
    const parsed = try parse(testing.allocator, &buf);
    defer testing.allocator.free(parsed);
    try testing.expectEqual(@as(usize, 0), parsed.len);
}

test "parse round-trips a single entry" {
    const hash = [_]u8{0xCD} ** 32;
    const topic = [_]u8{0xAA} ** bloom.BLOOM_SIZE;
    const addr = [_]u8{0xBB} ** bloom.ADDR_BLOOM_SIZE;
    const lz4 = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };

    var buf: [4 + FIXED_ENTRY_SIZE + 4]u8 = undefined;
    const written = writeOne(&buf, 12345, hash, &topic, &addr, &lz4);

    const parsed = try parse(testing.allocator, buf[0..written]);
    defer testing.allocator.free(parsed);

    try testing.expectEqual(@as(usize, 1), parsed.len);
    try testing.expectEqual(@as(u64, 12345), parsed[0].block_number);
    try testing.expectEqualSlices(u8, &hash, &parsed[0].hash);
    try testing.expectEqualSlices(u8, &topic, &parsed[0].topic_bloom);
    try testing.expectEqualSlices(u8, &addr, &parsed[0].addr_bloom);
    try testing.expectEqualSlices(u8, &lz4, parsed[0].lz4_entry);
}

test "parse rejects truncated entry header" {
    var buf: [4 + 10]u8 = undefined;
    std.mem.writeInt(u32, buf[0..4], 1, .little);
    try testing.expectError(error.Truncated, parse(testing.allocator, &buf));
}

test "parse rejects truncated lz4 payload" {
    const hash = [_]u8{0} ** 32;
    const topic = [_]u8{0} ** bloom.BLOOM_SIZE;
    const addr = [_]u8{0} ** bloom.ADDR_BLOOM_SIZE;

    var buf: [4 + FIXED_ENTRY_SIZE + 2]u8 = undefined;
    std.mem.writeInt(u32, buf[0..4], 1, .little);
    var pos: usize = 4;
    std.mem.writeInt(u64, buf[pos..][0..8], 1, .big);
    pos += 8;
    @memcpy(buf[pos..][0..32], &hash);
    pos += 32;
    @memcpy(buf[pos..][0..bloom.BLOOM_SIZE], &topic);
    pos += bloom.BLOOM_SIZE;
    @memcpy(buf[pos..][0..bloom.ADDR_BLOOM_SIZE], &addr);
    pos += bloom.ADDR_BLOOM_SIZE;
    // Claim 10 bytes of payload but only provide 2.
    std.mem.writeInt(u32, buf[pos..][0..4], 10, .little);

    try testing.expectError(error.Truncated, parse(testing.allocator, &buf));
}
