/// Wire format for `pending.bin`, the engine's pre-finality block buffer.
///
/// Engine owns the write path (insert, persist, truncate). SDK owns the read
/// path. Single source of truth on the byte layout so the two never drift.
///
/// Layout:
///   magic "EMITPEND" (8 bytes)
///   count(u32 LE)
///   [count × Entry]:
///     block_number(u64 BE)
///     timestamp(u32 LE)      exact block time, 0 when unknown
///     hash(32)
///     topic_bloom(BLOOM_SIZE = 256)
///     addr_bloom(ADDR_BLOOM_SIZE = 1024)
///     lz4_len(u32 LE)
///     lz4_data(lz4_len)
///
/// Magic distinguishes a pre-magic/foreign file from a corrupt one. Mismatch
/// surfaces `error.InvalidMagic`, treated as "no usable ring" (follower
/// re-baselines from meta). Matching magic with a short body is `error.Truncated`.
const std = @import("std");

const bloom = @import("bloom.zig");
const types = @import("types.zig");

pub const FINALITY_DEPTH = types.FINALITY_DEPTH;
pub const HASH_SIZE: usize = 32;
pub const MAGIC: [8]u8 = "EMITPEND".*;
pub const HEADER_SIZE: usize = MAGIC.len + 4; // magic(8) + count(u32 LE)
pub const FIXED_ENTRY_SIZE: usize =
    8 + 4 + HASH_SIZE + bloom.BLOOM_SIZE + bloom.ADDR_BLOOM_SIZE + 4;

/// A parsed entry. `lz4_entry` is a non-owning slice into the source buffer
/// passed to `parse`. Callers must keep that buffer alive while reading.
pub const Entry = struct {
    block_number: u64,
    timestamp: u32 = 0,
    hash: [HASH_SIZE]u8,
    topic_bloom: [bloom.BLOOM_SIZE]u8,
    addr_bloom: [bloom.ADDR_BLOOM_SIZE]u8,
    lz4_entry: []const u8,
};

pub const ParseError = error{ Truncated, InvalidMagic, OutOfMemory };
pub const ValidateError = ParseError || error{ NonDense, Oversized };

/// Serialize entries into a freshly-allocated `pending.bin` buffer. Caller
/// frees with `allocator.free(buf)`. `entries` is `anytype` to accept both
/// `pending_format.Entry` (zero-copy view) and engine-side owning variants.
pub fn serialize(allocator: std.mem.Allocator, entries: anytype) ![]u8 {
    var total_size: usize = HEADER_SIZE;
    for (entries) |e| total_size += FIXED_ENTRY_SIZE + e.lz4_entry.len;

    const buf = try allocator.alloc(u8, total_size);
    errdefer allocator.free(buf);

    @memcpy(buf[0..MAGIC.len], &MAGIC);
    std.mem.writeInt(u32, buf[MAGIC.len..][0..4], @intCast(entries.len), .little);
    var pos: usize = HEADER_SIZE;

    for (entries) |e| {
        std.mem.writeInt(u64, buf[pos..][0..8], e.block_number, .big);
        pos += 8;
        std.mem.writeInt(u32, buf[pos..][0..4], e.timestamp, .little);
        pos += 4;
        @memcpy(buf[pos..][0..HASH_SIZE], &e.hash);
        pos += HASH_SIZE;
        @memcpy(buf[pos..][0..bloom.BLOOM_SIZE], &e.topic_bloom);
        pos += bloom.BLOOM_SIZE;
        @memcpy(buf[pos..][0..bloom.ADDR_BLOOM_SIZE], &e.addr_bloom);
        pos += bloom.ADDR_BLOOM_SIZE;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(e.lz4_entry.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..e.lz4_entry.len], e.lz4_entry);
        pos += e.lz4_entry.len;
    }
    return buf;
}

/// Parse `pending.bin` contents into a slice of entries. Entries are
/// zero-copy views into `buf`. The returned slice itself is owned by
/// `allocator` and freed with `allocator.free`.
pub fn parse(allocator: std.mem.Allocator, buf: []const u8) ParseError![]Entry {
    if (buf.len < HEADER_SIZE) return try allocator.alloc(Entry, 0);
    if (!std.mem.eql(u8, buf[0..MAGIC.len], &MAGIC)) return error.InvalidMagic;
    const entry_count: usize = std.mem.readInt(u32, buf[MAGIC.len..][0..4], .little);
    if (entry_count == 0) return try allocator.alloc(Entry, 0);

    const entries = try allocator.alloc(Entry, entry_count);
    errdefer allocator.free(entries);

    var pos: usize = HEADER_SIZE;
    for (entries) |*e| {
        if (pos + FIXED_ENTRY_SIZE > buf.len) return error.Truncated;
        e.block_number = std.mem.readInt(u64, buf[pos..][0..8], .big);
        pos += 8;
        e.timestamp = std.mem.readInt(u32, buf[pos..][0..4], .little);
        pos += 4;
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

/// Parse and enforce the ring's structural invariants: strictly monotonic,
/// gap-free block numbers, at most FINALITY_DEPTH entries. Either failing
/// means pending.bin is corrupt. Reorg recovery indexes the ring as a dense
/// array and would silently mis-report block presence on a non-dense ring.
pub fn parseValidated(allocator: std.mem.Allocator, buf: []const u8) ValidateError![]Entry {
    if (buf.len >= HEADER_SIZE and std.mem.eql(u8, buf[0..MAGIC.len], &MAGIC)) {
        const declared: usize = std.mem.readInt(u32, buf[MAGIC.len..][0..4], .little);
        if (declared > FINALITY_DEPTH) return error.Oversized;
    }
    const entries = try parse(allocator, buf);
    errdefer allocator.free(entries);

    var i: usize = 1;
    while (i < entries.len) : (i += 1) {
        if (entries[i].block_number != entries[i - 1].block_number + 1) return error.NonDense;
    }
    return entries;
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

test "parse empty buffer returns empty slice" {
    const empty = try parse(testing.allocator, &.{});
    defer testing.allocator.free(empty);
    try testing.expectEqual(@as(usize, 0), empty.len);
}

test "parse magic + count=0 returns empty slice" {
    var buf: [HEADER_SIZE]u8 = undefined;
    @memcpy(buf[0..MAGIC.len], &MAGIC);
    std.mem.writeInt(u32, buf[MAGIC.len..][0..4], 0, .little);
    const parsed = try parse(testing.allocator, &buf);
    defer testing.allocator.free(parsed);
    try testing.expectEqual(@as(usize, 0), parsed.len);
}

test "parse rejects a buffer without the magic (old/foreign format)" {
    var buf: [HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u32, buf[0..4], 1, .little); // old format started with the count
    @memset(buf[4..], 0);
    try testing.expectError(error.InvalidMagic, parse(testing.allocator, &buf));
}

test "serialize + parse round-trip preserves every field" {
    const hash = [_]u8{0xCD} ** 32;
    const topic = [_]u8{0xAA} ** bloom.BLOOM_SIZE;
    const addr = [_]u8{0xBB} ** bloom.ADDR_BLOOM_SIZE;
    const lz4 = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };

    const original = [_]Entry{.{
        .block_number = 12345,
        .timestamp = 1_700_000_000,
        .hash = hash,
        .topic_bloom = topic,
        .addr_bloom = addr,
        .lz4_entry = &lz4,
    }};

    const buf = try serialize(testing.allocator, &original);
    defer testing.allocator.free(buf);
    try testing.expectEqualSlices(u8, &MAGIC, buf[0..MAGIC.len]);

    const parsed = try parse(testing.allocator, buf);
    defer testing.allocator.free(parsed);

    try testing.expectEqual(@as(usize, 1), parsed.len);
    try testing.expectEqual(@as(u64, 12345), parsed[0].block_number);
    try testing.expectEqual(@as(u32, 1_700_000_000), parsed[0].timestamp);
    try testing.expectEqualSlices(u8, &hash, &parsed[0].hash);
    try testing.expectEqualSlices(u8, &topic, &parsed[0].topic_bloom);
    try testing.expectEqualSlices(u8, &addr, &parsed[0].addr_bloom);
    try testing.expectEqualSlices(u8, &lz4, parsed[0].lz4_entry);
}

test "serialize empty slice produces just the magic + count header" {
    const buf = try serialize(testing.allocator, &[_]Entry{});
    defer testing.allocator.free(buf);
    try testing.expectEqual(HEADER_SIZE, buf.len);
    try testing.expectEqualSlices(u8, &MAGIC, buf[0..MAGIC.len]);
    try testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, buf[MAGIC.len..][0..4], .little));
}

test "parse rejects truncated entry header" {
    var buf: [HEADER_SIZE + 10]u8 = undefined;
    @memcpy(buf[0..MAGIC.len], &MAGIC);
    std.mem.writeInt(u32, buf[MAGIC.len..][0..4], 1, .little);
    try testing.expectError(error.Truncated, parse(testing.allocator, &buf));
}

test "parseValidated accepts a dense ring" {
    const hash = [_]u8{0} ** 32;
    const topic = [_]u8{0} ** bloom.BLOOM_SIZE;
    const addr = [_]u8{0} ** bloom.ADDR_BLOOM_SIZE;
    const lz4 = [_]u8{ 1, 2, 3 };

    var entries: [3]Entry = undefined;
    for (&entries, 100..) |*e, blk| e.* = .{
        .block_number = blk,
        .hash = hash,
        .topic_bloom = topic,
        .addr_bloom = addr,
        .lz4_entry = &lz4,
    };

    const buf = try serialize(testing.allocator, &entries);
    defer testing.allocator.free(buf);

    const parsed = try parseValidated(testing.allocator, buf);
    defer testing.allocator.free(parsed);
    try testing.expectEqual(@as(usize, 3), parsed.len);
}

test "parseValidated rejects a gap" {
    const hash = [_]u8{0} ** 32;
    const topic = [_]u8{0} ** bloom.BLOOM_SIZE;
    const addr = [_]u8{0} ** bloom.ADDR_BLOOM_SIZE;
    const lz4 = [_]u8{1};

    const entries = [_]Entry{
        .{ .block_number = 100, .hash = hash, .topic_bloom = topic, .addr_bloom = addr, .lz4_entry = &lz4 },
        .{ .block_number = 102, .hash = hash, .topic_bloom = topic, .addr_bloom = addr, .lz4_entry = &lz4 },
    };

    const buf = try serialize(testing.allocator, &entries);
    defer testing.allocator.free(buf);

    try testing.expectError(error.NonDense, parseValidated(testing.allocator, buf));
}

test "parseValidated rejects ring larger than FINALITY_DEPTH" {
    var buf: [HEADER_SIZE]u8 = undefined;
    @memcpy(buf[0..MAGIC.len], &MAGIC);
    std.mem.writeInt(u32, buf[MAGIC.len..][0..4], FINALITY_DEPTH + 1, .little);
    try testing.expectError(error.Oversized, parseValidated(testing.allocator, &buf));
}

test "parse rejects truncated lz4 payload" {
    const hash = [_]u8{0} ** 32;
    const topic = [_]u8{0} ** bloom.BLOOM_SIZE;
    const addr = [_]u8{0} ** bloom.ADDR_BLOOM_SIZE;

    var buf: [HEADER_SIZE + FIXED_ENTRY_SIZE + 2]u8 = undefined;
    @memcpy(buf[0..MAGIC.len], &MAGIC);
    std.mem.writeInt(u32, buf[MAGIC.len..][0..4], 1, .little);
    var pos: usize = HEADER_SIZE;
    std.mem.writeInt(u64, buf[pos..][0..8], 1, .big);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], 0, .little); // timestamp
    pos += 4;
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
