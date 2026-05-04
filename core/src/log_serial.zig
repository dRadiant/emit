/// Packed binary log serialization and LZ4 entry helpers.
/// Format: log_count(u32 LE) || [log_count × LogEntry]
/// LogEntry: tx_index(u16) || log_index(u16) || address(20) || topic_count(u8)
///           || topics(count×32) || data_len(u32) || data(var) || tx_hash(32)
const std = @import("std");
const lz4 = @import("lz4");
const types = @import("types.zig");

const RawLog = types.RawLog;

// ── Serialization ────────────────────────────────────────────────────────
// Variable-length per log: 25 bytes fixed + topic_count×32 + data_len + 32 (tx_hash).

/// Pack logs into the flat store binary format. Returns bytes written.
/// Caller must ensure buf is large enough (BLOCK_BUF_SIZE handles worst case).
pub fn serializeLogs(logs: []const RawLog, buf: []u8) usize {
    var pos: usize = 0;

    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(logs.len), .little);
    pos += 4;

    for (logs) |log| {
        std.mem.writeInt(u16, buf[pos..][0..2], log.tx_index, .little);
        pos += 2;
        std.mem.writeInt(u16, buf[pos..][0..2], log.log_index, .little);
        pos += 2;
        @memcpy(buf[pos..][0..20], &log.address);
        pos += 20;
        buf[pos] = log.topic_count;
        pos += 1;
        for (0..log.topic_count) |t| {
            @memcpy(buf[pos..][0..32], &log.topics[t]);
            pos += 32;
        }
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(log.data.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..log.data.len], log.data);
        pos += log.data.len;
        @memcpy(buf[pos..][0..32], &log.tx_hash);
        pos += 32;
    }
    return pos;
}

/// Unpack logs from the flat store binary format. Returns log count.
/// RawLog.data slices point into `buf` — valid until buf is overwritten.
/// block_number is zeroed; caller fills it from the index key or context.
pub fn deserializeLogs(buf: []const u8, out: []RawLog) usize {
    var pos: usize = 0;

    const log_count: usize = std.mem.readInt(u32, buf[pos..][0..4], .little);
    pos += 4;

    for (0..log_count) |i| {
        var log: RawLog = undefined;
        log.block_number = 0;
        log.tx_index = std.mem.readInt(u16, buf[pos..][0..2], .little);
        pos += 2;
        log.log_index = std.mem.readInt(u16, buf[pos..][0..2], .little);
        pos += 2;
        log.address = buf[pos..][0..20].*;
        pos += 20;
        log.topic_count = buf[pos];
        pos += 1;
        for (0..log.topic_count) |t| {
            log.topics[t] = buf[pos..][0..32].*;
            pos += 32;
        }
        const data_len: usize = std.mem.readInt(u32, buf[pos..][0..4], .little);
        pos += 4;
        log.data = buf[pos..][0..data_len];
        pos += data_len;
        log.tx_hash = buf[pos..][0..32].*;
        pos += 32;

        out[i] = log;
    }
    return log_count;
}

// ── LZ4 entry helpers ────────────────────────────────────────────────────
// Every block in blocks.dat is stored as: lz4_len(u32 LE) || lz4_data.
// LZ4 achieves ~1.74x on log data (80% random bytes: hashes, addresses).

/// Compress serialized log data into entry format. Returns total entry length.
pub fn compressEntry(serialized: []const u8, out: []u8) !usize {
    const compressed_len = try lz4.compressDefault(serialized, out[4..]);
    std.mem.writeInt(u32, out[0..4], @intCast(compressed_len), .little);
    return 4 + compressed_len;
}

/// Decompress an entry (lz4_len prefix + lz4 payload) into `out`.
/// Returns the decompressed slice.
pub fn decompressEntry(entry: []const u8, out: []u8) ![]const u8 {
    if (entry.len < 4) return error.InvalidEntry;
    const lz4_len: usize = std.mem.readInt(u32, entry[0..4], .little);
    if (4 + lz4_len > entry.len) return error.InvalidEntry;
    const dlen = try lz4.decompressSafe(entry[4..][0..lz4_len], out);
    return out[0..dlen];
}

// ── Bloom builders ───────────────────────────────────────────────────────
// Built during import (engine) and during filtered index build (sdk).
// Topic bloom: insert topic0 of each log. Address bloom: insert emitting address.

const bloom = @import("bloom.zig");

pub fn buildTopicBloom(logs: []const RawLog) bloom.Bloom {
    var b = bloom.Bloom.init();
    for (logs) |log| {
        if (log.topic_count > 0) b.insert(log.topics[0]);
    }
    return b;
}

pub fn buildAddrBloom(logs: []const RawLog) bloom.AddrBloom {
    var b = bloom.AddrBloom.init();
    for (logs) |log| {
        b.insert(bloom.AddrBloom.addrToBloomKey(log.address));
    }
    return b;
}

// ── Tests ────────────────────────────────────────────────────────────────

test "serializeLogs/deserializeLogs roundtrip" {
    const data1 = [_]u8{ 0xAA, 0xBB, 0xCC };
    const data2 = [_]u8{0xFF} ** 64;

    const logs = [_]RawLog{
        .{
            .block_number = 100,
            .tx_index = 5,
            .log_index = 0,
            .address = [_]u8{0x11} ** 20,
            .topic_count = 0,
            .topics = std.mem.zeroes([types.MAX_TOPICS][32]u8),
            .data = &data1,
            .tx_hash = [_]u8{0x22} ** 32,
        },
        .{
            .block_number = 100,
            .tx_index = 5,
            .log_index = 1,
            .address = [_]u8{0x33} ** 20,
            .topic_count = 1,
            .topics = .{ [_]u8{0xAA} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
            .data = &.{},
            .tx_hash = [_]u8{0x44} ** 32,
        },
        .{
            .block_number = 100,
            .tx_index = 7,
            .log_index = 2,
            .address = [_]u8{0x55} ** 20,
            .topic_count = 3,
            .topics = .{ [_]u8{0xBB} ** 32, [_]u8{0xCC} ** 32, [_]u8{0xDD} ** 32, [_]u8{0} ** 32 },
            .data = &data2,
            .tx_hash = [_]u8{0x66} ** 32,
        },
    };

    var buf: [4096]u8 = undefined;
    const written = serializeLogs(&logs, &buf);
    try std.testing.expect(written > 4);

    var out: [16]RawLog = undefined;
    const count = deserializeLogs(buf[0..written], &out);
    try std.testing.expectEqual(@as(usize, 3), count);

    for (0..count) |i| {
        try std.testing.expectEqual(logs[i].tx_index, out[i].tx_index);
        try std.testing.expectEqual(logs[i].log_index, out[i].log_index);
        try std.testing.expectEqualSlices(u8, &logs[i].address, &out[i].address);
        try std.testing.expectEqual(logs[i].topic_count, out[i].topic_count);
        try std.testing.expectEqualSlices(u8, logs[i].data, out[i].data);
        try std.testing.expectEqualSlices(u8, &logs[i].tx_hash, &out[i].tx_hash);
        for (0..logs[i].topic_count) |t| {
            try std.testing.expectEqualSlices(u8, &logs[i].topics[t], &out[i].topics[t]);
        }
        try std.testing.expectEqual(@as(u64, 0), out[i].block_number);
    }
}

test "compressEntry/decompressEntry roundtrip" {
    const raw = "hello world, this is a test of lz4 compression in the log serial module!";
    var compressed: [4096]u8 = undefined;
    const entry_len = try compressEntry(raw, &compressed);
    try std.testing.expect(entry_len > 4);

    var decompressed: [4096]u8 = undefined;
    const result = try decompressEntry(compressed[0..entry_len], &decompressed);
    try std.testing.expectEqualSlices(u8, raw, result);
}

test "buildTopicBloom" {
    const topic = [_]u8{0xdd} ++ [_]u8{0xf2} ++ [_]u8{0x52} ++ [_]u8{0xad} ++ [_]u8{0} ** 28;
    const other = [_]u8{0x8c} ++ [_]u8{0x5b} ++ [_]u8{0xe1} ++ [_]u8{0xe5} ++ [_]u8{0} ** 28;

    const logs = [_]RawLog{.{
        .block_number = 100,
        .tx_index = 0,
        .log_index = 0,
        .address = [_]u8{0} ** 20,
        .topic_count = 1,
        .topics = .{ topic, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0} ** 32,
    }};

    const tb = buildTopicBloom(&logs);
    try std.testing.expect(tb.mightContain(topic));
    try std.testing.expect(!tb.mightContain(other));
}
