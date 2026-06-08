/// Decode Nethermind CompactReceiptStore format (0x7F marker + compact RLP).
/// Extracts log entries from receipt byte arrays.
const std = @import("std");

const core = @import("core");

const Rlp = @import("rlp.zig").Rlp;

const types = core.types;

const COMPACT_MARKER: u8 = 0x7F;

/// Decode all logs from a Nethermind CompactReceiptStore value.
/// Topics right-padded from zero-stripped form. Data reconstructed from
/// (zero_prefix, data_remainder). tx_hash zeroed: Nethermind strips it from
/// compact format to save space.
pub fn decodeReceipts(
    block_number: u64,
    value: []const u8,
    log_buf: []types.RawLog,
    data_buf: []u8,
) !usize {
    if (value.len == 0) return 0;
    if (value[0] == 0xC0) return 0;
    if (value[0] != COMPACT_MARKER) return 0;

    var rlp = Rlp.init(value[1..]);
    _ = try rlp.enterList();

    var log_count: usize = 0;
    var tx_index: u16 = 0;
    var data_pos: usize = 0;
    var global_log_index: u16 = 0;

    while (!rlp.done()) {
        const receipt_end = try rlp.enterList();

        // Skip: status, sender, gas_used_total
        inline for (0..3) |_| try rlp.skip();

        const logs_end = try rlp.enterList();

        while (rlp.pos < logs_end) {
            if (log_count >= log_buf.len) return error.TooManyLogsInBlock;

            _ = try rlp.enterList();

            const addr_raw = try rlp.bytes();
            const address: [20]u8 = if (addr_raw.len == 20) addr_raw[0..20].* else std.mem.zeroes([20]u8);

            // Nethermind strips leading zeros from topics. Right-align to 32.
            const topics_end = try rlp.enterList();
            var topics: [types.MAX_TOPICS][32]u8 = undefined;
            var topic_count: u8 = 0;
            while (rlp.pos < topics_end) {
                const stripped = try rlp.bytes();
                if (topic_count < types.MAX_TOPICS)
                    topics[topic_count] = Rlp.padLeft(32, stripped);
                topic_count += 1;
            }
            topic_count = @min(topic_count, types.MAX_TOPICS);

            // Data: zero_prefix zero bytes ++ data_remainder
            const zero_prefix: usize = @intCast(try rlp.uint());
            const data_rem = try rlp.bytes();
            const data_len = zero_prefix + data_rem.len;

            if (data_len > 0 and data_pos + data_len > data_buf.len)
                return error.Overflow;

            var data_slice: []const u8 = &.{};
            if (data_len > 0) {
                @memset(data_buf[data_pos..][0..zero_prefix], 0);
                @memcpy(data_buf[data_pos + zero_prefix ..][0..data_rem.len], data_rem);
                data_slice = data_buf[data_pos..][0..data_len];
                data_pos += data_len;
            }

            log_buf[log_count] = .{
                .block_number = block_number,
                .log_index = global_log_index,
                .tx_index = tx_index,
                .address = address,
                .topic_count = topic_count,
                .topics = topics,
                .data = data_slice,
                .tx_hash = std.mem.zeroes([32]u8),
            };

            log_count += 1;
            global_log_index += 1;
        }

        rlp.pos = receipt_end;
        tx_index += 1;
    }

    return log_count;
}

// ── Tests ────────────────────────────────────────────────────────────────

/// Build compact receipt test data at comptime.
fn buildTestReceipt(comptime build_fn: fn (*TestBuf) void) []const u8 {
    comptime {
        var buf = TestBuf{};
        build_fn(&buf);
        const result: [buf.len]u8 = buf.data[0..buf.len].*;
        return &result;
    }
}

const TestBuf = struct {
    data: [2048]u8 = undefined,
    len: usize = 0,

    fn put(self: *TestBuf, b: []const u8) void {
        @memcpy(self.data[self.len..][0..b.len], b);
        self.len += b.len;
    }
    fn putByte(self: *TestBuf, b: u8) void {
        self.data[self.len] = b;
        self.len += 1;
    }
    fn rlpStr(self: *TestBuf, d: []const u8) void {
        if (d.len == 0) { self.putByte(0x80); } else if (d.len == 1 and d[0] < 0x80) { self.putByte(d[0]); } else if (d.len <= 55) { self.putByte(@intCast(0x80 + d.len)); self.put(d); } else { self.putByte(0xB8); self.putByte(@intCast(d.len)); self.put(d); }
    }
    fn listHdr(self: *TestBuf, content_len: usize) void {
        if (content_len <= 55) { self.putByte(@intCast(0xC0 + content_len)); } else { self.putByte(0xF8); self.putByte(@intCast(content_len)); }
    }
    fn rlpStrLen(d: []const u8) usize {
        if (d.len == 0) return 1;
        if (d.len == 1 and d[0] < 0x80) return 1;
        if (d.len <= 55) return 1 + d.len;
        return 2 + d.len;
    }
    fn listHdrLen(content_len: usize) usize {
        return if (content_len <= 55) 1 else 2;
    }
};

test "empty receipt returns zero logs" {
    var log_buf: [10]types.RawLog = undefined;
    var data_buf: [256]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 0), try decodeReceipts(0, &.{0xC0}, &log_buf, &data_buf));
}

test "non-compact receipt is skipped" {
    var log_buf: [10]types.RawLog = undefined;
    var data_buf: [256]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 0), try decodeReceipts(100, &.{ 0xF8, 0x01, 0xC0 }, &log_buf, &data_buf));
}

test "compact receipt with one log decodes correctly" {
    const addr: [20]u8 = .{0xAA} ** 20;
    const topic0: [32]u8 = .{0xDD} ** 32;
    const from_stripped: [20]u8 = .{0xCC} ** 20;
    const to_stripped: [20]u8 = .{0xEE} ** 20;

    const value = comptime buildTestReceipt(struct {
        fn build(b: *TestBuf) void {
            const topics_len = TestBuf.rlpStrLen(&topic0) + TestBuf.rlpStrLen(&from_stripped) + TestBuf.rlpStrLen(&to_stripped);
            const log_len = TestBuf.rlpStrLen(&addr) + TestBuf.listHdrLen(topics_len) + topics_len + 1 + 1;
            const logs_len = TestBuf.listHdrLen(log_len) + log_len;
            const sender: [20]u8 = .{0x55} ** 20;
            const rcpt_len = 1 + TestBuf.rlpStrLen(&sender) + 1 + TestBuf.listHdrLen(logs_len) + logs_len;
            const outer_len = TestBuf.listHdrLen(rcpt_len) + rcpt_len;

            b.putByte(0x7F);
            b.listHdr(outer_len);
            b.listHdr(rcpt_len);
            b.putByte(0x01);
            b.rlpStr(&sender);
            b.putByte(0x01);
            b.listHdr(logs_len);
            b.listHdr(log_len);
            b.rlpStr(&addr);
            b.listHdr(topics_len);
            b.rlpStr(&topic0);
            b.rlpStr(&from_stripped);
            b.rlpStr(&to_stripped);
            b.putByte(0x80);
            b.putByte(0x42);
        }
    }.build);

    var log_buf: [10]types.RawLog = undefined;
    var data_buf: [512]u8 = undefined;
    const count = try decodeReceipts(42, value, &log_buf, &data_buf);

    try std.testing.expectEqual(@as(usize, 1), count);
    const log = log_buf[0];
    try std.testing.expectEqual(@as(u64, 42), log.block_number);
    try std.testing.expectEqual(@as(u16, 0), log.tx_index);
    try std.testing.expectEqual(@as(u16, 0), log.log_index);
    try std.testing.expectEqual(@as(u8, 3), log.topic_count);
    try std.testing.expectEqualSlices(u8, &addr, &log.address);
    try std.testing.expectEqualSlices(u8, &topic0, &log.topics[0]);

    var expected_from: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(expected_from[12..], &from_stripped);
    try std.testing.expectEqualSlices(u8, &expected_from, &log.topics[1]);
}

test "zero-stripped data reconstructs correctly" {
    const addr: [20]u8 = .{0xBB} ** 20;
    const topic0: [32]u8 = .{0xFF} ** 32;

    const value = comptime buildTestReceipt(struct {
        fn build(b: *TestBuf) void {
            const zero_pfx: [1]u8 = .{30};
            const data_rem: [2]u8 = .{ 0x03, 0xE8 };
            const topics_len = TestBuf.rlpStrLen(&topic0);
            const log_len = TestBuf.rlpStrLen(&addr) + TestBuf.listHdrLen(topics_len) + topics_len + TestBuf.rlpStrLen(&zero_pfx) + TestBuf.rlpStrLen(&data_rem);
            const logs_len = TestBuf.listHdrLen(log_len) + log_len;
            const sender: [20]u8 = .{0x55} ** 20;
            const rcpt_len = 1 + TestBuf.rlpStrLen(&sender) + 1 + TestBuf.listHdrLen(logs_len) + logs_len;
            const outer_len = TestBuf.listHdrLen(rcpt_len) + rcpt_len;

            b.putByte(0x7F);
            b.listHdr(outer_len);
            b.listHdr(rcpt_len);
            b.putByte(0x01);
            b.rlpStr(&sender);
            b.putByte(0x01);
            b.listHdr(logs_len);
            b.listHdr(log_len);
            b.rlpStr(&addr);
            b.listHdr(topics_len);
            b.rlpStr(&topic0);
            b.rlpStr(&zero_pfx);
            b.rlpStr(&data_rem);
        }
    }.build);

    var log_buf: [10]types.RawLog = undefined;
    var data_buf: [512]u8 = undefined;
    const count = try decodeReceipts(99, value, &log_buf, &data_buf);

    try std.testing.expectEqual(@as(usize, 1), count);
    const log = log_buf[0];
    try std.testing.expectEqual(@as(usize, 32), log.data.len);
    for (log.data[0..30]) |b| try std.testing.expectEqual(@as(u8, 0), b);
    try std.testing.expectEqual(@as(u8, 0x03), log.data[30]);
    try std.testing.expectEqual(@as(u8, 0xE8), log.data[31]);
}
