/// Derived presentation helpers.
const std = @import("std");

const core = @import("core");

const MERGE_BLOCK = core.types.MERGE_BLOCK;
const MERGE_TIMESTAMP: u64 = 1_663_224_162;

/// Approximate block timestamp from block number alone (`MERGE_TS + n*12`).
/// This is the FALLBACK used when no `timestamps.bin` is present. It is not
/// exact: 12s is the *slot* interval, but missed slots make block number lag
/// slot number, so the estimate drifts by the accumulated miss count (~10
/// days at the chain tip, ~0.75% of slots). Pre-merge it is rougher still
/// (~13s mean). For exact times the engine writes `timestamps.bin` and
/// `timestampOf` prefers it. Saturating subtract guards overflow at block 0.
pub fn blockTimestamp(block_number: u64) u64 {
    if (block_number >= MERGE_BLOCK) {
        return MERGE_TIMESTAMP + (block_number - MERGE_BLOCK) * 12;
    }
    return MERGE_TIMESTAMP -| (MERGE_BLOCK - block_number) * 13;
}

/// Exact block timestamp from the engine's `timestamps.bin` when the context
/// carries a reader and the block is covered, else the `blockTimestamp`
/// approximation. Comptime-guarded so counter-shaped test contexts (which have
/// no `_timestamps` field) compile straight to the formula path.
pub fn timestampOf(ctx: anytype, block_number: u64) u64 {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime @hasField(T, "_timestamps")) {
        if (ctx._timestamps) |*r| return r.get(block_number) orelse blockTimestamp(block_number);
    }
    return blockTimestamp(block_number);
}

// ── Tests ────────────────────────────────────────────────────────────────

test "post-merge timestamp matches consensus formula" {
    try std.testing.expectEqual(MERGE_TIMESTAMP, blockTimestamp(MERGE_BLOCK));
    try std.testing.expectEqual(MERGE_TIMESTAMP + 12, blockTimestamp(MERGE_BLOCK + 1));
    try std.testing.expectEqual(MERGE_TIMESTAMP + 12_000, blockTimestamp(MERGE_BLOCK + 1000));
}

test "pre-merge approximation is monotonic" {
    const a = blockTimestamp(MERGE_BLOCK - 1000);
    const b = blockTimestamp(MERGE_BLOCK - 500);
    const c = blockTimestamp(MERGE_BLOCK - 1);
    try std.testing.expect(a < b);
    try std.testing.expect(b < c);
    try std.testing.expect(c < MERGE_TIMESTAMP);
}

test "block zero saturates without overflow" {
    const ts = blockTimestamp(0);
    try std.testing.expect(ts == 0 or ts < MERGE_TIMESTAMP);
}

test "timestampOf prefers the reader and falls back to the formula" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // timestamps.bin covering blocks 100..101: 100 -> real ts, 101 -> unknown (0).
    {
        var f = try tmp.dir.createFile(core.timestamps.FILE_NAME, .{});
        defer f.close();
        var hdr: [core.timestamps.HEADER_SIZE]u8 = undefined;
        @memcpy(hdr[0..8], &core.timestamps.MAGIC);
        std.mem.writeInt(u64, hdr[8..16], 100, .little);
        std.mem.writeInt(u64, hdr[16..24], 2, .little);
        try f.writeAll(&hdr);
        var e: [4]u8 = undefined;
        std.mem.writeInt(u32, &e, 1_700_000_000, .little);
        try f.writeAll(&e);
        std.mem.writeInt(u32, &e, 0, .little);
        try f.writeAll(&e);
    }
    var r = (try core.TimestampReader.open(tmp.dir)).?;
    defer r.deinit();

    var ctx = struct { _timestamps: ?core.TimestampReader }{ ._timestamps = r };
    try std.testing.expectEqual(@as(u64, 1_700_000_000), timestampOf(&ctx, 100)); // covered
    try std.testing.expectEqual(blockTimestamp(101), timestampOf(&ctx, 101)); // zero -> formula
    try std.testing.expectEqual(blockTimestamp(999), timestampOf(&ctx, 999)); // out of range -> formula

    // A context without the field compiles straight to the formula path.
    var counter = struct { n: u32 }{ .n = 0 };
    try std.testing.expectEqual(blockTimestamp(100), timestampOf(&counter, 100));
}

/// Zero-alloc fixed-point renderer for token amounts. Pair with a `decimals`
/// pulled from `ctx.ethCall(u8, token, "decimals()")`. Trailing fractional
/// zeros and the decimal point itself are omitted when redundant, so
/// `amount(2_000…000, 18)` prints `"2"` rather than `"2.000000000000000000"`.
pub const Amount = struct {
    value: u256,
    decimals: u8,

    pub fn format(self: Amount, writer: anytype) !void {
        if (self.decimals == 0) {
            try writer.print("{d}", .{self.value});
            return;
        }
        const scale = pow10(self.decimals);
        const integer_part = self.value / scale;
        const fractional_part = self.value % scale;

        try writer.print("{d}", .{integer_part});
        if (fractional_part == 0) return;

        // Render fractional left-padded to `decimals` width, then trim
        // trailing zeros. 78 digits covers u256.MAX (10^78 > 2^256).
        var buf: [78]u8 = undefined;
        const slice = buf[0..self.decimals];
        var remainder = fractional_part;
        var i: usize = self.decimals;
        while (i > 0) {
            i -= 1;
            slice[i] = '0' + @as(u8, @intCast(remainder % 10));
            remainder /= 10;
        }
        var end = self.decimals;
        while (end > 0 and slice[end - 1] == '0') end -= 1;
        try writer.writeAll(".");
        try writer.writeAll(slice[0..end]);
    }
};

pub fn amount(value: u256, decimals: u8) Amount {
    return .{ .value = value, .decimals = decimals };
}

fn pow10(exp: u8) u256 {
    var p: u256 = 1;
    var i: u8 = 0;
    while (i < exp) : (i += 1) p *= 10;
    return p;
}

fn formatAmount(a: Amount, buf: []u8) ![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    var writer = stream.writer();
    try a.format(&writer);
    return stream.getWritten();
}

test "Amount formats 1.5 ETH at 18 decimals" {
    var buf: [80]u8 = undefined;
    try std.testing.expectEqualStrings(
        "1.5",
        try formatAmount(amount(1_500_000_000_000_000_000, 18), &buf),
    );
}

test "Amount formats 12.345678 USDC at 6 decimals" {
    var buf: [80]u8 = undefined;
    try std.testing.expectEqualStrings(
        "12.345678",
        try formatAmount(amount(12_345_678, 6), &buf),
    );
}

test "Amount strips redundant trailing zeros and decimal point" {
    var buf: [80]u8 = undefined;
    try std.testing.expectEqualStrings(
        "2",
        try formatAmount(amount(2_000_000_000_000_000_000, 18), &buf),
    );
}

test "Amount renders zero without a decimal point" {
    var buf: [80]u8 = undefined;
    try std.testing.expectEqualStrings("0", try formatAmount(amount(0, 18), &buf));
}

test "Amount renders the smallest representable fraction" {
    var buf: [80]u8 = undefined;
    try std.testing.expectEqualStrings(
        "0.000000000000000001",
        try formatAmount(amount(1, 18), &buf),
    );
}

test "Amount with decimals=0 falls through to integer rendering" {
    var buf: [80]u8 = undefined;
    try std.testing.expectEqualStrings("42", try formatAmount(amount(42, 0), &buf));
}

test "Amount handles u256 values near the type limit" {
    var buf: [80]u8 = undefined;
    // 10**30 (above u64::MAX) at 18 decimals → "1000000000000".
    const v: u256 = 1_000_000_000_000_000_000_000_000_000_000;
    try std.testing.expectEqualStrings(
        "1000000000000",
        try formatAmount(amount(v, 18), &buf),
    );
}
