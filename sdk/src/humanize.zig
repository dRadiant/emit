/// Derived presentation helpers.
const std = @import("std");

const core = @import("core");

const MERGE_BLOCK = core.types.MERGE_BLOCK;
const MERGE_TIMESTAMP: u64 = 1_663_224_162;

/// Post-merge block times are exactly 12 seconds (Gasper consensus
/// invariant). Pre-merge varied 1-30s with a ~13s mean and is approximated
/// here, accumulating drift over millions of blocks. Saturating subtract
/// guards against overflow at block 0.
///
/// TODO: For protocols with pre-merge history that need precise
/// timestamps, add a `timestamps.bin` (u64 per block, ~75 MB at chain
/// tip) populated at import time. Out of scope for v1.
pub fn blockTimestamp(block_number: u64) u64 {
    if (block_number >= MERGE_BLOCK) {
        return MERGE_TIMESTAMP + (block_number - MERGE_BLOCK) * 12;
    }
    return MERGE_TIMESTAMP -| (MERGE_BLOCK - block_number) * 13;
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
