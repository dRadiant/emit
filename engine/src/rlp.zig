/// Minimal RLP decoder for walking Ethereum receipt structures.
/// Cursor over an input byte slice — no allocations, no copying.
/// Handles Nethermind's CompactReceiptStore format (0x7F marker + compact RLP).
const std = @import("std");

pub const Error = error{
    InvalidPrefix,
    Truncated,
};

pub const Rlp = struct {
    data: []const u8,
    pos: usize,

    pub fn init(data: []const u8) Rlp {
        return .{ .data = data, .pos = 0 };
    }

    pub fn done(self: *const Rlp) bool {
        return self.pos >= self.data.len;
    }

    /// Enter an RLP list. Returns the end position (caller iterates until pos reaches it).
    pub fn enterList(self: *Rlp) Error!usize {
        if (self.pos >= self.data.len) return error.Truncated;
        const prefix = self.data[self.pos];
        self.pos += 1;

        // Short list: length in prefix byte
        if (prefix >= 0xC0 and prefix <= 0xF7) {
            return self.pos + @as(usize, prefix - 0xC0);
        }
        // Long list: length-of-length follows
        if (prefix >= 0xF8) {
            const ll = @as(usize, prefix - 0xF7);
            if (self.pos + ll > self.data.len) return error.Truncated;
            const len = readBE(self.data[self.pos..][0..ll]);
            self.pos += ll;
            return self.pos + len;
        }
        return error.InvalidPrefix;
    }

    /// Decode a byte string. Returns a slice into the input (zero-copy).
    pub fn bytes(self: *Rlp) Error![]const u8 {
        if (self.pos >= self.data.len) return error.Truncated;
        const prefix = self.data[self.pos];
        self.pos += 1;

        // Single byte < 0x80 is its own encoding
        if (prefix < 0x80) return self.data[self.pos - 1 ..][0..1];
        if (prefix == 0x80) return &.{};
        // Short string
        if (prefix <= 0xB7) {
            const len = @as(usize, prefix - 0x80);
            if (self.pos + len > self.data.len) return error.Truncated;
            const result = self.data[self.pos..][0..len];
            self.pos += len;
            return result;
        }
        // Long string
        if (prefix <= 0xBF) {
            const ll = @as(usize, prefix - 0xB7);
            if (self.pos + ll > self.data.len) return error.Truncated;
            const len = readBE(self.data[self.pos..][0..ll]);
            self.pos += ll;
            if (self.pos + len > self.data.len) return error.Truncated;
            const result = self.data[self.pos..][0..len];
            self.pos += len;
            return result;
        }
        return error.InvalidPrefix;
    }

    /// Decode a big-endian unsigned integer from a stripped byte string.
    pub fn uint(self: *Rlp) Error!u64 {
        const b = try self.bytes();
        return readBE(b);
    }

    /// Skip one RLP item (string or list) without decoding.
    pub fn skip(self: *Rlp) Error!void {
        if (self.pos >= self.data.len) return error.Truncated;
        const prefix = self.data[self.pos];
        if (prefix < 0xC0) {
            _ = try self.bytes();
        } else {
            self.pos = try self.enterList();
        }
    }

    /// Reconstruct a zero-stripped value into a fixed-size array.
    /// Topics are 32 bytes, addresses 20 bytes — Nethermind strips leading zeros.
    pub fn padLeft(comptime N: usize, stripped: []const u8) [N]u8 {
        var out = std.mem.zeroes([N]u8);
        if (stripped.len <= N) @memcpy(out[N - stripped.len ..], stripped);
        return out;
    }

    fn readBE(b: []const u8) usize {
        return switch (b.len) {
            0 => 0,
            1 => b[0],
            2 => @as(usize, b[0]) << 8 | b[1],
            3 => @as(usize, b[0]) << 16 | @as(usize, b[1]) << 8 | b[2],
            else => {
                var r: usize = 0;
                for (b) |v| r = (r << 8) | v;
                return r;
            },
        };
    }
};

/// Extract the `timestamp` (field 11) from an RLP-encoded block header.
/// Header field order is fixed: parentHash, ommersHash, beneficiary,
/// stateRoot, txRoot, receiptsRoot, logsBloom, difficulty, number, gasLimit,
/// gasUsed, **timestamp**, extraData, ... so we enter the list and skip the
/// first 11 fields. Post-1559/Shanghai/Cancun fields trail the timestamp and
/// are irrelevant here.
pub fn headerTimestamp(header_rlp: []const u8) Error!u64 {
    var rlp = Rlp.init(header_rlp);
    _ = try rlp.enterList();
    var i: usize = 0;
    while (i < 11) : (i += 1) try rlp.skip();
    return rlp.uint();
}

// ── Tests ────────────────────────────────────────────────────────────────

test "empty list has zero length" {
    var rlp = Rlp.init(&.{0xC0});
    const end = try rlp.enterList();
    try std.testing.expect(rlp.pos == end);
}

test "short string decodes correctly" {
    var rlp = Rlp.init(&.{ 0x83, 0x64, 0x6F, 0x67 });
    const b = try rlp.bytes();
    try std.testing.expectEqualSlices(u8, "dog", b);
}

test "single byte below 0x80 is self-encoded" {
    var rlp = Rlp.init(&.{0x42});
    const b = try rlp.bytes();
    try std.testing.expectEqual(@as(u8, 0x42), b[0]);
}

test "0x80 decodes to empty bytes" {
    var rlp = Rlp.init(&.{0x80});
    const b = try rlp.bytes();
    try std.testing.expectEqual(@as(usize, 0), b.len);
}

test "uint decodes big-endian" {
    var rlp = Rlp.init(&.{ 0x82, 0x04, 0x00 });
    try std.testing.expectEqual(@as(u64, 1024), try rlp.uint());
}

test "skip advances past items" {
    var rlp = Rlp.init(&.{ 0xC5, 0x83, 0x64, 0x6F, 0x67, 0x42 });
    const end = try rlp.enterList();
    try rlp.skip();
    const b = try rlp.bytes();
    try std.testing.expectEqual(@as(u8, 0x42), b[0]);
    try std.testing.expectEqual(end, rlp.pos);
}

test "padLeft reconstructs zero-stripped topic" {
    const stripped = [_]u8{ 0xAB, 0xCD };
    const padded = Rlp.padLeft(32, &stripped);
    try std.testing.expectEqual(@as(u8, 0), padded[0]);
    try std.testing.expectEqual(@as(u8, 0), padded[29]);
    try std.testing.expectEqual(@as(u8, 0xAB), padded[30]);
    try std.testing.expectEqual(@as(u8, 0xCD), padded[31]);
}

test "headerTimestamp reads field 11" {
    // Synthetic header: 11 empty fields (0..10), then a 4-byte timestamp.
    // List payload = 11*1 + 5 = 16 bytes, so the list prefix is 0xC0+16 = 0xD0.
    const header = [_]u8{0xD0} ++ ([_]u8{0x80} ** 11) ++ [_]u8{ 0x84, 0x64, 0x32, 0x5a, 0x80 };
    try std.testing.expectEqual(@as(u64, 0x64325a80), try headerTimestamp(&header));
}
