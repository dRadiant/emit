//! `to`/`value` extraction from raw signed transactions as they appear in a
//! block body's transaction list. Legacy txs are RLP lists, typed envelopes
//! (EIP-2718) are type byte ‖ rlp list. Field positions are fixed per type,
//! so extraction is a skip-walk over the existing `rlp.zig` cursor. No field
//! materialization, no allocation, no signature work. Senders come from the
//! receipt rows (ADR-006), never from recovery here.
const std = @import("std");

const Rlp = @import("rlp.zig").Rlp;

pub const Error = error{ UnknownTxType, Malformed } || @import("rlp.zig").Error;

pub const TxFields = struct {
    tx_type: u8,
    /// Null = contract creation (empty `to` on the wire).
    to: ?[20]u8,
    value: u256,
};

/// Items to skip before `to`, per envelope type.
///   legacy        nonce, gas_price, gas
///   0x01 (2930)   chain_id, nonce, gas_price, gas
///   0x02 (1559)   chain_id, nonce, max_priority_fee, max_fee, gas
///   0x03 (4844)   same head as 1559, blob fields trail `data`
///   0x04 (7702)   same head as 1559, authorization_list trails
/// A new fork type fails loud here. Add its row, nothing else changes.
fn skipsBeforeTo(tx_type: u8) Error!usize {
    return switch (tx_type) {
        0x00 => 3,
        0x01 => 4,
        0x02, 0x03, 0x04 => 5,
        else => error.UnknownTxType,
    };
}

/// Decode one transaction item: a legacy tx (RLP list bytes, first byte
/// ≥ 0xC0) or a typed envelope payload (type ‖ rlp list, first byte ≤ 0x7F).
/// `BodyTxs.next` yields items in exactly this shape.
pub fn decode(raw: []const u8) Error!TxFields {
    if (raw.len == 0) return error.Malformed;

    var tx_type: u8 = 0;
    var body = raw;
    if (raw[0] >= 0x01 and raw[0] <= 0x7F) {
        tx_type = raw[0];
        body = raw[1..];
    } else if (raw[0] < 0xC0) {
        // A string prefix means the caller passed the enclosing RLP string
        // instead of its payload. 0x00 is not a valid envelope type.
        return error.Malformed;
    }

    var rlp = Rlp.init(body);
    _ = try rlp.enterList();
    for (0..try skipsBeforeTo(tx_type)) |_| try rlp.skip();

    const to_raw = try rlp.bytes();
    const to: ?[20]u8 = switch (to_raw.len) {
        0 => null,
        20 => to_raw[0..20].*,
        else => return error.Malformed,
    };

    const value_raw = try rlp.bytes();
    if (value_raw.len > 32) return error.Malformed;
    const padded = Rlp.padLeft(32, value_raw);
    return .{
        .tx_type = tx_type,
        .to = to,
        .value = std.mem.readInt(u256, &padded, .big),
    };
}

/// Forward iterator over the transactions list of a full block RLP
/// (`[header, [txs…], [ommers…], …]`, the Nethermind blocks-DB value shape).
/// Yields each tx item as decode-ready bytes: the list slice for legacy txs,
/// the string payload for typed envelopes.
pub const BodyTxs = struct {
    rlp: Rlp,
    end: usize,

    pub fn init(block_rlp: []const u8) Error!BodyTxs {
        var rlp = Rlp.init(block_rlp);
        _ = try rlp.enterList();
        try rlp.skip(); // header
        const end = try rlp.enterList();
        return .{ .rlp = rlp, .end = end };
    }

    pub fn next(self: *BodyTxs) Error!?[]const u8 {
        if (self.rlp.pos >= self.end) return null;
        if (self.rlp.data[self.rlp.pos] >= 0xC0) {
            const start = self.rlp.pos;
            try self.rlp.skip();
            return self.rlp.data[start..self.rlp.pos];
        }
        return try self.rlp.bytes();
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

const TO = [_]u8{0xAA} ** 20;

/// Legacy tx list: [nonce=1, gas_price=2, gas=3, to, value, data="", v, r, s].
fn legacyTx(comptime to_item: []const u8, comptime value_item: []const u8) []const u8 {
    const body = [_]u8{ 0x01, 0x02, 0x03 } ++ to_item[0..to_item.len].* ++
        value_item[0..value_item.len].* ++ [_]u8{ 0x80, 0x1B, 0x01, 0x01 };
    return &(listHdr(body.len) ++ body);
}

/// Typed payload: type ‖ [head_items…, to, value, data="", trailing…].
fn typedTx(comptime tx_type: u8, comptime head: []const u8, comptime trailing: []const u8) []const u8 {
    const to_item = [_]u8{0x94} ++ TO;
    const value_item = [_]u8{ 0x82, 0x01, 0x02 }; // 0x0102
    const body = head[0..head.len].* ++ to_item ++ value_item ++
        [_]u8{0x80} ++ trailing[0..trailing.len].*;
    return &([_]u8{tx_type} ++ listHdr(body.len) ++ body);
}

fn listHdr(comptime len: usize) [if (len <= 55) 1 else 2]u8 {
    if (len <= 55) return .{0xC0 + len};
    return .{ 0xF8, len };
}

test "legacy tx decodes to/value at positions 3/4" {
    const raw = comptime legacyTx(&([_]u8{0x94} ++ TO), &[_]u8{ 0x82, 0x01, 0x02 });
    const f = try decode(raw);
    try testing.expectEqual(@as(u8, 0x00), f.tx_type);
    try testing.expectEqualSlices(u8, &TO, &f.to.?);
    try testing.expectEqual(@as(u256, 0x0102), f.value);
}

test "legacy contract creation has null to" {
    const raw = comptime legacyTx(&[_]u8{0x80}, &[_]u8{0x05});
    const f = try decode(raw);
    try testing.expectEqual(@as(?[20]u8, null), f.to);
    try testing.expectEqual(@as(u256, 5), f.value);
}

test "eip-2930 decodes after four head items" {
    // [chain=1, nonce=2, gas_price=3, gas=4, to, value, data, access=[], y, r, s]
    const raw = comptime typedTx(0x01, &[_]u8{ 0x01, 0x02, 0x03, 0x04 }, &[_]u8{ 0xC0, 0x80, 0x01, 0x01 });
    const f = try decode(raw);
    try testing.expectEqual(@as(u8, 0x01), f.tx_type);
    try testing.expectEqualSlices(u8, &TO, &f.to.?);
    try testing.expectEqual(@as(u256, 0x0102), f.value);
}

test "eip-1559, 4844, and 7702 decode after five head items" {
    // 1559: [chain, nonce, max_pri, max_fee, gas, to, value, data, access, y, r, s]
    // 4844 adds blob fields after access, 7702 an authorization list. Both
    // trail `value`, so the same head applies. Trailing items vary per type.
    const head = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05 };
    inline for (.{ 0x02, 0x03, 0x04 }) |t| {
        const raw = comptime typedTx(t, &head, &[_]u8{ 0xC0, 0xC0, 0x80, 0x01, 0x01 });
        const f = try decode(raw);
        try testing.expectEqual(@as(u8, t), f.tx_type);
        try testing.expectEqualSlices(u8, &TO, &f.to.?);
        try testing.expectEqual(@as(u256, 0x0102), f.value);
    }
}

test "32-byte value round-trips as u256" {
    const value_bytes = [_]u8{0xFF} ** 32;
    const raw = comptime legacyTx(&([_]u8{0x94} ++ TO), &([_]u8{0xA0} ++ value_bytes));
    const f = try decode(raw);
    try testing.expectEqual(std.math.maxInt(u256), f.value);
}

test "unknown envelope type fails loud" {
    try testing.expectError(error.UnknownTxType, decode(&[_]u8{ 0x05, 0xC3, 0x01, 0x02, 0x03 }));
}

test "string-prefixed input is rejected, not misparsed" {
    // The enclosing RLP string of a typed tx, not its payload.
    try testing.expectError(error.Malformed, decode(&[_]u8{ 0x83, 0x02, 0xC1, 0x01 }));
}

test "BodyTxs walks a block's tx list yielding decode-ready items" {
    // Block: [header=[], [legacy, typed], ommers=[]]
    const legacy_s = comptime legacyTx(&([_]u8{0x94} ++ TO), &[_]u8{0x07});
    const typed_s = comptime typedTx(0x02, &[_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05 }, &[_]u8{ 0xC0, 0x80, 0x01, 0x01 });
    const legacy = legacy_s[0..legacy_s.len].*;
    const typed = typed_s[0..typed_s.len].*;
    const typed_item = [_]u8{0x80 + typed.len} ++ typed; // typed tx rides as an RLP string
    const txs_body = legacy ++ typed_item;
    const content = [_]u8{0xC0} ++ listHdr(txs_body.len) ++ txs_body ++ [_]u8{0xC0};
    const block = listHdr(content.len) ++ content;

    var it = try BodyTxs.init(&block);
    const f0 = try decode((try it.next()).?);
    try testing.expectEqual(@as(u8, 0x00), f0.tx_type);
    try testing.expectEqual(@as(u256, 7), f0.value);
    const f1 = try decode((try it.next()).?);
    try testing.expectEqual(@as(u8, 0x02), f1.tx_type);
    try testing.expectEqual(@as(u256, 0x0102), f1.value);
    try testing.expectEqual(@as(?[]const u8, null), try it.next());
}
