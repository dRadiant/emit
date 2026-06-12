//! `to`/`value` extraction from raw signed transactions as they appear in a
//! block body's transaction list. Legacy txs are RLP lists, typed envelopes
//! (EIP-2718) are type byte ‖ rlp list. Field positions are fixed per type,
//! so extraction is a skip-walk over the existing `rlp.zig` cursor with no
//! field materialization.
//!
//! `decodeSigned` adds the sender-recovery inputs via the *splice sighash*
//! (ADR-006): a typed envelope's signing payload is its signed bytes with the
//! trailing `(y_parity, r, s)` dropped and the list header re-lengthened, so
//! the hash needs no per-type re-encoder and is shape-generic over future
//! types. Used when a receipt row carries no stored sender (Nethermind's
//! compact format leaves the slot empty).
const std = @import("std");

const eth = @import("eth");

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

pub const SignedTx = struct {
    fields: TxFields,
    sighash: [32]u8,
    /// Recovery-ready: `v` is the y-parity (0/1) `secp256k1.recover` expects.
    sig: eth.signature.Signature,
};

/// Generous bound on tx list items. The largest current envelope (4844) has
/// 14. A fork pushing past 16 fails loud here.
const MAX_TX_ITEMS = 16;

/// Decode fields plus the signing hash and signature for sender recovery.
/// `scratch` holds the rebuilt unsigned payload, sized ≥ raw.len + 16 and
/// 8-aligned (the XKCP keccak does u64 lane loads).
pub fn decodeSigned(raw: []const u8, scratch: []u8) Error!SignedTx {
    const fields = try decode(raw);
    const body = if (fields.tx_type != 0) raw[1..] else raw;

    var rlp = Rlp.init(body);
    const end = try rlp.enterList();
    const payload_start = rlp.pos;

    var starts: [MAX_TX_ITEMS]usize = undefined;
    var n: usize = 0;
    while (rlp.pos < end) {
        if (n >= MAX_TX_ITEMS) return error.Malformed;
        starts[n] = rlp.pos;
        try rlp.skip();
        n += 1;
    }
    if (n < 4) return error.Malformed;
    const sig_off = starts[n - 3];

    var sig_rlp = Rlp.init(body);
    sig_rlp.pos = sig_off;
    const v_raw = try sig_rlp.uint();
    const r_b = try sig_rlp.bytes();
    const s_b = try sig_rlp.bytes();
    if (r_b.len > 32 or s_b.len > 32) return error.Malformed;
    var sig = eth.signature.Signature{
        .r = Rlp.padLeft(32, r_b),
        .s = Rlp.padLeft(32, s_b),
        .v = 0,
    };

    const content = body[payload_start..sig_off];
    var hash_len: usize = 0;

    if (fields.tx_type != 0) {
        // Typed: keccak(type ‖ rlp([fields…])), the splice.
        if (v_raw > 1) return error.Malformed;
        sig.v = @intCast(v_raw);
        scratch[0] = fields.tx_type;
        const hdr = writeListHdr(scratch[1..], content.len);
        if (1 + hdr + content.len > scratch.len) return error.Malformed;
        @memcpy(scratch[1 + hdr ..][0..content.len], content);
        hash_len = 1 + hdr + content.len;
    } else {
        // Legacy: pre-155 hashes the six fields, EIP-155 appends
        // (chain_id, 0, 0) with chain_id derived from v.
        var suffix_buf: [11]u8 = undefined;
        var suffix: []const u8 = &.{};
        if (v_raw == 27 or v_raw == 28) {
            sig.v = @intCast(v_raw - 27);
        } else if (v_raw >= 35) {
            sig.v = @intCast((v_raw - 35) & 1);
            const sl = uintRlp(&suffix_buf, (v_raw - 35) >> 1);
            suffix_buf[sl] = 0x80;
            suffix_buf[sl + 1] = 0x80;
            suffix = suffix_buf[0 .. sl + 2];
        } else return error.Malformed;
        const hdr = writeListHdr(scratch, content.len + suffix.len);
        if (hdr + content.len + suffix.len > scratch.len) return error.Malformed;
        @memcpy(scratch[hdr..][0..content.len], content);
        @memcpy(scratch[hdr + content.len ..][0..suffix.len], suffix);
        hash_len = hdr + content.len + suffix.len;
    }

    return .{ .fields = fields, .sighash = eth.keccak.hash(scratch[0..hash_len]), .sig = sig };
}

/// Sender address from a recovered signature. Equivalent to eth.zig's
/// `recoverAddress`, but hashes an 8-aligned copy of the pubkey: the XKCP
/// keccak does u64 lane loads, and eth.zig feeds it `pubkey[1..]` (odd
/// offset), which UBSan rejects in Debug test builds.
pub fn recoverSender(sig: eth.signature.Signature, sighash: [32]u8) ![20]u8 {
    const pk = try eth.secp256k1.recover(sig, sighash);
    var xy: [64]u8 align(8) = undefined;
    @memcpy(&xy, pk[1..65]);
    const h = eth.keccak.hash(&xy);
    return h[12..32].*;
}

fn writeListHdr(buf: []u8, len: usize) usize {
    if (len <= 55) {
        buf[0] = 0xC0 + @as(u8, @intCast(len));
        return 1;
    }
    const nbytes: usize = (64 - @as(usize, @clz(@as(u64, @intCast(len)))) + 7) / 8;
    buf[0] = 0xF7 + @as(u8, @intCast(nbytes));
    for (0..nbytes) |i| buf[1 + i] = @truncate(len >> @intCast(8 * (nbytes - 1 - i)));
    return 1 + nbytes;
}

fn uintRlp(buf: []u8, v: u64) usize {
    if (v == 0) {
        buf[0] = 0x80;
        return 1;
    }
    if (v < 0x80) {
        buf[0] = @intCast(v);
        return 1;
    }
    const nbytes: usize = (64 - @as(usize, @clz(v)) + 7) / 8;
    buf[0] = 0x80 + @as(u8, @intCast(nbytes));
    for (0..nbytes) |i| buf[1 + i] = @truncate(v >> @intCast(8 * (nbytes - 1 - i)));
    return 1 + nbytes;
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

/// Test signer derivation through the same aligned-hash path as
/// `recoverSender`.
fn signerOf(priv: [32]u8) ![20]u8 {
    const pk = try eth.secp256k1.derivePublicKey(priv);
    var xy: [64]u8 align(8) = undefined;
    @memcpy(&xy, pk[1..65]);
    const h = eth.keccak.hash(&xy);
    return h[12..32].*;
}

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

test "decodeSigned recovers the signer eth.zig signed with (closed loop)" {
    // Independent implementations validate each other: eth.zig encodes and
    // signs, the splice sighash must reproduce the hash and recover the key.
    const priv = [_]u8{0x42} ** 31 ++ [_]u8{0x01};
    const signer = try signerOf(priv);
    var scratch: [1024]u8 align(8) = undefined;

    const cases = [_]eth.transaction.Transaction{
        .{ .legacy = .{ .nonce = 7, .gas_price = 30, .gas_limit = 21_000, .to = TO, .value = 12_345, .data = &.{ 0xAB, 0xCD }, .chain_id = 1 } },
        .{ .legacy = .{ .nonce = 7, .gas_price = 30, .gas_limit = 21_000, .to = TO, .value = 5, .data = &.{}, .chain_id = null } },
        .{ .eip2930 = .{ .chain_id = 1, .nonce = 9, .gas_price = 30, .gas_limit = 50_000, .to = TO, .value = 0, .data = &.{0x01}, .access_list = &.{} } },
        .{ .eip1559 = .{ .chain_id = 1, .nonce = 3, .max_priority_fee_per_gas = 2, .max_fee_per_gas = 100, .gas_limit = 21_000, .to = TO, .value = 1_000_000_000_000_000_000, .data = &.{}, .access_list = &.{} } },
    };
    for (cases) |tx| {
        const sighash = try eth.transaction.hashForSigning(testing.allocator, tx);
        const sig = try eth.secp256k1.sign(priv, sighash);
        // Wire v: typed carries the parity, legacy folds it into 27/28 or
        // the EIP-155 form.
        const wire_v: u8 = switch (tx) {
            .legacy => |l| if (l.chain_id) |cid| @intCast(35 + 2 * cid + sig.v) else 27 + sig.v,
            else => sig.v,
        };
        const raw = try eth.transaction.serializeSigned(testing.allocator, tx, sig.r, sig.s, wire_v);
        defer testing.allocator.free(raw);

        const st = try decodeSigned(raw, &scratch);
        try testing.expectEqualSlices(u8, &sighash, &st.sighash);
        const recovered = try recoverSender(st.sig, st.sighash);
        try testing.expectEqualSlices(u8, &signer, &recovered);
    }
}

test "decodeSigned recovers a hand-built EIP-7702 envelope (shape-generic)" {
    // eth.zig has no 7702 type. Build the envelope by hand: the splice must
    // still reproduce the signing payload because it never names the fields.
    // [chain, nonce, max_pri, max_fee, gas, to, value, data, access, auth_list]
    const priv = [_]u8{0x42} ** 31 ++ [_]u8{0x01};
    const signer = try signerOf(priv);

    const head = [_]u8{ 0x01, 0x07, 0x02, 0x64, 0x83, 0x01, 0x00, 0x00 }; // chain,nonce,pri,fee,gas(3B)
    const to_item = [_]u8{0x94} ++ TO;
    const tail = [_]u8{ 0x05, 0x80, 0xC0, 0xC0 }; // value=5, data="", access=[], auth=[]
    const payload = head ++ to_item ++ tail;

    var unsigned_buf: [64]u8 = undefined;
    unsigned_buf[0] = 0x04;
    unsigned_buf[1] = 0xC0 + @as(u8, payload.len);
    @memcpy(unsigned_buf[2..][0..payload.len], &payload);
    const sighash = eth.keccak.hash(unsigned_buf[0 .. 2 + payload.len]);
    const sig = try eth.secp256k1.sign(priv, sighash);

    // Signed envelope: 0x04 ‖ rlp([fields…, y_parity, r, s]). The content
    // (~100 B) needs the long-form list header.
    var signed_buf: [160]u8 = undefined;
    signed_buf[0] = 0x04;
    signed_buf[1] = 0xF8;
    var pos: usize = 3;
    @memcpy(signed_buf[pos..][0..payload.len], &payload);
    pos += payload.len;
    signed_buf[pos] = if (sig.v == 0) 0x80 else 0x01;
    pos += 1;
    signed_buf[pos] = 0xA0;
    @memcpy(signed_buf[pos + 1 ..][0..32], &sig.r);
    pos += 33;
    signed_buf[pos] = 0xA0;
    @memcpy(signed_buf[pos + 1 ..][0..32], &sig.s);
    pos += 33;
    signed_buf[2] = @intCast(pos - 3);

    var scratch: [256]u8 align(8) = undefined;
    const st = try decodeSigned(signed_buf[0..pos], &scratch);
    try testing.expectEqual(@as(u8, 0x04), st.fields.tx_type);
    try testing.expectEqual(@as(u256, 5), st.fields.value);
    try testing.expectEqualSlices(u8, &sighash, &st.sighash);
    const recovered = try recoverSender(st.sig, st.sighash);
    try testing.expectEqualSlices(u8, &signer, &recovered);
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
