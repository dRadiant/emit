//! JSON-RPC transaction objects → `core.txs` records. Shared by the
//! follower's per-block fetch and the RPC import's tx pass. The node returns
//! pre-recovered senders, so this path does no signature work (ADR-006).
const std = @import("std");

const core = @import("core");

pub const Error = error{ MalformedTx, MissingTx, TooManyRecords };

/// The tx-object subset we read, hex strings per the JSON-RPC spec.
pub const JsonTx = struct {
    transactionIndex: []const u8,
    type: ?[]const u8 = null,
    from: []const u8,
    /// Null for contract creation.
    to: ?[]const u8 = null,
    value: []const u8,
};

/// Ascending unique tx indexes from a block's logs. Logs are canonically
/// ordered by `(tx_index, log_index)`, so adjacent dedup suffices. `out`
/// sized ≥ logs.len.
pub fn logMask(logs: []const core.RawLog, out: []u16) []u16 {
    var n: usize = 0;
    for (logs) |l| {
        if (n > 0 and out[n - 1] == l.tx_index) continue;
        out[n] = l.tx_index;
        n += 1;
    }
    return out[0..n];
}

/// Build records for `mask` (ascending log-producing tx indexes) from one
/// block's JSON tx objects. Strict: every masked index must resolve or the
/// block fails loud, no partial tables.
pub fn buildRecords(txs: []const JsonTx, mask: []const u16, out: []core.txs.TxRecord) Error![]core.txs.TxRecord {
    if (mask.len > out.len) return error.TooManyRecords;
    var found: usize = 0;
    for (txs) |t| {
        const idx = parseQuantity(u16, t.transactionIndex) catch return error.MalformedTx;
        if (std.mem.indexOfScalar(u16, mask, idx) == null) continue;
        if (found >= mask.len) return error.MissingTx; // duplicate index in response

        var rec = core.txs.TxRecord{
            .tx_index = idx,
            .tx_type = if (t.type) |ty| parseQuantity(u8, ty) catch return error.MalformedTx else 0,
            .flags = 0,
            .from = parseAddress(t.from) catch return error.MalformedTx,
            .to = undefined,
            .value = undefined,
        };
        if (t.to) |to_hex| {
            rec.to = parseAddress(to_hex) catch return error.MalformedTx;
        } else {
            rec.to = std.mem.zeroes([20]u8);
            rec.flags |= core.txs.FLAG_TO_ABSENT;
        }
        const v = parseQuantity(u256, t.value) catch return error.MalformedTx;
        std.mem.writeInt(u256, &rec.value, v, .little);

        out[found] = rec;
        found += 1;
    }
    if (found != mask.len) return error.MissingTx;
    // Every known node returns ascending order, but the table format requires
    // it. Sort defensively, cheap at table size.
    std.mem.sort(core.txs.TxRecord, out[0..found], {}, recLess);
    return out[0..found];
}

fn recLess(_: void, a: core.txs.TxRecord, b: core.txs.TxRecord) bool {
    return a.tx_index < b.tx_index;
}

fn parseQuantity(comptime T: type, s: []const u8) !T {
    if (!std.mem.startsWith(u8, s, "0x")) return error.MalformedTx;
    return std.fmt.parseInt(T, s[2..], 16) catch error.MalformedTx;
}

fn parseAddress(s: []const u8) ![20]u8 {
    if (s.len != 42 or !std.mem.startsWith(u8, s, "0x")) return error.MalformedTx;
    var out: [20]u8 = undefined;
    const decoded = std.fmt.hexToBytes(&out, s[2..]) catch return error.MalformedTx;
    if (decoded.len != 20) return error.MalformedTx;
    return out;
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

test "buildRecords keeps masked txs with parsed fields, sorted" {
    const txs = [_]JsonTx{
        .{ .transactionIndex = "0x0", .type = "0x2", .from = "0x" ++ "aa" ** 20, .to = "0x" ++ "bb" ** 20, .value = "0x0" },
        .{ .transactionIndex = "0x1", .type = "0x0", .from = "0x" ++ "cc" ** 20, .to = null, .value = "0xde0b6b3a7640000" },
        .{ .transactionIndex = "0x2", .type = "0x2", .from = "0x" ++ "dd" ** 20, .to = "0x" ++ "ee" ** 20, .value = "0x5" },
    };
    const mask = [_]u16{ 1, 2 }; // tx 0 produced no logs
    var out: [4]core.txs.TxRecord = undefined;
    const recs = try buildRecords(&txs, &mask, &out);

    try testing.expectEqual(@as(usize, 2), recs.len);
    try testing.expectEqual(@as(u16, 1), recs[0].tx_index);
    try testing.expectEqualSlices(u8, &([_]u8{0xCC} ** 20), &recs[0].from);
    try testing.expect(recs[0].flags & core.txs.FLAG_TO_ABSENT != 0); // creation
    try testing.expectEqual(@as(u256, 1_000_000_000_000_000_000), recs[0].valueU256());
    try testing.expectEqual(@as(u16, 2), recs[1].tx_index);
    try testing.expectEqual(@as(u256, 5), recs[1].valueU256());
}

test "buildRecords fails loud when a masked tx is missing" {
    const txs = [_]JsonTx{
        .{ .transactionIndex = "0x0", .from = "0x" ++ "aa" ** 20, .to = "0x" ++ "bb" ** 20, .value = "0x0" },
    };
    const mask = [_]u16{ 0, 3 };
    var out: [4]core.txs.TxRecord = undefined;
    try testing.expectError(error.MissingTx, buildRecords(&txs, &mask, &out));
}

test "logMask dedupes adjacent tx indexes" {
    var logs: [4]core.RawLog = undefined;
    for (&logs, [_]u16{ 0, 0, 3, 7 }) |*l, ti| {
        l.* = std.mem.zeroes(core.RawLog);
        l.tx_index = ti;
    }
    var buf: [4]u16 = undefined;
    try testing.expectEqualSlices(u16, &.{ 0, 3, 7 }, logMask(&logs, &buf));
}
