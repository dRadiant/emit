//! Per-block log filtering after bloom scan rejects non-matching blocks.
//! Keeps the logs in one block's decompressed packed logs that match a
//! `Filter`, re-emits them as a fresh LZ4 entry in the same packed wire format.
//!
//! Shared by two callers that must produce byte-identical output:
//!   sdk `filter_builder` worker (backfill filtered-index build)
//!   engine `tcp_server` (streams filtered blocks to remote indexers)
//! Engine cannot import sdk, so the shared primitive lives in core.
//!
//! `decompressed` comes from the importer-produced flat store (LZ4-validated
//! upstream), so the in-place walk trusts the packed layout.
//!
//! Safe builds bounds-check the slice accesses. ReleaseFast does not.
const std = @import("std");
const builtin = @import("builtin");

const log_serial = @import("log_serial.zig");

/// Above this many addresses the membership test binary-searches instead of
/// scanning. Small sets (the single/few-contract common case) stay linear,
/// where the branch-free scan beats a binary search's overhead. A factory
/// indexer with thousands of children crosses it and pays `O(log n)` per log.
pub const ADDR_BINARY_SEARCH_THRESHOLD = 24;

/// Per-log keep predicate. `match_addrs` and `match_topics` are positive
/// match sets. `exclude_addrs` is a negative filter applied after the positives
/// pass, letting a phase suppress addresses already covered elsewhere.
///
/// `addrs_sorted` is an opt-in: when set, `match_addrs` and `exclude_addrs` are
/// ascending-sorted and the per-log test binary-searches. Default false keeps
/// every caller on the order-tolerant linear scan. Only set it after sorting.
pub const Filter = struct {
    match_addrs: []const [20]u8,
    match_topics: []const [32]u8,
    exclude_addrs: []const [20]u8,
    addrs_sorted: bool = false,
};

fn addrLessThan(_: void, a: [20]u8, b: [20]u8) bool {
    return std.mem.order(u8, &a, &b) == .lt;
}

/// Sort an address slice ascending in place. Establishes the `addrs_sorted`
/// precondition for binary-search membership.
pub fn sortAddresses(addrs: [][20]u8) void {
    std.sort.pdq([20]u8, addrs, {}, addrLessThan);
}

/// Recompressed LZ4 entry (a slice into the caller's `compress_buf`) plus the
/// number of logs it carries.
pub const Filtered = struct {
    entry: []const u8,
    log_count: u32,
    /// Ascending-deduped tx indexes of the kept logs, a slice into the
    /// caller's `tx_mask_buf`. Empty unless the caller passed one.
    tx_mask: []const u16 = &.{},
};

inline fn contains(comptime N: usize, haystack: []const [N]u8, needle: *const [N]u8) bool {
    for (haystack) |h| if (std.mem.eql(u8, &h, needle)) return true;
    return false;
}

/// Binary-search membership. `haystack` MUST be ascending-sorted.
fn containsSorted(haystack: []const [20]u8, needle: *const [20]u8) bool {
    var lo: usize = 0;
    var hi: usize = haystack.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        switch (std.mem.order(u8, &haystack[mid], needle)) {
            .eq => return true,
            .lt => lo = mid + 1,
            .gt => hi = mid,
        }
    }
    return false;
}

pub inline fn containsAddress(haystack: []const [20]u8, needle: *const [20]u8) bool {
    return contains(20, haystack, needle);
}

pub inline fn containsTopic(haystack: []const [32]u8, needle: *const [32]u8) bool {
    return contains(32, haystack, needle);
}

/// Membership against a (possibly sorted) address set. Binary-searches when
/// `sorted` and the set is large, else scans linearly.
inline fn addrMatch(haystack: []const [20]u8, needle: *const [20]u8, sorted: bool) bool {
    if (sorted and haystack.len > ADDR_BINARY_SEARCH_THRESHOLD)
        return containsSorted(haystack, needle);
    return contains(20, haystack, needle);
}

/// Filter `decompressed` (one block's packed logs, `u32` count then logs) to
/// the logs matching `filter`, repacking the keepers and recompressing into
/// `compress_buf`. Returns `null` when no log matches (caller skips the block).
///
/// Zero-copy walk. Iterates logs in place, tests the filter against raw bytes
/// at known offsets, memcpies whole-log byte ranges of keepers into
/// `serialize_buf`. Skips `deserializeLogs` and per-log `RawLog`
/// materialization entirely. Both buffers must be at least `BLOCK_BUF_SIZE`.
/// Propagates the compress error (`error.BufferTooSmall` on an oversize block).
/// Caller decides whether that is a recoverable drop or fatal.
///
/// `tx_mask_buf`, when non-null, receives the kept logs' tx indexes
/// (ascending-deduped, returned via `Filtered.tx_mask`). Sized to the u16
/// domain (65,536) it can never overflow. Feeds the tx-fields carry-through.
pub fn filterBlockEntry(
    decompressed: []const u8,
    filter: Filter,
    serialize_buf: []u8,
    compress_buf: []u8,
    tx_mask_buf: ?[]u16,
) !?Filtered {
    // Opt-in binary search requires the precondition the flag asserts. Check it
    // once per block in safe builds. Compiled out in ReleaseFast.
    if (builtin.mode == .Debug or builtin.mode == .ReleaseSafe) {
        if (filter.addrs_sorted) {
            std.debug.assert(std.sort.isSorted([20]u8, filter.match_addrs, {}, addrLessThan));
            std.debug.assert(std.sort.isSorted([20]u8, filter.exclude_addrs, {}, addrLessThan));
        }
    }

    var pos: usize = 0;
    const log_count: usize = std.mem.readInt(u32, decompressed[pos..][0..4], .little);
    pos += 4;

    var out_pos: usize = 4;
    var kept: u32 = 0;
    var mask_len: usize = 0;

    for (0..log_count) |_| {
        const log_start = pos;
        const address: *const [20]u8 = @ptrCast(decompressed[pos + 4 ..][0..20]);
        const topic_count = decompressed[pos + 24];
        const topics_end = pos + 25 + @as(usize, topic_count) * 32;
        const data_len: usize = std.mem.readInt(u32, decompressed[topics_end..][0..4], .little);
        const log_end = topics_end + 4 + data_len + 32;
        pos = log_end;

        if (topic_count == 0) continue;
        // An empty positive set is a wildcard on that axis, mirroring the bloom
        // scan, so a one-sided filter (addresses ^ topics) matches on the
        // populated axis only. Both-empty is rejected upstream (tcp_server).
        if (filter.match_addrs.len > 0 and !addrMatch(filter.match_addrs, address, filter.addrs_sorted)) continue;
        if (filter.match_topics.len > 0) {
            const topic0: *const [32]u8 = @ptrCast(decompressed[log_start + 25 ..][0..32]);
            if (!containsTopic(filter.match_topics, topic0)) continue;
        }
        if (addrMatch(filter.exclude_addrs, address, filter.addrs_sorted)) continue;

        const len = log_end - log_start;
        @memcpy(serialize_buf[out_pos..][0..len], decompressed[log_start..log_end]);
        out_pos += len;
        kept += 1;

        // Logs are tx-ordered within a block, adjacent dedup suffices.
        if (tx_mask_buf) |mb| {
            const tx_index = std.mem.readInt(u16, decompressed[log_start..][0..2], .little);
            if (mask_len == 0 or mb[mask_len - 1] != tx_index) {
                mb[mask_len] = tx_index;
                mask_len += 1;
            }
        }
    }

    if (kept == 0) return null;

    std.mem.writeInt(u32, serialize_buf[0..4], kept, .little);
    const entry_len = try log_serial.compressEntry(serialize_buf[0..out_pos], compress_buf);
    return .{
        .entry = compress_buf[0..entry_len],
        .log_count = kept,
        .tx_mask = if (tx_mask_buf) |mb| mb[0..mask_len] else &.{},
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;
const types = @import("types.zig");
const RawLog = types.RawLog;

const ADDR_A: [20]u8 = [_]u8{0xAA} ** 20;
const ADDR_B: [20]u8 = [_]u8{0xBB} ** 20;
const ADDR_C: [20]u8 = [_]u8{0xCC} ** 20;
const TOPIC_X: [32]u8 = [_]u8{0x01} ** 32;
const TOPIC_Y: [32]u8 = [_]u8{0x02} ** 32;
const TOPIC_Z: [32]u8 = [_]u8{0x03} ** 32;

fn rawLog(address: [20]u8, topic0: [32]u8, topic_count: u8, log_index: u16) RawLog {
    return .{
        .block_number = 100,
        .tx_index = 0,
        .log_index = log_index,
        .address = address,
        .topic_count = topic_count,
        .topics = .{ topic0, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };
}

/// Pack `logs` into the decompressed wire format `filterBlockEntry` consumes.
fn packLogs(logs: []const RawLog, buf: []u8) []const u8 {
    return buf[0..log_serial.serializeLogs(logs, buf)];
}

/// Decompress and deserialize a `Filtered.entry` back into RawLogs for assertions.
fn unpack(entry: []const u8, decompress_buf: []u8, log_buf: []RawLog) ![]RawLog {
    const decoded = try log_serial.decompressEntry(entry, decompress_buf);
    const n = log_serial.deserializeLogs(decoded, log_buf);
    return log_buf[0..n];
}

test "filterBlockEntry keeps only address+topic matches, drops the rest" {
    var pack_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var serialize_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var compress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var decompress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var log_buf: [8]RawLog = undefined;

    const logs = [_]RawLog{
        rawLog(ADDR_A, TOPIC_X, 1, 0), // keep: A + X
        rawLog(ADDR_B, TOPIC_Y, 1, 1), // drop: B not matched
        rawLog(ADDR_A, TOPIC_Z, 1, 2), // drop: Z not matched
        rawLog(ADDR_A, TOPIC_Y, 1, 3), // keep: A + Y
    };
    const packed_logs = packLogs(&logs, &pack_buf);

    const filter: Filter = .{
        .match_addrs = &.{ADDR_A},
        .match_topics = &.{ TOPIC_X, TOPIC_Y },
        .exclude_addrs = &.{},
    };
    const filtered = (try filterBlockEntry(packed_logs, filter, &serialize_buf, &compress_buf, null)).?;
    try testing.expectEqual(@as(u32, 2), filtered.log_count);

    const kept = try unpack(filtered.entry, &decompress_buf, &log_buf);
    try testing.expectEqual(@as(usize, 2), kept.len);
    try testing.expectEqualSlices(u8, &ADDR_A, &kept[0].address);
    try testing.expectEqualSlices(u8, &TOPIC_X, &kept[0].topics[0]);
    try testing.expectEqual(@as(u16, 0), kept[0].log_index);
    try testing.expectEqualSlices(u8, &TOPIC_Y, &kept[1].topics[0]);
    try testing.expectEqual(@as(u16, 3), kept[1].log_index);
}

test "filterBlockEntry captures kept tx indexes, ascending-deduped" {
    var pack_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var serialize_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var compress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var mask_buf: [8]u16 = undefined;

    var logs = [_]RawLog{
        rawLog(ADDR_A, TOPIC_X, 1, 0), // keep, tx 2
        rawLog(ADDR_A, TOPIC_X, 1, 1), // keep, tx 2 (dedups)
        rawLog(ADDR_B, TOPIC_X, 1, 2), // drop, tx 5 must not appear
        rawLog(ADDR_A, TOPIC_X, 1, 3), // keep, tx 7
    };
    logs[0].tx_index = 2;
    logs[1].tx_index = 2;
    logs[2].tx_index = 5;
    logs[3].tx_index = 7;
    const packed_logs = packLogs(&logs, &pack_buf);

    const filter: Filter = .{
        .match_addrs = &.{ADDR_A},
        .match_topics = &.{TOPIC_X},
        .exclude_addrs = &.{},
    };
    const filtered = (try filterBlockEntry(packed_logs, filter, &serialize_buf, &compress_buf, &mask_buf)).?;
    try testing.expectEqual(@as(u32, 3), filtered.log_count);
    try testing.expectEqualSlices(u16, &.{ 2, 7 }, filtered.tx_mask);
}

test "filterBlockEntry applies exclude_addrs after the positive match" {
    var pack_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var serialize_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var compress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var decompress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var log_buf: [8]RawLog = undefined;

    const logs = [_]RawLog{
        rawLog(ADDR_A, TOPIC_X, 1, 0), // excluded
        rawLog(ADDR_B, TOPIC_X, 1, 1), // kept
    };
    const packed_logs = packLogs(&logs, &pack_buf);

    const filter: Filter = .{
        .match_addrs = &.{ ADDR_A, ADDR_B },
        .match_topics = &.{TOPIC_X},
        .exclude_addrs = &.{ADDR_A},
    };
    const filtered = (try filterBlockEntry(packed_logs, filter, &serialize_buf, &compress_buf, null)).?;
    try testing.expectEqual(@as(u32, 1), filtered.log_count);

    const kept = try unpack(filtered.entry, &decompress_buf, &log_buf);
    try testing.expectEqual(@as(usize, 1), kept.len);
    try testing.expectEqualSlices(u8, &ADDR_B, &kept[0].address);
}

test "filterBlockEntry binary-searches a large sorted address set" {
    var pack_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var serialize_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var compress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var decompress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var log_buf: [8]RawLog = undefined;

    // 30 distinct addresses, past ADDR_BINARY_SEARCH_THRESHOLD, sorted ascending
    // so `addrs_sorted` holds and the per-log test binary-searches.
    var addrs: [30][20]u8 = undefined;
    for (&addrs, 0..) |*a, i| a.* = [_]u8{@intCast(i + 1)} ** 20;
    sortAddresses(&addrs);

    const in_set = [_]u8{17} ** 20; // present in the set
    const absent = [_]u8{200} ** 20; // beyond the largest member
    const logs = [_]RawLog{
        rawLog(in_set, TOPIC_X, 1, 0), // keep
        rawLog(absent, TOPIC_X, 1, 1), // drop: not a member
    };
    const packed_logs = packLogs(&logs, &pack_buf);

    const filter: Filter = .{
        .match_addrs = &addrs,
        .match_topics = &.{TOPIC_X},
        .exclude_addrs = &.{},
        .addrs_sorted = true,
    };
    const filtered = (try filterBlockEntry(packed_logs, filter, &serialize_buf, &compress_buf, null)).?;
    try testing.expectEqual(@as(u32, 1), filtered.log_count);

    const kept = try unpack(filtered.entry, &decompress_buf, &log_buf);
    try testing.expectEqual(@as(usize, 1), kept.len);
    try testing.expectEqualSlices(u8, &in_set, &kept[0].address);
}

test "filterBlockEntry returns null when nothing matches" {
    var pack_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var serialize_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var compress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;

    const logs = [_]RawLog{rawLog(ADDR_C, TOPIC_Z, 1, 0)};
    const packed_logs = packLogs(&logs, &pack_buf);

    const filter: Filter = .{
        .match_addrs = &.{ADDR_A},
        .match_topics = &.{TOPIC_X},
        .exclude_addrs = &.{},
    };
    try testing.expectEqual(@as(?Filtered, null), try filterBlockEntry(packed_logs, filter, &serialize_buf, &compress_buf, null));
}

test "filterBlockEntry skips logs with zero topics" {
    var pack_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var serialize_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var compress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;

    // Topic-less log at a matched address must not survive. topic0 is the
    // dispatch key, an anonymous event has nothing to match on.
    const logs = [_]RawLog{rawLog(ADDR_A, TOPIC_X, 0, 0)};
    const packed_logs = packLogs(&logs, &pack_buf);

    const filter: Filter = .{
        .match_addrs = &.{ADDR_A},
        .match_topics = &.{TOPIC_X},
        .exclude_addrs = &.{},
    };
    try testing.expectEqual(@as(?Filtered, null), try filterBlockEntry(packed_logs, filter, &serialize_buf, &compress_buf, null));
}

test "filterBlockEntry treats an empty positive set as a wildcard on that axis" {
    var pack_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var serialize_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var compress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var decompress_buf: [types.BLOCK_BUF_SIZE]u8 = undefined;
    var log_buf: [8]RawLog = undefined;

    const logs = [_]RawLog{
        rawLog(ADDR_A, TOPIC_X, 1, 0),
        rawLog(ADDR_B, TOPIC_X, 1, 1),
        rawLog(ADDR_C, TOPIC_Y, 1, 2),
    };
    const packed_logs = packLogs(&logs, &pack_buf);

    // Topics-only: any address with topic X. Keeps the two TOPIC_X logs.
    const topics_only: Filter = .{ .match_addrs = &.{}, .match_topics = &.{TOPIC_X}, .exclude_addrs = &.{} };
    const f1 = (try filterBlockEntry(packed_logs, topics_only, &serialize_buf, &compress_buf, null)).?;
    try testing.expectEqual(@as(u32, 2), f1.log_count);
    const k1 = try unpack(f1.entry, &decompress_buf, &log_buf);
    try testing.expectEqualSlices(u8, &ADDR_A, &k1[0].address);
    try testing.expectEqualSlices(u8, &ADDR_B, &k1[1].address);

    // Address-only: address A with any topic. Keeps the single ADDR_A log.
    const addr_only: Filter = .{ .match_addrs = &.{ADDR_A}, .match_topics = &.{}, .exclude_addrs = &.{} };
    const f2 = (try filterBlockEntry(packed_logs, addr_only, &serialize_buf, &compress_buf, null)).?;
    try testing.expectEqual(@as(u32, 1), f2.log_count);
    const k2 = try unpack(f2.entry, &decompress_buf, &log_buf);
    try testing.expectEqualSlices(u8, &ADDR_A, &k2[0].address);
    try testing.expectEqualSlices(u8, &TOPIC_X, &k2[0].topics[0]);
}
