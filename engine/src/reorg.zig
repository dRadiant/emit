/// Reorg detection and recovery over the pending ring.
///
/// Walk backwards from a divergent block, comparing stored hashes against
/// canonical hashes (provided by caller), to find the fork point. Truncate
/// invalidated entries from the pending ring. Caller re-fetches and re-inserts
/// canonical blocks after this returns.
///
/// No network I/O — the caller is responsible for fetching canonical hashes
/// and re-inserting blocks. This keeps the module testable without mocks.
const std = @import("std");
const PendingRing = @import("pending_ring.zig").PendingRing;

/// Result of a reorg detection pass.
pub const ReorgResult = struct {
    /// Block number where the fork diverges. Blocks >= fork_block were invalidated.
    fork_block: u64,
    /// Number of entries removed from the pending ring.
    blocks_removed: u64,
};

/// Callback type for fetching the canonical hash of a block number.
/// Returns null if the block is unknown (e.g., before the pending range).
pub const GetCanonicalHash = *const fn (block_number: u64) ?[32]u8;

/// Detect the fork point and truncate the pending ring.
///
/// Walks backwards from `divergent_block` comparing the pending ring's stored
/// hash against the canonical hash (via `getHash`). Stops when hashes match
/// or when we reach the oldest pending block. Then truncates everything at
/// and above the fork point.
///
/// Returns the fork block and count of removed entries. Caller should
/// re-fetch and re-insert canonical blocks for fork_block..divergent_block.
pub fn handleReorg(
    ring: *PendingRing,
    divergent_block: u64,
    getCanonicalHash: GetCanonicalHash,
) !ReorgResult {
    const oldest = ring.oldestBlock() orelse return .{ .fork_block = divergent_block, .blocks_removed = 0 };

    // Walk backwards to find where our chain and the canonical chain agree
    var fork_block = divergent_block;
    while (fork_block > oldest) {
        const stored = ring.getHash(fork_block - 1) orelse break;
        const canonical = getCanonicalHash(fork_block - 1) orelse break;
        if (std.mem.eql(u8, &stored, &canonical)) break;
        fork_block -= 1;
    }

    const removed = try ring.truncateFrom(fork_block);
    return .{ .fork_block = fork_block, .blocks_removed = removed };
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;
const bloom_mod = @import("core").bloom;

const dummy_topic = [_]u8{0} ** bloom_mod.BLOOM_SIZE;
const dummy_addr = [_]u8{0} ** bloom_mod.ADDR_BLOOM_SIZE;
const dummy_entry = [_]u8{ 1, 0, 0, 0, 0x42 };

fn makeHash(v: u8) [32]u8 {
    return [_]u8{v} ** 32;
}

test "no reorg when hashes match at divergent-1" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    // Insert blocks 100-104 with hash 0xAA
    for (100..105) |i| {
        try ring.insert(i, makeHash(0xAA), &dummy_topic, &dummy_addr, &dummy_entry);
    }

    // Canonical chain agrees at block 104 (divergent=105, check 104 matches)
    const result = try handleReorg(&ring, 105, struct {
        fn get(bn: u64) ?[32]u8 {
            _ = bn;
            return makeHash(0xAA); // all match
        }
    }.get);

    // Fork at 105 (nothing removed since 105 wasn't in ring)
    try testing.expectEqual(@as(u64, 105), result.fork_block);
    try testing.expectEqual(@as(u64, 0), result.blocks_removed);
    try testing.expectEqual(@as(usize, 5), ring.count());
}

test "1-block reorg at tip" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    for (100..105) |i| {
        try ring.insert(i, makeHash(0xAA), &dummy_topic, &dummy_addr, &dummy_entry);
    }

    // Block 104 diverges — canonical has different hash
    const result = try handleReorg(&ring, 104, struct {
        fn get(bn: u64) ?[32]u8 {
            // 103 matches, 104 would not (but we check bn-1 so 103 is checked)
            if (bn == 103) return makeHash(0xAA);
            return makeHash(0xBB);
        }
    }.get);

    try testing.expectEqual(@as(u64, 104), result.fork_block);
    try testing.expectEqual(@as(u64, 1), result.blocks_removed);
    try testing.expectEqual(@as(usize, 4), ring.count());
    try testing.expectEqual(@as(u64, 103), ring.latestBlock().?);
}

test "deep reorg removes multiple blocks" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    for (100..110) |i| {
        try ring.insert(i, makeHash(0xAA), &dummy_topic, &dummy_addr, &dummy_entry);
    }

    // Canonical chain diverges at block 106 — blocks 106-109 are bad
    const result = try handleReorg(&ring, 109, struct {
        fn get(bn: u64) ?[32]u8 {
            if (bn <= 105) return makeHash(0xAA); // match
            return makeHash(0xCC); // diverged
        }
    }.get);

    try testing.expectEqual(@as(u64, 106), result.fork_block);
    try testing.expectEqual(@as(u64, 4), result.blocks_removed);
    try testing.expectEqual(@as(usize, 6), ring.count());
    try testing.expectEqual(@as(u64, 105), ring.latestBlock().?);
}

test "reorg on empty ring is a no-op" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    const result = try handleReorg(&ring, 100, struct {
        fn get(_: u64) ?[32]u8 { return null; }
    }.get);

    try testing.expectEqual(@as(u64, 100), result.fork_block);
    try testing.expectEqual(@as(u64, 0), result.blocks_removed);
}
