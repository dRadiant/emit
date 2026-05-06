/// Derived presentation helpers.
/// TODO: add token-metadata formatting (decimals, symbol) atop the ethcall cache.
const std = @import("std");

const MERGE_BLOCK: u64 = 15_537_394;
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
