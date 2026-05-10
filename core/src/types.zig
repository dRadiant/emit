/// Shared types and constants for the flat log store format.
/// Used by engine (writing) and sdk (reading + filtering).
const std = @import("std");

// ── Tuning constants ─────────────────────────────────────────────────────

/// Maximum event topics per EVM log entry (LOG0..LOG4).
pub const MAX_TOPICS = 4;

/// Maximum logs per block for pre-allocated buffers.
/// 16,384 covers the current outlier with 2x headroom;
/// callers (importer, filter pipeline) MUST fail loudly rather than truncate
/// if a future block ever exceeds it.
pub const MAX_LOGS_PER_BLOCK: usize = 16_384;

/// Serialize/compress/decompress buffer size. Must fit the largest block's
/// serialized log data (~1.5 MB typical, 3+ MB worst case). Also used as the
/// io_uring per-slot read buffer — undersizing it (we previously had a
/// separate IO_BUF_SIZE = 256 KB) silently truncates large compressed
/// entries; LZ4 then errors and the whole block is dropped from the index.
pub const BLOCK_BUF_SIZE: usize = 4 * 1024 * 1024;

/// Blocks between meta commits during import. Each commit fsyncs (~1ms).
/// 10K balances crash resilience (~3s of lost work) against fsync overhead (<1%).
pub const COMMIT_INTERVAL: usize = 10_000;

// ── Raw log ──────────────────────────────────────────────────────────────

/// A single EVM log entry. Core interchange type between import, serialization,
/// filtering, and handler dispatch. `data` is a borrowed slice — valid only
/// within the current decompression buffer's lifetime.
pub const RawLog = struct {
    block_number: u64,
    tx_index: u16,
    log_index: u16,
    address: [20]u8,
    topic_count: u8,
    topics: [MAX_TOPICS][32]u8,
    data: []const u8,
    tx_hash: [32]u8,
};

// ── Tests ────────────────────────────────────────────────────────────────

test "RawLog size sanity" {
    // Ensure RawLog doesn't accidentally grow (catches field additions)
    try std.testing.expect(@sizeOf(RawLog) < 256);
}
