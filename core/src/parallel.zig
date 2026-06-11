//! Thread pool: split a range across N workers, run each, join all.
//!
//! Buffer-allocation rule. Buffers sized off `core.types.BLOCK_BUF_SIZE`
//! (4 MB) or `core.types.MAX_LOGS_PER_BLOCK` (~2 MB) overflow Linux's default
//! 8 MB `RLIMIT_STACK` when a function holds two or three. Main-thread-reachable
//! functions (`scanner.replay`, `scanner.scanCreations`, anything `sdk.run`
//! calls directly) heap-allocate such buffers. Worker-only functions
//! (`filter_builder.filterWorker`, `engine/rocksdb_import.workerFn`) may
//! stack-allocate. Workers always receive `WORKER_STACK_SIZE`, and stack
//! avoids the per-run page-fault cost of fresh mmap.
//!
//! `parallel.run` always spawns, including for `num_workers == 1`. No
//! inline-on-caller path. A worker fn cannot land on the caller's stack.
const std = @import("std");

pub const MAX_WORKERS = 7;

/// Worker thread stack size. Sized above the largest worker frame:
/// `engine/rocksdb_import.workerFn` at ~24 MB (`MAX_LOGS_PER_BLOCK` × RawLog
/// + 2 × BLOCK_BUF_SIZE). `filter_builder.filterWorker` is ~12 MB.
pub const WORKER_STACK_SIZE: usize = 32 * 1024 * 1024;

/// Run `worker_fn` across `num_workers` threads. Always spawns, even for
/// `num_workers == 1`, so the worker sees `WORKER_STACK_SIZE` regardless of
/// the caller's stack limit. Spawn cost ~50 µs, negligible against buffer setup.
pub fn run(
    comptime Args: type,
    args_array: []Args,
    num_workers: usize,
    comptime worker_fn: fn (*Args) void,
) !void {
    std.debug.assert(num_workers <= args_array.len);
    std.debug.assert(num_workers <= MAX_WORKERS);
    if (num_workers == 0 or args_array.len == 0) return;

    var threads: [MAX_WORKERS]std.Thread = undefined;
    for (0..num_workers) |i| {
        threads[i] = try std.Thread.spawn(
            .{ .stack_size = WORKER_STACK_SIZE },
            worker_fn,
            .{&args_array[i]},
        );
    }
    for (0..num_workers) |i| {
        threads[i].join();
    }
}

/// Split `total` items into `num_workers` ranges. Last worker gets remainder.
pub fn chunkRanges(total: usize, num_workers: usize) [MAX_WORKERS][2]usize {
    std.debug.assert(num_workers > 0 and num_workers <= MAX_WORKERS);
    var ranges: [MAX_WORKERS][2]usize = undefined;
    const per = total / num_workers;
    for (0..num_workers) |i| {
        ranges[i] = .{
            i * per,
            if (i == num_workers - 1) total else (i + 1) * per,
        };
    }
    return ranges;
}

/// Choose worker count: MAX_WORKERS for large workloads, 1 for small.
pub fn workerCount(total: usize, threshold: usize) usize {
    return if (total > threshold) MAX_WORKERS else 1;
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

test "chunkRanges covers the total contiguously, remainder on the last worker" {
    const r = chunkRanges(10, 3);
    try testing.expectEqual([2]usize{ 0, 3 }, r[0]);
    try testing.expectEqual([2]usize{ 3, 6 }, r[1]);
    try testing.expectEqual([2]usize{ 6, 10 }, r[2]);
}

test "chunkRanges with total below worker count gives leading empty ranges" {
    // per = 0: every worker but the last gets an empty range, the last gets
    // everything. Degenerate but valid, the workers no-op on empty ranges.
    const r = chunkRanges(2, MAX_WORKERS);
    for (r[0 .. MAX_WORKERS - 1]) |range| {
        try testing.expectEqual(@as(usize, 0), range[1] - range[0]);
    }
    try testing.expectEqual([2]usize{ 0, 2 }, r[MAX_WORKERS - 1]);
}

test "workerCount switches at the threshold" {
    try testing.expectEqual(@as(usize, 1), workerCount(1_000, 1_000));
    try testing.expectEqual(@as(usize, MAX_WORKERS), workerCount(1_001, 1_000));
}

test "run executes every worker and joins" {
    const Args = struct { in: usize, out: usize = 0 };
    const worker = struct {
        fn f(a: *Args) void {
            a.out = a.in * 2;
        }
    }.f;

    var args = [_]Args{ .{ .in = 1 }, .{ .in = 2 }, .{ .in = 3 } };
    try run(Args, &args, 3, worker);
    try testing.expectEqual(@as(usize, 2), args[0].out);
    try testing.expectEqual(@as(usize, 4), args[1].out);
    try testing.expectEqual(@as(usize, 6), args[2].out);
}
