/// Thread pool utility: split a range across N workers, run each, join all.
///
/// **Buffer-allocation rule.** Buffers sized off `core.types.BLOCK_BUF_SIZE`
/// (4 MB) or `core.types.MAX_LOGS_PER_BLOCK` (~2 MB) overflow Linux's
/// default 8 MB `RLIMIT_STACK` when a function holds two or three of them.
/// Functions reachable from the main thread (`scanner.replay`,
/// `scanner.scanCreations`, anything `sdk.run` calls directly) heap-allocate
/// such buffers. Functions that only run on a `parallel.run`-spawned worker
/// (`filter_builder.filterWorker`, `engine/rocksdb_import.workerFn`) may
/// stack-allocate — workers always receive `WORKER_STACK_SIZE`, and stack
/// avoids the per-run page-fault cost of fresh mmap.
///
/// `parallel.run` always spawns, including for `num_workers == 1`. There is
/// no inline-on-caller path; a worker fn cannot land on the caller's stack.
const std = @import("std");

pub const MAX_WORKERS = 7;

/// Worker thread stack size. Sized above the largest worker frame:
/// `engine/rocksdb_import.workerFn` at ~24 MB (`MAX_LOGS_PER_BLOCK` × RawLog
/// + 2 × BLOCK_BUF_SIZE). `filter_builder.filterWorker` is ~12 MB.
pub const WORKER_STACK_SIZE: usize = 32 * 1024 * 1024;

/// Run `worker_fn` across `num_workers` threads. Even when `num_workers == 1`
/// we spawn a thread (rather than running inline on the caller) so the
/// worker always sees `WORKER_STACK_SIZE` regardless of the caller's stack
/// limit. Spawn cost is ~50 µs — negligible against the buffer setup.
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
