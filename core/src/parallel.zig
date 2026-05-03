/// Thread pool utility: split a range across N workers, run each, join all.
const std = @import("std");

pub const MAX_WORKERS = 7;

/// Run `worker_fn` across `num_workers` threads. Single-worker runs inline.
pub fn run(
    comptime Args: type,
    args_array: []Args,
    num_workers: usize,
    comptime worker_fn: fn (*Args) void,
) !void {
    std.debug.assert(num_workers <= args_array.len);
    std.debug.assert(num_workers <= MAX_WORKERS);

    if (num_workers <= 1) {
        if (args_array.len > 0) worker_fn(&args_array[0]);
        return;
    }

    var threads: [MAX_WORKERS]std.Thread = undefined;
    for (0..num_workers) |i| {
        threads[i] = try std.Thread.spawn(.{}, worker_fn, .{&args_array[i]});
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
