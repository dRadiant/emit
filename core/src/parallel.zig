/// Thread pool utility: split a range across N workers, run each, join all.
///
/// **Project rule — no big buffers on the stack.**
///
/// Linux's default `RLIMIT_STACK` is 8 MB. Several hot paths in this
/// codebase use buffers sized off `core.types.BLOCK_BUF_SIZE` (4 MB) and
/// `core.types.MAX_LOGS_PER_BLOCK` (~2 MB at 216 B/RawLog). A function with
/// two or three of these on the stack overflows the main-thread ceiling
/// the moment it's invoked inline (no thread spawn). The first occurrence
/// segfaulted the Uniswap V2 example only because the static-only paths
/// happened to skirt the inline branch — a fragile guarantee.
///
/// The rule, applied across `core`, `engine`, and `sdk`:
///   - Any buffer sized off `BLOCK_BUF_SIZE` or `MAX_LOGS_PER_BLOCK` is
///     **heap-allocated** (via the caller's allocator or a per-worker
///     arena), never `var x: [N]u8 = undefined;` on the stack.
///   - Worker threads spawned via `parallel.run` get `WORKER_STACK_SIZE`
///     anyway, as defense-in-depth against future stack growth.
///   - Functions that must run on the caller's thread (e.g. `scanner.replay`
///     because its MDBX write txn is thread-bound) follow the heap rule
///     unconditionally.
const std = @import("std");

pub const MAX_WORKERS = 7;

/// Worker thread stack size. Used both as the explicit `SpawnConfig.stack_size`
/// for `parallel.run` and elsewhere. Sized comfortably above the largest
/// observed frame (~15 MB in `replayImpl`'s legacy stack-allocated form, now
/// removed) so a future regression that introduces a stack-allocated buffer
/// has a soft landing instead of an immediate SIGSEGV.
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
