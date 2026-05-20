/// CLI shell shared by every example indexer in `examples/`.
///
/// Each example's `main.zig` is essentially boilerplate around `sdk.run`:
/// allocator setup, parse standard args, run the indexer, print stats.
/// `cli.run(manifest, handlers, entities)` collapses all of that.
///
/// Examples that need custom CLI behavior (config files, env vars,
/// structured logging) should skip `cli.run` and call `sdk.run` directly,
/// composing `parseStandardArgs` and `printStats` if they want.
const std = @import("std");
const sdk = @import("sdk");

pub const StandardArgs = struct {
    engine_data_dir: []const u8,
    data_dir: []const u8,
    commit_interval: u32 = 100_000,
    /// Optional JSON-RPC URL for Phase 4 prefetch. When absent, prefetch
    /// gathers but skips Multicall3 — handlers see `error.NotPrefetched`
    /// for any uncached pair.
    node_rpc: ?[]const u8 = null,
    /// When true, enter the live head-following loop after backfill +
    /// gap-fill. The process never returns under normal operation.
    follow: bool = false,
};

/// Parse the standard arg set: `--engine-data-dir`, `--data-dir`,
/// `--commit-interval`, `--node-rpc`. Missing required args print usage and
/// return `error.MissingArgs`. Caller frees the duped string fields.
pub fn parseStandardArgs(allocator: std.mem.Allocator, prog_name: []const u8) !StandardArgs {
    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    var engine_data_dir: ?[]const u8 = null;
    var data_dir: ?[]const u8 = null;
    var commit_interval: u32 = 100_000;
    var node_rpc: ?[]const u8 = null;
    var follow: bool = false;

    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const a = argv[i];
        if (std.mem.eql(u8, a, "--engine-data-dir") and i + 1 < argv.len) {
            engine_data_dir = try allocator.dupe(u8, argv[i + 1]);
            i += 1;
        } else if (std.mem.eql(u8, a, "--data-dir") and i + 1 < argv.len) {
            data_dir = try allocator.dupe(u8, argv[i + 1]);
            i += 1;
        } else if (std.mem.eql(u8, a, "--commit-interval") and i + 1 < argv.len) {
            commit_interval = try std.fmt.parseInt(u32, argv[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, a, "--node-rpc") and i + 1 < argv.len) {
            node_rpc = try allocator.dupe(u8, argv[i + 1]);
            i += 1;
        } else if (std.mem.eql(u8, a, "--follow")) {
            follow = true;
        }
    }

    if (engine_data_dir == null or data_dir == null) {
        std.debug.print(
            "usage: {s} --engine-data-dir <path> --data-dir <path> [--commit-interval N] [--node-rpc URL] [--follow]\n",
            .{prog_name},
        );
        if (engine_data_dir) |s| allocator.free(s);
        if (data_dir) |s| allocator.free(s);
        if (node_rpc) |s| allocator.free(s);
        return error.MissingArgs;
    }

    return .{
        .engine_data_dir = engine_data_dir.?,
        .data_dir = data_dir.?,
        .commit_interval = commit_interval,
        .node_rpc = node_rpc,
        .follow = follow,
    };
}

/// One canonical RunStats printout that covers both factory and
/// non-factory manifests. Factory fields read zero for non-factory
/// indexers — kept visible so users can sanity-check that their
/// non-factory indexer didn't unexpectedly discover children.
pub fn printStats(prog_name: []const u8, stats: sdk.RunStats) void {
    const ms = std.time.ns_per_ms;
    const phases = stats.filter_build_ns + stats.scan_creations_ns + stats.append_children_ns + stats.prefetch_ns + stats.replay_ns;
    const overhead_ns = if (stats.elapsed_ns > phases) stats.elapsed_ns - phases else 0;
    // Derive batch count from executed pairs and the default Multicall3 chunk;
    // an exact match would require routing the per-run override through stats.
    const batches = (stats.prefetch_calls_executed + sdk.DEFAULT_BATCH_SIZE - 1) / sdk.DEFAULT_BATCH_SIZE;
    std.debug.print(
        \\{s} indexer complete
        \\  blocks scanned:    {d}
        \\  blocks matched:    {d}
        \\  filter logs:       {d}
        \\  discovered child:  {d}
        \\  child blocks:      {d}
        \\  child logs:        {d}
        \\  logs dispatched:   {d}
        \\  blocks dispatched: {d}
        \\  commits:           {d}
        \\  phases skipped:    {}
        \\  prefetch gathered: {d}
        \\  prefetch executed: {d}
        \\  prefetch batches:  {d}
        \\  ── timing ──
        \\  filter build:      {d} ms
        \\  scan creations:    {d} ms
        \\  append children:   {d} ms
        \\  prefetch:          {d} ms
        \\  replay:            {d} ms
        \\  overhead:          {d} ms
        \\  elapsed:           {d} ms
        \\
    , .{
        prog_name,
        stats.filter_blocks_scanned,
        stats.filter_blocks_matched,
        stats.filter_total_logs,
        stats.discovered_children,
        stats.children_blocks_matched,
        stats.children_total_logs,
        stats.logs_dispatched,
        stats.blocks_dispatched,
        stats.commits_performed,
        stats.phases_skipped,
        stats.prefetch_calls_gathered,
        stats.prefetch_calls_executed,
        batches,
        stats.filter_build_ns / ms,
        stats.scan_creations_ns / ms,
        stats.append_children_ns / ms,
        stats.prefetch_ns / ms,
        stats.replay_ns / ms,
        overhead_ns / ms,
        stats.elapsed_ns / ms,
    });
}

/// All-in-one: GPA, parse args, run, print stats. Returns the wrapped
/// error if anything fails before stats print.
pub fn run(
    comptime manifest: sdk.Manifest,
    comptime handlers: type,
    comptime entities: anytype,
) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try parseStandardArgs(allocator, manifest.name);
    defer allocator.free(args.engine_data_dir);
    defer allocator.free(args.data_dir);
    defer if (args.node_rpc) |s| allocator.free(s);

    const stats = try sdk.run(manifest, handlers, entities, .{
        .engine_data_dir = args.engine_data_dir,
        .data_dir = args.data_dir,
        .commit_interval = args.commit_interval,
        .node_rpc = args.node_rpc,
        .follow = args.follow,
    }, allocator);

    // Unreachable under `--follow`: sdk.run enters the live loop and
    // never returns. Falling through here means backfill-only completed.
    printStats(manifest.name, stats);
}
