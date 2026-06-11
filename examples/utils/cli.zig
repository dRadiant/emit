/// CLI shell shared by every example indexer in `examples/`.
/// `cli.run(manifest, handlers, entities)` collapses the per-example
/// boilerplate: allocator setup, arg parse, run, print stats.
///
/// Examples needing custom CLI behavior (config files, env vars,
/// structured logging) skip `cli.run` and call `sdk.run` directly,
/// composing `parseStandardArgs` and `printStats`.
const std = @import("std");
const builtin = @import("builtin");
const sdk = @import("sdk");

pub const StandardArgs = struct {
    engine_data_dir: []const u8,
    data_dir: []const u8,
    commit_interval: u32 = 100_000,
    /// Optional JSON-RPC URL for prefetch. When absent, prefetch gathers
    /// but skips Multicall3. Handlers see `error.NotPrefetched` for any
    /// uncached pair.
    node_rpc: ?[]const u8 = null,
    /// Enter the live head-following loop after backfill + gap-fill.
    /// The process never returns under normal operation.
    follow: bool = false,
    /// Stream the filtered backfill from a remote engine `serve` listener
    /// (`host:port`) instead of reading a local flat store. Backfill only.
    remote_engine: ?sdk.RemoteEngine = null,
};

/// Parse `--engine-data-dir`, `--data-dir`, `--commit-interval`,
/// `--node-rpc`, `--follow`. Missing required args print usage and return
/// `error.MissingArgs`. Caller frees the duped string fields. `passthrough`
/// lists value-taking flags owned by the caller (erc20-api's `--port`),
/// skipped here. Anything else unrecognized is an error: a typo'd flag
/// silently changing behavior (`--folow` running backfill-only) is worse
/// than a startup failure.
pub fn parseStandardArgs(allocator: std.mem.Allocator, prog_name: []const u8, passthrough: []const []const u8) !StandardArgs {
    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    var engine_data_dir: ?[]const u8 = null;
    errdefer if (engine_data_dir) |s| allocator.free(s);
    var data_dir: ?[]const u8 = null;
    errdefer if (data_dir) |s| allocator.free(s);
    var commit_interval: u32 = 100_000;
    var node_rpc: ?[]const u8 = null;
    errdefer if (node_rpc) |s| allocator.free(s);
    var follow: bool = false;
    var remote_engine: ?sdk.RemoteEngine = null;
    errdefer if (remote_engine) |re| allocator.free(re.host);
    var silent = false;
    var verbose = false;

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
        } else if (std.mem.eql(u8, a, "--remote-engine") and i + 1 < argv.len) {
            const hp = argv[i + 1];
            const colon = std.mem.lastIndexOfScalar(u8, hp, ':') orelse return error.BadRemoteEngine;
            remote_engine = .{
                .host = try allocator.dupe(u8, hp[0..colon]),
                .port = try std.fmt.parseInt(u16, hp[colon + 1 ..], 10),
            };
            i += 1;
        } else if (std.mem.eql(u8, a, "--follow")) {
            follow = true;
        } else if (std.mem.eql(u8, a, "--silent")) {
            silent = true;
        } else if (std.mem.eql(u8, a, "--verbose")) {
            verbose = true;
        } else {
            const skipped = for (passthrough) |p| {
                if (std.mem.eql(u8, a, p)) {
                    i += 1; // skip the caller-owned flag's value
                    break true;
                }
            } else false;
            if (!skipped) {
                // Also catches a value-taking flag as the last arg (its own
                // branch fails the `i + 1 < argv.len` check and lands here).
                sdk.log.err("{s}: unknown flag or missing value: {s}\n", .{ prog_name, a });
                return error.BadFlag;
            }
        }
    }

    sdk.log.setLevel(sdk.log.levelFromFlags(silent, verbose));

    // Remote mode reads no local store, so engine_data_dir is unused. Default
    // it to data_dir to keep `Options.engine_data_dir` populated.
    if (remote_engine != null and engine_data_dir == null) {
        if (data_dir) |d| engine_data_dir = try allocator.dupe(u8, d);
    }

    if (engine_data_dir == null or data_dir == null) {
        sdk.log.err(
            "usage: {s} --engine-data-dir <path> --data-dir <path> [--commit-interval N] [--node-rpc URL] [--remote-engine host:port] [--follow] [--silent] [--verbose]\n",
            .{prog_name},
        );
        // The errdefers at the declarations free the duped fields.
        return error.MissingArgs;
    }

    return .{
        .engine_data_dir = engine_data_dir.?,
        .data_dir = data_dir.?,
        .commit_interval = commit_interval,
        .node_rpc = node_rpc,
        .follow = follow,
        .remote_engine = remote_engine,
    };
}

/// Canonical RunStats printout covering both factory and non-factory
/// manifests. Factory fields read zero for non-factory indexers, kept
/// visible so users can confirm no children were unexpectedly discovered.
pub fn printStats(prog_name: []const u8, stats: sdk.RunStats) void {
    const ms = std.time.ns_per_ms;
    const phases = stats.filter_build_ns + stats.scan_creations_ns + stats.append_children_ns + stats.prefetch_ns + stats.replay_ns;
    const overhead_ns = if (stats.elapsed_ns > phases) stats.elapsed_ns - phases else 0;
    // Batch count derived from executed pairs and the default Multicall3
    // chunk. Exact count would need the per-run override routed through stats.
    const batches = (stats.prefetch_calls_executed + sdk.DEFAULT_BATCH_SIZE - 1) / sdk.DEFAULT_BATCH_SIZE;
    sdk.log.info(
        \\{s} indexer complete
        \\  start block:       {d}
        \\  end block:         {d}
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
        stats.start_block,
        stats.end_block,
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
    // DebugAllocator catches leaks under Debug/ReleaseSafe. ReleaseFast
    // benchmarks use smp_allocator to drop the per-alloc safety metadata and
    // bucket bookkeeping. The store hot path runs on per-thread arenas, so this
    // allocator only sees setup and entity-store growth.
    const dev = builtin.mode == .Debug or builtin.mode == .ReleaseSafe;
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    const allocator = if (comptime dev) gpa.allocator() else std.heap.smp_allocator;
    defer if (comptime dev) {
        _ = gpa.deinit();
    } else {
        _ = &gpa;
    };

    const args = try parseStandardArgs(allocator, manifest.name, &.{});
    defer allocator.free(args.engine_data_dir);
    defer allocator.free(args.data_dir);
    defer if (args.node_rpc) |s| allocator.free(s);
    defer if (args.remote_engine) |re| allocator.free(re.host);

    const stats = try sdk.run(manifest, handlers, entities, .{
        .engine_data_dir = args.engine_data_dir,
        .data_dir = args.data_dir,
        .commit_interval = args.commit_interval,
        .node_rpc = args.node_rpc,
        .follow = args.follow,
        .remote_engine = args.remote_engine,
    }, allocator);

    // Unreachable under `--follow`. sdk.run enters the live loop and never
    // returns. Reaching here means backfill-only completed.
    printStats(manifest.name, stats);
}
