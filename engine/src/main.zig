//! emit-engine CLI. Imports EVM logs, follows the chain head, serves remote
//! indexers, reports store status. Command table lives in `usage()`.
const std = @import("std");

const core = @import("core");
const log = core.log;

const head_follower = @import("head_follower.zig");
const rpc_import = @import("rpc_import.zig");
const tcp_server = @import("tcp_server.zig");

pub fn main() !void {
    const alloc = std.heap.page_allocator;
    const args = try std.process.argsAlloc(alloc);

    if (args.len < 2) return usage();

    const command = args[1];
    log.setLevel(log.levelFromFlags(hasFlag(args, "--silent"), hasFlag(args, "--verbose")));

    if (std.mem.eql(u8, command, "status")) {
        const data_dir = getFlag(args, "--data-dir") orelse return usage();
        return status(data_dir);
    }

    if (std.mem.eql(u8, command, "follow")) {
        const rpc_url = getFlag(args, "--rpc") orelse return usage();
        const data_dir = getFlag(args, "--data-dir") orelse return usage();
        return head_follower.run(.{
            .rpc_url = rpc_url,
            .ws_url = getFlag(args, "--ws"),
            .data_dir = data_dir,
            .allow_rpc_catchup = hasFlag(args, "--catch-up-rpc"),
        });
    }

    if (std.mem.eql(u8, command, "serve")) {
        const data_dir = getFlag(args, "--data-dir") orelse return usage();
        const listen = getFlag(args, "--listen") orelse "127.0.0.1:9090";
        const colon = std.mem.lastIndexOfScalar(u8, listen, ':') orelse return usage();
        return tcp_server.run(.{
            .data_dir = data_dir,
            .host = listen[0..colon],
            .port = try std.fmt.parseInt(u16, listen[colon + 1 ..], 10),
            .max_connections = if (getFlag(args, "--max-connections")) |mc| try std.fmt.parseInt(u32, mc, 10) else 16,
        });
    }

    if (std.mem.eql(u8, command, "import")) {
        const data_dir = getFlag(args, "--data-dir") orelse return usage();

        if (getFlag(args, "--rocksdb")) |rocksdb_path| {
            // RocksDB import is a separate binary, avoids linking ~20MB of C into engine.
            // Forward --rpc (canonical-row resolution for reorg-history receipt
            // duplicates), the --start/--end bounds, and the log-level flags.
            var argv_list: std.ArrayListUnmanaged([]const u8) = .{};
            defer argv_list.deinit(alloc);
            try argv_list.appendSlice(alloc, &.{ "rocksdb-import", rocksdb_path, data_dir });
            if (getFlag(args, "--rpc")) |v| try argv_list.appendSlice(alloc, &.{ "--rpc", v });
            if (getFlag(args, "--start")) |v| try argv_list.appendSlice(alloc, &.{ "--start", v });
            if (getFlag(args, "--end")) |v| try argv_list.appendSlice(alloc, &.{ "--end", v });
            if (hasFlag(args, "--silent")) try argv_list.append(alloc, "--silent");
            if (hasFlag(args, "--verbose")) try argv_list.append(alloc, "--verbose");
            const argv = argv_list.items;
            // Inherit stdio so the importer's progress streams live. `Child.run`
            // buffered output into a 50 KB pipe, which a full import overflows
            // (then surfaced as a spawn failure), and it never checked the exit
            // status, so a failed import looked successful.
            var child = std.process.Child.init(argv, alloc);
            child.stdout_behavior = .Inherit;
            child.stderr_behavior = .Inherit;
            const term = child.spawnAndWait() catch |err| {
                log.err("Failed to exec rocksdb-import ({s}). Build it with: zig build import\n", .{@errorName(err)});
                return error.RocksdbImportFailed;
            };
            switch (term) {
                .Exited => |code| if (code != 0) {
                    log.err("rocksdb-import exited with code {d}\n", .{code});
                    return error.RocksdbImportFailed;
                },
                else => {
                    log.err("rocksdb-import terminated abnormally\n", .{});
                    return error.RocksdbImportFailed;
                },
            }
            return;
        }

        if (getFlag(args, "--rpc")) |rpc_url| {
            return rpc_import.run(.{
                .rpc_url = rpc_url,
                .data_dir = data_dir,
                .from_block = if (getFlag(args, "--from")) |f| try std.fmt.parseInt(u64, f, 10) else null,
                .to_block = if (getFlag(args, "--to")) |t| try std.fmt.parseInt(u64, t, 10) else null,
                .timestamps = !hasFlag(args, "--no-timestamps"),
            });
        }

        return usage();
    }

    return usage();
}

fn status(data_dir: []const u8) void {
    const alloc = std.heap.page_allocator;
    const m = core.flat_reader.FlatStoreReader.readMeta(data_dir) orelse {
        log.err("No flat store found at {s}\n", .{data_dir});
        std.process.exit(1);
    };
    log.info(
        \\=== Flat Store Status ===
        \\Data dir:            {s}
        \\Last finalized block: {d}
        \\blocks.dat size:     {d} bytes ({d:.1} GB)
        \\Index entries:       {d}
        \\Bloom entries:       {d}
        \\
    , .{
        data_dir,
        m.last_finalized_block,
        m.blocks_dat_size,
        @as(f64, @floatFromInt(m.blocks_dat_size)) / (1024 * 1024 * 1024),
        m.blocks_idx_count,
        m.blooms_count,
    });

    // Pending ring span: the pre-finalization window above the finalized tip.
    if (core.head_watch.readPending(alloc, data_dir)) |snap| {
        var s = snap;
        defer s.deinit(alloc);
        if (s.entries.len > 0)
            log.info("Pending ring:        blocks {d}..{d} ({d} blocks)\n", .{ s.entries[0].block_number, s.entries[s.entries.len - 1].block_number, s.entries.len })
        else
            log.info("Pending ring:        empty\n", .{});
    } else |_| {}

    // Timestamps coverage (u32 LE per block, dense from first_block).
    var dir = std.fs.cwd().openDir(data_dir, .{}) catch return;
    defer dir.close();
    if (dir.statFile("timestamps.bin")) |st|
        log.info("Timestamps:          {d} blocks covered\n", .{st.size / 4})
    else |_|
        log.info("Timestamps:          absent (formula fallback)\n", .{});
}

fn getFlag(args: []const [:0]u8, flag: []const u8) ?[]const u8 {
    for (args, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, flag) and i + 1 < args.len) return args[i + 1];
    }
    return null;
}

fn hasFlag(args: []const [:0]u8, flag: []const u8) bool {
    for (args) |arg| if (std.mem.eql(u8, arg, flag)) return true;
    return false;
}

fn usage() noreturn {
    log.err(
        \\Usage: emit-engine <command> [options]
        \\
        \\Commands:
        \\  import --rocksdb <path> --data-dir <path>   Bulk import from Nethermind
        \\  import --rpc <url> --data-dir <path> [--from N] [--to N] [--no-timestamps]
        \\                                              Import via eth_getLogs (+ timestamps)
        \\  follow --rpc <url> [--ws <url>] --data-dir <path> [--catch-up-rpc]
        \\                                              Follow chain head
        \\  serve [--listen <host:port>] [--max-connections <n>] --data-dir <path>
        \\                                              Stream filtered blocks to remote indexers (default 127.0.0.1:9090, 16 workers)
        \\  status --data-dir <path>                    Print store status
        \\
        \\Global: [--silent] errors only · [--verbose] add per-step detail
        \\
    , .{});
    std.process.exit(2);
}
