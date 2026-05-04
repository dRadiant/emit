/// emit-engine — imports EVM logs and follows chain head.
///
/// Commands:
///   import --rocksdb <path> --data-dir <path>   Bulk import from Nethermind receipts DB
///   import --rpc <url> --data-dir <path>        Import via eth_getLogs range queries
///   follow --rpc <url> --data-dir <path>        Follow chain head (HTTP polling)
///   status --data-dir <path>                    Print flat store status
const std = @import("std");
const core = @import("core");
const flat_writer_mod = @import("flat_writer.zig");
const head_follower = @import("head_follower.zig");

pub fn main() !void {
    const alloc = std.heap.page_allocator;
    const args = try std.process.argsAlloc(alloc);

    if (args.len < 2) return usage();

    const command = args[1];

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
        });
    }

    if (std.mem.eql(u8, command, "import")) {
        const data_dir = getFlag(args, "--data-dir") orelse return usage();

        if (getFlag(args, "--rocksdb")) |rocksdb_path| {
            // RocksDB import is a separate binary (avoids linking ~20MB of C into engine).
            // Exec it directly once built.
            const result = std.process.Child.run(.{
                .allocator = alloc,
                .argv = &.{ "rocksdb-import", rocksdb_path, data_dir },
            });
            if (result) |r| {
                if (r.stdout.len > 0) std.debug.print("{s}", .{r.stdout});
                if (r.stderr.len > 0) std.debug.print("{s}", .{r.stderr});
            } else |_| {
                std.debug.print("Failed to exec rocksdb-import. Build it with: zig build import\n", .{});
            }
            return;
        }

        if (getFlag(args, "--rpc")) |_| {
            std.debug.print("RPC import is experimental and not yet implemented.\n", .{});
            std.debug.print("Use --rocksdb for production imports.\n", .{});
            std.debug.print("RPC import is intended for unsupported chains or remote nodes only.\n", .{});
            return;
        }

        return usage();
    }

    return usage();
}

fn status(data_dir: []const u8) void {
    const meta = core.flat_reader.FlatStoreReader.readMeta(data_dir);
    if (meta) |m| {
        std.debug.print(
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
    } else {
        std.debug.print("No flat store found at {s}\n", .{data_dir});
    }
}

fn getFlag(args: []const [:0]u8, flag: []const u8) ?[]const u8 {
    for (args, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, flag) and i + 1 < args.len) return args[i + 1];
    }
    return null;
}

fn usage() void {
    std.debug.print(
        \\Usage: emit-engine <command> [options]
        \\
        \\Commands:
        \\  import --rocksdb <path> --data-dir <path>   Bulk import from Nethermind
        \\  import --rpc <url> --data-dir <path>        Import via eth_getLogs
        \\  follow --rpc <url> [--ws <url>] --data-dir <path>  Follow chain head
        \\  status --data-dir <path>                    Print store status
        \\
    , .{});
}
