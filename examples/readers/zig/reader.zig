/// Reference Zig reader for emit's state.snap files. No dependency on emit-sdk.
///
/// Decodes a MutableStore's records straight from
/// the binary format documented in docs/entity-format.md.
///
/// Usage against the ERC20 example:
///
///     zig build-exe reader.zig -O ReleaseFast
///     ./reader /path/to/erc20/data/state.snap
///
/// The schema (mutable count, immutable count, per-slot record size, field
/// layout) is comptime-known in the indexer. Readers supply it as external
/// knowledge. No per-slot descriptor exists on disk.
const std = @import("std");

const MAGIC: *const [8]u8 = "EMITSTAT";
const VERSION: u32 = 1;

// ERC20 schema. Adjust these constants for other indexers.
const NUM_MUTABLE = 2; // Account, Allowance
const NUM_IMMUTABLE = 2; // Transfer, Approval

const Account = struct {
    id: [20]u8, // big-endian (primary key)
    balance: u256, // little-endian

    const RECORD_SIZE: usize = 20 + 32;
};

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);
    if (args.len != 2) {
        std.debug.print("usage: {s} <state.snap>\n", .{args[0]});
        return error.MissingArg;
    }

    const file = try std.fs.cwd().openFile(args[1], .{});
    defer file.close();
    const stat = try file.stat();
    const buf = try allocator.alloc(u8, stat.size);
    defer allocator.free(buf);
    if ((try file.readAll(buf)) != stat.size) return error.Truncated;

    if (!std.mem.eql(u8, buf[0..8], MAGIC)) return error.InvalidMagic;
    const version = std.mem.readInt(u32, buf[8..12], .little);
    if (version != VERSION) return error.VersionMismatch;
    const cursor = std.mem.readInt(u64, buf[12..20], .little);

    var pos: usize = 20;
    var mutable_bytes: [NUM_MUTABLE]u64 = undefined;
    for (&mutable_bytes) |*b| {
        b.* = std.mem.readInt(u64, buf[pos..][0..8], .little);
        pos += 8;
    }
    var immutable_counts: [NUM_IMMUTABLE]u64 = undefined;
    for (&immutable_counts) |*c| {
        c.* = std.mem.readInt(u64, buf[pos..][0..8], .little);
        pos += 8;
    }

    // Slot 0 = Account slab. Slot 1 = Allowance, not decoded here (same
    // struct/parse pattern with a different layout).
    const account_slab = buf[pos .. pos + mutable_bytes[0]];
    std.debug.assert(account_slab.len % Account.RECORD_SIZE == 0);
    const account_count = account_slab.len / Account.RECORD_SIZE;

    std.debug.print("cursor:            {d}\n", .{cursor});
    std.debug.print("accounts:          {d}\n", .{account_count});
    std.debug.print("transfer count:    {d}\n", .{immutable_counts[0]});
    std.debug.print("approval count:    {d}\n", .{immutable_counts[1]});

    if (account_count > 0) {
        const first = parseAccount(account_slab[0..Account.RECORD_SIZE]);
        std.debug.print("first account:     id=0x", .{});
        for (first.id) |b| std.debug.print("{x:0>2}", .{b});
        std.debug.print(" balance={d}\n", .{first.balance});
    }
}

fn parseAccount(record: *const [Account.RECORD_SIZE]u8) Account {
    // First field is big-endian (sorted-by-bytes order). Data fields are
    // little-endian.
    return .{
        .id = record[0..20].*,
        .balance = std.mem.readInt(u256, record[20..52], .little),
    };
}
