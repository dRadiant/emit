/// emit core — shared infrastructure for the flat log store.
///
/// Provides read-only access to blocks.dat/blooms.bin/blocks.idx,
/// bloom filter operations, parallel block filtering by address,
/// packed log serialization, and io_uring read pipeline.
///
/// Imported by both engine (import + head follow) and sdk (filtered index build).
const std = @import("std");

pub const types = @import("types.zig");
pub const bloom = @import("bloom.zig");
pub const flat_reader = @import("flat_reader.zig");
pub const block_filter = @import("block_filter.zig");
pub const log_serial = @import("log_serial.zig");
pub const io_pipeline = @import("io_pipeline.zig");
pub const parallel = @import("parallel.zig");
pub const pending_format = @import("pending_format.zig");

// Re-export commonly used types at top level for convenience.
pub const RawLog = types.RawLog;
pub const Bloom = bloom.Bloom;
pub const AddrBloom = bloom.AddrBloom;
pub const FlatStoreReader = flat_reader.FlatStoreReader;
pub const Meta = flat_reader.Meta;

/// `tmp + fsync + rename` snapshot writer. Any reader of `final_name` sees
/// either the previous content or the new — never torn. Used by the
/// engine's `pending.bin` + `meta.bin` writers and the SDK's fake engine.
pub fn writeAtomicFile(
    dir: std.fs.Dir,
    tmp_name: []const u8,
    final_name: []const u8,
    bytes: []const u8,
) !void {
    {
        const tmp = try dir.createFile(tmp_name, .{});
        defer tmp.close();
        try tmp.writeAll(bytes);
        try tmp.sync();
    }
    try dir.rename(tmp_name, final_name);
}

test {
    _ = types;
    _ = bloom;
    _ = flat_reader;
    _ = block_filter;
    _ = log_serial;
    _ = io_pipeline;
    _ = parallel;
    _ = pending_format;
}

test "writeAtomicFile rename places content under final name" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeAtomicFile(tmp.dir, "x.tmp", "x", "hello");

    const file = try tmp.dir.openFile("x", .{});
    defer file.close();
    var buf: [5]u8 = undefined;
    _ = try file.readAll(&buf);
    try std.testing.expectEqualSlices(u8, "hello", &buf);
    try std.testing.expectError(error.FileNotFound, tmp.dir.openFile("x.tmp", .{}));
}
