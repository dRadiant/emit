/// SDK head-following loop: reads the engine's pending ring on each
/// inotify wakeup, classifies the diff against the prior tick, dispatches
/// new blocks through the per-block overlay, promotes finalized blocks
/// to MDBX, and recovers from reorgs.
const std = @import("std");

const builtin = @import("builtin");

const core = @import("core");

const flat_reader = core.flat_reader;
const pending_format = core.pending_format;

/// Linux-only: `Watcher` falls back to a sleep-based stub elsewhere.
pub const inotify_supported = builtin.target.os.tag == .linux;

pub const Entry = pending_format.Entry;

const PENDING_FILE = "pending.bin";
const META_FILE = "meta.bin";

/// Entries view into `buf`; both must stay alive together.
pub const PendingSnapshot = struct {
    buf: []u8,
    entries: []Entry,

    pub fn deinit(self: *PendingSnapshot, allocator: std.mem.Allocator) void {
        allocator.free(self.buf);
        allocator.free(self.entries);
        self.* = .{ .buf = &.{}, .entries = &.{} };
    }
};

/// Missing or empty pending.bin returns an empty snapshot — the engine
/// may not have written anything yet.
pub fn readPending(allocator: std.mem.Allocator, engine_data_dir: []const u8) !PendingSnapshot {
    var dir = try std.fs.cwd().openDir(engine_data_dir, .{});
    defer dir.close();

    const file = dir.openFile(PENDING_FILE, .{}) catch |err| switch (err) {
        error.FileNotFound => return PendingSnapshot{ .buf = &.{}, .entries = &.{} },
        else => return err,
    };
    defer file.close();

    const stat = try file.stat();
    if (stat.size == 0) return PendingSnapshot{ .buf = &.{}, .entries = &.{} };

    const buf = try allocator.alloc(u8, stat.size);
    errdefer allocator.free(buf);
    _ = try file.readAll(buf);

    const entries = try pending_format.parse(allocator, buf);
    return .{ .buf = buf, .entries = entries };
}

/// Missing or malformed meta degrades to 0 — the safe default for
/// `classifyChanges` (every disappeared block routes to `reorged_out`).
pub fn readMeta(engine_data_dir: []const u8) !u64 {
    var dir = try std.fs.cwd().openDir(engine_data_dir, .{});
    defer dir.close();

    const file = dir.openFile(META_FILE, .{}) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer file.close();

    var buf: [flat_reader.META_SIZE]u8 = undefined;
    const n = try file.readAll(&buf);
    if (n < flat_reader.META_SIZE) return 0;
    const meta = flat_reader.Meta.deserialize(&buf) orelse return 0;
    return meta.last_finalized_block;
}

/// Four-bucket diff between two pending snapshots.
///
/// `reorg_from`: lowest block where prev and curr disagree on hash.
/// `new_blocks`: tail of curr beyond prev's max block — borrowed from curr.
/// `finalized`: blocks in prev, absent from curr, ≤ `last_finalized`.
/// `reorged_out`: blocks in prev, absent from curr, > `last_finalized`.
///
/// The finalized/reorged_out split needs the meta.bin cross-check —
/// pending.bin alone can't distinguish age-out from `truncateFrom`.
pub const Classification = struct {
    reorg_from: ?u64 = null,
    new_blocks: []const Entry = &.{},
    finalized: []const u64 = &.{},
    reorged_out: []const u64 = &.{},

    pub fn deinit(self: *Classification, allocator: std.mem.Allocator) void {
        if (self.finalized.len > 0) allocator.free(self.finalized);
        if (self.reorged_out.len > 0) allocator.free(self.reorged_out);
        self.* = .{};
    }
};

pub fn classifyChanges(
    allocator: std.mem.Allocator,
    prev: []const Entry,
    curr: []const Entry,
    last_finalized: u64,
) !Classification {
    // Lowest same-block hash mismatch wins — deeper forks must converge there.
    var reorg_from: ?u64 = null;
    for (prev) |p| {
        for (curr) |c| {
            if (c.block_number != p.block_number) continue;
            if (!std.mem.eql(u8, &c.hash, &p.hash)) {
                if (reorg_from) |rf| {
                    if (p.block_number < rf) reorg_from = p.block_number;
                } else {
                    reorg_from = p.block_number;
                }
            }
            break;
        }
    }

    const prev_max: u64 = if (prev.len > 0) prev[prev.len - 1].block_number else 0;
    var new_start: usize = 0;
    if (prev.len > 0) {
        while (new_start < curr.len and curr[new_start].block_number <= prev_max) : (new_start += 1) {}
    }
    const new_blocks = curr[new_start..];

    var finalized: std.ArrayListUnmanaged(u64) = .{};
    errdefer finalized.deinit(allocator);
    var reorged_out: std.ArrayListUnmanaged(u64) = .{};
    errdefer reorged_out.deinit(allocator);

    for (prev) |p| {
        var present = false;
        for (curr) |c| {
            if (c.block_number == p.block_number) {
                present = true;
                break;
            }
        }
        if (present) continue;

        if (p.block_number <= last_finalized) {
            try finalized.append(allocator, p.block_number);
        } else {
            try reorged_out.append(allocator, p.block_number);
        }
    }

    return .{
        .reorg_from = reorg_from,
        .new_blocks = new_blocks,
        .finalized = try finalized.toOwnedSlice(allocator),
        .reorged_out = try reorged_out.toOwnedSlice(allocator),
    };
}

// ── Watcher: inotify on the engine data dir ──────────────────────────────

/// Inotify-driven wakeup on `IN_MOVED_TO` in the watched dir. Non-Linux
/// falls back to a `timeout_ms` sleep so the SDK still functions.
pub const Watcher = struct {
    fd: i32 = -1,

    pub fn init(dir_path: []const u8) !Watcher {
        if (!inotify_supported) return .{};
        const linux = std.os.linux;

        const init_rc = linux.inotify_init1(linux.IN.CLOEXEC | linux.IN.NONBLOCK);
        switch (std.posix.errno(init_rc)) {
            .SUCCESS => {},
            else => |e| return std.posix.unexpectedErrno(e),
        }
        const fd: i32 = @intCast(init_rc);
        errdefer std.posix.close(fd);

        if (dir_path.len >= std.fs.max_path_bytes) return error.NameTooLong;
        var path_buf: [std.fs.max_path_bytes:0]u8 = undefined;
        @memcpy(path_buf[0..dir_path.len], dir_path);
        path_buf[dir_path.len] = 0;

        const watch_rc = linux.inotify_add_watch(fd, @ptrCast(&path_buf), linux.IN.MOVED_TO);
        switch (std.posix.errno(watch_rc)) {
            .SUCCESS => {},
            else => |e| return std.posix.unexpectedErrno(e),
        }

        return .{ .fd = fd };
    }

    pub fn deinit(self: *Watcher) void {
        if (!inotify_supported) return;
        if (self.fd >= 0) {
            std.posix.close(self.fd);
            self.fd = -1;
        }
    }

    /// Returns on event or after `timeout_ms`. Drains queued event records
    /// so the next call blocks until the next event.
    pub fn wait(self: *Watcher, timeout_ms: u32) !void {
        if (!inotify_supported) {
            std.Thread.sleep(@as(u64, timeout_ms) * std.time.ns_per_ms);
            return;
        }

        var fds = [_]std.posix.pollfd{.{
            .fd = self.fd,
            .events = std.posix.POLL.IN,
            .revents = 0,
        }};
        const r = std.posix.poll(&fds, @intCast(timeout_ms)) catch return;
        if (r > 0 and (fds[0].revents & std.posix.POLL.IN) != 0) {
            var drain: [4096]u8 = undefined;
            _ = std.posix.read(self.fd, &drain) catch {};
        }
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

fn mkEntry(block: u64, hash_byte: u8) Entry {
    return .{
        .block_number = block,
        .hash = [_]u8{hash_byte} ** 32,
        .topic_bloom = std.mem.zeroes([core.bloom.BLOOM_SIZE]u8),
        .addr_bloom = std.mem.zeroes([core.bloom.ADDR_BLOOM_SIZE]u8),
        .lz4_entry = &.{},
    };
}

test "classifyChanges: append-only diff produces new_blocks only" {
    const prev = [_]Entry{mkEntry(100, 0xAA)};
    const curr = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA) };

    var c = try classifyChanges(testing.allocator, &prev, &curr, 0);
    defer c.deinit(testing.allocator);

    try testing.expect(c.reorg_from == null);
    try testing.expectEqual(@as(usize, 1), c.new_blocks.len);
    try testing.expectEqual(@as(u64, 101), c.new_blocks[0].block_number);
    try testing.expectEqual(@as(usize, 0), c.finalized.len);
    try testing.expectEqual(@as(usize, 0), c.reorged_out.len);
}

test "classifyChanges: empty prev makes everything new" {
    const curr = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA) };

    var c = try classifyChanges(testing.allocator, &.{}, &curr, 0);
    defer c.deinit(testing.allocator);

    try testing.expect(c.reorg_from == null);
    try testing.expectEqual(@as(usize, 2), c.new_blocks.len);
    try testing.expectEqual(@as(usize, 0), c.finalized.len);
    try testing.expectEqual(@as(usize, 0), c.reorged_out.len);
}

test "classifyChanges: hash mismatch at depth 1 sets reorg_from" {
    const prev = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA) };
    const curr = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xBB) };

    var c = try classifyChanges(testing.allocator, &prev, &curr, 0);
    defer c.deinit(testing.allocator);

    try testing.expectEqual(@as(u64, 101), c.reorg_from.?);
}

test "classifyChanges: deepest fork wins on multi-block reorg" {
    // Three blocks diverge; reorg_from must point at the lowest disagreement.
    const prev = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA), mkEntry(102, 0xAA), mkEntry(103, 0xAA) };
    const curr = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xBB), mkEntry(102, 0xBB), mkEntry(103, 0xBB) };

    var c = try classifyChanges(testing.allocator, &prev, &curr, 0);
    defer c.deinit(testing.allocator);

    try testing.expectEqual(@as(u64, 101), c.reorg_from.?);
}

test "classifyChanges: finalization promotes the disappeared oldest block" {
    const prev = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA) };
    const curr = [_]Entry{mkEntry(101, 0xAA)};

    var c = try classifyChanges(testing.allocator, &prev, &curr, 100);
    defer c.deinit(testing.allocator);

    try testing.expect(c.reorg_from == null);
    try testing.expectEqual(@as(usize, 1), c.finalized.len);
    try testing.expectEqual(@as(u64, 100), c.finalized[0]);
    try testing.expectEqual(@as(usize, 0), c.reorged_out.len);
    try testing.expectEqual(@as(usize, 0), c.new_blocks.len);
}

test "classifyChanges: reorg-truncation routes through reorged_out, not finalized" {
    // Block 102 disappeared but meta says last_finalized=99 — engine
    // truncated the tail mid-reorg, not aged it out of pending.
    const prev = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA), mkEntry(102, 0xAA) };
    const curr = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA) };

    var c = try classifyChanges(testing.allocator, &prev, &curr, 99);
    defer c.deinit(testing.allocator);

    try testing.expect(c.reorg_from == null);
    try testing.expectEqual(@as(usize, 0), c.finalized.len);
    try testing.expectEqual(@as(usize, 1), c.reorged_out.len);
    try testing.expectEqual(@as(u64, 102), c.reorged_out[0]);
}

test "classifyChanges: mixed tick — finalize oldest, new block appears, same tick" {
    const prev = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA) };
    const curr = [_]Entry{ mkEntry(101, 0xAA), mkEntry(102, 0xAA) };

    var c = try classifyChanges(testing.allocator, &prev, &curr, 100);
    defer c.deinit(testing.allocator);

    try testing.expect(c.reorg_from == null);
    try testing.expectEqual(@as(usize, 1), c.finalized.len);
    try testing.expectEqual(@as(u64, 100), c.finalized[0]);
    try testing.expectEqual(@as(usize, 1), c.new_blocks.len);
    try testing.expectEqual(@as(u64, 102), c.new_blocks[0].block_number);
    try testing.expectEqual(@as(usize, 0), c.reorged_out.len);
}

test "classifyChanges: identical snapshots produce no work" {
    const prev = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA) };
    const curr = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA) };

    var c = try classifyChanges(testing.allocator, &prev, &curr, 100);
    defer c.deinit(testing.allocator);

    try testing.expect(c.reorg_from == null);
    try testing.expectEqual(@as(usize, 0), c.new_blocks.len);
    try testing.expectEqual(@as(usize, 0), c.finalized.len);
    try testing.expectEqual(@as(usize, 0), c.reorged_out.len);
}

test "Watcher.wait honors timeout when no event arrives" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);

    var watcher = try Watcher.init(path);
    defer watcher.deinit();

    var timer = try std.time.Timer.start();
    try watcher.wait(50);
    const elapsed_ms = timer.read() / std.time.ns_per_ms;
    try testing.expect(elapsed_ms >= 40);
}

test "Watcher wakes on atomic rename into the watched dir (Linux)" {
    if (!inotify_supported) return error.SkipZigTest;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);

    var watcher = try Watcher.init(path);
    defer watcher.deinit();

    {
        const f = try tmp.dir.createFile("incoming.tmp", .{});
        f.close();
    }
    try tmp.dir.rename("incoming.tmp", "incoming");

    var timer = try std.time.Timer.start();
    try watcher.wait(5_000);
    const elapsed_ms = timer.read() / std.time.ns_per_ms;
    try testing.expect(elapsed_ms < 1_000);
}

test "readPending: missing pending.bin returns empty snapshot" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);

    var snap = try readPending(testing.allocator, path);
    defer snap.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 0), snap.entries.len);
}

test "readPending + readMeta against a FakeEngine snapshot" {
    const fake_engine = @import("fake_engine.zig");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(tmp.dir, testing.allocator);
    defer fake.deinit();

    const hash_a = [_]u8{0xAA} ** 32;
    try fake.ingest(100, hash_a, &.{});
    try fake.ingest(101, hash_a, &.{});
    try fake.finalize(100); // meta now reflects 100

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buf);

    var snap = try readPending(testing.allocator, path);
    defer snap.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 1), snap.entries.len);
    try testing.expectEqual(@as(u64, 101), snap.entries[0].block_number);

    try testing.expectEqual(@as(u64, 100), try readMeta(path));
}
