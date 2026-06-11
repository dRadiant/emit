/// Watching the engine's head. Read the pending ring, diff successive snapshots
/// into new/finalized/reorged buckets, and wake on ring updates via inotify.
/// Shared by the sdk live loop and the engine's remote streaming server, which
/// both tail `pending.bin` the same way.
const std = @import("std");
const builtin = @import("builtin");

const pending_format = @import("pending_format.zig");
const flat_reader = @import("flat_reader.zig");

const Entry = pending_format.Entry;

const PENDING_FILE = "pending.bin";
const META_FILE = "meta.bin";

/// Linux-only. `Watcher` falls back to a sleep-based stub elsewhere.
pub const inotify_supported = builtin.target.os.tag == .linux;

/// Entries view into `buf`. Both must stay alive together.
pub const PendingSnapshot = struct {
    buf: []u8,
    entries: []Entry,

    pub fn deinit(self: *PendingSnapshot, allocator: std.mem.Allocator) void {
        allocator.free(self.buf);
        allocator.free(self.entries);
        self.* = .{ .buf = &.{}, .entries = &.{} };
    }
};

/// Missing or empty pending.bin returns an empty snapshot. The engine may not
/// have written anything yet.
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

    const entries = pending_format.parse(allocator, buf) catch |err| switch (err) {
        // An old (pre-magic) or unrecognized pending.bin reads as empty. The
        // engine rewrites it in the current format on its next ingest. Genuine
        // truncation of a current-format file still surfaces.
        error.InvalidMagic => {
            allocator.free(buf);
            return PendingSnapshot{ .buf = &.{}, .entries = &.{} };
        },
        else => return err,
    };
    return .{ .buf = buf, .entries = entries };
}

/// Build a `prev` seed for the live loop holding only entries at or below
/// `bound`. `classifyChanges` reads just `block_number` and `hash` from `prev`,
/// so the lz4 payload is dropped (left empty) and `buf` is unused. The pending
/// ring holds the pre-finalization window above the committed cursor, and those
/// blocks must surface as new on the first tick. Seeding `prev` with the whole
/// ring hides them. The diff sees no new tail, so they are never dispatched and
/// are silently skipped when they later finalize past the cursor.
pub fn snapshotUpTo(allocator: std.mem.Allocator, src: PendingSnapshot, bound: u64) !PendingSnapshot {
    var k: usize = 0;
    while (k < src.entries.len and src.entries[k].block_number <= bound) : (k += 1) {}
    if (k == 0) return PendingSnapshot{ .buf = &.{}, .entries = &.{} };

    const entries = try allocator.alloc(Entry, k);
    for (entries, src.entries[0..k]) |*dst, s| {
        dst.* = s;
        // prev's payload is never read. Drop the alias into src.buf so the seed
        // owns nothing and stays valid after the source snapshot is freed.
        dst.lz4_entry = &.{};
    }
    return .{ .buf = &.{}, .entries = entries };
}

/// Missing or malformed meta degrades to 0. Safe default for `classifyChanges`
/// (every disappeared block routes to `reorged_out`).
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
/// `new_blocks`: tail of curr beyond prev's max block, borrowed from curr.
/// `finalized`: blocks in prev, absent from curr, at or below `last_finalized`.
/// `reorged_out`: blocks in prev, absent from curr, above `last_finalized`.
///
/// The finalized/reorged_out split needs the meta.bin cross-check. pending.bin
/// alone can't distinguish age-out from `truncateFrom`.
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
    // Lowest same-block hash mismatch wins. Deeper forks must converge there.
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

/// Inotify-driven wakeup on `IN_MOVED_TO` in the watched dir (the engine
/// commits pending.bin by atomic rename). Non-Linux falls back to a
/// `timeout_ms` sleep so callers still function.
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

    /// Returns on event or after `timeout_ms`. Drains queued event records so
    /// the next call blocks until the next event.
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

    /// Which sources woke a `waitWith`. `pending`: pending.bin changed (or the
    /// non-inotify fallback ticked). `extra`: the extra fd has data to read.
    pub const Ready = struct { pending: bool = false, extra: bool = false };

    /// Like `wait`, but also polls `extra_fd` (a client socket). Lets the engine
    /// stream new blocks and read inbound frames on one thread. Drains inotify
    /// records when they fire. The non-inotify fallback reports `pending` each
    /// tick so callers still re-scan.
    pub fn waitWith(self: *Watcher, extra_fd: i32, timeout_ms: u32) Ready {
        if (!inotify_supported) {
            std.Thread.sleep(@as(u64, timeout_ms) * std.time.ns_per_ms);
            return .{ .pending = true };
        }

        var fds = [_]std.posix.pollfd{
            .{ .fd = self.fd, .events = std.posix.POLL.IN, .revents = 0 },
            .{ .fd = extra_fd, .events = std.posix.POLL.IN, .revents = 0 },
        };
        const r = std.posix.poll(&fds, @intCast(timeout_ms)) catch return .{};
        if (r <= 0) return .{};

        var ready = Ready{};
        if ((fds[0].revents & std.posix.POLL.IN) != 0) {
            var drain: [4096]u8 = undefined;
            _ = std.posix.read(self.fd, &drain) catch {};
            ready.pending = true;
        }
        if ((fds[1].revents & std.posix.POLL.IN) != 0) ready.extra = true;
        return ready;
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;
const bloom = @import("bloom.zig");

fn mkEntry(block: u64, hash_byte: u8) Entry {
    return .{
        .block_number = block,
        .hash = [_]u8{hash_byte} ** 32,
        .topic_bloom = std.mem.zeroes([bloom.BLOOM_SIZE]u8),
        .addr_bloom = std.mem.zeroes([bloom.ADDR_BLOOM_SIZE]u8),
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
}

test "classifyChanges: hash mismatch sets reorg_from at the lowest disagreement" {
    const prev = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA), mkEntry(102, 0xAA) };
    const curr = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xBB), mkEntry(102, 0xBB) };

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
}

test "classifyChanges: a finalize and a new block in the same tick" {
    const prev = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA) };
    const curr = [_]Entry{ mkEntry(101, 0xAA), mkEntry(102, 0xAA) };

    var c = try classifyChanges(testing.allocator, &prev, &curr, 100);
    defer c.deinit(testing.allocator);

    try testing.expect(c.reorg_from == null);
    try testing.expectEqual(@as(usize, 1), c.finalized.len);
    try testing.expectEqual(@as(u64, 100), c.finalized[0]);
    try testing.expectEqual(@as(usize, 1), c.new_blocks.len);
    try testing.expectEqual(@as(u64, 102), c.new_blocks[0].block_number);
}

test "classifyChanges: reorg-truncation routes through reorged_out, not finalized" {
    const prev = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA), mkEntry(102, 0xAA) };
    const curr = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA) };

    var c = try classifyChanges(testing.allocator, &prev, &curr, 99);
    defer c.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 0), c.finalized.len);
    try testing.expectEqual(@as(usize, 1), c.reorged_out.len);
    try testing.expectEqual(@as(u64, 102), c.reorged_out[0]);
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

test "snapshotUpTo keeps only entries at or below the bound" {
    const full = [_]Entry{ mkEntry(100, 0xAA), mkEntry(101, 0xAA), mkEntry(102, 0xAA) };
    const src = PendingSnapshot{ .buf = &.{}, .entries = @constCast(full[0..]) };

    // Bound below the ring keeps nothing. The whole ring is backlog to dispatch.
    var none = try snapshotUpTo(testing.allocator, src, 99);
    defer none.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 0), none.entries.len);

    // Bound inside the ring keeps the covered prefix only.
    var some = try snapshotUpTo(testing.allocator, src, 101);
    defer some.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 2), some.entries.len);
    try testing.expectEqual(@as(u64, 101), some.entries[1].block_number);
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
