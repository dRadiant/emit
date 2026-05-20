/// SDK head-following loop: reads the engine's pending ring on each
/// inotify wakeup, classifies the diff against the prior tick, dispatches
/// new blocks through the per-block overlay, promotes finalized blocks
/// to MDBX, and recovers from reorgs.
const std = @import("std");

const builtin = @import("builtin");

const core = @import("core");

const handler_mod = @import("handler.zig");
const humanize = @import("humanize.zig");
const sdk_manifest = @import("manifest.zig");

const flat_reader = core.flat_reader;
const log_serial = core.log_serial;
const pending_format = core.pending_format;
const types = core.types;

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

// ── Live session ─────────────────────────────────────────────────────────

pub const RunOptions = struct {
    engine_data_dir: []const u8,
    /// Maximum wait between inotify wakeups. On non-Linux hosts the stub
    /// sleeps for this duration before every tick.
    tick_timeout_ms: u32 = 500,
};

/// Resources that persist across ticks: the watcher, the last-seen
/// pending snapshot, and the per-block decompress + log buffers. Held
/// here so `tick` doesn't reallocate per call. `run` constructs one
/// session and loops; tests drive `tick` directly.
pub const LiveSession = struct {
    allocator: std.mem.Allocator,
    engine_data_dir: []const u8,
    watcher: Watcher,
    prev: PendingSnapshot,
    decompress_buf: []u8,
    log_buf: []core.RawLog,

    pub fn init(allocator: std.mem.Allocator, engine_data_dir: []const u8) !LiveSession {
        var watcher = try Watcher.init(engine_data_dir);
        errdefer watcher.deinit();

        var prev = try readPending(allocator, engine_data_dir);
        errdefer prev.deinit(allocator);

        const decompress_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
        errdefer allocator.free(decompress_buf);
        const log_buf = try allocator.alloc(core.RawLog, types.MAX_LOGS_PER_BLOCK);
        errdefer allocator.free(log_buf);

        return .{
            .allocator = allocator,
            .engine_data_dir = engine_data_dir,
            .watcher = watcher,
            .prev = prev,
            .decompress_buf = decompress_buf,
            .log_buf = log_buf,
        };
    }

    pub fn deinit(self: *LiveSession) void {
        self.allocator.free(self.log_buf);
        self.allocator.free(self.decompress_buf);
        self.prev.deinit(self.allocator);
        self.watcher.deinit();
    }

    /// One pass of the live loop: wait, classify, dispatch new blocks,
    /// commit.
    pub fn tick(
        self: *LiveSession,
        comptime m: sdk_manifest.Manifest,
        comptime Handler: type,
        ctx: anytype,
        timeout_ms: u32,
    ) !void {
        try self.watcher.wait(timeout_ms);

        var curr = try readPending(self.allocator, self.engine_data_dir);
        errdefer curr.deinit(self.allocator);

        const last_finalized = try readMeta(self.engine_data_dir);

        var classification = try classifyChanges(
            self.allocator,
            self.prev.entries,
            curr.entries,
            last_finalized,
        );
        defer classification.deinit(self.allocator);

        for (classification.new_blocks) |entry| {
            setLiveBlock(ctx, entry.block_number);
            try dispatchBlock(m, Handler, ctx, entry, self.decompress_buf, self.log_buf);
        }

        // No commit on dispatch — saves landed in the overlay, not MDBX.

        self.prev.deinit(self.allocator);
        self.prev = curr;
    }
};

/// Block forever, dispatching new pending blocks as they arrive.
pub fn run(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    options: RunOptions,
) !void {
    comptime handler_mod.dispatcherFor(m).validateHandler(Handler);

    enter(ctx);

    var session = try LiveSession.init(ctx._allocator, options.engine_data_dir);
    defer session.deinit();

    while (true) {
        try session.tick(m, Handler, ctx, options.tick_timeout_ms);
    }
}

/// Flip `live = true` on every entity store. Saves from this point go
/// to the per-block overlay instead of MDBX; `commitBlock` is the only
/// path that promotes overlay state to durable storage. Idempotent —
/// counter-shaped test contexts without a `stores` field are skipped.
pub fn enter(ctx: anytype) void {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime !@hasField(T, "stores")) return;
    const Stores = @FieldType(T, "stores");
    inline for (std.meta.fields(Stores)) |f| {
        @field(ctx.stores, f.name).live = true;
    }
}

inline fn setLiveBlock(ctx: anytype, block: u64) void {
    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime !@hasField(T, "stores")) return;
    const Stores = @FieldType(T, "stores");
    inline for (std.meta.fields(Stores)) |f| {
        @field(ctx.stores, f.name).live_block = block;
    }
}

fn dispatchBlock(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    entry: Entry,
    decompress_buf: []u8,
    log_buf: []core.RawLog,
) !void {
    const decoded = try log_serial.decompressEntry(entry.lz4_entry, decompress_buf);
    const log_count = log_serial.deserializeLogs(decoded, log_buf);

    ctx.block_number = entry.block_number;
    ctx.timestamp = humanize.blockTimestamp(entry.block_number);

    for (log_buf[0..log_count]) |*log| {
        log.block_number = entry.block_number;
        try handler_mod.dispatchLog(m, Handler, ctx, log.*);
    }

    const T = std.meta.Child(@TypeOf(ctx));
    if (comptime @hasField(T, "_last_dispatched_block")) {
        ctx._last_dispatched_block = entry.block_number;
    }
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

// ── Live tick integration ────────────────────────────────────────────────

const fake_engine = @import("fake_engine.zig");

const TestTransfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
};

const TEST_CONTRACT: [20]u8 = [_]u8{0xAB} ** 20;

const TestManifest: sdk_manifest.Manifest = .{
    .name = "live-tick",
    .chain_id = 1,
    .start_block = 0,
    .contracts = &.{.{ .name = "T", .address = TEST_CONTRACT, .events = &.{TestTransfer} }},
};

/// Counter-shaped ctx + handler: cheap stand-in for a real Context that
/// still exercises the dispatch pipeline. No entity stores, so the
/// overlay path is a no-op via the @hasField gate on `enter` / `tick`.
const TestRunner = struct {
    _allocator: std.mem.Allocator,
    block_number: u64 = 0,
    timestamp: u64 = 0,
    transfers: u32 = 0,
    _last_dispatched_block: u64 = 0,

    pub fn handleTransfer(_: handler_mod.Log(TestTransfer), self: *TestRunner) !void {
        self.transfers += 1;
    }
};

fn makeTransferLog(from: [20]u8, to: [20]u8, value: u64, data_buf: *[32]u8) core.RawLog {
    var from_topic: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(from_topic[12..32], &from);
    var to_topic: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(to_topic[12..32], &to);
    @memset(data_buf, 0);
    std.mem.writeInt(u64, data_buf[24..32], value, .big);
    return .{
        .block_number = 0,
        .tx_index = 0,
        .log_index = 0,
        .address = TEST_CONTRACT,
        .topic_count = 3,
        .topics = .{ sdk_manifest.eventTopic0(TestTransfer), from_topic, to_topic, [_]u8{0} ** 32 },
        .data = data_buf,
        .tx_hash = [_]u8{0xFE} ** 32,
    };
}

test "tick dispatches a pending block ingested by FakeEngine" {
    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &path_buf);

    // Session must initialize BEFORE the ingest so its `prev` snapshot
    // starts empty — otherwise the planted block would already be in
    // `prev` and the diff would report no new blocks.
    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    var data_buf: [32]u8 = undefined;
    const log = makeTransferLog([_]u8{0} ** 20, ALICE, 100, &data_buf);
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{log});

    var runner = TestRunner{ ._allocator = testing.allocator };
    try session.tick(TestManifest, TestRunner, &runner, 200);

    try testing.expectEqual(@as(u32, 1), runner.transfers);
    try testing.expectEqual(@as(u64, 100), runner._last_dispatched_block);
}

test "tick routes saves through the per-block overlay, not MDBX" {
    const lmdbx = @import("lmdbx");
    const root = @import("root.zig");
    const mutable_store = @import("mutable_store.zig");
    const entity_serial = @import("entity_serial.zig");

    const Account = struct {
        pub const storage: root.StorageMode = .mutable;
        id: [20]u8,
        balance: u64,
    };

    const OverlayHandler = struct {
        pub fn handleTransfer(log: handler_mod.Log(TestTransfer), ctx: anytype) !void {
            const to = log.topics[2][12..32].*;
            const value: u64 = std.mem.readInt(u64, log.data[24..32], .big);
            var receiver = try ctx.stores.accounts.loadOrInit(to);
            receiver.balance +%= value;
            try ctx.stores.accounts.save(receiver);
        }
    };

    const Manifest: sdk_manifest.Manifest = .{
        .name = "overlay",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = TEST_CONTRACT, .events = &.{TestTransfer} }},
    };

    const TestStores = struct { accounts: mutable_store.MutableStore(Account) };
    const TestCtx = struct {
        _allocator: std.mem.Allocator,
        _env: lmdbx.Environment,
        _active_txn: lmdbx.Transaction,
        block_number: u64 = 0,
        timestamp: u64 = 0,
        stores: TestStores,
        _last_dispatched_block: u64 = 0,
    };

    // Engine + SDK data dirs.
    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();
    var engine_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &engine_path_buf);

    var entity_tmp = testing.tmpDir(.{});
    defer entity_tmp.cleanup();
    var entity_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const entity_path = try entity_tmp.dir.realpathZ(".", &entity_path_buf);
    var entity_path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(entity_path_z[0..entity_path.len], entity_path);
    entity_path_z[entity_path.len] = 0;

    const env = try lmdbx.Environment.init(@ptrCast(&entity_path_z), .{ .max_dbs = 4 });
    defer env.deinit() catch {};

    const ctx = try testing.allocator.create(TestCtx);
    defer testing.allocator.destroy(ctx);
    ctx.* = .{
        ._allocator = testing.allocator,
        ._env = env,
        ._active_txn = try env.transaction(.{}),
        .stores = undefined,
    };
    ctx.stores.accounts = try mutable_store.MutableStore(Account).open(testing.allocator, &ctx._active_txn, "accounts");
    defer ctx.stores.accounts.deinit();
    defer ctx._active_txn.abort() catch {};

    enter(ctx);

    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    // Three pending blocks, each crediting a different recipient.
    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    const BOB: [20]u8 = [_]u8{0xB2} ** 20;
    const CARL: [20]u8 = [_]u8{0xC3} ** 20;
    var bufs: [3][32]u8 = undefined;
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, ALICE, 100, &bufs[0])});
    try fake.ingest(101, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, BOB, 200, &bufs[1])});
    try fake.ingest(102, [_]u8{0xAA} ** 32, &.{makeTransferLog([_]u8{0} ** 20, CARL, 300, &bufs[2])});

    try session.tick(Manifest, OverlayHandler, ctx, 200);

    // Overlay holds one entry per key, each tagged with its dispatch block.
    try testing.expectEqual(@as(u32, 3), ctx.stores.accounts.pendingCount());

    const alice_entry = ctx.stores.accounts.pending.get(ALICE).?;
    try testing.expectEqual(@as(u64, 100), alice_entry.block);
    try testing.expectEqual(@as(u64, 100), alice_entry.value.balance);

    const bob_entry = ctx.stores.accounts.pending.get(BOB).?;
    try testing.expectEqual(@as(u64, 101), bob_entry.block);
    try testing.expectEqual(@as(u64, 200), bob_entry.value.balance);

    const carl_entry = ctx.stores.accounts.pending.get(CARL).?;
    try testing.expectEqual(@as(u64, 102), carl_entry.block);
    try testing.expectEqual(@as(u64, 300), carl_entry.value.balance);

    // MDBX is empty for every key — nothing was committed.
    const db = lmdbx.Database{ .txn = ctx._active_txn, .dbi = ctx.stores.accounts.dbi };
    inline for (.{ ALICE, BOB, CARL }) |addr| {
        var key_buf: [20]u8 = undefined;
        entity_serial.encodeKey([20]u8, addr, &key_buf);
        try testing.expect((try db.get(&key_buf)) == null);
    }

    // `load` returns the overlay value (not MDBX).
    const loaded = (try ctx.stores.accounts.load(ALICE)).?;
    try testing.expectEqual(@as(u64, 100), loaded.balance);
}

test "tick is a no-op when pending is unchanged" {
    var engine_tmp = testing.tmpDir(.{});
    defer engine_tmp.cleanup();
    var fake = fake_engine.FakeEngine.init(engine_tmp.dir, testing.allocator);
    defer fake.deinit();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const engine_path = try engine_tmp.dir.realpath(".", &path_buf);

    var session = try LiveSession.init(testing.allocator, engine_path);
    defer session.deinit();

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    var data_buf: [32]u8 = undefined;
    const log = makeTransferLog([_]u8{0} ** 20, ALICE, 100, &data_buf);
    try fake.ingest(100, [_]u8{0xAA} ** 32, &.{log});

    var runner = TestRunner{ ._allocator = testing.allocator };
    try session.tick(TestManifest, TestRunner, &runner, 100);
    try testing.expectEqual(@as(u32, 1), runner.transfers);

    try session.tick(TestManifest, TestRunner, &runner, 100);
    try testing.expectEqual(@as(u32, 1), runner.transfers);
}

test "readPending + readMeta against a FakeEngine snapshot" {
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
