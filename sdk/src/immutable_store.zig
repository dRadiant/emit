/// ImmutableStore(T): append-only entity store backed by an `EventLog(T)`
/// over `<entity>.events.dat`. `load` is a `@compileError` so a
/// MutableStore/ImmutableStore mixup fails at compile time.
///
/// Keys are big-endian (via `entity_serial.serialize`'s first-field rule)
/// so sorted-by-bytes equals sorted-by-numeric.
///
/// In live mode, saves accumulate in a per-block buffer; `commitBlock`
/// drains one block's list into a regular append queue; `flushAppends`
/// writes the queue to `events.dat`. The authoritative record count is
/// owned by `state.snap`; this store tracks it locally so out-of-order
/// `save` is caught before the disk write.
const std = @import("std");

const event_log_mod = @import("event_log.zig");
const entity_serial = @import("entity_serial.zig");

pub const AppendError = error{ KeyOutOfOrder, OutOfMemory };

pub fn ImmutableStore(comptime T: type) type {
    const fields = @typeInfo(T).@"struct".fields;
    if (fields.len == 0) @compileError("ImmutableStore: entity '" ++ @typeName(T) ++ "' has no fields. The first field must be the primary key.");

    const KEY_SIZE = entity_serial.fixedSize(fields[0].type, @typeName(T) ++ "." ++ fields[0].name);
    const VALUE_SIZE = entity_serial.entitySize(T);

    return struct {
        const Self = @This();
        pub const Entity = T;
        pub const Log = event_log_mod.EventLog(T);
        pub const key_size = KEY_SIZE;
        pub const value_size = VALUE_SIZE;

        allocator: std.mem.Allocator,
        log: *Log,
        /// Authoritative count from `state.snap.immutable_counts`. Updated
        /// in lockstep with the matching `state_snap.commit`.
        committed_count: u64,
        /// Largest key already in the log (or pending), for monotonic checks.
        last_key: ?[KEY_SIZE]u8 = null,
        /// Records appended since the last `state.snap` commit. Drained by
        /// `flushAppends`.
        pending_appended: std.ArrayListUnmanaged(T) = .{},

        live: bool = false,
        live_block: u64 = 0,
        block_pending: std.AutoHashMapUnmanaged(u64, std.ArrayListUnmanaged(T)) = .{},

        /// `log` is owned by the caller (typically the SDK Context). The
        /// store does not close it on deinit; it only frees its own buffers.
        pub fn open(
            allocator: std.mem.Allocator,
            log: *Log,
            initial_count: u64,
        ) !Self {
            var self = Self{
                .allocator = allocator,
                .log = log,
                .committed_count = initial_count,
            };
            if (initial_count > 0) self.last_key = try log.readKey(initial_count - 1);
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.pending_appended.deinit(self.allocator);
            var it = self.block_pending.iterator();
            while (it.next()) |entry| entry.value_ptr.deinit(self.allocator);
            self.block_pending.deinit(self.allocator);
        }

        pub fn save(self: *Self, entity: T) AppendError!void {
            var key_buf: [KEY_SIZE]u8 = undefined;
            entity_serial.serializeKey(T, entity, &key_buf);
            if (self.last_key) |lk| {
                if (std.mem.order(u8, &key_buf, &lk) != .gt) return error.KeyOutOfOrder;
            }
            self.last_key = key_buf;

            if (self.live) {
                const gop = self.block_pending.getOrPut(self.allocator, self.live_block) catch return error.OutOfMemory;
                if (!gop.found_existing) gop.value_ptr.* = .{};
                gop.value_ptr.append(self.allocator, entity) catch return error.OutOfMemory;
                return;
            }
            self.pending_appended.append(self.allocator, entity) catch return error.OutOfMemory;
        }

        /// Immutable entities are never loaded during backfill. The cache
        /// in MutableStore exists for the mutable case; calling `load` on
        /// an ImmutableStore is almost always a MutableStore/ImmutableStore
        /// mixup, so catch it at compile time.
        pub fn load(_: Self, _: anytype) !?T {
            @compileError("ImmutableStore.load is not supported: cannot load immutable entities during backfill");
        }

        /// Drain one block's overlay into the pending-append queue. The
        /// actual disk write happens later via `flushAppends`.
        pub fn commitBlock(self: *Self, block: u64) AppendError!void {
            const removed = self.block_pending.fetchRemove(block) orelse return;
            var entities = removed.value;
            defer entities.deinit(self.allocator);
            for (entities.items) |e| {
                self.pending_appended.append(self.allocator, e) catch return error.OutOfMemory;
            }
        }

        /// Append every queued record to `events.dat`. Caller must follow
        /// with `state_snap.commit` to publish the new authoritative count,
        /// then call `markCommitted` to advance this store's view.
        pub fn flushAppends(self: *Self) !void {
            if (self.pending_appended.items.len == 0) return;
            try self.log.append(self.pending_appended.items, self.committed_count);
        }

        /// New authoritative count to embed in `state.snap.immutable_counts`.
        pub fn nextCommittedCount(self: *const Self) u64 {
            return self.committed_count + self.pending_appended.items.len;
        }

        /// Advance the local committed-count view after `state_snap.commit`
        /// publishes the new count.
        pub fn markCommitted(self: *Self) void {
            self.committed_count += self.pending_appended.items.len;
            self.pending_appended.clearRetainingCapacity();
        }

        /// Drop the entire overlay. Used on reorg.
        pub fn discardAll(self: *Self) void {
            var it = self.block_pending.iterator();
            while (it.next()) |entry| entry.value_ptr.deinit(self.allocator);
            self.block_pending.clearRetainingCapacity();
            self.pending_appended.clearRetainingCapacity();
            // last_key remains anchored to whatever is durably committed.
            if (self.committed_count > 0) {
                self.last_key = self.log.readKey(self.committed_count - 1) catch null;
            } else {
                self.last_key = null;
            }
        }

        /// Total entries across every per-block buffer plus the append queue.
        pub fn pendingCount(self: *const Self) u32 {
            var total: u32 = @intCast(self.pending_appended.items.len);
            var it = self.block_pending.iterator();
            while (it.next()) |entry| total += @intCast(entry.value_ptr.items.len);
            return total;
        }
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

fn idKey(block: u32, log_index: u32) [8]u8 {
    var out: [8]u8 = undefined;
    std.mem.writeInt(u32, out[0..4], block, .big);
    std.mem.writeInt(u32, out[4..8], log_index, .big);
    return out;
}

const E = struct { id: [8]u8, value: u64 };

test "comptime sizes" {
    const Big = struct { id: [8]u8, addr: [20]u8, value: u256, block: u64 };
    const S = ImmutableStore(Big);
    try testing.expectEqual(@as(usize, 8), S.key_size);
    try testing.expectEqual(@as(usize, 8 + 20 + 32 + 8), S.value_size);
}

test "save buffers to pending; monotonic checks fire before any disk write" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try event_log_mod.EventLog(E).open(testing.allocator, tmp.dir, "ev.events.dat");
    defer log.deinit();

    var store = try ImmutableStore(E).open(testing.allocator, &log, 0);
    defer store.deinit();

    try store.save(.{ .id = idKey(1, 0), .value = 100 });
    try store.save(.{ .id = idKey(1, 1), .value = 101 });
    try store.save(.{ .id = idKey(2, 0), .value = 200 });

    // Out-of-order: (1, 2) < (2, 0).
    try testing.expectError(error.KeyOutOfOrder, store.save(.{ .id = idKey(1, 2), .value = 102 }));

    // Nothing past the magic header until flushAppends runs.
    const end = try log.file.getEndPos();
    try testing.expectEqual(@as(u64, event_log_mod.HEADER_SIZE), end);
}

test "flushAppends + markCommitted advance the durable count" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try event_log_mod.EventLog(E).open(testing.allocator, tmp.dir, "ev.events.dat");
    defer log.deinit();

    var store = try ImmutableStore(E).open(testing.allocator, &log, 0);
    defer store.deinit();

    try store.save(.{ .id = idKey(1, 0), .value = 100 });
    try store.save(.{ .id = idKey(1, 1), .value = 101 });
    try testing.expectEqual(@as(u64, 2), store.nextCommittedCount());

    try store.flushAppends();
    store.markCommitted();
    try testing.expectEqual(@as(u64, 2), store.committed_count);
    try testing.expectEqual(@as(u32, 0), store.pendingCount());

    // Records readable via the log.
    const got = try log.read(0);
    try testing.expectEqual(@as(u64, 100), got.value);
}

test "live save buffers per block; commitBlock moves into the append queue" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try event_log_mod.EventLog(E).open(testing.allocator, tmp.dir, "ev.events.dat");
    defer log.deinit();

    var store = try ImmutableStore(E).open(testing.allocator, &log, 0);
    defer store.deinit();
    store.live = true;

    store.live_block = 100;
    try store.save(.{ .id = idKey(100, 0), .value = 1 });
    try store.save(.{ .id = idKey(100, 1), .value = 2 });
    store.live_block = 101;
    try store.save(.{ .id = idKey(101, 0), .value = 3 });

    try testing.expectEqual(@as(u32, 3), store.pendingCount());

    try store.commitBlock(100);
    // Block 100's entries are now in the append queue; block 101 still in overlay.
    try testing.expectEqual(@as(u32, 3), store.pendingCount());
    try testing.expectEqual(@as(usize, 2), store.pending_appended.items.len);
    try testing.expectEqual(@as(usize, 1), (store.block_pending.get(101).?).items.len);
}

test "discardAll drops the overlay and the append queue" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try event_log_mod.EventLog(E).open(testing.allocator, tmp.dir, "ev.events.dat");
    defer log.deinit();

    var store = try ImmutableStore(E).open(testing.allocator, &log, 0);
    defer store.deinit();
    store.live = true;
    store.live_block = 100;

    try store.save(.{ .id = idKey(100, 0), .value = 1 });
    store.live_block = 101;
    try store.save(.{ .id = idKey(101, 0), .value = 2 });
    try testing.expectEqual(@as(u32, 2), store.pendingCount());

    store.discardAll();
    try testing.expectEqual(@as(u32, 0), store.pendingCount());
    try testing.expectEqual(@as(?[8]u8, null), store.last_key);
}

test "backfill leaves the overlay empty" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try event_log_mod.EventLog(E).open(testing.allocator, tmp.dir, "ev.events.dat");
    defer log.deinit();

    var store = try ImmutableStore(E).open(testing.allocator, &log, 0);
    defer store.deinit();

    try store.save(.{ .id = idKey(1, 0), .value = 1 });
    try store.save(.{ .id = idKey(1, 1), .value = 2 });
    // pendingCount includes the append queue; the block_pending overlay alone is empty.
    try testing.expectEqual(@as(usize, 0), store.block_pending.count());
}

test "open against an existing log anchors last_key for monotonic checks" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try event_log_mod.EventLog(E).open(testing.allocator, tmp.dir, "ev.events.dat");
    defer log.deinit();

    // Seed two records via a first lifecycle.
    {
        var store = try ImmutableStore(E).open(testing.allocator, &log, 0);
        defer store.deinit();
        try store.save(.{ .id = idKey(10, 0), .value = 1 });
        try store.save(.{ .id = idKey(10, 1), .value = 2 });
        try store.flushAppends();
        store.markCommitted();
    }

    // Reopen at count=2. Out-of-order save against the durable max key fails loud.
    var store = try ImmutableStore(E).open(testing.allocator, &log, 2);
    defer store.deinit();
    try testing.expectError(error.KeyOutOfOrder, store.save(.{ .id = idKey(10, 1), .value = 999 }));
    try store.save(.{ .id = idKey(11, 0), .value = 3 });
}
