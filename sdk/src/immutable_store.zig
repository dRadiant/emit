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
    const KeyField = fields[0].type;
    const key_field_name = fields[0].name;

    return struct {
        const Self = @This();
        pub const Entity = T;
        pub const Key = KeyField;
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
            const entry = self.block_pending.getPtr(block) orelse return;
            // Reserve capacity before draining so a partial OOM can't strand entries.
            try self.pending_appended.ensureUnusedCapacity(self.allocator, entry.items.len);
            for (entry.items) |e| self.pending_appended.appendAssumeCapacity(e);
            var removed = self.block_pending.fetchRemove(block).?;
            removed.value.deinit(self.allocator);
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

        // ── In-process read surface ────────────────────────────────────────
        // Overlay-aware so an API reader sees the live tip.
        // Callers take the Context lock
        // Reader never straddles a commit, so `committed_count`
        // and the overlay are mutually consistent

        /// Live record count: finalized records plus the live overlay.
        pub fn count(self: *const Self) u64 {
            return self.committed_count + self.pendingCount();
        }

        /// Point lookup by primary key. Binary-searches the finalized log,
        /// then scans the live overlay. Null when absent.
        pub fn get(self: *Self, key: KeyField) !?T {
            var target: [KEY_SIZE]u8 = undefined;
            entity_serial.encodeKey(KeyField, key, &target);
            if (try self.log.binarySearch(self.committed_count, target)) |idx| {
                return try self.log.read(idx);
            }
            return self.overlayGet(&target);
        }

        /// Fill `out` with up to `out.len` records starting at logical index
        /// `start` (0 = oldest) in ascending key order, returning the filled
        /// prefix. Spans the finalized log then the live overlay. The overlay
        /// is collected and sorted once per call (bounded by the pending-ring
        /// depth), so a page costs one sort, not one per record. Build a
        /// newest-first page with `start = count() - n`.
        pub fn range(self: *Self, start: u64, out: []T) ![]T {
            const total = self.count();
            if (start >= total or out.len == 0) return out[0..0];
            const end = @min(start + out.len, total);

            var n: usize = 0;
            var i = start;
            while (i < end and i < self.committed_count) : (i += 1) {
                out[n] = try self.log.read(i);
                n += 1;
            }
            if (i >= end) return out[0..n];

            // The window reaches the overlay: sort it once, then index in.
            const tmp = try self.allocator.alloc(T, self.pendingCount());
            defer self.allocator.free(tmp);
            const ordered = self.collectOverlaySorted(tmp);
            var j: usize = @intCast(i - self.committed_count);
            while (i < end) : (i += 1) {
                out[n] = ordered[j];
                n += 1;
                j += 1;
            }
            return out[0..n];
        }

        /// Scan the live overlay (drained-append queue, then per-block buffers)
        /// for a record whose key matches `target`. Keys are unique, so the
        /// first match wins.
        fn overlayGet(self: *Self, target: *const [KEY_SIZE]u8) ?T {
            for (self.pending_appended.items) |e| {
                if (keyMatches(e, target)) return e;
            }
            var it = self.block_pending.valueIterator();
            while (it.next()) |list| {
                for (list.items) |e| if (keyMatches(e, target)) return e;
            }
            return null;
        }

        /// Copy every overlay record into `tmp` and sort ascending by key.
        /// `tmp.len` must equal `pendingCount()`. Returns the filled slice.
        fn collectOverlaySorted(self: *Self, tmp: []T) []T {
            var idx: usize = 0;
            for (self.pending_appended.items) |e| {
                tmp[idx] = e;
                idx += 1;
            }
            var it = self.block_pending.valueIterator();
            while (it.next()) |list| {
                for (list.items) |e| {
                    tmp[idx] = e;
                    idx += 1;
                }
            }
            std.sort.pdq(T, tmp[0..idx], {}, lessThanByKey);
            return tmp[0..idx];
        }

        fn keyMatches(entity: T, target: *const [KEY_SIZE]u8) bool {
            var kb: [KEY_SIZE]u8 = undefined;
            entity_serial.encodeKey(KeyField, @field(entity, key_field_name), &kb);
            return std.mem.eql(u8, &kb, target);
        }

        fn lessThanByKey(_: void, a: T, b: T) bool {
            var ka: [KEY_SIZE]u8 = undefined;
            var kb: [KEY_SIZE]u8 = undefined;
            entity_serial.encodeKey(KeyField, @field(a, key_field_name), &ka);
            entity_serial.encodeKey(KeyField, @field(b, key_field_name), &kb);
            return std.mem.order(u8, &ka, &kb) == .lt;
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

test "count, get, range span the finalized log and the live overlay" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try event_log_mod.EventLog(E).open(testing.allocator, tmp.dir, "ev.events.dat");
    defer log.deinit();

    var store = try ImmutableStore(E).open(testing.allocator, &log, 0);
    defer store.deinit();

    // Two finalized records via a backfill lifecycle.
    try store.save(.{ .id = idKey(1, 0), .value = 10 });
    try store.save(.{ .id = idKey(1, 1), .value = 11 });
    try store.flushAppends();
    store.markCommitted();
    try testing.expectEqual(@as(u64, 2), store.committed_count);

    // Two more in the live per-block overlay (not yet finalized).
    store.live = true;
    store.live_block = 2;
    try store.save(.{ .id = idKey(2, 0), .value = 20 });
    store.live_block = 3;
    try store.save(.{ .id = idKey(3, 0), .value = 30 });

    // count spans both regions.
    try testing.expectEqual(@as(u64, 4), store.count());

    // get hits the finalized log, the live overlay, and misses cleanly.
    try testing.expectEqual(@as(u64, 11), (try store.get(idKey(1, 1))).?.value);
    try testing.expectEqual(@as(u64, 30), (try store.get(idKey(3, 0))).?.value);
    try testing.expectEqual(@as(?E, null), try store.get(idKey(9, 9)));

    // range over the whole store, ascending across the durable/overlay seam.
    var buf: [8]E = undefined;
    const all = try store.range(0, &buf);
    try testing.expectEqual(@as(usize, 4), all.len);
    try testing.expectEqual(@as(u64, 10), all[0].value);
    try testing.expectEqual(@as(u64, 11), all[1].value);
    try testing.expectEqual(@as(u64, 20), all[2].value);
    try testing.expectEqual(@as(u64, 30), all[3].value);

    // A window that begins inside the overlay region only.
    const tail = try store.range(2, buf[0..2]);
    try testing.expectEqual(@as(usize, 2), tail.len);
    try testing.expectEqual(@as(u64, 20), tail[0].value);
    try testing.expectEqual(@as(u64, 30), tail[1].value);

    // Newest-first page of size 2 (start = count - 2); caller reverses.
    const page = try store.range(store.count() - 2, buf[0..2]);
    try testing.expectEqual(@as(u64, 20), page[0].value);
    try testing.expectEqual(@as(u64, 30), page[1].value);

    // Out-of-range start yields an empty slice.
    try testing.expectEqual(@as(usize, 0), (try store.range(99, &buf)).len);
}
