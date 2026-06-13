/// MutableStore(T): mutable entity store. In-memory HashMap cache fronts a
/// sorted slab inside `state.snap`. Cold loads fall through to binary search
/// on the slab. Saves update the cache only.
///
/// `materialize` produces sorted slab bytes for the next `state.snap` commit.
/// `refreshSlab` rebinds the borrowed slab pointer post-commit and clears
/// dirty flags. Disk I/O happens via `state_snap`, not this module.
///
/// Live mode routes saves through per-block overlay submaps keyed by block
/// number. `commitBlock(N)` drains submap N into the dirty cache. `discardAll`
/// drops every submap on reorg recovery. Per-block isolation preserves each
/// block's mutation even when later blocks touch the same key.
///
/// Read-your-writes is a contract on both paths. A `load` after a `save`
/// returns the saved value (cache front on backfill, newest overlay submap
/// live). Handler correctness depends on it: the canonical sequential
/// balance update (debit, save, then load the credit side) nets a
/// self-transfer to zero only because the second load observes the first
/// save.
///
/// Not thread-safe.
const std = @import("std");

const entity_serial = @import("entity_serial.zig");

pub fn MutableStore(comptime T: type) type {
    const fields = @typeInfo(T).@"struct".fields;
    if (fields.len == 0) @compileError("MutableStore: entity '" ++ @typeName(T) ++ "' has no fields. The first field must be the primary key.");

    const KeyField = fields[0].type;
    const key_field_name = fields[0].name;
    const KEY_SIZE = entity_serial.fixedSize(KeyField, @typeName(T) ++ "." ++ fields[0].name);
    const VALUE_SIZE = entity_serial.entitySize(T);

    return struct {
        const Self = @This();
        pub const Entity = T;
        pub const Key = KeyField;
        pub const key_size = KEY_SIZE;
        pub const value_size = VALUE_SIZE;

        const CacheEntry = struct { entity: T, dirty: bool };
        const BlockMap = std.AutoHashMapUnmanaged(KeyField, T);
        const KeyBytes = [KEY_SIZE]u8;
        /// One changed row for `materialize`'s merge. `entity` points into the
        /// cache, valid because materialize never mutates the cache.
        const DirtyRef = struct { key: KeyBytes, entity: *const T };

        allocator: std.mem.Allocator,
        cache: std.AutoHashMap(KeyField, CacheEntry),
        /// Borrowed sorted-by-primary-key slab from `state.snap`.
        slab: []const u8,

        live: bool = false,
        live_block: u64 = 0,
        /// Per-block overlay submaps. Bounded by FINALITY_DEPTH (~64).
        pending: std.AutoHashMapUnmanaged(u64, BlockMap) = .{},

        /// `slab` is borrowed from the owning `StateSnap`. Caller must call
        /// `refreshSlab` after every `state_snap.commit` to avoid dangling.
        pub fn open(allocator: std.mem.Allocator, slab: []const u8) Self {
            return .{
                .allocator = allocator,
                .cache = std.AutoHashMap(KeyField, CacheEntry).init(allocator),
                .slab = slab,
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.pending.valueIterator();
            while (it.next()) |bm| bm.deinit(self.allocator);
            self.pending.deinit(self.allocator);
            self.cache.deinit();
        }

        pub fn load(self: *Self, key: KeyField) !?T {
            if (self.live) {
                // Newest-block wins. Walk all pending submaps. Bounded ~64.
                var winner_block: ?u64 = null;
                var winner_value: ?T = null;
                var it = self.pending.iterator();
                while (it.next()) |entry| {
                    if (entry.value_ptr.get(key)) |v| {
                        if (winner_block == null or entry.key_ptr.* > winner_block.?) {
                            winner_block = entry.key_ptr.*;
                            winner_value = v;
                        }
                    }
                }
                if (winner_value) |v| return v;
            }
            if (self.cache.get(key)) |entry| return entry.entity;

            const idx = self.slabIndexOf(key) orelse return null;
            const record = self.slab[idx * VALUE_SIZE ..][0..VALUE_SIZE];
            const entity = entity_serial.deserialize(T, record);
            try self.cache.put(key, .{ .entity = entity, .dirty = false });
            return entity;
        }

        /// Load `key`, or initialize a fresh zeroed entity with the primary-key
        /// field set to `key`. Fresh entity inserted dirty to persist at next
        /// commit.
        pub fn loadOrInit(self: *Self, key: KeyField) !T {
            if (try self.load(key)) |existing| return existing;
            var entity = std.mem.zeroes(T);
            @field(entity, key_field_name) = key;
            if (self.live) {
                try self.putPending(key, entity);
            } else {
                try self.cache.put(key, .{ .entity = entity, .dirty = true });
            }
            return entity;
        }

        /// Single-arg save derives the primary key from `entity`'s first field.
        pub fn save(self: *Self, entity: T) !void {
            const key = @field(entity, key_field_name);
            if (self.live) {
                try self.putPending(key, entity);
                return;
            }
            try self.cache.put(key, .{ .entity = entity, .dirty = true });
        }

        fn putPending(self: *Self, key: KeyField, value: T) !void {
            const gop = try self.pending.getOrPut(self.allocator, self.live_block);
            if (!gop.found_existing) gop.value_ptr.* = .{};
            try gop.value_ptr.put(self.allocator, key, value);
        }

        /// Drain block `N`'s overlay submap into the dirty cache. Disk write
        /// happens later via `materialize` + `state_snap.commit`.
        pub fn commitBlock(self: *Self, block: u64) !void {
            const sub = self.pending.getPtr(block) orelse return;
            // Reserve cache capacity before removing the submap. A partial OOM
            // would otherwise strand entries between pending and cache.
            try self.cache.ensureUnusedCapacity(sub.count());
            var removed = self.pending.fetchRemove(block).?;
            defer removed.value.deinit(self.allocator);
            var it = removed.value.iterator();
            while (it.next()) |entry| {
                self.cache.putAssumeCapacity(entry.key_ptr.*, .{ .entity = entry.value_ptr.*, .dirty = true });
            }
        }

        /// Drop every overlay submap. Used on reorg. Caller re-dispatches the
        /// fresh canonical chain.
        pub fn discardAll(self: *Self) void {
            var it = self.pending.valueIterator();
            while (it.next()) |bm| bm.deinit(self.allocator);
            self.pending.clearRetainingCapacity();
        }

        /// Drop overlay submaps at or above `block`, keeping canonical blocks
        /// below the fork. Partial reorg rollback for a streamed REORG, where
        /// finalized-but-uncommitted blocks below the fork must survive. The
        /// committed cache (finalized mutations) is untouched. Restart on each
        /// removal since `fetchRemove` invalidates the live iterator. Overlay is
        /// bounded by FINALITY_DEPTH, so convergence is quick.
        pub fn discardFrom(self: *Self, block: u64) void {
            outer: while (true) {
                var it = self.pending.keyIterator();
                while (it.next()) |k| {
                    if (k.* >= block) {
                        var removed = self.pending.fetchRemove(k.*).?;
                        removed.value.deinit(self.allocator);
                        continue :outer;
                    }
                }
                break;
            }
        }

        pub fn count(self: *const Self) u32 {
            return self.cache.count();
        }

        pub fn pendingCount(self: *const Self) u32 {
            var n: u32 = 0;
            var it = self.pending.valueIterator();
            while (it.next()) |bm| n += bm.count();
            return n;
        }

        /// Produce sorted slab bytes for the next `state.snap` commit. Caller
        /// owns the returned buffer (frees via `allocator`). The slab is already
        /// sorted, so only the dirty (changed) cache rows are merged in: clean
        /// rows equal their slab record and ride through a bulk copy of the
        /// unchanged runs. Avoids rebuilding a full map, sorting every key, and
        /// re-serializing rows that did not move.
        pub fn materialize(self: *Self, allocator: std.mem.Allocator) ![]u8 {
            const num_in_slab = self.slab.len / VALUE_SIZE;

            var dirty = std.ArrayListUnmanaged(DirtyRef){};
            defer dirty.deinit(allocator);
            var cit = self.cache.iterator();
            while (cit.next()) |e| {
                if (!e.value_ptr.dirty) continue;
                var key_be: KeyBytes = undefined;
                entity_serial.encodeKey(KeyField, e.key_ptr.*, &key_be);
                try dirty.append(allocator, .{ .key = key_be, .entity = &e.value_ptr.entity });
            }
            std.sort.pdq(DirtyRef, dirty.items, {}, dirtyLessThan);

            // Output rows = slab rows + dirty keys absent from the slab.
            // Overwrites (dirty key present) replace a row in place.
            var new_rows: usize = 0;
            for (dirty.items) |d| {
                if (!self.slabHas(&d.key)) new_rows += 1;
            }
            const out = try allocator.alloc(u8, (num_in_slab + new_rows) * VALUE_SIZE);
            errdefer allocator.free(out);

            // Two-way merge in key order. For each dirty row, bulk-copy the run
            // of slab rows below its key, then emit the dirty row (skipping the
            // slab row it overwrites). Finally copy the remaining slab tail.
            var si: usize = 0;
            var oi: usize = 0;
            for (dirty.items) |d| {
                const run_start = si;
                while (si < num_in_slab and std.mem.order(u8, self.slab[si * VALUE_SIZE ..][0..KEY_SIZE], &d.key) == .lt) si += 1;
                if (si > run_start) {
                    const n = (si - run_start) * VALUE_SIZE;
                    @memcpy(out[oi..][0..n], self.slab[run_start * VALUE_SIZE ..][0..n]);
                    oi += n;
                }
                if (si < num_in_slab and std.mem.order(u8, self.slab[si * VALUE_SIZE ..][0..KEY_SIZE], &d.key) == .eq) si += 1;
                entity_serial.serialize(T, d.entity.*, out[oi..][0..VALUE_SIZE]);
                oi += VALUE_SIZE;
            }
            if (si < num_in_slab) {
                const n = (num_in_slab - si) * VALUE_SIZE;
                @memcpy(out[oi..][0..n], self.slab[si * VALUE_SIZE ..][0..n]);
                oi += n;
            }
            return out;
        }

        /// Rebind the borrowed slab pointer after `state_snap.commit` succeeds.
        /// Clears dirty flags on cache entries.
        pub fn refreshSlab(self: *Self, new_slab: []const u8) void {
            self.slab = new_slab;
            var it = self.cache.valueIterator();
            while (it.next()) |v| v.dirty = false;
        }

        fn slabIndexOf(self: *const Self, key: KeyField) ?usize {
            const num_records = self.slab.len / VALUE_SIZE;
            if (num_records == 0) return null;
            var target: KeyBytes = undefined;
            entity_serial.encodeKey(KeyField, key, &target);
            var lo: usize = 0;
            var hi: usize = num_records;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const record_key = self.slab[mid * VALUE_SIZE ..][0..KEY_SIZE];
                switch (std.mem.order(u8, record_key, &target)) {
                    .eq => return mid,
                    .lt => lo = mid + 1,
                    .gt => hi = mid,
                }
            }
            return null;
        }

        fn dirtyLessThan(_: void, a: DirtyRef, b: DirtyRef) bool {
            return std.mem.order(u8, &a.key, &b.key) == .lt;
        }

        /// Binary search the sorted slab for an encoded key. Used to size the
        /// merge output (a dirty key absent from the slab adds a row).
        fn slabHas(self: *const Self, key_be: *const KeyBytes) bool {
            const num_records = self.slab.len / VALUE_SIZE;
            var lo: usize = 0;
            var hi: usize = num_records;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const record_key = self.slab[mid * VALUE_SIZE ..][0..KEY_SIZE];
                switch (std.mem.order(u8, record_key, key_be)) {
                    .eq => return true,
                    .lt => lo = mid + 1,
                    .gt => hi = mid,
                }
            }
            return false;
        }
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

const Account = struct {
    id: [20]u8,
    balance: u256,
};

test "load on empty slab returns null" {
    const S = MutableStore(Account);
    var store = S.open(testing.allocator, &.{});
    defer store.deinit();
    try testing.expectEqual(@as(?Account, null), try store.load([_]u8{0xAA} ** 20));
}

test "load after save returns cached value; no slab read needed" {
    const S = MutableStore(Account);
    var store = S.open(testing.allocator, &.{});
    defer store.deinit();

    const alice = [_]u8{0xAA} ** 20;
    try store.save(.{ .id = alice, .balance = 100 });
    const got = (try store.load(alice)).?;
    try testing.expectEqual(@as(u256, 100), got.balance);
}

test "load falls through to slab binary search" {
    const S = MutableStore(Account);
    const alice = [_]u8{0xAA} ** 20;
    const bob = [_]u8{0xBB} ** 20;
    var slab_buf: [2 * S.value_size]u8 = undefined;
    entity_serial.serialize(Account, .{ .id = alice, .balance = 100 }, slab_buf[0..S.value_size]);
    entity_serial.serialize(Account, .{ .id = bob, .balance = 200 }, slab_buf[S.value_size..]);

    var store = S.open(testing.allocator, &slab_buf);
    defer store.deinit();

    try testing.expectEqual(@as(u256, 100), (try store.load(alice)).?.balance);
    try testing.expectEqual(@as(u256, 200), (try store.load(bob)).?.balance);
    try testing.expectEqual(@as(?Account, null), try store.load([_]u8{0xCC} ** 20));
}

test "materialize merges slab and cache, overwriting on key match" {
    const S = MutableStore(Account);
    const alice = [_]u8{0xAA} ** 20;
    const bob = [_]u8{0xBB} ** 20;
    var slab_buf: [2 * S.value_size]u8 = undefined;
    entity_serial.serialize(Account, .{ .id = alice, .balance = 100 }, slab_buf[0..S.value_size]);
    entity_serial.serialize(Account, .{ .id = bob, .balance = 200 }, slab_buf[S.value_size..]);

    var store = S.open(testing.allocator, &slab_buf);
    defer store.deinit();

    // Overwrite alice's balance, add a new key.
    try store.save(.{ .id = alice, .balance = 999 });
    const carl = [_]u8{0xCC} ** 20;
    try store.save(.{ .id = carl, .balance = 50 });

    const new_slab = try store.materialize(testing.allocator);
    defer testing.allocator.free(new_slab);

    try testing.expectEqual(@as(usize, 3 * S.value_size), new_slab.len);

    // Verify by re-binding the slab and reading back.
    store.refreshSlab(new_slab);
    store.cache.clearRetainingCapacity();
    try testing.expectEqual(@as(u256, 999), (try store.load(alice)).?.balance);
    try testing.expectEqual(@as(u256, 200), (try store.load(bob)).?.balance);
    try testing.expectEqual(@as(u256, 50), (try store.load(carl)).?.balance);
}

test "materialize preserves a clean-cached (loaded, unmodified) slab row" {
    const S = MutableStore(Account);
    const alice = [_]u8{0xAA} ** 20;
    const bob = [_]u8{0xBB} ** 20;
    var slab_buf: [2 * S.value_size]u8 = undefined;
    entity_serial.serialize(Account, .{ .id = alice, .balance = 100 }, slab_buf[0..S.value_size]);
    entity_serial.serialize(Account, .{ .id = bob, .balance = 200 }, slab_buf[S.value_size..]);

    var store = S.open(testing.allocator, &slab_buf);
    defer store.deinit();

    // Load alice (clean-cached, dirty=false); modify bob (dirty). The merge must
    // keep alice via the slab copy, not drop it for being absent from `dirty`.
    _ = try store.load(alice);
    try store.save(.{ .id = bob, .balance = 999 });

    const slab = try store.materialize(testing.allocator);
    defer testing.allocator.free(slab);
    try testing.expectEqual(@as(usize, 2 * S.value_size), slab.len);

    store.refreshSlab(slab);
    store.cache.clearRetainingCapacity();
    try testing.expectEqual(@as(u256, 100), (try store.load(alice)).?.balance);
    try testing.expectEqual(@as(u256, 999), (try store.load(bob)).?.balance);
}

test "refreshSlab clears dirty flags" {
    const S = MutableStore(Account);
    var store = S.open(testing.allocator, &.{});
    defer store.deinit();

    const alice = [_]u8{0xAA} ** 20;
    try store.save(.{ .id = alice, .balance = 1 });

    const buf = try store.materialize(testing.allocator);
    defer testing.allocator.free(buf);
    store.refreshSlab(buf);

    const e = store.cache.get(alice).?;
    try testing.expectEqual(false, e.dirty);
}

test "live save buffers to overlay, not cache or slab" {
    const S = MutableStore(Account);
    var store = S.open(testing.allocator, &.{});
    defer store.deinit();
    store.live = true;
    store.live_block = 100;

    const alice = [_]u8{0xAA} ** 20;
    try store.save(.{ .id = alice, .balance = 100 });

    try testing.expectEqual(@as(u32, 1), store.pendingCount());
    try testing.expectEqual(@as(u32, 0), store.count());
}

test "live load merges overlay over slab" {
    const S = MutableStore(Account);
    const alice = [_]u8{0xAA} ** 20;
    var slab_buf: [S.value_size]u8 = undefined;
    entity_serial.serialize(Account, .{ .id = alice, .balance = 100 }, &slab_buf);

    var store = S.open(testing.allocator, &slab_buf);
    defer store.deinit();
    store.live = true;
    store.live_block = 200;
    try store.save(.{ .id = alice, .balance = 500 });

    try testing.expectEqual(@as(u256, 500), (try store.load(alice)).?.balance);
}

test "commitBlock moves overlay entries to cache, marked dirty" {
    const S = MutableStore(Account);
    var store = S.open(testing.allocator, &.{});
    defer store.deinit();
    store.live = true;

    const alice = [_]u8{0xAA} ** 20;
    const bob = [_]u8{0xBB} ** 20;
    store.live_block = 100;
    try store.save(.{ .id = alice, .balance = 100 });
    store.live_block = 101;
    try store.save(.{ .id = bob, .balance = 200 });

    try store.commitBlock(100);
    try testing.expectEqual(@as(u32, 1), store.pendingCount());
    try testing.expectEqual(@as(u32, 1), store.count());

    const alice_cached = store.cache.get(alice).?;
    try testing.expectEqual(@as(u256, 100), alice_cached.entity.balance);
    try testing.expectEqual(true, alice_cached.dirty);
}

test "discardAll drops the overlay" {
    const S = MutableStore(Account);
    var store = S.open(testing.allocator, &.{});
    defer store.deinit();
    store.live = true;
    store.live_block = 100;

    try store.save(.{ .id = [_]u8{0xAA} ** 20, .balance = 100 });
    store.live_block = 101;
    try store.save(.{ .id = [_]u8{0xBB} ** 20, .balance = 200 });
    try testing.expectEqual(@as(u32, 2), store.pendingCount());

    store.discardAll();
    try testing.expectEqual(@as(u32, 0), store.pendingCount());
}

test "discardFrom drops overlay at or above the fork, keeps blocks below" {
    const S = MutableStore(Account);
    var store = S.open(testing.allocator, &.{});
    defer store.deinit();
    store.live = true;

    const alice = [_]u8{0xAA} ** 20;
    const bob = [_]u8{0xBB} ** 20;
    const carol = [_]u8{0xCC} ** 20;
    store.live_block = 100;
    try store.save(.{ .id = alice, .balance = 100 });
    store.live_block = 101;
    try store.save(.{ .id = bob, .balance = 200 });
    store.live_block = 102;
    try store.save(.{ .id = carol, .balance = 300 });
    try testing.expectEqual(@as(u32, 3), store.pendingCount());

    // Fork at 101: blocks 101 and 102 roll back, block 100 survives.
    store.discardFrom(101);
    try testing.expectEqual(@as(u32, 1), store.pendingCount());
    try testing.expectEqual(@as(u256, 100), (try store.load(alice)).?.balance);
    try testing.expect((try store.load(bob)) == null);
    try testing.expect((try store.load(carol)) == null);
}

test "loadOrInit returns existing or fresh zero-init entity" {
    const S = MutableStore(Account);
    const alice = [_]u8{0xAA} ** 20;
    var slab_buf: [S.value_size]u8 = undefined;
    entity_serial.serialize(Account, .{ .id = alice, .balance = 100 }, &slab_buf);

    var store = S.open(testing.allocator, &slab_buf);
    defer store.deinit();

    const got_alice = try store.loadOrInit(alice);
    try testing.expectEqual(@as(u256, 100), got_alice.balance);

    const bob = [_]u8{0xBB} ** 20;
    const got_bob = try store.loadOrInit(bob);
    try testing.expectEqualSlices(u8, &bob, &got_bob.id);
    try testing.expectEqual(@as(u256, 0), got_bob.balance);
    try testing.expectEqual(@as(u256, 0), (try store.load(bob)).?.balance);
}

test "materialize produces records sorted by primary key" {
    const S = MutableStore(Account);
    var store = S.open(testing.allocator, &.{});
    defer store.deinit();

    // Save in reverse order to verify sorting in materialize, not insertion.
    try store.save(.{ .id = [_]u8{0xCC} ** 20, .balance = 3 });
    try store.save(.{ .id = [_]u8{0xAA} ** 20, .balance = 1 });
    try store.save(.{ .id = [_]u8{0xBB} ** 20, .balance = 2 });

    const slab = try store.materialize(testing.allocator);
    defer testing.allocator.free(slab);
    try testing.expectEqual(@as(usize, 3 * S.value_size), slab.len);

    // Records are BE-sorted by key (the first field's bytes).
    const k0 = slab[0..S.key_size];
    const k1 = slab[S.value_size .. S.value_size + S.key_size];
    const k2 = slab[2 * S.value_size .. 2 * S.value_size + S.key_size];
    try testing.expect(std.mem.order(u8, k0, k1) == .lt);
    try testing.expect(std.mem.order(u8, k1, k2) == .lt);
}
