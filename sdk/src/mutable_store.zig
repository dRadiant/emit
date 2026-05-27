/// MutableStore(T): mutable entity store with an in-memory HashMap cache
/// fronting a sorted slab inside `state.snap`. Cold loads fall through to
/// a binary search on the slab; saves update the cache only.
///
/// `materialize` produces the sorted slab bytes for the next `state.snap`
/// commit; `refreshSlab` rebinds the borrowed slab pointer after the
/// commit succeeds and clears dirty flags. Disk I/O happens via
/// `state_snap`, not this module.
///
/// In live mode, saves route through a per-block overlay tagged with the
/// block that produced them. `commitBlock` drains a block's slice into
/// the regular dirty cache so the next `state.snap` commit picks it up.
/// `discardAll` drops the overlay on reorg recovery.
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
        const PendingEntry = struct { block: u64, value: T };
        const KeyBytes = [KEY_SIZE]u8;

        allocator: std.mem.Allocator,
        cache: std.AutoHashMap(KeyField, CacheEntry),
        /// Borrowed sorted-by-primary-key slab from `state.snap`.
        slab: []const u8,

        live: bool = false,
        live_block: u64 = 0,
        pending: std.AutoHashMapUnmanaged(KeyField, PendingEntry) = .{},

        /// `slab` is borrowed from the owning `StateSnap`. Caller must
        /// call `refreshSlab` after every `state_snap.commit` so this
        /// pointer doesn't dangle.
        pub fn open(allocator: std.mem.Allocator, slab: []const u8) Self {
            return .{
                .allocator = allocator,
                .cache = std.AutoHashMap(KeyField, CacheEntry).init(allocator),
                .slab = slab,
            };
        }

        pub fn deinit(self: *Self) void {
            self.pending.deinit(self.allocator);
            self.cache.deinit();
        }

        pub fn load(self: *Self, key: KeyField) !?T {
            if (self.live) {
                if (self.pending.get(key)) |e| return e.value;
            }
            if (self.cache.get(key)) |entry| return entry.entity;

            const idx = self.slabIndexOf(key) orelse return null;
            const record = self.slab[idx * VALUE_SIZE ..][0..VALUE_SIZE];
            const entity = entity_serial.deserialize(T, record);
            try self.cache.put(key, .{ .entity = entity, .dirty = false });
            return entity;
        }

        /// Load `key`, or initialize a fresh entity with all fields zeroed
        /// and the primary-key field set to `key`. The fresh entity is
        /// inserted dirty so it persists at the next commit.
        pub fn loadOrInit(self: *Self, key: KeyField) !T {
            if (try self.load(key)) |existing| return existing;
            var entity = std.mem.zeroes(T);
            @field(entity, key_field_name) = key;
            if (self.live) {
                try self.pending.put(self.allocator, key, .{ .block = self.live_block, .value = entity });
            } else {
                try self.cache.put(key, .{ .entity = entity, .dirty = true });
            }
            return entity;
        }

        /// Single-arg save derives the primary key from `entity`'s first field.
        pub fn save(self: *Self, entity: T) !void {
            const key = @field(entity, key_field_name);
            if (self.live) {
                try self.pending.put(self.allocator, key, .{ .block = self.live_block, .value = entity });
                return;
            }
            try self.cache.put(key, .{ .entity = entity, .dirty = true });
        }

        /// Move every overlay entry tagged with `block` into the regular
        /// cache (marked dirty). The disk write happens later via
        /// `materialize` + `state_snap.commit`.
        pub fn commitBlock(self: *Self, block: u64) !void {
            if (self.pending.count() == 0) return;
            var to_remove: std.ArrayListUnmanaged(KeyField) = .{};
            defer to_remove.deinit(self.allocator);
            var it = self.pending.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.block != block) continue;
                try self.cache.put(entry.key_ptr.*, .{ .entity = entry.value_ptr.value, .dirty = true });
                try to_remove.append(self.allocator, entry.key_ptr.*);
            }
            for (to_remove.items) |k| _ = self.pending.remove(k);
        }

        /// Drop the entire overlay. Used on reorg — the caller re-dispatches
        /// every block in the fresh pending file.
        pub fn discardAll(self: *Self) void {
            self.pending.clearRetainingCapacity();
        }

        pub fn count(self: *const Self) u32 {
            return self.cache.count();
        }

        pub fn pendingCount(self: *const Self) u32 {
            return self.pending.count();
        }

        /// Produce the sorted slab bytes for the next `state.snap` commit.
        /// Caller owns the returned buffer (frees via `allocator`). Slab is
        /// the union of (current slab) and (cache), with cache overwriting.
        pub fn materialize(self: *Self, allocator: std.mem.Allocator) ![]u8 {
            var union_map = std.AutoHashMapUnmanaged(KeyBytes, T){};
            defer union_map.deinit(allocator);

            const num_in_slab = self.slab.len / VALUE_SIZE;
            var i: usize = 0;
            while (i < num_in_slab) : (i += 1) {
                const rec = self.slab[i * VALUE_SIZE ..][0..VALUE_SIZE];
                const entity = entity_serial.deserialize(T, rec);
                const key_be: KeyBytes = rec[0..KEY_SIZE].*;
                try union_map.put(allocator, key_be, entity);
            }

            var it = self.cache.iterator();
            while (it.next()) |e| {
                var key_be: KeyBytes = undefined;
                entity_serial.encodeKey(KeyField, e.key_ptr.*, &key_be);
                try union_map.put(allocator, key_be, e.value_ptr.entity);
            }

            const total_count = union_map.count();
            const sorted_keys = try allocator.alloc(KeyBytes, total_count);
            defer allocator.free(sorted_keys);
            var k_it = union_map.keyIterator();
            var idx: usize = 0;
            while (k_it.next()) |k| : (idx += 1) sorted_keys[idx] = k.*;
            std.sort.pdq(KeyBytes, sorted_keys, {}, keyLessThan);

            const out = try allocator.alloc(u8, @as(usize, total_count) * VALUE_SIZE);
            errdefer allocator.free(out);
            for (sorted_keys, 0..) |k, j| {
                const entity = union_map.get(k).?;
                entity_serial.serialize(T, entity, out[j * VALUE_SIZE ..][0..VALUE_SIZE]);
            }
            return out;
        }

        /// Rebind the borrowed slab pointer after `state_snap.commit`
        /// succeeds. Clears dirty flags on cache entries.
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

        fn keyLessThan(_: void, a: KeyBytes, b: KeyBytes) bool {
            return std.mem.order(u8, &a, &b) == .lt;
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

    // Overwrite alice's balance; add a new key.
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
