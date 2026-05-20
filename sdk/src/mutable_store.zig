/// MutableStore(T): MDBX-backed mutable entities with an in-memory HashMap
/// cache. `flush` writes dirty entries; the cache survives commits.
///
/// In live mode, saves route through a per-block overlay tagged with the
/// block that produced them. `commitBlock` drains a block's slice into
/// the active txn; `discardAll` drops the overlay on reorg recovery.
/// Backfill keeps the overlay's backing storage unallocated.
///
/// Not thread-safe.
const std = @import("std");

const lmdbx = @import("lmdbx");

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

        dbi: lmdbx.Database.DBI,
        cache: std.AutoHashMap(KeyField, CacheEntry),
        /// Borrow into the owning Context's `_active_txn` field. The
        /// Context replaces its own `_active_txn` value on every commit
        /// boundary, so reading through this pointer always sees the
        /// transaction that's currently active. Stores never own or
        /// rebind the txn themselves.
        active_txn: *const lmdbx.Transaction,

        // Live-mode state. `pending` is zero-init so backfill never
        // allocates. The caller sets `live_block` before each block's
        // dispatch and `live = true` once at loop entry.
        allocator: std.mem.Allocator,
        live: bool = false,
        live_block: u64 = 0,
        pending: std.AutoHashMapUnmanaged(KeyField, PendingEntry) = .{},

        pub fn open(allocator: std.mem.Allocator, txn_ref: *const lmdbx.Transaction, name: [*:0]const u8) !Self {
            const db = try lmdbx.Database.open(txn_ref.*, name, .{ .create = true });
            return .{
                .dbi = db.dbi,
                .cache = std.AutoHashMap(KeyField, CacheEntry).init(allocator),
                .active_txn = txn_ref,
                .allocator = allocator,
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

            var key_buf: [KEY_SIZE]u8 = undefined;
            entity_serial.encodeKey(KeyField, key, &key_buf);
            const db = lmdbx.Database{ .txn = self.active_txn.*, .dbi = self.dbi };
            const data = (try db.get(&key_buf)) orelse return null;
            if (data.len != VALUE_SIZE) return error.MalformedEntity;
            const entity = entity_serial.deserialize(T, data[0..VALUE_SIZE]);
            try self.cache.put(key, .{ .entity = entity, .dirty = false });
            return entity;
        }

        /// Load `key`, or initialize a fresh entity with all fields zeroed
        /// and the primary-key field set to `key`. The fresh entity is
        /// inserted dirty so it persists at the next flush.
        ///
        /// Use this for the common "credit/debit a counter" pattern where
        /// the per-field starting value is zero (balances, counters, etc.).
        /// For richer defaults, fall back to `(try load(k)) orelse build(k)`
        /// and explicit `save`.
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

        /// Save derives the primary key from `entity`'s first field. Single-arg
        /// save matches `ImmutableStore.save(entity)` so the two store types
        /// feel uniform from a handler's perspective.
        pub fn save(self: *Self, entity: T) !void {
            const key = @field(entity, key_field_name);
            if (self.live) {
                try self.pending.put(self.allocator, key, .{ .block = self.live_block, .value = entity });
                return;
            }
            try self.cache.put(key, .{ .entity = entity, .dirty = true });
        }

        pub fn flush(self: *Self) !void {
            const db = lmdbx.Database{ .txn = self.active_txn.*, .dbi = self.dbi };
            var it = self.cache.iterator();
            while (it.next()) |entry| {
                if (!entry.value_ptr.dirty) continue;
                var key_buf: [KEY_SIZE]u8 = undefined;
                var val_buf: [VALUE_SIZE]u8 = undefined;
                entity_serial.encodeKey(KeyField, entry.key_ptr.*, &key_buf);
                entity_serial.serialize(T, entry.value_ptr.entity, &val_buf);
                try db.set(&key_buf, &val_buf, .Upsert);
                entry.value_ptr.dirty = false;
            }
        }

        /// Upsert every overlay entry tagged with `block` into MDBX via the
        /// active txn, then drop them. Entries with other tags stay pending.
        pub fn commitBlock(self: *Self, block: u64) !void {
            if (self.pending.count() == 0) return;
            const db = lmdbx.Database{ .txn = self.active_txn.*, .dbi = self.dbi };
            var to_remove: std.ArrayListUnmanaged(KeyField) = .{};
            defer to_remove.deinit(self.allocator);
            var it = self.pending.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.block != block) continue;
                var key_buf: [KEY_SIZE]u8 = undefined;
                var val_buf: [VALUE_SIZE]u8 = undefined;
                entity_serial.encodeKey(KeyField, entry.key_ptr.*, &key_buf);
                entity_serial.serialize(T, entry.value_ptr.value, &val_buf);
                try db.set(&key_buf, &val_buf, .Upsert);
                try to_remove.append(self.allocator, entry.key_ptr.*);
            }
            for (to_remove.items) |k| _ = self.pending.remove(k);
        }

        /// Drop the entire overlay. Used on reorg — the caller re-dispatches
        /// every block in the fresh pending file. Partial discard is not
        /// exposed because the single-value-per-key overlay loses pre-fork
        /// state for keys written across multiple pending blocks.
        pub fn discardAll(self: *Self) void {
            self.pending.clearRetainingCapacity();
        }

        pub fn count(self: *const Self) u32 {
            return self.cache.count();
        }

        pub fn pendingCount(self: *const Self) u32 {
            return self.pending.count();
        }
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const Account = struct {
    id: [20]u8,
    balance: u256,
};

fn openTestEnv(tmp: *std.testing.TmpDir) !lmdbx.Environment {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpathZ(".", &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    return try lmdbx.Environment.init(@ptrCast(&path_z), .{ .max_dbs = 4 });
}

test "load after save returns cached value without touching MDBX" {
    const S = MutableStore(Account);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openTestEnv(&tmp);
    defer env.deinit() catch {};

    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "accounts");
    defer store.deinit();

    const alice = [_]u8{0xAA} ** 20;
    try store.save(.{ .id = alice, .balance = 100 });
    // Nothing flushed yet, so MDBX is empty. If load went to MDBX it would
    // miss. The cache hit is the only path that returns a value.
    const got = (try store.load(alice)).?;
    try std.testing.expectEqual(@as(u256, 100), got.balance);

    try current_txn.abort();
}

test "flush writes only dirty entries" {
    const S = MutableStore(Account);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openTestEnv(&tmp);
    defer env.deinit() catch {};

    const alice = [_]u8{0xAA} ** 20;
    const bob = [_]u8{0xBB} ** 20;

    {
        var current_txn = try env.transaction(.{});
        var store = try S.open(std.testing.allocator, &current_txn, "accounts");
        defer store.deinit();

        try store.save(.{ .id = alice, .balance = 100 });
        try store.flush();
        try current_txn.commit();
    }

    // Re-open: load alice (clean, dirty=false), save bob (dirty), flush.
    // Both keys must be present in MDBX after flush, and alice's bytes must
    // be unchanged from the first commit.
    {
        var current_txn = try env.transaction(.{});
        var store = try S.open(std.testing.allocator, &current_txn, "accounts");
        defer store.deinit();

        const alice_loaded = (try store.load(alice)).?;
        try std.testing.expectEqual(@as(u256, 100), alice_loaded.balance);

        try store.save(.{ .id = bob, .balance = 200 });
        try store.flush();

        // Read both keys directly from MDBX (bypassing the cache) to verify
        // the flush actually wrote them. Avoids lmdbx-zig's `dbi_stat`
        // wrapper bug (CLAUDE.md "lmdbx-zig has wrapper bugs").
        const db = lmdbx.Database{ .txn = current_txn, .dbi = store.dbi };
        var key_buf: [S.key_size]u8 = undefined;
        entity_serial.encodeKey(S.Key, alice, &key_buf);
        try std.testing.expect((try db.get(&key_buf)) != null);
        entity_serial.encodeKey(S.Key, bob, &key_buf);
        try std.testing.expect((try db.get(&key_buf)) != null);

        try current_txn.commit();
    }
}

test "cache survives flush, commit, and a new transaction" {
    const S = MutableStore(Account);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openTestEnv(&tmp);
    defer env.deinit() catch {};

    const alice = [_]u8{0xAA} ** 20;
    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "accounts");
    defer store.deinit();

    try store.save(.{ .id = alice, .balance = 100 });
    try std.testing.expectEqual(@as(u32, 1), store.count());
    try store.flush();
    try current_txn.commit();

    // Cache survives the commit; only dirty flags reset.
    try std.testing.expectEqual(@as(u32, 1), store.count());

    // Reassign current_txn to a fresh read txn; the store sees it through
    // its `active_txn` pointer without re-opening.
    current_txn = try env.transaction(.{});
    const got = (try store.load(alice)).?;
    try std.testing.expectEqual(@as(u256, 100), got.balance);
    try current_txn.abort();
}

test "live save buffers to overlay, not cache or MDBX" {
    const S = MutableStore(Account);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openTestEnv(&tmp);
    defer env.deinit() catch {};

    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "accounts");
    defer store.deinit();
    store.live = true;
    store.live_block = 100;

    const alice = [_]u8{0xAA} ** 20;
    try store.save(.{ .id = alice, .balance = 100 });

    // Overlay holds the live write; cache and MDBX are untouched.
    try std.testing.expectEqual(@as(u32, 1), store.pendingCount());
    try std.testing.expectEqual(@as(u32, 0), store.count());
    const db = lmdbx.Database{ .txn = current_txn, .dbi = store.dbi };
    var key_buf: [S.key_size]u8 = undefined;
    entity_serial.encodeKey(S.Key, alice, &key_buf);
    try std.testing.expect((try db.get(&key_buf)) == null);

    try current_txn.abort();
}

test "live load merges overlay over base" {
    const S = MutableStore(Account);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openTestEnv(&tmp);
    defer env.deinit() catch {};

    const alice = [_]u8{0xAA} ** 20;

    // Seed MDBX with alice.balance = 100 via the backfill path.
    {
        var current_txn = try env.transaction(.{});
        var store = try S.open(std.testing.allocator, &current_txn, "accounts");
        defer store.deinit();
        try store.save(.{ .id = alice, .balance = 100 });
        try store.flush();
        try current_txn.commit();
    }

    // Open in live mode; overlay holds a fresher value for the same key.
    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "accounts");
    defer store.deinit();
    store.live = true;
    store.live_block = 200;
    try store.save(.{ .id = alice, .balance = 500 });

    const got = (try store.load(alice)).?;
    try std.testing.expectEqual(@as(u256, 500), got.balance);

    try current_txn.abort();
}

test "commitBlock flushes only the matching block's slice" {
    const S = MutableStore(Account);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openTestEnv(&tmp);
    defer env.deinit() catch {};

    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "accounts");
    defer store.deinit();
    store.live = true;

    const alice = [_]u8{0xAA} ** 20;
    const bob = [_]u8{0xBB} ** 20;
    store.live_block = 100;
    try store.save(.{ .id = alice, .balance = 100 });
    store.live_block = 101;
    try store.save(.{ .id = bob, .balance = 200 });

    try store.commitBlock(100);

    // Block 100's slice landed in MDBX; alice is gone from overlay.
    const db = lmdbx.Database{ .txn = current_txn, .dbi = store.dbi };
    var key_buf: [S.key_size]u8 = undefined;
    entity_serial.encodeKey(S.Key, alice, &key_buf);
    try std.testing.expect((try db.get(&key_buf)) != null);
    // Block 101's slice (bob) is still pending — no MDBX entry yet.
    entity_serial.encodeKey(S.Key, bob, &key_buf);
    try std.testing.expect((try db.get(&key_buf)) == null);
    try std.testing.expectEqual(@as(u32, 1), store.pendingCount());

    try current_txn.commit();
}

test "discardAll drops the entire overlay" {
    const S = MutableStore(Account);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openTestEnv(&tmp);
    defer env.deinit() catch {};

    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "accounts");
    defer store.deinit();
    store.live = true;
    store.live_block = 100;

    try store.save(.{ .id = [_]u8{0xAA} ** 20, .balance = 100 });
    store.live_block = 101;
    try store.save(.{ .id = [_]u8{0xBB} ** 20, .balance = 200 });
    try std.testing.expectEqual(@as(u32, 2), store.pendingCount());

    store.discardAll();
    try std.testing.expectEqual(@as(u32, 0), store.pendingCount());

    try current_txn.abort();
}

test "backfill leaves the overlay empty" {
    const S = MutableStore(Account);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openTestEnv(&tmp);
    defer env.deinit() catch {};

    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "accounts");
    defer store.deinit();

    // live defaults to false; save takes the M2 cache path.
    try store.save(.{ .id = [_]u8{0xAA} ** 20, .balance = 100 });
    try store.save(.{ .id = [_]u8{0xBB} ** 20, .balance = 200 });
    try std.testing.expectEqual(@as(u32, 0), store.pendingCount());
    try std.testing.expectEqual(@as(u32, 2), store.count());

    try current_txn.abort();
}

test "loadOrInit returns existing entity, else zeroed entity with key set" {
    const S = MutableStore(Account);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openTestEnv(&tmp);
    defer env.deinit() catch {};

    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "accounts");
    defer store.deinit();

    const alice = [_]u8{0xAA} ** 20;
    const bob = [_]u8{0xBB} ** 20;

    try store.save(.{ .id = alice, .balance = 100 });

    // Existing key returns existing entity unchanged.
    const got_alice = try store.loadOrInit(alice);
    try std.testing.expectEqual(@as(u256, 100), got_alice.balance);

    // Missing key returns a zeroed entity with the primary key field set,
    // and inserts it dirty so a subsequent load finds it.
    const got_bob = try store.loadOrInit(bob);
    try std.testing.expectEqualSlices(u8, &bob, &got_bob.id);
    try std.testing.expectEqual(@as(u256, 0), got_bob.balance);
    const reload = (try store.load(bob)).?;
    try std.testing.expectEqualSlices(u8, &bob, &reload.id);

    try current_txn.abort();
}
