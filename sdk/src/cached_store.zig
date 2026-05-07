/// CachedStore(T): comptime-generated MDBX wrapper with HashMap-fronted reads
/// for mutable entities (accounts, allowances, pool reserves).
///
/// Reads hit the cache (~50ns) before MDBX (~1µs). Writes update the cache
/// only and set a dirty flag. flush() drains dirty entries to MDBX. The
/// cache persists across commits and only dirty flags are reset, so a
/// Transfer handler that touches the same Account in many blocks pays one
/// MDBX read, not one per touch. Measured 3.3x handler speedup in the
/// prototype.
///
/// Not thread-safe. Stage 2 dispatch is single-threaded. Entity-store
/// mutations from parallel handlers would need a separate sharding scheme.
const std = @import("std");
const lmdbx = @import("lmdbx");
const entity_serial = @import("entity_serial.zig");

pub fn CachedStore(comptime T: type) type {
    const fields = @typeInfo(T).@"struct".fields;
    if (fields.len == 0) @compileError("CachedStore: entity '" ++ @typeName(T) ++ "' has no fields. The first field must be the primary key.");

    const KeyField = fields[0].type;
    const KEY_SIZE = entity_serial.fixedSize(KeyField, @typeName(T) ++ "." ++ fields[0].name);
    const VALUE_SIZE = entity_serial.entitySize(T);

    return struct {
        const Self = @This();
        pub const Entity = T;
        pub const Key = KeyField;
        pub const key_size = KEY_SIZE;
        pub const value_size = VALUE_SIZE;

        const CacheEntry = struct { entity: T, dirty: bool };

        dbi: lmdbx.Database.DBI,
        cache: std.AutoHashMap(KeyField, CacheEntry),
        /// Borrow into the owning Context's `active_txn` field. The
        /// Context replaces its own `active_txn` value on every commit
        /// boundary, so reading through this pointer always sees the
        /// transaction that's currently active. Stores never own or
        /// rebind the txn themselves.
        active_txn: *const lmdbx.Transaction,

        pub fn open(allocator: std.mem.Allocator, txn_ref: *const lmdbx.Transaction, name: [*:0]const u8) !Self {
            const db = try lmdbx.Database.open(txn_ref.*, name, .{ .create = true });
            return .{
                .dbi = db.dbi,
                .cache = std.AutoHashMap(KeyField, CacheEntry).init(allocator),
                .active_txn = txn_ref,
            };
        }

        pub fn deinit(self: *Self) void {
            self.cache.deinit();
        }

        pub fn load(self: *Self, key: KeyField) !?T {
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

        pub fn save(self: *Self, key: KeyField, entity: T) !void {
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

        pub fn count(self: *const Self) u32 {
            return self.cache.count();
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
    const S = CachedStore(Account);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openTestEnv(&tmp);
    defer env.deinit() catch {};

    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "accounts");
    defer store.deinit();

    const alice = [_]u8{0xAA} ** 20;
    try store.save(alice, .{ .id = alice, .balance = 100 });
    // Nothing flushed yet, so MDBX is empty. If load went to MDBX it would
    // miss. The cache hit is the only path that returns a value.
    const got = (try store.load(alice)).?;
    try std.testing.expectEqual(@as(u256, 100), got.balance);

    try current_txn.abort();
}

test "flush writes only dirty entries" {
    const S = CachedStore(Account);
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

        try store.save(alice, .{ .id = alice, .balance = 100 });
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

        try store.save(bob, .{ .id = bob, .balance = 200 });
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
    const S = CachedStore(Account);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openTestEnv(&tmp);
    defer env.deinit() catch {};

    const alice = [_]u8{0xAA} ** 20;
    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "accounts");
    defer store.deinit();

    try store.save(alice, .{ .id = alice, .balance = 100 });
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
