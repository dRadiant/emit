/// AppendStore(T): comptime-generated MDBX writer for append-only entities.
///
/// Writes use MDBX_APPEND (sequential insert, no B-tree traversal,
/// ~5x faster than upsert). load() is a @compileError. The API exists so a
/// typo on a CachedStore vs AppendStore choice fails at compile time, not
/// at runtime.
///
/// Key encoding is big-endian for integer fields so MDBX byte order matches
/// numeric order. Fixed-size arrays pass through unchanged: callers
/// construct their primary keys (event IDs, addresses) with the byte layout
/// they want.
const std = @import("std");
const lmdbx = @import("lmdbx");
const entity_serial = @import("entity_serial.zig");

/// SDK-stable error for an out-of-order save. Decoupled from lmdbx-zig's
/// MDBX_EKEYMISMATCH so wrapper or upstream renames don't leak.
pub const AppendError = error{KeyOutOfOrder} || lmdbx.Error;

pub fn AppendStore(comptime T: type) type {
    const fields = @typeInfo(T).@"struct".fields;
    if (fields.len == 0) @compileError("AppendStore: entity '" ++ @typeName(T) ++ "' has no fields. The first field must be the primary key.");

    const KEY_SIZE = entity_serial.fixedSize(fields[0].type, @typeName(T) ++ "." ++ fields[0].name);
    const VALUE_SIZE = entity_serial.entitySize(T);

    return struct {
        const Self = @This();
        pub const Entity = T;
        pub const key_size = KEY_SIZE;
        pub const value_size = VALUE_SIZE;

        dbi: lmdbx.Database.DBI,

        pub fn open(txn: lmdbx.Transaction, name: [*:0]const u8) !Self {
            const db = try lmdbx.Database.open(txn, name, .{ .create = true });
            return .{ .dbi = db.dbi };
        }

        pub fn save(self: Self, txn: lmdbx.Transaction, entity: T) AppendError!void {
            var key_buf: [KEY_SIZE]u8 = undefined;
            var val_buf: [VALUE_SIZE]u8 = undefined;
            entity_serial.serializeKey(T, entity, &key_buf);
            entity_serial.serialize(T, entity, &val_buf);
            const db = lmdbx.Database{ .txn = txn, .dbi = self.dbi };
            db.set(&key_buf, &val_buf, .Append) catch |err| switch (err) {
                error.MDBX_EKEYMISMATCH, error.MDBX_KEYEXIST => return error.KeyOutOfOrder,
                else => return err,
            };
        }

        /// Append-only entities are never loaded during backfill. The cache
        /// in CachedStore exists for the mutable case; calling load on an
        /// AppendStore is almost always a CachedStore/AppendStore mixup, so
        /// catch it at compile time.
        pub fn load(_: Self, _: lmdbx.Transaction, _: anytype) !?T {
            @compileError("AppendStore.load is not supported: cannot load append-only entities during backfill");
        }

        pub fn flush(_: Self, _: lmdbx.Transaction) void {}

        pub fn serializeKey(entity: T, out: *[KEY_SIZE]u8) void {
            entity_serial.serializeKey(T, entity, out);
        }

        pub fn serialize(entity: T, out: *[VALUE_SIZE]u8) void {
            entity_serial.serialize(T, entity, out);
        }

        pub fn deserialize(buf: *const [VALUE_SIZE]u8) T {
            return entity_serial.deserialize(T, buf);
        }
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

test "comptime sizes" {
    const E = struct {
        id: [8]u8,
        addr: [20]u8,
        value: u256,
        block: u64,
    };
    const S = AppendStore(E);
    try std.testing.expectEqual(@as(usize, 8), S.key_size);
    try std.testing.expectEqual(@as(usize, 8 + 20 + 32 + 8), S.value_size);
}

test "serialize roundtrip" {
    const E = struct {
        id: [8]u8,
        addr: [20]u8,
        value: u256,
        block: u64,
    };
    const S = AppendStore(E);
    const original = E{
        .id = [_]u8{ 0, 0, 0, 1, 0, 0, 0, 7 },
        .addr = [_]u8{0xAB} ** 20,
        .value = 0x1122334455667788,
        .block = 18_600_100,
    };
    var key_buf: [S.key_size]u8 = undefined;
    var val_buf: [S.value_size]u8 = undefined;
    S.serializeKey(original, &key_buf);
    S.serialize(original, &val_buf);
    try std.testing.expectEqualSlices(u8, &original.id, &key_buf);
    const restored = S.deserialize(&val_buf);
    try std.testing.expectEqualSlices(u8, &original.id, &restored.id);
    try std.testing.expectEqualSlices(u8, &original.addr, &restored.addr);
    try std.testing.expectEqual(original.value, restored.value);
    try std.testing.expectEqual(original.block, restored.block);
}

test "integer key serializes big-endian for monotonic byte order" {
    const E = struct { id: u64, payload: u32 };
    const S = AppendStore(E);
    var key_a: [8]u8 = undefined;
    var key_b: [8]u8 = undefined;
    S.serializeKey(.{ .id = 100, .payload = 0 }, &key_a);
    S.serializeKey(.{ .id = 101, .payload = 0 }, &key_b);
    try std.testing.expect(std.mem.lessThan(u8, &key_a, &key_b));
}

test "save monotonic and out-of-order against MDBX" {
    const E = struct {
        id: [8]u8,
        value: u64,
    };
    const S = AppendStore(E);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpathZ(".", &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const env = try lmdbx.Environment.init(@ptrCast(&path_z), .{ .max_dbs = 4 });
    defer env.deinit() catch {};

    const txn = try env.transaction(.{});
    const store = try S.open(txn, "events");

    try store.save(txn, .{ .id = idKey(1, 0), .value = 100 });
    try store.save(txn, .{ .id = idKey(1, 1), .value = 101 });
    try store.save(txn, .{ .id = idKey(2, 0), .value = 200 });

    // Out-of-order: id (1, 2) is less than the just-inserted (2, 0).
    const out_of_order = store.save(txn, .{ .id = idKey(1, 2), .value = 102 });
    try std.testing.expectError(error.KeyOutOfOrder, out_of_order);

    try txn.commit();
}

fn idKey(block: u32, log_index: u32) [8]u8 {
    var out: [8]u8 = undefined;
    std.mem.writeInt(u32, out[0..4], block, .big);
    std.mem.writeInt(u32, out[4..8], log_index, .big);
    return out;
}
