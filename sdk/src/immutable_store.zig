/// ImmutableStore(T): comptime-generated MDBX writer for immutable
/// entities (event records, audit logs).
///
/// Writes use `MDBX_APPEND` (sequential insert, no B-tree traversal,
/// ~5x faster than upsert). `load` is a `@compileError`. The API exists
/// so a typo on a MutableStore-vs-ImmutableStore choice fails at compile
/// time, not at runtime.
///
/// Key encoding is big-endian for integer fields so MDBX byte order
/// matches numeric order. Fixed-size arrays pass through unchanged:
/// callers construct their primary keys (event IDs, addresses) with the
/// byte layout they want.
const std = @import("std");

const lmdbx = @import("lmdbx");

const entity_serial = @import("entity_serial.zig");

/// SDK-stable error for an out-of-order save. Decoupled from lmdbx-zig's
/// MDBX_EKEYMISMATCH so wrapper or upstream renames don't leak.
pub const AppendError = error{KeyOutOfOrder} || lmdbx.Error;

pub fn ImmutableStore(comptime T: type) type {
    const fields = @typeInfo(T).@"struct".fields;
    if (fields.len == 0) @compileError("ImmutableStore: entity '" ++ @typeName(T) ++ "' has no fields. The first field must be the primary key.");

    const KEY_SIZE = entity_serial.fixedSize(fields[0].type, @typeName(T) ++ "." ++ fields[0].name);
    const VALUE_SIZE = entity_serial.entitySize(T);

    return struct {
        const Self = @This();
        pub const Entity = T;
        pub const key_size = KEY_SIZE;
        pub const value_size = VALUE_SIZE;

        dbi: lmdbx.Database.DBI,
        /// Borrow into the owning Context's `_active_txn` field. Updated
        /// implicitly when the Context replaces its txn at commit
        /// boundaries; the store reads through the pointer at every save.
        active_txn: *const lmdbx.Transaction,

        /// `allocator` is unused; the parameter exists so the signature
        /// matches `MutableStore(T).open` and the SDK orchestration layer
        /// can iterate the entities tuple with a single `store.open` call.
        pub fn open(_: std.mem.Allocator, txn_ref: *const lmdbx.Transaction, name: [*:0]const u8) !Self {
            const db = try lmdbx.Database.open(txn_ref.*, name, .{ .create = true });
            return .{ .dbi = db.dbi, .active_txn = txn_ref };
        }

        pub fn deinit(_: *Self) void {}

        pub fn save(self: Self, entity: T) AppendError!void {
            var key_buf: [KEY_SIZE]u8 = undefined;
            var val_buf: [VALUE_SIZE]u8 = undefined;
            entity_serial.serializeKey(T, entity, &key_buf);
            entity_serial.serialize(T, entity, &val_buf);
            const db = lmdbx.Database{ .txn = self.active_txn.*, .dbi = self.dbi };
            db.set(&key_buf, &val_buf, .Append) catch |err| switch (err) {
                error.MDBX_EKEYMISMATCH, error.MDBX_KEYEXIST => return error.KeyOutOfOrder,
                else => return err,
            };
        }

        /// Immutable entities are never loaded during backfill. The cache
        /// in MutableStore exists for the mutable case; calling `load` on
        /// an ImmutableStore is almost always a MutableStore/ImmutableStore
        /// mixup, so catch it at compile time.
        pub fn load(_: Self, _: anytype) !?T {
            @compileError("ImmutableStore.load is not supported: cannot load immutable entities during backfill");
        }

        pub fn flush(_: *Self) !void {}
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

test "comptime sizes" {
    const E = struct { id: [8]u8, addr: [20]u8, value: u256, block: u64 };
    const S = ImmutableStore(E);
    try std.testing.expectEqual(@as(usize, 8), S.key_size);
    try std.testing.expectEqual(@as(usize, 8 + 20 + 32 + 8), S.value_size);
}

test "save monotonic and out-of-order against MDBX" {
    const E = struct {
        id: [8]u8,
        value: u64,
    };
    const S = ImmutableStore(E);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpathZ(".", &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const env = try lmdbx.Environment.init(@ptrCast(&path_z), .{ .max_dbs = 4 });
    defer env.deinit() catch {};

    var current_txn = try env.transaction(.{});
    const store = try S.open(std.testing.allocator, &current_txn, "events");

    try store.save(.{ .id = idKey(1, 0), .value = 100 });
    try store.save(.{ .id = idKey(1, 1), .value = 101 });
    try store.save(.{ .id = idKey(2, 0), .value = 200 });

    // Out-of-order: id (1, 2) is less than the just-inserted (2, 0).
    const out_of_order = store.save(.{ .id = idKey(1, 2), .value = 102 });
    try std.testing.expectError(error.KeyOutOfOrder, out_of_order);

    try current_txn.commit();
}

fn idKey(block: u32, log_index: u32) [8]u8 {
    var out: [8]u8 = undefined;
    std.mem.writeInt(u32, out[0..4], block, .big);
    std.mem.writeInt(u32, out[4..8], log_index, .big);
    return out;
}
