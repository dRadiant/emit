/// ImmutableStore(T): MDBX-backed append-only entity store via `MDBX_APPEND`
/// (sequential insert, no B-tree traversal). `load` is a `@compileError` so
/// a MutableStore/ImmutableStore mixup fails at compile time.
///
/// Keys are big-endian so MDBX byte order matches numeric order. Fixed-size
/// arrays pass through unchanged.
///
/// In live mode, saves accumulate in a per-block buffer; `commitBlock` drains
/// one block's list via `MDBX_APPEND`; `discardAll` drops everything on reorg.
const std = @import("std");

const lmdbx = @import("lmdbx");

const entity_serial = @import("entity_serial.zig");

/// Stable error for an out-of-order save. Decoupled from lmdbx-zig's
/// MDBX_EKEYMISMATCH so wrapper renames don't leak. `OutOfMemory` only
/// surfaces from live-mode buffer growth.
pub const AppendError = error{ KeyOutOfOrder, OutOfMemory } || lmdbx.Error;

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

        // Live-mode per-block buffer. `pending` is zero-init so backfill
        // never allocates. Keyed by block number; events can't shadow
        // each other so each block holds its own list.
        allocator: std.mem.Allocator,
        live: bool = false,
        live_block: u64 = 0,
        pending: std.AutoHashMapUnmanaged(u64, std.ArrayListUnmanaged(T)) = .{},

        pub fn open(allocator: std.mem.Allocator, txn_ref: *const lmdbx.Transaction, name: [*:0]const u8) !Self {
            const db = try lmdbx.Database.open(txn_ref.*, name, .{ .create = true });
            return .{ .dbi = db.dbi, .active_txn = txn_ref, .allocator = allocator };
        }

        pub fn deinit(self: *Self) void {
            var it = self.pending.iterator();
            while (it.next()) |entry| entry.value_ptr.deinit(self.allocator);
            self.pending.deinit(self.allocator);
        }

        pub fn save(self: *Self, entity: T) AppendError!void {
            if (self.live) {
                const gop = self.pending.getOrPut(self.allocator, self.live_block) catch return error.OutOfMemory;
                if (!gop.found_existing) gop.value_ptr.* = .{};
                gop.value_ptr.append(self.allocator, entity) catch return error.OutOfMemory;
                return;
            }
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

        /// Append every buffered entry for `block` via `MDBX_APPEND`, then
        /// drop the block's buffer. No-op when nothing is buffered for `block`.
        pub fn commitBlock(self: *Self, block: u64) AppendError!void {
            const removed = self.pending.fetchRemove(block) orelse return;
            var entities = removed.value;
            defer entities.deinit(self.allocator);
            const db = lmdbx.Database{ .txn = self.active_txn.*, .dbi = self.dbi };
            for (entities.items) |e| {
                var key_buf: [KEY_SIZE]u8 = undefined;
                var val_buf: [VALUE_SIZE]u8 = undefined;
                entity_serial.serializeKey(T, e, &key_buf);
                entity_serial.serialize(T, e, &val_buf);
                db.set(&key_buf, &val_buf, .Append) catch |err| switch (err) {
                    error.MDBX_EKEYMISMATCH, error.MDBX_KEYEXIST => return error.KeyOutOfOrder,
                    else => return err,
                };
            }
        }

        /// Drop the entire overlay. Used on reorg.
        pub fn discardAll(self: *Self) void {
            var it = self.pending.iterator();
            while (it.next()) |entry| entry.value_ptr.deinit(self.allocator);
            self.pending.clearRetainingCapacity();
        }

        /// Total entries across every per-block buffer.
        pub fn pendingCount(self: *const Self) u32 {
            var total: u32 = 0;
            var it = self.pending.iterator();
            while (it.next()) |entry| total += @intCast(entry.value_ptr.items.len);
            return total;
        }
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
    var store = try S.open(std.testing.allocator, &current_txn, "events");
    defer store.deinit();

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

fn openLiveTestEnv(tmp: *std.testing.TmpDir) !lmdbx.Environment {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpathZ(".", &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    return try lmdbx.Environment.init(@ptrCast(&path_z), .{ .max_dbs = 4 });
}

test "live save buffers per block" {
    const E = struct { id: [8]u8, value: u64 };
    const S = ImmutableStore(E);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openLiveTestEnv(&tmp);
    defer env.deinit() catch {};

    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "events");
    defer store.deinit();
    store.live = true;

    store.live_block = 100;
    try store.save(.{ .id = idKey(100, 0), .value = 1 });
    try store.save(.{ .id = idKey(100, 1), .value = 2 });
    store.live_block = 101;
    try store.save(.{ .id = idKey(101, 0), .value = 3 });

    try std.testing.expectEqual(@as(u32, 3), store.pendingCount());

    // Nothing in MDBX yet.
    const db = lmdbx.Database{ .txn = current_txn, .dbi = store.dbi };
    const key = idKey(100, 0);
    try std.testing.expect((try db.get(&key)) == null);

    try current_txn.abort();
}

test "commitBlock drains only the matching block via MDBX_APPEND" {
    const E = struct { id: [8]u8, value: u64 };
    const S = ImmutableStore(E);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openLiveTestEnv(&tmp);
    defer env.deinit() catch {};

    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "events");
    defer store.deinit();
    store.live = true;

    store.live_block = 100;
    try store.save(.{ .id = idKey(100, 0), .value = 1 });
    try store.save(.{ .id = idKey(100, 1), .value = 2 });
    store.live_block = 101;
    try store.save(.{ .id = idKey(101, 0), .value = 3 });

    try store.commitBlock(100);

    // Block 100 entries committed; block 101 still pending.
    try std.testing.expectEqual(@as(u32, 1), store.pendingCount());

    const db = lmdbx.Database{ .txn = current_txn, .dbi = store.dbi };
    const k100_0 = idKey(100, 0);
    const k100_1 = idKey(100, 1);
    const k101_0 = idKey(101, 0);
    try std.testing.expect((try db.get(&k100_0)) != null);
    try std.testing.expect((try db.get(&k100_1)) != null);
    try std.testing.expect((try db.get(&k101_0)) == null);

    try current_txn.commit();
}

test "discardAll drops every block buffer" {
    const E = struct { id: [8]u8, value: u64 };
    const S = ImmutableStore(E);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openLiveTestEnv(&tmp);
    defer env.deinit() catch {};

    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "events");
    defer store.deinit();
    store.live = true;

    store.live_block = 100;
    try store.save(.{ .id = idKey(100, 0), .value = 1 });
    store.live_block = 101;
    try store.save(.{ .id = idKey(101, 0), .value = 2 });
    try std.testing.expectEqual(@as(u32, 2), store.pendingCount());

    store.discardAll();
    try std.testing.expectEqual(@as(u32, 0), store.pendingCount());

    try current_txn.abort();
}

test "backfill leaves the overlay empty" {
    const E = struct { id: [8]u8, value: u64 };
    const S = ImmutableStore(E);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const env = try openLiveTestEnv(&tmp);
    defer env.deinit() catch {};

    var current_txn = try env.transaction(.{});
    var store = try S.open(std.testing.allocator, &current_txn, "events");
    defer store.deinit();

    // live defaults to false; saves go straight to MDBX_APPEND, overlay stays empty.
    try store.save(.{ .id = idKey(1, 0), .value = 1 });
    try store.save(.{ .id = idKey(1, 1), .value = 2 });
    try std.testing.expectEqual(@as(u32, 0), store.pendingCount());

    try current_txn.commit();
}
