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
const blob_log_mod = @import("blob_log.zig");

const BlobLog = blob_log_mod.BlobLog;
const BlobRef = entity_serial.BlobRef;

pub fn MutableStore(comptime T: type) type {
    const fields = @typeInfo(T).@"struct".fields;
    if (fields.len == 0) @compileError("MutableStore: entity '" ++ @typeName(T) ++ "' has no fields. The first field must be the primary key.");
    comptime entity_serial.validate(T);

    const KeyField = fields[0].type;
    const key_field_name = fields[0].name;
    const KEY_SIZE = entity_serial.fixedSize(KeyField, @typeName(T) ++ "." ++ fields[0].name);
    const VALUE_SIZE = entity_serial.entitySize(T);
    const HAS_BLOBS = entity_serial.hasBlobs(T);
    const BLOB_COUNT = entity_serial.blobCount(T);

    return struct {
        const Self = @This();
        pub const Entity = T;
        pub const Key = KeyField;
        pub const key_size = KEY_SIZE;
        pub const value_size = VALUE_SIZE;
        pub const has_blobs = HAS_BLOBS;

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

        /// `<entity>.blobs.dat` for variable-length fields (ADR-005). Holds the
        /// payloads the fixed records address by `BlobRef`. `void` for a
        /// numeric entity, which compiles to the exact pre-blob layout.
        blob_log: if (HAS_BLOBS) BlobLog else void = if (HAS_BLOBS) undefined else {},
        /// Backs the blob bytes of backfill saves. `save` copies each blob
        /// field here so the cached slice outlives the transient `log.data` it
        /// came from. Reset after each commit. Unused on the live path, which
        /// uses per-block arenas instead.
        blob_arena: if (HAS_BLOBS) std.heap.ArenaAllocator else void = if (HAS_BLOBS) undefined else {},
        /// Live path: one blob arena per pending block, so a reorg that drops
        /// a block frees exactly that block's blob bytes. Parallel to the
        /// `pending` submaps. Bounded by FINALITY_DEPTH.
        pending_arenas: if (HAS_BLOBS) std.AutoHashMapUnmanaged(u64, *std.heap.ArenaAllocator) else void = if (HAS_BLOBS) .{} else {},
        /// Blocks drained by `commitBlock` whose arenas back cache entries
        /// awaiting `materialize`. Freed in `refreshSlab` once those blobs are
        /// durable in `blobs.dat`.
        committing_arenas: if (HAS_BLOBS) std.ArrayListUnmanaged(*std.heap.ArenaAllocator) else void = if (HAS_BLOBS) .{} else {},

        /// `slab` is borrowed from the owning `StateSnap`. Caller must call
        /// `refreshSlab` after every `state_snap.commit` to avoid dangling.
        /// Numeric entities only. Blob entities use `openWithBlobs`.
        pub fn open(allocator: std.mem.Allocator, slab: []const u8) Self {
            if (comptime HAS_BLOBS) @compileError(
                "MutableStore(" ++ @typeName(T) ++ "): entity has blob fields; use openWithBlobs",
            );
            return .{
                .allocator = allocator,
                .cache = std.AutoHashMap(KeyField, CacheEntry).init(allocator),
                .slab = slab,
            };
        }

        /// Open a blob-bearing store, also opening `<name>` in `dir` at
        /// `committed_blob_len` (from `state.snap.blob_bytes[slot]`).
        pub fn openWithBlobs(
            allocator: std.mem.Allocator,
            slab: []const u8,
            dir: std.fs.Dir,
            name: []const u8,
            committed_blob_len: u64,
        ) !Self {
            if (comptime !HAS_BLOBS) @compileError(
                "MutableStore(" ++ @typeName(T) ++ "): entity has no blob fields; use open",
            );
            return .{
                .allocator = allocator,
                .cache = std.AutoHashMap(KeyField, CacheEntry).init(allocator),
                .slab = slab,
                .blob_log = try BlobLog.open(allocator, dir, name, committed_blob_len),
                .blob_arena = std.heap.ArenaAllocator.init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.pending.valueIterator();
            while (it.next()) |bm| bm.deinit(self.allocator);
            self.pending.deinit(self.allocator);
            self.cache.deinit();
            if (comptime HAS_BLOBS) {
                var ait = self.pending_arenas.valueIterator();
                while (ait.next()) |a| self.destroyArena(a.*);
                self.pending_arenas.deinit(self.allocator);
                for (self.committing_arenas.items) |a| self.destroyArena(a);
                self.committing_arenas.deinit(self.allocator);
                self.blob_arena.deinit();
                self.blob_log.deinit();
            }
        }

        fn destroyArena(self: *Self, arena: *std.heap.ArenaAllocator) void {
            arena.deinit();
            self.allocator.destroy(arena);
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
            // Blob entities re-resolve from the slab + mmap each cold read: a
            // cached slice would dangle when the next commit remaps blobs.dat.
            // The cache holds only this cycle's dirty saves. Numeric entities
            // keep the clean-read cache (the HashMap-front speedup).
            if (comptime HAS_BLOBS) return self.slabEntity(idx);
            const entity = self.slabEntity(idx);
            try self.cache.put(key, .{ .entity = entity, .dirty = false });
            return entity;
        }

        /// Load `key`, or initialize a fresh zeroed entity with the primary-key
        /// field set to `key`. Fresh entity inserted dirty to persist at next
        /// commit. A fresh blob entity's slices are empty (`std.mem.zeroes`
        /// yields zero-length slices), so no arena copy is needed yet.
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
        /// Blob payloads are copied into a store-owned arena (the cycle arena
        /// on backfill, the current block's arena live) so the stored entity
        /// owns bytes that outlive the handler's transient `log.data` slice.
        pub fn save(self: *Self, entity: T) !void {
            const key = @field(entity, key_field_name);
            if (self.live) {
                try self.putPending(key, entity);
                return;
            }
            const owned = if (comptime HAS_BLOBS) try self.ownBlobs(entity, self.blob_arena.allocator()) else entity;
            try self.cache.put(key, .{ .entity = owned, .dirty = true });
        }

        fn putPending(self: *Self, key: KeyField, value: T) !void {
            const owned = if (comptime HAS_BLOBS) try self.ownBlobs(value, try self.pendingArena()) else value;
            const gop = try self.pending.getOrPut(self.allocator, self.live_block);
            if (!gop.found_existing) gop.value_ptr.* = .{};
            try gop.value_ptr.put(self.allocator, key, owned);
        }

        /// Allocator for the current `live_block`'s blob arena, creating it on
        /// first save to the block. Freed when the block commits or reorgs out.
        fn pendingArena(self: *Self) !std.mem.Allocator {
            const gop = try self.pending_arenas.getOrPut(self.allocator, self.live_block);
            if (!gop.found_existing) {
                const arena = try self.allocator.create(std.heap.ArenaAllocator);
                arena.* = std.heap.ArenaAllocator.init(self.allocator);
                gop.value_ptr.* = arena;
            }
            return gop.value_ptr.*.allocator();
        }

        /// Deserialize the slab record at `idx`, resolving blob refs against
        /// the committed `blobs.dat` mmap for blob entities.
        fn slabEntity(self: *const Self, idx: usize) T {
            const record = self.slab[idx * VALUE_SIZE ..][0..VALUE_SIZE];
            if (comptime HAS_BLOBS) return entity_serial.deserializeWithBlobs(T, record, self.blob_log.map_bytes());
            return entity_serial.deserialize(T, record);
        }

        /// Copy `entity`'s blob payloads into `a`, repointing each slice at the
        /// stable copy. Blob entities only.
        fn ownBlobs(_: *Self, entity: T, a: std.mem.Allocator) !T {
            var out = entity;
            inline for (fields) |f| {
                if (comptime entity_serial.fieldKind(f.type, @typeName(T) ++ "." ++ f.name) == .blob) {
                    const src = @field(entity, f.name);
                    if (src.len > 0) @field(out, f.name) = try a.dupe(@typeInfo(f.type).pointer.child, src);
                }
            }
            return out;
        }

        /// Drain block `N`'s overlay submap into the dirty cache. Disk write
        /// happens later via `materialize` + `state_snap.commit`.
        pub fn commitBlock(self: *Self, block: u64) !void {
            const sub = self.pending.getPtr(block) orelse return;
            // Reserve cache capacity before removing the submap. A partial OOM
            // would otherwise strand entries between pending and cache.
            try self.cache.ensureUnusedCapacity(sub.count());
            // The drained cache entries' blob slices reference this block's
            // arena, so it must outlive the drain. Hand it to `committing`,
            // freed by `refreshSlab` once the blobs are durable. Reserve the
            // list slot first so the move cannot half-fail.
            if (comptime HAS_BLOBS) {
                if (self.pending_arenas.get(block)) |_| try self.committing_arenas.ensureUnusedCapacity(self.allocator, 1);
            }
            var removed = self.pending.fetchRemove(block).?;
            defer removed.value.deinit(self.allocator);
            var it = removed.value.iterator();
            while (it.next()) |entry| {
                self.cache.putAssumeCapacity(entry.key_ptr.*, .{ .entity = entry.value_ptr.*, .dirty = true });
            }
            if (comptime HAS_BLOBS) {
                if (self.pending_arenas.fetchRemove(block)) |kv| self.committing_arenas.appendAssumeCapacity(kv.value);
            }
        }

        /// Drop every overlay submap. Used on reorg. Caller re-dispatches the
        /// fresh canonical chain.
        pub fn discardAll(self: *Self) void {
            var it = self.pending.valueIterator();
            while (it.next()) |bm| bm.deinit(self.allocator);
            self.pending.clearRetainingCapacity();
            if (comptime HAS_BLOBS) {
                var ait = self.pending_arenas.valueIterator();
                while (ait.next()) |a| self.destroyArena(a.*);
                self.pending_arenas.clearRetainingCapacity();
            }
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
                        if (comptime HAS_BLOBS) {
                            if (self.pending_arenas.fetchRemove(k.*)) |kv| self.destroyArena(kv.value);
                        }
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
                try self.serializeRow(d.entity.*, out[oi..][0..VALUE_SIZE]);
                oi += VALUE_SIZE;
            }
            if (si < num_in_slab) {
                const n = (num_in_slab - si) * VALUE_SIZE;
                @memcpy(out[oi..][0..n], self.slab[si * VALUE_SIZE ..][0..n]);
                oi += n;
            }
            return out;
        }

        /// Serialize one row, staging its blob payloads to `blob_log` first
        /// (so the record's `BlobRef`s carry the assigned offsets). Clean rows
        /// in `materialize` are bulk-copied instead, keeping their committed
        /// refs. Numeric rows take the plain fixed serialize.
        fn serializeRow(self: *Self, entity: T, out: []u8) !void {
            if (comptime !HAS_BLOBS) {
                entity_serial.serialize(T, entity, out);
                return;
            }
            var refs: [BLOB_COUNT]BlobRef = undefined;
            comptime var bi: usize = 0;
            inline for (fields) |f| {
                if (comptime entity_serial.fieldKind(f.type, @typeName(T) ++ "." ++ f.name) == .blob) {
                    const slice = @field(entity, f.name);
                    const elem_align = @alignOf(@typeInfo(f.type).pointer.child);
                    refs[bi] = try self.blob_log.stage(std.mem.sliceAsBytes(slice), elem_align);
                    bi += 1;
                }
            }
            entity_serial.serializeWithBlobs(T, entity, &refs, out);
        }

        /// Flush this cycle's staged blob payloads to `blobs.dat` (write +
        /// fsync + remap). MUST run after `materialize` and before the
        /// `state.snap` rename. No-op for numeric entities.
        pub fn flushBlobs(self: *Self) !void {
            if (comptime HAS_BLOBS) try self.blob_log.flush();
        }

        /// Committed `blobs.dat` payload length, written to
        /// `state.snap.blob_bytes[slot]` after `flushBlobs`.
        pub fn committedBlobLen(self: *const Self) u64 {
            if (comptime HAS_BLOBS) return self.blob_log.committed_len;
            return 0;
        }

        /// Rebind the borrowed slab pointer after `state_snap.commit` succeeds.
        /// Numeric stores clear dirty flags. Blob stores drop the whole cache
        /// and reset the arena: cached blob slices pointed into the arena or
        /// the pre-remap mmap, both now stale, and the durable rows reload from
        /// the new slab + remapped `blobs.dat`.
        pub fn refreshSlab(self: *Self, new_slab: []const u8) void {
            self.slab = new_slab;
            if (comptime HAS_BLOBS) {
                self.cache.clearRetainingCapacity();
                // Backfill saves lived in blob_arena, finalized live blocks in
                // committing arenas. Both are now durable in blobs.dat.
                _ = self.blob_arena.reset(.retain_capacity);
                for (self.committing_arenas.items) |a| self.destroyArena(a);
                self.committing_arenas.clearRetainingCapacity();
                return;
            }
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

// ── Blob fields (ADR-005) ──────────────────────────────────────────────────

const Named = struct { id: [8]u8, label: []const u8 };

fn id8(n: u8) [8]u8 {
    return [_]u8{ 0, 0, 0, 0, 0, 0, 0, n };
}

/// One commit cycle against a blob store: materialize → flush → refresh.
/// Returns the slab buffer (caller frees once the store no longer borrows it).
fn cycleNamed(store: *MutableStore(Named), alloc: std.mem.Allocator) ![]u8 {
    const slab = try store.materialize(alloc);
    try store.flushBlobs();
    store.refreshSlab(slab);
    return slab;
}

test "mutable blob entity: save, commit, reload from slab + blobs.dat" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try MutableStore(Named).openWithBlobs(alloc, &.{}, tmp.dir, "named.blobs.dat", 0);

    try store.save(.{ .id = id8(1), .label = "vitalik.eth" });
    try store.save(.{ .id = id8(2), .label = "" });

    // Read-your-writes before the commit (served from the cache + arena).
    try testing.expectEqualSlices(u8, "vitalik.eth", (try store.load(id8(1))).?.label);

    const slab = try cycleNamed(&store, alloc);

    // Cache was cleared by refresh, so these reload from the slab + mmap.
    try testing.expectEqualSlices(u8, "vitalik.eth", (try store.load(id8(1))).?.label);
    try testing.expectEqual(@as(usize, 0), (try store.load(id8(2))).?.label.len);

    const blob_len = store.committedBlobLen();
    store.deinit();

    // Reopen at the committed blob length: payloads persist across restart.
    var store2 = try MutableStore(Named).openWithBlobs(alloc, slab, tmp.dir, "named.blobs.dat", blob_len);
    try testing.expectEqualSlices(u8, "vitalik.eth", (try store2.load(id8(1))).?.label);
    store2.deinit();
    alloc.free(slab);
}

test "mutable blob overwrite across commits: later value wins" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try MutableStore(Named).openWithBlobs(alloc, &.{}, tmp.dir, "named.blobs.dat", 0);
    defer store.deinit();

    try store.save(.{ .id = id8(1), .label = "first-name-here" });
    const slab1 = try cycleNamed(&store, alloc);
    try testing.expectEqualSlices(u8, "first-name-here", (try store.load(id8(1))).?.label);

    // Overwrite the same key with a different-length value. The old blob bytes
    // orphan in blobs.dat (the accepted mutable-blob garbage), the record now
    // points at the fresh payload.
    try store.save(.{ .id = id8(1), .label = "second" });
    const slab2 = try cycleNamed(&store, alloc);
    try testing.expectEqualSlices(u8, "second", (try store.load(id8(1))).?.label);

    alloc.free(slab1);
    alloc.free(slab2);
}

test "mutable array blob: []const [20]u8 round-trips" {
    const Members = struct { id: u64, addrs: []const [20]u8 };
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try MutableStore(Members).openWithBlobs(alloc, &.{}, tmp.dir, "members.blobs.dat", 0);
    defer store.deinit();

    const addrs = [_][20]u8{ [_]u8{0x11} ** 20, [_]u8{0x22} ** 20, [_]u8{0x33} ** 20 };
    try store.save(.{ .id = 7, .addrs = &addrs });

    const slab = try store.materialize(alloc);
    defer alloc.free(slab);
    try store.flushBlobs();
    store.refreshSlab(slab);

    const got = (try store.load(@as(u64, 7))).?;
    try testing.expectEqual(@as(usize, 3), got.addrs.len);
    try testing.expectEqualSlices(u8, &addrs[0], &got.addrs[0]);
    try testing.expectEqualSlices(u8, &addrs[2], &got.addrs[2]);
}

test "live blob: per-block overlay, newest-block wins, commit reloads" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try MutableStore(Named).openWithBlobs(alloc, &.{}, tmp.dir, "named.blobs.dat", 0);
    defer store.deinit();
    store.live = true;

    // Block 100 saves into its own overlay arena; read-your-writes from it.
    store.live_block = 100;
    try store.save(.{ .id = id8(1), .label = "live-name" });
    try testing.expectEqualSlices(u8, "live-name", (try store.load(id8(1))).?.label);

    // Block 101 overwrites the key. Newest pending block wins on load.
    store.live_block = 101;
    try store.save(.{ .id = id8(1), .label = "newer-name" });
    try testing.expectEqualSlices(u8, "newer-name", (try store.load(id8(1))).?.label);

    // Finalize both, then commit. The committing arenas back the cache until
    // materialize stages their blobs; refreshSlab frees them (leak checker
    // catches a mismanaged arena).
    try store.commitBlock(100);
    try store.commitBlock(101);
    const slab = try cycleNamed(&store, alloc);
    defer alloc.free(slab);
    try testing.expectEqualSlices(u8, "newer-name", (try store.load(id8(1))).?.label);
}

test "live blob reorg: discardFrom frees the dropped block's arena, keeps below" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try MutableStore(Named).openWithBlobs(alloc, &.{}, tmp.dir, "named.blobs.dat", 0);
    defer store.deinit();
    store.live = true;

    store.live_block = 100;
    try store.save(.{ .id = id8(1), .label = "kept-below-fork" });
    store.live_block = 101;
    try store.save(.{ .id = id8(2), .label = "dropped-at-fork" });

    // Reorg at 101: block 101's arena + submap drop, block 100 survives.
    store.discardFrom(101);
    try testing.expectEqualSlices(u8, "kept-below-fork", (try store.load(id8(1))).?.label);
    try testing.expectEqual(@as(?Named, null), try store.load(id8(2)));

    // discardAll then frees the remaining block 100 arena (leak checker
    // verifies nothing is stranded).
    store.discardAll();
    try testing.expectEqual(@as(?Named, null), try store.load(id8(1)));
}
