/// Owns one `<entity>.events.dat` file for a single ImmutableStore type.
/// Append-only flat record file mirroring the engine's `blocks.dat` pattern.
/// Authoritative record count lives in `state.snap`, not in this file's
/// header. Trailing bytes past `count * record_size` are orphan records
/// from a crashed prior commit. Readers ignore them, next append overwrites.
///
/// Layout:
///   magic            [8]u8       "EMITEVTS"
///   records          [count × record_size]   fixed-size, sorted by primary key
const std = @import("std");

const core = @import("core");

const entity_serial = @import("entity_serial.zig");
const blob_log_mod = @import("blob_log.zig");

const BlobLog = blob_log_mod.BlobLog;
const BlobRef = entity_serial.BlobRef;

pub const MAGIC: core.flat_format.Magic = "EMITEVTS".*;
pub const HEADER_SIZE: usize = core.flat_format.MAGIC_SIZE;

pub fn EventLog(comptime T: type) type {
    const fields = @typeInfo(T).@"struct".fields;
    if (fields.len == 0) @compileError("EventLog: entity '" ++ @typeName(T) ++ "' has no fields. The first field must be the primary key.");
    const RECORD_SIZE = entity_serial.entitySize(T);
    const KEY_SIZE = entity_serial.fixedSize(fields[0].type, @typeName(T) ++ "." ++ fields[0].name);
    const HAS_BLOBS = entity_serial.hasBlobs(T);
    const BLOB_COUNT = entity_serial.blobCount(T);

    return struct {
        const Self = @This();
        pub const record_size = RECORD_SIZE;
        pub const key_size = KEY_SIZE;
        pub const Entity = T;
        pub const KeyBytes = [KEY_SIZE]u8;

        allocator: std.mem.Allocator,
        file: std.fs.File,

        /// Open `<dir>/<name>` for append + random read. Creates the file
        /// with a fresh magic header on first use. Wrong magic surfaces
        /// `error.InvalidMagic` so the caller can fail loud.
        pub fn open(allocator: std.mem.Allocator, dir: std.fs.Dir, name: []const u8) !Self {
            const file = try core.flat_format.openOrCreateWithMagic(dir, name, MAGIC);
            return Self{ .allocator = allocator, .file = file };
        }

        pub fn deinit(self: *Self) void {
            self.file.close();
        }

        /// Append records at byte offset `HEADER_SIZE + at_count * record_size`.
        /// Caller passes `at_count` from `state.snap.immutable_counts[slot]`
        /// so orphan trailing bytes from a crashed prior commit get
        /// overwritten by the new records. For a blob entity, `blob_log` stages
        /// each record's variable-length payloads (caller flushes it before the
        /// `state.snap` rename); `void` for a numeric entity.
        pub fn append(self: *Self, records: []const T, at_count: u64, blob_log: if (HAS_BLOBS) *BlobLog else void) !void {
            if (records.len == 0) return;
            const total = records.len * RECORD_SIZE;
            const buf = try self.allocator.alloc(u8, total);
            defer self.allocator.free(buf);

            var pos: usize = 0;
            for (records) |r| {
                if (comptime HAS_BLOBS) {
                    var refs: [BLOB_COUNT]BlobRef = undefined;
                    comptime var bi: usize = 0;
                    inline for (fields) |f| {
                        if (comptime entity_serial.fieldKind(f.type, @typeName(T) ++ "." ++ f.name) == .blob) {
                            const slice = @field(r, f.name);
                            const elem_align = @alignOf(@typeInfo(f.type).pointer.child);
                            refs[bi] = try blob_log.stage(std.mem.sliceAsBytes(slice), elem_align);
                            bi += 1;
                        }
                    }
                    entity_serial.serializeWithBlobs(T, r, &refs, buf[pos..][0..RECORD_SIZE]);
                } else {
                    entity_serial.serialize(T, r, buf[pos..][0..RECORD_SIZE]);
                }
                pos += RECORD_SIZE;
            }

            const offset = HEADER_SIZE + at_count * RECORD_SIZE;
            try self.file.pwriteAll(buf, offset);
        }

        pub fn sync(self: *Self) !void {
            try self.file.sync();
        }

        /// Read the record at index `i`. Caller must ensure
        /// `i < state.snap.immutable_counts[slot]`. Reading past the
        /// authoritative count may return orphan bytes from a crashed
        /// prior commit. `blobs_map` resolves blob refs for a blob entity
        /// (the committed `blobs.dat` mmap); `void` for a numeric entity.
        pub fn read(self: *Self, i: u64, blobs_map: if (HAS_BLOBS) []const u8 else void) !T {
            const offset = HEADER_SIZE + i * RECORD_SIZE;
            var buf: [RECORD_SIZE]u8 = undefined;
            const n = try self.file.pread(&buf, offset);
            if (n != RECORD_SIZE) return error.Truncated;
            if (comptime HAS_BLOBS) return entity_serial.deserializeWithBlobs(T, &buf, blobs_map);
            return entity_serial.deserialize(T, &buf);
        }

        /// Read records `[start, start + out.len)` with chunked preads, one
        /// syscall per ~16 KB of records instead of one per record. Same
        /// contract as `read`: caller must ensure the range is within the
        /// authoritative count.
        pub fn readRange(self: *Self, start: u64, out: []T, blobs_map: if (HAS_BLOBS) []const u8 else void) !void {
            const per_chunk = comptime @max(1, (16 * 1024) / RECORD_SIZE);
            var buf: [per_chunk * RECORD_SIZE]u8 = undefined;
            var done: usize = 0;
            while (done < out.len) {
                // Explicit usize. `@min` with a comptime bound narrows the
                // result type and `n * RECORD_SIZE` would overflow it.
                const n: usize = @min(per_chunk, out.len - done);
                const bytes = buf[0 .. n * RECORD_SIZE];
                const offset = HEADER_SIZE + (start + done) * RECORD_SIZE;
                if ((try self.file.pread(bytes, offset)) != bytes.len) return error.Truncated;
                for (out[done..][0..n], 0..) |*rec, k| {
                    const rbuf = bytes[k * RECORD_SIZE ..][0..RECORD_SIZE];
                    rec.* = if (comptime HAS_BLOBS) entity_serial.deserializeWithBlobs(T, rbuf, blobs_map) else entity_serial.deserialize(T, rbuf);
                }
                done += n;
            }
        }

        /// Read the key bytes of record `i` without deserializing the full
        /// entity. Used by binary search.
        pub fn readKey(self: *Self, i: u64) !KeyBytes {
            const offset = HEADER_SIZE + i * RECORD_SIZE;
            var buf: KeyBytes = undefined;
            const n = try self.file.pread(&buf, offset);
            if (n != KEY_SIZE) return error.Truncated;
            return buf;
        }

        /// Binary search for the record whose key equals `target`. `count`
        /// is the authoritative record count (from `state.snap`). Returns
        /// the record index, or null if absent.
        /// Keys are big-endian, so byte order matches numeric order.
        pub fn binarySearch(self: *Self, count: u64, target: KeyBytes) !?u64 {
            if (count == 0) return null;
            var lo: u64 = 0;
            var hi: u64 = count;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const key = try self.readKey(mid);
                switch (std.mem.order(u8, &key, &target)) {
                    .eq => return mid,
                    .lt => lo = mid + 1,
                    .gt => hi = mid,
                }
            }
            return null;
        }
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

const TransferEvent = struct {
    pub const storage = enum { mutable, immutable }.immutable;
    id: [16]u8,
    from: [20]u8,
    to: [20]u8,
    value: u256,
};

fn keyAt(block: u64, log_index: u64) [16]u8 {
    var k: [16]u8 = undefined;
    std.mem.writeInt(u64, k[0..8], block, .big);
    std.mem.writeInt(u64, k[8..16], log_index, .big);
    return k;
}

test "open on a fresh dir creates the file with the magic header" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var log = try EventLog(TransferEvent).open(testing.allocator, tmp.dir, "transfer.events.dat");
        defer log.deinit();
    }

    const file = try tmp.dir.openFile("transfer.events.dat", .{});
    defer file.close();
    var buf: [HEADER_SIZE]u8 = undefined;
    const n = try file.readAll(&buf);
    try testing.expectEqual(HEADER_SIZE, n);
    try core.flat_format.validateMagic(&buf, MAGIC);
}

test "append then read round-trips records" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try EventLog(TransferEvent).open(testing.allocator, tmp.dir, "transfer.events.dat");
    defer log.deinit();

    const recs = [_]TransferEvent{
        .{ .id = keyAt(100, 0), .from = [_]u8{0xAA} ** 20, .to = [_]u8{0xBB} ** 20, .value = 1000 },
        .{ .id = keyAt(100, 1), .from = [_]u8{0xCC} ** 20, .to = [_]u8{0xDD} ** 20, .value = 2000 },
        .{ .id = keyAt(101, 0), .from = [_]u8{0xEE} ** 20, .to = [_]u8{0xFF} ** 20, .value = 3000 },
    };
    try log.append(&recs, 0, {});

    const r0 = try log.read(0, {});
    const r2 = try log.read(2, {});
    try testing.expectEqualSlices(u8, &recs[0].from, &r0.from);
    try testing.expectEqual(@as(u256, 1000), r0.value);
    try testing.expectEqualSlices(u8, &recs[2].to, &r2.to);
    try testing.expectEqual(@as(u256, 3000), r2.value);
}

test "readRange crosses the pread chunk boundary" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try EventLog(TransferEvent).open(testing.allocator, tmp.dir, "transfer.events.dat");
    defer log.deinit();

    // 400 records at 88 bytes each spans three ~16 KB pread chunks, so the
    // loop's chunk stitching and per-chunk offsets are exercised.
    const COUNT = 400;
    const recs = try testing.allocator.alloc(TransferEvent, COUNT);
    defer testing.allocator.free(recs);
    for (recs, 0..) |*r, i| {
        r.* = .{ .id = keyAt(i, 0), .from = [_]u8{0xAA} ** 20, .to = [_]u8{0xBB} ** 20, .value = i };
    }
    try log.append(recs, 0, {});

    const out = try testing.allocator.alloc(TransferEvent, COUNT - 1);
    defer testing.allocator.free(out);
    try log.readRange(1, out, {});
    for (out, 1..) |r, i| {
        try testing.expectEqual(@as(u256, i), r.value);
        try testing.expectEqualSlices(u8, &keyAt(i, 0), &r.id);
    }
}

test "binarySearch finds present keys and returns null for absent" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try EventLog(TransferEvent).open(testing.allocator, tmp.dir, "transfer.events.dat");
    defer log.deinit();

    const recs = [_]TransferEvent{
        .{ .id = keyAt(100, 0), .from = [_]u8{0} ** 20, .to = [_]u8{0} ** 20, .value = 1 },
        .{ .id = keyAt(100, 5), .from = [_]u8{0} ** 20, .to = [_]u8{0} ** 20, .value = 2 },
        .{ .id = keyAt(200, 0), .from = [_]u8{0} ** 20, .to = [_]u8{0} ** 20, .value = 3 },
        .{ .id = keyAt(300, 7), .from = [_]u8{0} ** 20, .to = [_]u8{0} ** 20, .value = 4 },
    };
    try log.append(&recs, 0, {});

    const i = (try log.binarySearch(recs.len, keyAt(200, 0))) orelse return error.NotFound;
    try testing.expectEqual(@as(u64, 2), i);
    try testing.expect((try log.binarySearch(recs.len, keyAt(150, 0))) == null);
    try testing.expect((try log.binarySearch(0, keyAt(100, 0))) == null);
}

test "orphan trailing bytes past count are invisible and overwritten by next append" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try EventLog(TransferEvent).open(testing.allocator, tmp.dir, "transfer.events.dat");
    defer log.deinit();

    const good = [_]TransferEvent{
        .{ .id = keyAt(100, 0), .from = [_]u8{0xAA} ** 20, .to = [_]u8{0} ** 20, .value = 1 },
        .{ .id = keyAt(100, 1), .from = [_]u8{0xBB} ** 20, .to = [_]u8{0} ** 20, .value = 2 },
    };
    try log.append(&good, 0, {});

    const orphan = [_]TransferEvent{
        .{ .id = keyAt(200, 0), .from = [_]u8{0xDE} ** 20, .to = [_]u8{0} ** 20, .value = 999 },
        .{ .id = keyAt(300, 0), .from = [_]u8{0xAD} ** 20, .to = [_]u8{0} ** 20, .value = 999 },
    };
    try log.append(&orphan, good.len, {});

    // Binary search bounded by the authoritative count (2) must NOT see orphans.
    try testing.expect((try log.binarySearch(good.len, keyAt(200, 0))) == null);

    // Next legitimate append overwrites the orphans starting at offset 2.
    const fresh = [_]TransferEvent{
        .{ .id = keyAt(150, 0), .from = [_]u8{0xCC} ** 20, .to = [_]u8{0} ** 20, .value = 50 },
    };
    try log.append(&fresh, good.len, {});

    const found = (try log.binarySearch(good.len + 1, keyAt(150, 0))) orelse return error.NotFound;
    try testing.expectEqual(@as(u64, 2), found);
    const back = try log.read(found, {});
    try testing.expectEqual(@as(u256, 50), back.value);
}

test "open against an existing file validates the magic" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var f = try tmp.dir.createFile("bad.events.dat", .{});
        defer f.close();
        try f.writeAll(&[_]u8{0xFF} ** 8);
    }

    try testing.expectError(error.InvalidMagic, EventLog(TransferEvent).open(
        testing.allocator,
        tmp.dir,
        "bad.events.dat",
    ));
}
