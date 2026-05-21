/// Owns one `<entity>.events.dat` file for a single ImmutableStore type.
/// Append-only flat record file mirroring the engine's `blocks.dat` pattern.
/// The authoritative record count lives in `state.snap`, not in this file's
/// header. Any trailing bytes past `count * record_size` are orphan records
/// from a crashed prior commit; readers ignore them and the next append
/// overwrites them.
///
/// Layout:
///   magic            [8]u8       "EMITEVTS"
///   records          [count × record_size]   fixed-size, sorted by primary key
const std = @import("std");

const core = @import("core");

const entity_serial = @import("entity_serial.zig");

pub const MAGIC: core.flat_format.Magic = "EMITEVTS".*;
pub const HEADER_SIZE: usize = core.flat_format.MAGIC_SIZE;

pub fn EventLog(comptime T: type) type {
    const fields = @typeInfo(T).@"struct".fields;
    if (fields.len == 0) @compileError("EventLog: entity '" ++ @typeName(T) ++ "' has no fields. The first field must be the primary key.");
    const RECORD_SIZE = entity_serial.entitySize(T);
    const KEY_SIZE = entity_serial.fixedSize(fields[0].type, @typeName(T) ++ "." ++ fields[0].name);

    return struct {
        const Self = @This();
        pub const record_size = RECORD_SIZE;
        pub const key_size = KEY_SIZE;
        pub const Entity = T;
        pub const KeyBytes = [KEY_SIZE]u8;

        allocator: std.mem.Allocator,
        file: std.fs.File,

        /// Open `<dir>/<name>` for append + random read. Creates the file
        /// with a fresh magic header on first use; a wrong magic surfaces
        /// `error.InvalidMagic` so the caller can fail loud.
        pub fn openOrCreate(allocator: std.mem.Allocator, dir: std.fs.Dir, name: []const u8) !Self {
            const file = try core.flat_format.openOrCreateWithMagic(dir, name, MAGIC);
            return Self{ .allocator = allocator, .file = file };
        }

        pub fn deinit(self: *Self) void {
            self.file.close();
        }

        /// Append records at byte offset `HEADER_SIZE + at_count * record_size`.
        /// Caller passes `at_count` from `state.snap.immutable_counts[slot]`
        /// so any orphan trailing bytes from a crashed prior commit are
        /// overwritten by the new records.
        pub fn append(self: *Self, records: []const T, at_count: u64) !void {
            if (records.len == 0) return;
            const total = records.len * RECORD_SIZE;
            const buf = try self.allocator.alloc(u8, total);
            defer self.allocator.free(buf);

            var pos: usize = 0;
            for (records) |r| {
                entity_serial.serialize(T, r, buf[pos..][0..RECORD_SIZE]);
                pos += RECORD_SIZE;
            }

            const offset = HEADER_SIZE + at_count * RECORD_SIZE;
            try self.file.pwriteAll(buf, offset);
        }

        pub fn sync(self: *Self) !void {
            try self.file.sync();
        }

        /// Read the record at index `i`. Caller is responsible for ensuring
        /// `i < state.snap.immutable_counts[slot]`; reading past the
        /// authoritative count may return orphan bytes from a crashed
        /// prior commit.
        pub fn read(self: *Self, i: u64) !T {
            const offset = HEADER_SIZE + i * RECORD_SIZE;
            var buf: [RECORD_SIZE]u8 = undefined;
            const n = try self.file.pread(&buf, offset);
            if (n != RECORD_SIZE) return error.Truncated;
            return entity_serial.deserialize(T, &buf);
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

test "openOrCreate on a fresh dir writes the magic header" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var log = try EventLog(TransferEvent).openOrCreate(testing.allocator, tmp.dir, "transfer.events.dat");
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

    var log = try EventLog(TransferEvent).openOrCreate(testing.allocator, tmp.dir, "transfer.events.dat");
    defer log.deinit();

    const recs = [_]TransferEvent{
        .{ .id = keyAt(100, 0), .from = [_]u8{0xAA} ** 20, .to = [_]u8{0xBB} ** 20, .value = 1000 },
        .{ .id = keyAt(100, 1), .from = [_]u8{0xCC} ** 20, .to = [_]u8{0xDD} ** 20, .value = 2000 },
        .{ .id = keyAt(101, 0), .from = [_]u8{0xEE} ** 20, .to = [_]u8{0xFF} ** 20, .value = 3000 },
    };
    try log.append(&recs, 0);

    const r0 = try log.read(0);
    const r2 = try log.read(2);
    try testing.expectEqualSlices(u8, &recs[0].from, &r0.from);
    try testing.expectEqual(@as(u256, 1000), r0.value);
    try testing.expectEqualSlices(u8, &recs[2].to, &r2.to);
    try testing.expectEqual(@as(u256, 3000), r2.value);
}

test "binarySearch finds present keys and returns null for absent" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try EventLog(TransferEvent).openOrCreate(testing.allocator, tmp.dir, "transfer.events.dat");
    defer log.deinit();

    const recs = [_]TransferEvent{
        .{ .id = keyAt(100, 0), .from = [_]u8{0} ** 20, .to = [_]u8{0} ** 20, .value = 1 },
        .{ .id = keyAt(100, 5), .from = [_]u8{0} ** 20, .to = [_]u8{0} ** 20, .value = 2 },
        .{ .id = keyAt(200, 0), .from = [_]u8{0} ** 20, .to = [_]u8{0} ** 20, .value = 3 },
        .{ .id = keyAt(300, 7), .from = [_]u8{0} ** 20, .to = [_]u8{0} ** 20, .value = 4 },
    };
    try log.append(&recs, 0);

    const i = (try log.binarySearch(recs.len, keyAt(200, 0))) orelse return error.NotFound;
    try testing.expectEqual(@as(u64, 2), i);
    try testing.expect((try log.binarySearch(recs.len, keyAt(150, 0))) == null);
    try testing.expect((try log.binarySearch(0, keyAt(100, 0))) == null);
}

test "orphan trailing bytes past count are invisible and overwritten by next append" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try EventLog(TransferEvent).openOrCreate(testing.allocator, tmp.dir, "transfer.events.dat");
    defer log.deinit();

    const good = [_]TransferEvent{
        .{ .id = keyAt(100, 0), .from = [_]u8{0xAA} ** 20, .to = [_]u8{0} ** 20, .value = 1 },
        .{ .id = keyAt(100, 1), .from = [_]u8{0xBB} ** 20, .to = [_]u8{0} ** 20, .value = 2 },
    };
    try log.append(&good, 0);

    const orphan = [_]TransferEvent{
        .{ .id = keyAt(200, 0), .from = [_]u8{0xDE} ** 20, .to = [_]u8{0} ** 20, .value = 999 },
        .{ .id = keyAt(300, 0), .from = [_]u8{0xAD} ** 20, .to = [_]u8{0} ** 20, .value = 999 },
    };
    try log.append(&orphan, good.len);

    // state.snap would still record count=2; the binary search bounded by the
    // authoritative count must NOT see the orphan records.
    try testing.expect((try log.binarySearch(good.len, keyAt(200, 0))) == null);

    // Next legitimate append overwrites the orphans starting at offset 2.
    const fresh = [_]TransferEvent{
        .{ .id = keyAt(150, 0), .from = [_]u8{0xCC} ** 20, .to = [_]u8{0} ** 20, .value = 50 },
    };
    try log.append(&fresh, good.len);

    const found = (try log.binarySearch(good.len + 1, keyAt(150, 0))) orelse return error.NotFound;
    try testing.expectEqual(@as(u64, 2), found);
    const back = try log.read(found);
    try testing.expectEqual(@as(u256, 50), back.value);
}

test "openOrCreate against an existing file validates the magic" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var f = try tmp.dir.createFile("bad.events.dat", .{});
        defer f.close();
        try f.writeAll(&[_]u8{0xFF} ** 8);
    }

    try testing.expectError(error.InvalidMagic, EventLog(TransferEvent).openOrCreate(
        testing.allocator,
        tmp.dir,
        "bad.events.dat",
    ));
}
