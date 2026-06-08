/// Comptime serialization helpers shared by MutableStore and ImmutableStore.
///
/// Entities are pure data structs whose fields are integers or fixed-size
/// `[N]u8` arrays. Keys (the first field) are big-endian so a sorted-by-bytes
/// slab matches key-numeric order. Values are little-endian for native reads.
const std = @import("std");

/// Byte size of a fixed-size entity field. `ctx` is a "Type.field" string
/// for locatable @compileError messages.
pub fn fixedSize(comptime F: type, comptime ctx: []const u8) comptime_int {
    return switch (@typeInfo(F)) {
        .int => @sizeOf(F),
        .array => |a| if (a.child == u8) a.len else @compileError(
            "entity field '" ++ ctx ++ "' is array of '" ++ @typeName(a.child) ++ "'; only [N]u8 arrays are supported",
        ),
        else => @compileError(
            "entity field '" ++ ctx ++ "' has type '" ++ @typeName(F) ++ "'; only ints and fixed-size [N]u8 arrays are supported",
        ),
    };
}

fn writeField(comptime F: type, value: F, out: []u8, endian: std.builtin.Endian) void {
    switch (@typeInfo(F)) {
        .int => std.mem.writeInt(F, out[0..@sizeOf(F)], value, endian),
        .array => @memcpy(out[0..@sizeOf(F)], &value),
        else => unreachable,
    }
}

fn readField(comptime F: type, in: []const u8, endian: std.builtin.Endian) F {
    return switch (@typeInfo(F)) {
        .int => std.mem.readInt(F, in[0..@sizeOf(F)], endian),
        .array => in[0..@sizeOf(F)].*,
        else => unreachable,
    };
}

/// Total byte size of an entity's serialized value (sum of all field sizes).
pub fn entitySize(comptime T: type) comptime_int {
    const fields = @typeInfo(T).@"struct".fields;
    var n: usize = 0;
    for (fields) |f| n += fixedSize(f.type, @typeName(T) ++ "." ++ f.name);
    return n;
}

/// Pack an entity into `out`. First field (primary key) big-endian so a
/// sorted-by-bytes slab matches key-numeric order, making binary search over
/// serialized records correct without a separate key encoding. Remaining
/// fields little-endian for native reads.
pub fn serialize(comptime T: type, entity: T, out: []u8) void {
    const fields = @typeInfo(T).@"struct".fields;
    var pos: usize = 0;
    inline for (fields, 0..) |f, i| {
        const sz = comptime fixedSize(f.type, @typeName(T) ++ "." ++ f.name);
        const endian: std.builtin.Endian = if (i == 0) .big else .little;
        writeField(f.type, @field(entity, f.name), out[pos..][0..sz], endian);
        pos += sz;
    }
}

pub fn deserialize(comptime T: type, in: []const u8) T {
    const fields = @typeInfo(T).@"struct".fields;
    var entity: T = undefined;
    var pos: usize = 0;
    inline for (fields, 0..) |f, i| {
        const sz = comptime fixedSize(f.type, @typeName(T) ++ "." ++ f.name);
        const endian: std.builtin.Endian = if (i == 0) .big else .little;
        @field(entity, f.name) = readField(f.type, in[pos..][0..sz], endian);
        pos += sz;
    }
    return entity;
}

/// Pack only the primary key (the first field) into `out`. Big-endian for
/// integer keys so a sorted-by-bytes layout matches numeric order.
pub fn serializeKey(comptime T: type, entity: T, out: []u8) void {
    const fields = @typeInfo(T).@"struct".fields;
    const KeyField = fields[0].type;
    const sz = comptime fixedSize(KeyField, @typeName(T) ++ "." ++ fields[0].name);
    writeField(KeyField, @field(entity, fields[0].name), out[0..sz], .big);
}

/// Encode a standalone key value (not from an entity). Same byte layout as
/// `serializeKey` for the corresponding entity. Used for cache lookups.
pub fn encodeKey(comptime KeyField: type, key: KeyField, out: []u8) void {
    const sz = comptime fixedSize(KeyField, @typeName(KeyField));
    writeField(KeyField, key, out[0..sz], .big);
}

// ── Tests ────────────────────────────────────────────────────────────────

test "fixedSize: ints and [N]u8 arrays" {
    try std.testing.expectEqual(@as(comptime_int, 1), fixedSize(u8, "x"));
    try std.testing.expectEqual(@as(comptime_int, 8), fixedSize(u64, "x"));
    try std.testing.expectEqual(@as(comptime_int, 32), fixedSize(u256, "x"));
    try std.testing.expectEqual(@as(comptime_int, 20), fixedSize([20]u8, "x"));
}

test "writeField/readField: int roundtrip both endians" {
    var buf: [8]u8 = undefined;
    writeField(u64, 0x1122334455667788, &buf, .little);
    try std.testing.expectEqual(@as(u64, 0x1122334455667788), readField(u64, &buf, .little));
    writeField(u64, 0x1122334455667788, &buf, .big);
    try std.testing.expectEqual(@as(u64, 0x1122334455667788), readField(u64, &buf, .big));
    try std.testing.expectEqual(@as(u8, 0x11), buf[0]);
}

test "writeField/readField: array roundtrip" {
    var buf: [20]u8 = undefined;
    const addr = [_]u8{0xAB} ** 20;
    writeField([20]u8, addr, &buf, .little);
    try std.testing.expectEqualSlices(u8, &addr, &readField([20]u8, &buf, .little));
}

test "entitySize: sum of all fields" {
    const E = struct { id: [8]u8, addr: [20]u8, value: u256, block: u64 };
    try std.testing.expectEqual(@as(comptime_int, 8 + 20 + 32 + 8), entitySize(E));
}

test "serialize/deserialize: full entity roundtrip" {
    const E = struct { id: [8]u8, value: u256, block: u64 };
    const original = E{
        .id = [_]u8{ 0, 0, 0, 1, 0, 0, 0, 7 },
        .value = 0xDEADBEEF_CAFEBABE,
        .block = 18_600_100,
    };
    var buf: [entitySize(E)]u8 = undefined;
    serialize(E, original, &buf);
    const restored = deserialize(E, &buf);
    try std.testing.expectEqualSlices(u8, &original.id, &restored.id);
    try std.testing.expectEqual(original.value, restored.value);
    try std.testing.expectEqual(original.block, restored.block);
}

test "serializeKey: integer first-field is big-endian" {
    const E = struct { id: u64, payload: u32 };
    var key_a: [8]u8 = undefined;
    var key_b: [8]u8 = undefined;
    serializeKey(E, .{ .id = 100, .payload = 0 }, &key_a);
    serializeKey(E, .{ .id = 101, .payload = 0 }, &key_b);
    try std.testing.expect(std.mem.lessThan(u8, &key_a, &key_b));
    try std.testing.expectEqual(@as(u8, 0), key_a[0]);
    try std.testing.expectEqual(@as(u8, 100), key_a[7]);
}

test "serializeKey: array first-field passes through unchanged" {
    const E = struct { id: [20]u8, value: u256 };
    const id = [_]u8{0xAB} ** 20;
    var buf: [20]u8 = undefined;
    serializeKey(E, .{ .id = id, .value = 0 }, &buf);
    try std.testing.expectEqualSlices(u8, &id, &buf);
}

test "encodeKey matches serializeKey for the same key value" {
    const E = struct { id: u64, value: u256 };
    var via_entity: [8]u8 = undefined;
    var via_key: [8]u8 = undefined;
    serializeKey(E, .{ .id = 42, .value = 0 }, &via_entity);
    encodeKey(u64, 42, &via_key);
    try std.testing.expectEqualSlices(u8, &via_entity, &via_key);
}
