/// Comptime serialization helpers shared by MutableStore and ImmutableStore.
///
/// Entities are pure data structs whose fields are integers, fixed-size
/// `[N]u8` arrays, or `[]const T` slices of a fixed-size `T` (variable-length
/// blob fields, ADR-005). Keys (the first field) are big-endian so a
/// sorted-by-bytes slab matches key-numeric order. Fixed value fields are
/// little-endian for native reads. A blob field packs to a fixed `BlobRef`
/// slot in the record, its bytes living out of line in `<entity>.blobs.dat`.
const std = @import("std");

/// Fixed-width reference to a variable-length payload in `<entity>.blobs.dat`.
/// Occupies a blob field's slot in the otherwise fixed-stride record, so the
/// slab stays binary-searchable. `(0, 0)` is the empty slice. `len` is the
/// byte length, the element count of an array blob is `len / @sizeOf(child)`.
/// Stored little-endian like any value field. Bit layout: offset low 40,
/// len high 24.
pub const BlobRef = packed struct(u64) {
    offset: u40, // byte offset into blobs.dat, 1 TB addressable
    len: u24, // byte length, 16 MB max field

    pub const SIZE = 8;
    pub const empty: BlobRef = .{ .offset = 0, .len = 0 };

    pub fn isEmpty(self: BlobRef) bool {
        return self.len == 0;
    }
};

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

/// How a field packs into the fixed record. `fixed` (int or `[N]u8`) inlines.
/// `blob` (a `[]const T` slice of a fixed-size `T`) packs as a `BlobRef`, the
/// bytes out of line. A non-const slice, a slice of a non-fixed element
/// (nested dynamic), or any other type is a loud compile error.
pub const FieldKind = enum { fixed, blob };

pub fn fieldKind(comptime F: type, comptime ctx: []const u8) FieldKind {
    return switch (@typeInfo(F)) {
        .int => .fixed,
        .array => |a| if (a.child == u8) .fixed else @compileError(
            "entity field '" ++ ctx ++ "' is array of '" ++ @typeName(a.child) ++ "'; only [N]u8 arrays are supported",
        ),
        .pointer => |p| blk: {
            if (p.size != .slice or !p.is_const) @compileError(
                "entity field '" ++ ctx ++ "' has type '" ++ @typeName(F) ++ "'; variable fields must be `[]const T` for a fixed-size T",
            );
            // The element must itself be fixed-size. A slice element (nested
            // dynamic) falls through fixedSize's compile error here.
            _ = comptime fixedSize(p.child, ctx ++ "[]");
            break :blk .blob;
        },
        else => @compileError(
            "entity field '" ++ ctx ++ "' has type '" ++ @typeName(F) ++ "'; only ints, [N]u8 arrays, and `[]const T` slices are supported",
        ),
    };
}

/// Bytes a field occupies in the fixed-stride record. A blob field takes a
/// `BlobRef` slot regardless of payload size.
pub fn slotSize(comptime F: type, comptime ctx: []const u8) comptime_int {
    return switch (fieldKind(F, ctx)) {
        .fixed => fixedSize(F, ctx),
        .blob => BlobRef.SIZE,
    };
}

/// True when any field is a blob. Blobless entities take the byte-identical
/// fixed path (`serialize`/`deserialize`), so the 2M-events/s case is
/// untouched.
pub fn hasBlobs(comptime T: type) bool {
    const fields = @typeInfo(T).@"struct".fields;
    inline for (fields) |f| {
        if (fieldKind(f.type, @typeName(T) ++ "." ++ f.name) == .blob) return true;
    }
    return false;
}

/// Comptime entity-shape check the stores run once at instantiation. Forces
/// every field through `fieldKind` (so an unsupported type errors at the
/// entity, not deep in a serialize call) and rejects a blob primary key,
/// which must stay fixed-width for the sorted slab.
pub fn validate(comptime T: type) void {
    const fields = @typeInfo(T).@"struct".fields;
    // Force every field through `fieldKind` so an unsupported type errors at
    // the entity definition.
    inline for (fields) |f| {
        _ = fieldKind(f.type, @typeName(T) ++ "." ++ f.name);
    }
    const key = fields[0];
    if (comptime fieldKind(key.type, @typeName(T) ++ "." ++ key.name) == .blob) @compileError(
        "entity '" ++ @typeName(T) ++ "': the primary key (first field '" ++ key.name ++ "') cannot be a blob; keys stay fixed-width for the sorted slab",
    );
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

/// Total byte size of an entity's serialized record (sum of field slot sizes).
/// A blob field counts its 8-byte `BlobRef` slot, not its payload.
pub fn entitySize(comptime T: type) comptime_int {
    const fields = @typeInfo(T).@"struct".fields;
    var n: usize = 0;
    for (fields) |f| n += slotSize(f.type, @typeName(T) ++ "." ++ f.name);
    return n;
}

/// Pack an entity into `out`. First field (primary key) big-endian so a
/// sorted-by-bytes slab matches key-numeric order, making binary search over
/// serialized records correct without a separate key encoding. Remaining
/// fields little-endian for native reads.
pub fn serialize(comptime T: type, entity: T, out: []u8) void {
    if (comptime hasBlobs(T)) @compileError(
        "entity '" ++ @typeName(T) ++ "' has blob fields; the store must call serializeWithBlobs",
    );
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
    if (comptime hasBlobs(T)) @compileError(
        "entity '" ++ @typeName(T) ++ "' has blob fields; the store must call deserializeWithBlobs",
    );
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

/// Count of blob fields in `T`, the length `serializeWithBlobs` expects for
/// its `refs` slice.
pub fn blobCount(comptime T: type) usize {
    const fields = @typeInfo(T).@"struct".fields;
    var n: usize = 0;
    inline for (fields) |f| {
        if (fieldKind(f.type, @typeName(T) ++ "." ++ f.name) == .blob) n += 1;
    }
    return n;
}

/// Pack a blob-bearing entity. Fixed fields as `serialize`. Blob fields take
/// their resolved `BlobRef` from `refs` in blob-field declaration order, the
/// store having staged the payload and assigned offsets first. `refs.len`
/// must equal `blobCount(T)`.
pub fn serializeWithBlobs(comptime T: type, entity: T, refs: []const BlobRef, out: []u8) void {
    const fields = @typeInfo(T).@"struct".fields;
    var pos: usize = 0;
    var bi: usize = 0;
    inline for (fields, 0..) |f, i| {
        const ctx = @typeName(T) ++ "." ++ f.name;
        switch (comptime fieldKind(f.type, ctx)) {
            .fixed => {
                const sz = comptime fixedSize(f.type, ctx);
                const endian: std.builtin.Endian = if (i == 0) .big else .little;
                writeField(f.type, @field(entity, f.name), out[pos..][0..sz], endian);
                pos += sz;
            },
            .blob => {
                std.mem.writeInt(u64, out[pos..][0..BlobRef.SIZE], @as(u64, @bitCast(refs[bi])), .little);
                pos += BlobRef.SIZE;
                bi += 1;
            },
        }
    }
}

/// Unpack a blob-bearing entity, resolving each `BlobRef` into a slice that
/// borrows `blobs_map`. The returned blob slices are valid only for the
/// lifetime of the mapping (under `ctx.lock()` / a read view on the live path).
pub fn deserializeWithBlobs(comptime T: type, in: []const u8, blobs_map: []const u8) T {
    const fields = @typeInfo(T).@"struct".fields;
    var entity: T = undefined;
    var pos: usize = 0;
    inline for (fields, 0..) |f, i| {
        const ctx = @typeName(T) ++ "." ++ f.name;
        switch (comptime fieldKind(f.type, ctx)) {
            .fixed => {
                const sz = comptime fixedSize(f.type, ctx);
                const endian: std.builtin.Endian = if (i == 0) .big else .little;
                @field(entity, f.name) = readField(f.type, in[pos..][0..sz], endian);
                pos += sz;
            },
            .blob => {
                const ref: BlobRef = @bitCast(std.mem.readInt(u64, in[pos..][0..BlobRef.SIZE], .little));
                @field(entity, f.name) = resolveBlob(f.type, ref, blobs_map);
                pos += BlobRef.SIZE;
            },
        }
    }
    return entity;
}

/// Slice a `BlobRef` out of the blob mapping as the field's `[]const T`. An
/// empty ref yields an empty slice. For `T` wider than `u8` the writer aligned
/// the offset to `@alignOf(T)`, so `base + offset` (base page-aligned) is
/// element-aligned and the cast is sound. Element bytes are host-order, which
/// equals the on-disk little-endian convention on the x86-64 target.
fn resolveBlob(comptime F: type, ref: BlobRef, blobs_map: []const u8) F {
    const Child = @typeInfo(F).pointer.child;
    if (ref.len == 0) return &.{};
    const bytes = blobs_map[ref.offset..][0..ref.len];
    if (Child == u8) return bytes;
    const ptr: [*]const Child = @ptrCast(@alignCast(bytes.ptr));
    return ptr[0 .. ref.len / @sizeOf(Child)];
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

// ── Blob fields (ADR-005) ──────────────────────────────────────────────────

const StringEntity = struct { id: [16]u8, owner: [20]u8, label: []const u8 };
const ArrayEntity = struct { id: u64, members: []const [20]u8 };

test "fieldKind classifies ints, arrays, and slices" {
    try std.testing.expectEqual(FieldKind.fixed, fieldKind(u256, "x"));
    try std.testing.expectEqual(FieldKind.fixed, fieldKind([20]u8, "x"));
    try std.testing.expectEqual(FieldKind.blob, fieldKind([]const u8, "x"));
    try std.testing.expectEqual(FieldKind.blob, fieldKind([]const u64, "x"));
    try std.testing.expectEqual(FieldKind.blob, fieldKind([]const [20]u8, "x"));
}

test "hasBlobs and blobCount reflect declared slice fields" {
    const Numeric = struct { id: [20]u8, balance: u256 };
    try std.testing.expect(!hasBlobs(Numeric));
    try std.testing.expectEqual(@as(usize, 0), blobCount(Numeric));
    try std.testing.expect(hasBlobs(StringEntity));
    try std.testing.expectEqual(@as(usize, 1), blobCount(StringEntity));
}

test "entitySize counts a blob field as an 8-byte slot, not its payload" {
    // id[16] + owner[20] + label BlobRef(8).
    try std.testing.expectEqual(@as(comptime_int, 16 + 20 + 8), entitySize(StringEntity));
}

test "serializeWithBlobs/deserializeWithBlobs roundtrip a string blob" {
    const label = "vitalik.eth";
    // A blobs.dat-shaped mapping: payload at a known offset.
    var blobs: [64]u8 = undefined;
    const off: usize = 8; // past a notional magic header
    @memcpy(blobs[off..][0..label.len], label);

    const e = StringEntity{ .id = [_]u8{0xAB} ** 16, .owner = [_]u8{0xCD} ** 20, .label = label };
    var rec: [entitySize(StringEntity)]u8 = undefined;
    serializeWithBlobs(StringEntity, e, &.{.{ .offset = off, .len = label.len }}, &rec);

    const got = deserializeWithBlobs(StringEntity, &rec, &blobs);
    try std.testing.expectEqualSlices(u8, &e.id, &got.id);
    try std.testing.expectEqualSlices(u8, &e.owner, &got.owner);
    try std.testing.expectEqualSlices(u8, label, got.label);
}

test "empty blob roundtrips to an empty slice" {
    const e = StringEntity{ .id = [_]u8{0} ** 16, .owner = [_]u8{0} ** 20, .label = &.{} };
    var rec: [entitySize(StringEntity)]u8 = undefined;
    serializeWithBlobs(StringEntity, e, &.{BlobRef.empty}, &rec);
    const got = deserializeWithBlobs(StringEntity, &rec, &.{});
    try std.testing.expectEqual(@as(usize, 0), got.label.len);
}

test "array blob resolves to []const T at an aligned offset" {
    const members = [_][20]u8{ [_]u8{0x11} ** 20, [_]u8{0x22} ** 20 };
    // [20]u8 has element alignment 1, so any offset is fine; place it past 8.
    var blobs: [128]u8 align(8) = undefined;
    const off: usize = 8;
    const payload_len = members.len * 20;
    @memcpy(blobs[off..][0..payload_len], std.mem.sliceAsBytes(&members));

    const e = ArrayEntity{ .id = 7, .members = &members };
    var rec: [entitySize(ArrayEntity)]u8 = undefined;
    serializeWithBlobs(ArrayEntity, e, &.{.{ .offset = off, .len = payload_len }}, &rec);

    const got = deserializeWithBlobs(ArrayEntity, &rec, &blobs);
    try std.testing.expectEqual(@as(usize, 2), got.members.len);
    try std.testing.expectEqualSlices(u8, &members[0], &got.members[0]);
    try std.testing.expectEqualSlices(u8, &members[1], &got.members[1]);
}

test "wide-element array blob reads back as aligned []const u64" {
    const vals = [_]u64{ 0x1122334455667788, 0xDEADBEEFCAFEBABE };
    var blobs: [128]u8 align(8) = undefined;
    const off: usize = 16; // 8-aligned, as the writer guarantees for u64
    @memcpy(blobs[off..][0 .. vals.len * 8], std.mem.sliceAsBytes(&vals));

    const E = struct { id: u64, words: []const u64 };
    const e = E{ .id = 1, .words = &vals };
    var rec: [entitySize(E)]u8 = undefined;
    serializeWithBlobs(E, e, &.{.{ .offset = off, .len = vals.len * 8 }}, &rec);

    const got = deserializeWithBlobs(E, &rec, &blobs);
    try std.testing.expectEqualSlices(u64, &vals, got.words);
}

test "validate accepts a fixed key with blob values" {
    validate(StringEntity);
    validate(ArrayEntity);
}
