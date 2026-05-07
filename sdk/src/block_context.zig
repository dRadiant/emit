/// BlockContext: comptime-generated handler context. Owns the entity stores
/// for the run, exposes `block_number` / `timestamp` updated by the scanner
/// per dispatched log, and stubs `ethCall` / `registerContract` so we can
/// fill them without breaking handler signatures.
///
/// Method-call syntax (`ctx.ethCall(...)`) requires a written-out struct
/// body. Field types depend on the entities tuple, which is only known at
/// comptime. The shape is therefore split: the outer struct is a written
/// body that holds methods; the inner `stores` field is a `@Type`-generated
/// struct with one field per entity. Handlers reach stores via
/// `ctx.stores.<name>`.
const std = @import("std");
const cached_store = @import("cached_store.zig");
const append_store = @import("append_store.zig");

pub fn BlockContext(comptime entities: anytype) type {
    const Stores = StoresStruct(entities);
    return struct {
        const Self = @This();

        block_number: u64 = 0,
        timestamp: u64 = 0,
        stores: Stores,

        /// Returns `error.NotYetImplemented` until the ethcall MDBX
        /// cache and Multicall3 batching are implemented.
        pub fn ethCall(_: *Self, _: [20]u8, _: []const u8) ![]const u8 {
            return error.NotYetImplemented;
        }

        /// Factory pre-pass discovers child addresses directly from
        /// logs, so this is a stub for the full dynamic-registration
        /// API that will arrive with head following.
        pub fn registerContract(_: *Self, _: [20]u8) !void {
            return error.NotYetImplemented;
        }
    };
}

/// Comptime-build the inner struct that holds one entity store per tuple
/// element. Field names derive from the entity type's basename: lowercase
/// the first byte and append `s`.
fn StoresStruct(comptime entities: anytype) type {
    const E = @TypeOf(entities);
    const info = @typeInfo(E);
    if (info != .@"struct" or !info.@"struct".is_tuple) @compileError(
        "BlockContext: entities must be a tuple of sdk.mutable / sdk.appendOnly markers, got '" ++ @typeName(E) ++ "'",
    );

    const tuple_fields = info.@"struct".fields;

    comptime var derived: [tuple_fields.len][:0]const u8 = undefined;
    inline for (tuple_fields, 0..) |f, i| {
        const Marker = @field(entities, f.name);
        if (@TypeOf(Marker) != type or !@hasDecl(Marker, "Entity") or !@hasDecl(Marker, "Store")) {
            @compileError(std.fmt.comptimePrint(
                "BlockContext: entities[{d}] is not a marker produced by sdk.mutable() / sdk.appendOnly()",
                .{i},
            ));
        }
        derived[i] = entityFieldName(Marker.Entity);
    }

    inline for (derived, 0..) |a, i| {
        if (i + 1 >= derived.len) break;
        inline for (derived[i + 1 ..], i + 1..) |b, j| {
            if (std.mem.eql(u8, a, b)) {
                const Ai = @field(entities, tuple_fields[i].name).Entity;
                const Aj = @field(entities, tuple_fields[j].name).Entity;
                @compileError(std.fmt.comptimePrint(
                    "BlockContext: entity types '{s}' and '{s}' both derive store field name '{s}'. Rename one of the entity types.",
                    .{ @typeName(Ai), @typeName(Aj), a },
                ));
            }
        }
    }

    var struct_fields: [tuple_fields.len]std.builtin.Type.StructField = undefined;
    inline for (tuple_fields, 0..) |f, i| {
        const Marker = @field(entities, f.name);
        struct_fields[i] = .{
            .name = derived[i],
            .type = Marker.Store,
            .default_value_ptr = null,
            .is_comptime = false,
            .alignment = @alignOf(Marker.Store),
        };
    }

    return @Type(.{ .@"struct" = .{
        .layout = .auto,
        .fields = &struct_fields,
        .decls = &.{},
        .is_tuple = false,
    } });
}

fn entityFieldName(comptime T: type) [:0]const u8 {
    return comptime blk: {
        const full = @typeName(T);
        const start = if (std.mem.lastIndexOfScalar(u8, full, '.')) |idx| idx + 1 else 0;
        const basename = full[start..];
        if (basename.len == 0) @compileError(
            "BlockContext: entity type '" ++ full ++ "' has empty basename. Cannot derive store field name.",
        );
        break :blk std.fmt.comptimePrint("{c}{s}s", .{ std.ascii.toLower(basename[0]), basename[1..] });
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const TestAccount = struct { id: [20]u8, balance: u256 };
const TestTransfer = struct { id: [16]u8, from: [20]u8, to: [20]u8, value: u256 };

const TestAccountMutable = struct {
    pub const Entity = TestAccount;
    pub const Store = cached_store.CachedStore(TestAccount);
};

const TestTransferAppend = struct {
    pub const Entity = TestTransfer;
    pub const Store = append_store.AppendStore(TestTransfer);
};

test "entityFieldName lowercases first byte and appends s" {
    try std.testing.expectEqualStrings("testAccounts", entityFieldName(TestAccount));
    try std.testing.expectEqualStrings("testTransfers", entityFieldName(TestTransfer));
}

test "BlockContext exposes block_number, timestamp, and stores" {
    const Ctx = BlockContext(.{ TestAccountMutable, TestTransferAppend });
    try std.testing.expect(@hasField(Ctx, "block_number"));
    try std.testing.expect(@hasField(Ctx, "timestamp"));
    try std.testing.expect(@hasField(Ctx, "stores"));
    try std.testing.expectEqual(u64, std.meta.fieldInfo(Ctx, .block_number).type);
    try std.testing.expectEqual(u64, std.meta.fieldInfo(Ctx, .timestamp).type);
}

test "BlockContext.stores has one typed field per entity" {
    const Ctx = BlockContext(.{ TestAccountMutable, TestTransferAppend });
    const Stores = std.meta.fieldInfo(Ctx, .stores).type;
    try std.testing.expect(@hasField(Stores, "testAccounts"));
    try std.testing.expect(@hasField(Stores, "testTransfers"));
    try std.testing.expectEqual(
        cached_store.CachedStore(TestAccount),
        @FieldType(Stores, "testAccounts"),
    );
    try std.testing.expectEqual(
        append_store.AppendStore(TestTransfer),
        @FieldType(Stores, "testTransfers"),
    );
}

test "ethCall returns NotYetImplemented" {
    const Ctx = BlockContext(.{TestAccountMutable});
    var ctx: Ctx = undefined;
    ctx.block_number = 0;
    ctx.timestamp = 0;
    try std.testing.expectError(error.NotYetImplemented, ctx.ethCall([_]u8{0} ** 20, &.{}));
}

test "registerContract returns NotYetImplemented" {
    const Ctx = BlockContext(.{TestAccountMutable});
    var ctx: Ctx = undefined;
    ctx.block_number = 0;
    ctx.timestamp = 0;
    try std.testing.expectError(error.NotYetImplemented, ctx.registerContract([_]u8{0} ** 20));
}
