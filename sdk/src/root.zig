/// emit sdk. Library for user indexer projects.
///
/// Built on top of core (flat-store reading) and lmdbx (entity stores +
/// filtered index).
const std = @import("std");

// Submodules.
pub const append_store = @import("append_store.zig");
pub const block_context = @import("block_context.zig");
pub const cached_store = @import("cached_store.zig");
pub const entity_serial = @import("entity_serial.zig");
pub const handler = @import("handler.zig");
pub const humanize = @import("humanize.zig");
pub const manifest = @import("manifest.zig");

// Top-level re-exports for the user-facing API.
pub const AddressParam = manifest.AddressParam;
pub const AppendError = append_store.AppendError;
pub const AppendStore = append_store.AppendStore;
pub const BlockContext = block_context.BlockContext;
pub const CachedStore = cached_store.CachedStore;
pub const ContractDef = manifest.ContractDef;
pub const DecodedLog = handler.DecodedLog;
pub const FactoryDef = manifest.FactoryDef;
pub const Manifest = manifest.Manifest;

/// Storage-mode marker for a mutable entity. The user passes
/// `sdk.mutable(Account)` in the entities tuple at the `sdk.run()` call
/// site. `BlockContext` reads `marker.Store` to generate a
/// `CachedStore(Account)` field at comptime.
pub fn mutable(comptime T: type) type {
    validateEntity(T);
    return struct {
        pub const Entity = T;
        pub const Store = CachedStore(T);
    };
}

/// Storage-mode marker for an append-only entity. See `mutable` for the
/// usage shape. `Store` resolves to `AppendStore(T)`.
pub fn appendOnly(comptime T: type) type {
    validateEntity(T);
    return struct {
        pub const Entity = T;
        pub const Store = AppendStore(T);
    };
}

/// Comptime check that `T` is a non-empty struct. Per-field type checks
/// (int or `[N]u8` array) are delegated to entity_serial, which fires its
/// own `@compileError` for unsupported types when the store is instantiated.
fn validateEntity(comptime T: type) void {
    const info = @typeInfo(T);
    if (info != .@"struct") @compileError(
        "sdk: entity type '" ++ @typeName(T) ++ "' is not a struct. Entities must be plain data structs whose first field is the primary key.",
    );
    if (info.@"struct".fields.len == 0) @compileError(
        "sdk: entity type '" ++ @typeName(T) ++ "' has no fields. The first field must be the primary key.",
    );
}

/// Comptime check that every element of `entities` is a marker produced by
/// `mutable` or `appendOnly`. A marker is identified by the presence of
/// both `Entity` and `Store` decls.
pub fn validateEntityTuple(comptime entities: anytype) void {
    const E = @TypeOf(entities);
    const info = @typeInfo(E);
    if (info != .@"struct" or !info.@"struct".is_tuple) @compileError(
        "sdk: entities argument must be a tuple of sdk.mutable(T) / sdk.appendOnly(T) markers, got '" ++ @typeName(E) ++ "'",
    );
    inline for (info.@"struct".fields, 0..) |f, i| {
        const Marker = @field(entities, f.name);
        if (@TypeOf(Marker) != type or !@hasDecl(Marker, "Entity") or !@hasDecl(Marker, "Store")) {
            @compileError(std.fmt.comptimePrint(
                "sdk: entities[{d}] is not a marker produced by sdk.mutable() or sdk.appendOnly()",
                .{i},
            ));
        }
    }
}

test {
    _ = entity_serial;
    _ = append_store;
    _ = cached_store;
    _ = manifest;
    _ = humanize;
    _ = handler;
    _ = block_context;
}

test "mutable and appendOnly produce distinct Store aliases" {
    const E = struct { id: [8]u8, value: u64 };
    const Mut = mutable(E);
    const App = appendOnly(E);
    try std.testing.expectEqual(E, Mut.Entity);
    try std.testing.expectEqual(E, App.Entity);
    try std.testing.expectEqual(CachedStore(E), Mut.Store);
    try std.testing.expectEqual(AppendStore(E), App.Store);
}

test "validateEntityTuple accepts a valid mix of markers" {
    const A = struct { id: [20]u8, balance: u256 };
    const B = struct { id: [8]u8, value: u64 };
    validateEntityTuple(.{ mutable(A), appendOnly(B) });
}

test "BlockContext built from mutable + appendOnly exposes typed stores" {
    const A = struct { id: [20]u8, balance: u256 };
    const B = struct { id: [8]u8, value: u64 };
    const Ctx = BlockContext(.{ mutable(A), appendOnly(B) });
    const Stores = std.meta.fieldInfo(Ctx, .stores).type;
    try std.testing.expectEqual(CachedStore(A), @FieldType(Stores, "as"));
    try std.testing.expectEqual(AppendStore(B), @FieldType(Stores, "bs"));
}
