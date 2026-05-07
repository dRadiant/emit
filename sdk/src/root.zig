/// emit sdk. Library for user indexer projects.
///
/// User code should reach for `sdk.run`, `sdk.init`, `sdk.mutable`,
/// `sdk.appendOnly`, the manifest types (`Manifest`, `ContractDef`,
/// `FactoryDef`, `AddressParam`), `DecodedLog`, `Context`, `Options`, and
/// `RunStats`. Everything else is implementation detail.
const std = @import("std");

// Implementation modules. Kept private so the user-facing surface stays
// small. Reach for the re-exports below instead.
const append_store = @import("append_store.zig");
const cached_store = @import("cached_store.zig");
const entity_serial = @import("entity_serial.zig");
const entry = @import("entry.zig");
const filter_builder = @import("filter_builder.zig");
const handler = @import("handler.zig");
const scanner = @import("scanner.zig");

// Public submodules: handler-helper utilities the user calls by name.
pub const humanize = @import("humanize.zig");
pub const manifest = @import("manifest.zig");

// User-facing top-level surface.
pub const AddressParam = manifest.AddressParam;
pub const Context = entry.Context;
pub const ContractDef = manifest.ContractDef;
pub const DecodedLog = handler.DecodedLog;
pub const FactoryDef = manifest.FactoryDef;
pub const init = entry.init;
pub const Manifest = manifest.Manifest;
pub const Options = entry.Options;
pub const run = entry.run;
pub const RunStats = entry.RunStats;

/// Storage-mode marker for a mutable entity. Pass `sdk.mutable(Account)`
/// in the entities tuple at the `sdk.run` call site; the SDK reads
/// `marker.Store` to generate the `Context.stores.<name>` field at
/// comptime.
pub fn mutable(comptime T: type) type {
    validateEntity(T);
    return struct {
        pub const Entity = T;
        pub const Store = cached_store.CachedStore(T);
    };
}

/// Storage-mode marker for an append-only entity. See `mutable` for the
/// usage shape. `Store` resolves to `AppendStore(T)` (writes use
/// `MDBX_APPEND`; `load` is a `@compileError`).
pub fn appendOnly(comptime T: type) type {
    validateEntity(T);
    return struct {
        pub const Entity = T;
        pub const Store = append_store.AppendStore(T);
    };
}

/// Comptime check that `Handler` exposes the required `handle<EventName>`
/// methods for every event declared in `m`. `sdk.run` runs the same check
/// internally; this helper lets users surface the error at the top of their
/// build (e.g. in a `comptime { sdk.validateHandler(...) }` block) instead
/// of waiting for the full dependency graph to compile.
pub fn validateHandler(comptime m: Manifest, comptime Handler: type) void {
    const D = handler.dispatcherFor(m);
    D.validateHandler(Handler);
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

test {
    _ = entity_serial;
    _ = append_store;
    _ = cached_store;
    _ = manifest;
    _ = humanize;
    _ = handler;
    _ = filter_builder;
    _ = scanner;
    _ = entry;
}

test "mutable and appendOnly produce distinct Store aliases" {
    const E = struct { id: [8]u8, value: u64 };
    const Mut = mutable(E);
    const App = appendOnly(E);
    try std.testing.expectEqual(E, Mut.Entity);
    try std.testing.expectEqual(E, App.Entity);
    try std.testing.expectEqual(cached_store.CachedStore(E), Mut.Store);
    try std.testing.expectEqual(append_store.AppendStore(E), App.Store);
}

test "Context.stores derives one typed field per entity" {
    const A = struct { id: [20]u8, balance: u256 };
    const B = struct { id: [8]u8, value: u64 };
    const Ctx = Context(.{ mutable(A), appendOnly(B) });
    const Stores = std.meta.fieldInfo(Ctx, .stores).type;
    try std.testing.expectEqual(cached_store.CachedStore(A), @FieldType(Stores, "as"));
    try std.testing.expectEqual(append_store.AppendStore(B), @FieldType(Stores, "bs"));
}
