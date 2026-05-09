/// emit sdk. Library for user indexer projects.
///
/// User code should reach for `sdk.run`, `sdk.init`, the manifest types
/// (`Manifest`, `ContractDef`, `FactoryDef`), `DecodedLog`, `Context`,
/// `Options`, `RunStats`, `StorageMode`, `address`, `concat`, and
/// `validateHandler`. Everything else is implementation detail.
const std = @import("std");

const eth = @import("eth");

// Implementation modules. Kept private so the user-facing surface stays
// small. Reach for the re-exports below instead.
const abi_parse = @import("abi_parse.zig");
const entity_serial = @import("entity_serial.zig");
const entry = @import("entry.zig");
const filter_builder = @import("filter_builder.zig");
const handler = @import("handler.zig");
const immutable_store = @import("immutable_store.zig");
const mutable_store = @import("mutable_store.zig");
const scanner = @import("scanner.zig");

// Public submodules: handler-helper utilities the user calls by name.
pub const humanize = @import("humanize.zig");
pub const manifest = @import("manifest.zig");

// User-facing top-level surface.
pub const Context = entry.Context;
pub const ContractDef = manifest.ContractDef;
pub const DecodedLog = handler.DecodedLog;
pub const FactoryDef = manifest.FactoryDef;
pub const init = entry.init;
pub const Manifest = manifest.Manifest;
pub const Options = entry.Options;
pub const run = entry.run;
pub const RunStats = entry.RunStats;

/// Storage mode declared per-entity via `pub const storage: sdk.StorageMode`.
/// `mutable` → MutableStore (HashMap-fronted, dirty-flag flush, supports
/// `load` / `loadOrInit` / `save`). `immutable` → ImmutableStore
/// (`MDBX_APPEND`, monotonic key invariant, `load` is a `@compileError`).
///
/// Each entity must declare its mode explicitly — there is no default.
/// The choice is a real design decision per entity (mutable counter vs.
/// append-only event log) and silent defaulting would mask mistakes.
pub const StorageMode = enum { mutable, immutable };

/// Parse a 20-byte Ethereum address from its hex string at compile time.
/// Accepts an optional `0x` prefix. If the input contains any uppercase
/// hex digit, it is interpreted as an EIP-55 checksum and rejected at
/// compile time when the checksum doesn't match. All-lowercase or
/// all-uppercase inputs skip checksum validation (consistent with EIP-55,
/// which makes mixed case the validation signal).
///
/// Use at the manifest call site:
/// `.address = sdk.address("0xae78736Cd615f374D3085123A210448E74Fc6393")`.
pub fn address(comptime hex: []const u8) [20]u8 {
    return comptime blk: {
        const parsed = eth.primitives.addressFromHex(hex) catch |err| @compileError(
            "sdk.address: failed to parse '" ++ hex ++ "': " ++ @errorName(err),
        );

        // Detect mixed case (the EIP-55 signal). All-lower or all-upper
        // means the user opted out of the checksum check.
        const body: []const u8 = if (hex.len >= 2 and hex[0] == '0' and (hex[1] == 'x' or hex[1] == 'X'))
            hex[2..]
        else
            hex;
        var has_upper = false;
        var has_lower = false;
        for (body) |c| {
            if (c >= 'a' and c <= 'f') has_lower = true;
            if (c >= 'A' and c <= 'F') has_upper = true;
        }
        if (has_upper and has_lower) {
            const checksum = eth.primitives.addressToChecksum(&parsed);
            // checksum is always "0x" + 40 hex chars; compare against body
            // so the comparison works whether the input had a "0x" prefix.
            if (!std.mem.eql(u8, body, checksum[2..])) @compileError(
                "sdk.address: '" ++ hex ++ "' fails EIP-55 checksum. Expected '" ++ checksum ++ "'.",
            );
        }
        break :blk parsed;
    };
}

/// Concatenate a tuple of fixed-size `[N]u8` arrays into a single
/// `[total]u8`. Use to build composite primary keys without spelling out
/// `@memcpy` calls — e.g.,
///
///     const id = sdk.concat(.{ owner, spender }); // [40]u8
///     try ctx.stores.allowances.save(.{ .id = id, .value = value });
///
/// All parts must be `[N]u8` arrays. Length is checked at comptime; the
/// return type is the sum of the parts' lengths.
pub fn concat(parts: anytype) [concatLen(@TypeOf(parts))]u8 {
    const T = @TypeOf(parts);
    var out: [concatLen(T)]u8 = undefined;
    var off: usize = 0;
    inline for (std.meta.fields(T)) |f| {
        const p = @field(parts, f.name);
        @memcpy(out[off..][0..p.len], &p);
        off += p.len;
    }
    return out;
}

fn concatLen(comptime T: type) comptime_int {
    const info = @typeInfo(T);
    if (info != .@"struct" or !info.@"struct".is_tuple) @compileError(
        "sdk.concat: parts must be a tuple of [N]u8 arrays, got '" ++ @typeName(T) ++ "'",
    );
    var total: comptime_int = 0;
    for (info.@"struct".fields) |f| {
        const ft_info = @typeInfo(f.type);
        if (ft_info != .array or ft_info.array.child != u8) @compileError(
            "sdk.concat: every part must be a [N]u8 array; field '" ++ f.name ++ "' has type '" ++ @typeName(f.type) ++ "'",
        );
        total += ft_info.array.len;
    }
    return total;
}

/// Resolve the store type for entity `T`. The entity must declare
/// `pub const storage: sdk.StorageMode = .mutable | .immutable;` —
/// no default is provided. Used internally by `Context` and exposed for
/// users writing their own context shapes.
pub fn storeFor(comptime T: type) type {
    validateEntity(T);
    if (!@hasDecl(T, "storage")) @compileError(
        "sdk: entity '" ++ @typeName(T) ++ "' must declare `pub const storage: sdk.StorageMode = .mutable;` or `.immutable;`. The choice is per-entity and intentional.",
    );
    const mode: StorageMode = T.storage;
    return switch (mode) {
        .mutable => mutable_store.MutableStore(T),
        .immutable => immutable_store.ImmutableStore(T),
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

/// Comptime check that `Handler` exposes the required `handle<EventName>`
/// methods for every event declared in `m`. `sdk.run` runs the same check
/// internally; this helper lets users surface the error at the top of their
/// build (e.g. in a `comptime { sdk.validateHandler(...) }` block) instead
/// of waiting for the full dependency graph to compile.
pub fn validateHandler(comptime m: Manifest, comptime Handler: type) void {
    const D = handler.dispatcherFor(m);
    D.validateHandler(Handler);
}

test {
    _ = abi_parse;
    _ = entity_serial;
    _ = immutable_store;
    _ = mutable_store;
    _ = manifest;
    _ = humanize;
    _ = handler;
    _ = filter_builder;
    _ = scanner;
    _ = entry;
}

test "concat composes fixed-size byte arrays" {
    const a: [3]u8 = .{ 1, 2, 3 };
    const b: [2]u8 = .{ 4, 5 };
    const c: [4]u8 = .{ 6, 7, 8, 9 };
    const out = concat(.{ a, b, c });
    try std.testing.expectEqual(@as(usize, 9), out.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9 }, &out);
}

test "concat builds owner+spender allowance key" {
    const owner: [20]u8 = [_]u8{0xAA} ** 20;
    const spender: [20]u8 = [_]u8{0xBB} ** 20;
    const key = concat(.{ owner, spender });
    try std.testing.expectEqual(@as(usize, 40), key.len);
    try std.testing.expectEqualSlices(u8, &owner, key[0..20]);
    try std.testing.expectEqualSlices(u8, &spender, key[20..40]);
}

test "address parses lowercase, EIP-55, and rejects bad checksum" {
    // EIP-55 checksum: vitalik.eth.
    const a = address("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045");
    try std.testing.expectEqual(@as(u8, 0xd8), a[0]);
    try std.testing.expectEqual(@as(u8, 0x45), a[19]);

    // All-lowercase: skips the checksum check.
    const b = address("0xd8da6bf26964af9d7eed9e03e53415d37aa96045");
    try std.testing.expectEqualSlices(u8, &a, &b);

    // No-prefix lowercase: also accepted.
    const c = address("d8da6bf26964af9d7eed9e03e53415d37aa96045");
    try std.testing.expectEqualSlices(u8, &a, &c);
}

test "storeFor picks MutableStore vs ImmutableStore by entity.storage" {
    const M = struct {
        pub const storage: StorageMode = .mutable;
        id: [20]u8,
        balance: u256,
    };
    const I = struct {
        pub const storage: StorageMode = .immutable;
        id: [16]u8,
        value: u64,
    };
    try std.testing.expectEqual(mutable_store.MutableStore(M), storeFor(M));
    try std.testing.expectEqual(immutable_store.ImmutableStore(I), storeFor(I));
}

test "Context.stores derives one typed field per entity" {
    const A = struct {
        pub const storage: StorageMode = .mutable;
        id: [20]u8,
        balance: u256,
    };
    const B = struct {
        pub const storage: StorageMode = .immutable;
        id: [8]u8,
        value: u64,
    };
    const Ctx = Context(.{ A, B });
    const Stores = std.meta.fieldInfo(Ctx, .stores).type;
    try std.testing.expectEqual(mutable_store.MutableStore(A), @FieldType(Stores, "as"));
    try std.testing.expectEqual(immutable_store.ImmutableStore(B), @FieldType(Stores, "bs"));
}
