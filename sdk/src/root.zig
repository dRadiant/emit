/// emit sdk. Library for user indexer projects.
///
/// Public surface: `sdk.run`, `sdk.init`, the manifest types
/// (`Manifest`, `ContractDef`, `FactoryDef`), `DecodedLog`, `Context`,
/// `Options`, `RunStats`, `StorageMode`, `address`, `concat`,
/// `validateHandler`. Everything else is implementation detail.
const std = @import("std");

const eth = @import("eth");

// Private implementation modules. Use the re-exports below instead.
const abi_parse = @import("abi_parse.zig");
const entity_serial = @import("entity_serial.zig");
const entry = @import("entry.zig");
const ethcall = @import("ethcall.zig");
const filter_builder = @import("filter_builder.zig");
const filtered_store = @import("filtered_store.zig");
const humanize = @import("humanize.zig");
const prefetch = @import("prefetch.zig");
const handler = @import("handler.zig");
const immutable_store = @import("immutable_store.zig");
const event_log = @import("event_log.zig");
const mutable_store = @import("mutable_store.zig");
const scanner = @import("scanner.zig");
const state_snap = @import("state_snap.zig");
const tcp_client = @import("tcp_client.zig");

// Public submodule: handler-helper utilities called by name.
pub const manifest = @import("manifest.zig");

// User-facing top-level surface.
pub const AddressSource = manifest.AddressSource;
pub const Amount = humanize.Amount;
pub const amount = humanize.amount;
pub const Context = entry.Context;
pub const DEFAULT_BATCH_SIZE = ethcall.DEFAULT_BATCH_SIZE;
pub const ContractDef = manifest.ContractDef;
pub const DecodedLog = handler.DecodedLog;
pub const EventId = handler.EventId;
pub const Log = handler.Log;
/// Zero-copy view over a decoded dynamic array `T[]` (`.len` / `.at(i)`).
pub const Array = handler.Array;
pub const FactoryDef = manifest.FactoryDef;
pub const init = entry.init;
pub const Manifest = manifest.Manifest;
pub const Options = entry.Options;
pub const RemoteEngine = entry.RemoteEngine;
pub const PrefetchCall = manifest.PrefetchCall;
pub const PrefetchDef = manifest.PrefetchDef;
pub const run = entry.run;
pub const spawn = entry.spawn;
pub const RunStats = entry.RunStats;
pub const printStats = entry.printStats;
pub const StaticCall = manifest.StaticCall;
/// The owning transaction's fields behind `log.tx` (ADR-006), on events
/// declaring `pub const tx_fields = true;`.
pub const Tx = handler.Tx;
/// Leveled CLI output, shared with the engine. Set via `log.setLevel` from the
/// indexer's `--silent` / `--verbose` flags.
pub const log = @import("core").log;

/// Storage mode declared per-entity via `pub const storage: sdk.StorageMode`.
/// `mutable` -> MutableStore (HashMap-fronted, dirty-flag flush, supports
/// `load` / `loadOrInit` / `save`). `immutable` -> ImmutableStore
/// (append-only events.dat, monotonic key invariant, `load` is a `@compileError`).
///
/// No default. Each entity declares its mode explicitly so silent
/// defaulting cannot mask a mutable-vs-append-only design mistake.
pub const StorageMode = enum { mutable, immutable };

/// Parse a 20-byte Ethereum address from hex at compile time.
/// Optional `0x` prefix. The EIP-55 mixed-case checksum is **required** and
/// verified at compile time: a manifest address is a write-once, security-
/// critical literal, so a typo must fail the build rather than silently match
/// no logs. An all-lowercase or all-uppercase form (which makes no checksum
/// claim) is rejected unless it happens to be the checksummed form. The error
/// prints the correct address to paste.
///
/// Manifest call site:
/// `.address = sdk.address("0xae78736Cd615f374D3085123A210448E74Fc6393")`.
pub fn address(comptime hex: []const u8) [20]u8 {
    return comptime blk: {
        const parsed = eth.primitives.addressFromHex(hex) catch |err| @compileError(
            "sdk.address: failed to parse '" ++ hex ++ "': " ++ @errorName(err),
        );

        // checksum is "0x" + 40 hex chars. Compare against the body so the
        // match holds whether or not the input carried a "0x" prefix.
        const body: []const u8 = if (hex.len >= 2 and hex[0] == '0' and (hex[1] == 'x' or hex[1] == 'X'))
            hex[2..]
        else
            hex;
        const checksum = eth.primitives.addressToChecksum(&parsed);
        if (!std.mem.eql(u8, body, checksum[2..])) @compileError(
            "sdk.address: '" ++ hex ++ "' is not EIP-55 checksummed. Use '" ++ checksum ++ "'.",
        );
        break :blk parsed;
    };
}

/// Concatenate a tuple of fixed-size `[N]u8` arrays into one `[total]u8`.
/// Builds composite primary keys without manual `@memcpy`:
///
///     const id = sdk.concat(.{ owner, spender }); // [40]u8
///     try ctx.stores.allowances.save(.{ .id = id, .value = value });
///
/// All parts must be `[N]u8` arrays, checked at comptime. Return length is
/// the sum of the parts' lengths.
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

/// Resolve the store type for entity `T`. `T` must declare
/// `pub const storage: sdk.StorageMode = .mutable | .immutable;`, no
/// default. Used by `Context`, exposed for custom context shapes.
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
/// own `@compileError` for unsupported types at store instantiation.
fn validateEntity(comptime T: type) void {
    const info = @typeInfo(T);
    if (info != .@"struct") @compileError(
        "sdk: entity type '" ++ @typeName(T) ++ "' is not a struct. Entities must be plain data structs whose first field is the primary key.",
    );
    if (info.@"struct".fields.len == 0) @compileError(
        "sdk: entity type '" ++ @typeName(T) ++ "' has no fields. The first field must be the primary key.",
    );
}

/// Comptime check that `Handler` exposes a `handle<EventName>` method for
/// every event in `m`. `sdk.run` runs the same check internally. This
/// helper surfaces the error early (e.g. `comptime { sdk.validateHandler(...) }`)
/// instead of waiting for the full dependency graph to compile.
pub fn validateHandler(comptime m: Manifest, comptime Handler: type) void {
    const D = handler.dispatcherFor(m);
    D.validateHandler(Handler);
}

test {
    _ = abi_parse;
    _ = entity_serial;
    _ = ethcall;
    _ = prefetch;
    _ = immutable_store;
    _ = mutable_store;
    _ = manifest;
    _ = @import("humanize.zig");
    _ = handler;
    _ = filter_builder;
    _ = filtered_store;
    _ = scanner;
    _ = state_snap;
    _ = tcp_client;
    _ = event_log;
    _ = @import("blob_log.zig");
    _ = entry;
    // Test-only fixture.
    _ = @import("testing/fake_engine.zig");
    _ = @import("live.zig");
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

test "address requires the EIP-55 checksum, with or without the 0x prefix" {
    // EIP-55 checksummed (vitalik.eth).
    const a = address("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045");
    try std.testing.expectEqual(@as(u8, 0xd8), a[0]);
    try std.testing.expectEqual(@as(u8, 0x45), a[19]);

    // Same checksum, no prefix.
    const c = address("d8dA6BF26964aF9D7eEd9e03E53415D37aA96045");
    try std.testing.expectEqualSlices(u8, &a, &c);

    // A non-checksummed (all-lowercase) literal is a compile error, covered by
    // test/compile_fail/address_not_checksummed.zig.
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
