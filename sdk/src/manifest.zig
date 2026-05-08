/// User-facing manifest types for `sdk.run` / `sdk.init`.
///
/// Each event type declares `pub const signature = "Name(types,...)";`.
/// The SDK derives `topic0` (keccak-256 of the signature) and the handler
/// method suffix (defaults to the prefix before `(`, override with
/// `pub const name = "...";`).
const std = @import("std");

const eth = @import("eth");

pub const Manifest = struct {
    name: []const u8,
    chain_id: u64,
    start_block: u64,
    /// Optional inclusive upper bound on the scan range. `null` (the
    /// default) scans to the flat store's `latest_block`. Set this to
    /// pin a benchmark or test to a fixed window — e.g., to compare
    /// entity counts byte-for-byte against an external reference that
    /// covers a specific block range.
    end_block: ?u64 = null,
    contracts: []const ContractDef = &.{},
    factories: []const FactoryDef = &.{},
};

pub const ContractDef = struct {
    name: []const u8,
    address: [20]u8,
    events: []const type,
};

/// Where a factory event carries the spawned contract's address. Indexed
/// parameters live in `topics[index + 1]` (index 0 is `topic0`); non-indexed
/// parameters live in `data[index * 32 ..][0..32]`.
pub const AddressParam = union(enum) {
    indexed: usize,
    data: usize,
};

pub const FactoryDef = struct {
    name: []const u8,
    address: [20]u8,
    create_event: type,
    address_param: AddressParam,
    child_events: []const type,
};

/// Comptime-only: eth.zig's runtime xkcp backend does unaligned u64 loads
/// that crash on aarch64, so we route every call through the stdlib keccak
/// path by forcing the body to comptime. The branch-quota bump covers
/// manifests with more events than the default 10000 budget allows.
pub fn eventTopic0(comptime E: type) [32]u8 {
    return comptime blk: {
        @setEvalBranchQuota(200_000);
        break :blk eth.keccak.hash(E.signature);
    };
}

/// Comptime-only: a runtime-evaluated `indexOfScalar` produces a runtime
/// optional, which makes the compiler treat the trailing `@compileError` as
/// reachable and unconditionally fire it. The comptime block forces the
/// optional to be statically known.
pub fn eventName(comptime E: type) []const u8 {
    return comptime blk: {
        if (@hasDecl(E, "name")) break :blk E.name;
        const sig: []const u8 = E.signature;
        const idx = std.mem.indexOfScalar(u8, sig, '(') orelse @compileError(
            "manifest: event '" ++ @typeName(E) ++ "' signature has no '('. Cannot derive name. Add `pub const name = \"...\";` to override.",
        );
        break :blk sig[0..idx];
    };
}

pub fn validateEvent(comptime E: type) void {
    if (!@hasDecl(E, "signature")) @compileError(
        "manifest: event type '" ++ @typeName(E) ++ "' must declare `pub const signature = \"Name(types,...)\";`. The SDK derives topic0 and name from it.",
    );
    _ = comptime eventName(E);
}

pub fn validateManifest(comptime m: Manifest) void {
    inline for (m.contracts) |c| {
        inline for (c.events) |E| validateEvent(E);
    }
    inline for (m.factories) |f| {
        validateEvent(f.create_event);
        inline for (f.child_events) |E| validateEvent(E);
    }
}

/// EVM addresses are right-padded inside their 32-byte word; the trailing
/// 20 bytes are the address.
pub fn extractAddress(topics: []const [32]u8, data: []const u8, address_param: AddressParam) [20]u8 {
    const word: [32]u8 = switch (address_param) {
        .indexed => |i| topics[i + 1],
        .data => |i| data[i * 32 ..][0..32].*,
    };
    return word[12..32].*;
}

/// Topic0-deduplicated flat list of every event referenced by `m`. Used to
/// generate the comptime dispatch table.
pub fn allEvents(comptime m: Manifest) []const type {
    comptime {
        var seen_topics: []const [32]u8 = &.{};
        var out: []const type = &.{};
        for (m.contracts) |c| {
            for (c.events) |E| {
                const t = eventTopic0(E);
                if (containsTopic(seen_topics, t)) continue;
                seen_topics = seen_topics ++ &[_][32]u8{t};
                out = out ++ &[_]type{E};
            }
        }
        for (m.factories) |f| {
            for ([_]type{f.create_event} ++ f.child_events) |E| {
                const t = eventTopic0(E);
                if (containsTopic(seen_topics, t)) continue;
                seen_topics = seen_topics ++ &[_][32]u8{t};
                out = out ++ &[_]type{E};
            }
        }
        return out;
    }
}

fn containsTopic(haystack: []const [32]u8, needle: [32]u8) bool {
    for (haystack) |t| {
        if (std.mem.eql(u8, &t, &needle)) return true;
    }
    return false;
}

// ── Tests ────────────────────────────────────────────────────────────────

const Transfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
    from: [20]u8,
    to: [20]u8,
    value: u256,
};

const Approval = struct {
    pub const signature = "Approval(address,address,uint256)";
    owner: [20]u8,
    spender: [20]u8,
    value: u256,
};

const PairCreated = struct {
    pub const signature = "PairCreated(address,address,address,uint256)";
};

const Sync = struct {
    pub const signature = "Sync(uint112,uint112)";
};

const RenamedTransfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
    pub const name = "Erc721Transfer";
};

test "validateEvent accepts well-formed events" {
    validateEvent(Transfer);
    validateEvent(Approval);
    validateEvent(PairCreated);
}

test "eventName derives from signature prefix" {
    try std.testing.expectEqualStrings("Transfer", eventName(Transfer));
    try std.testing.expectEqualStrings("PairCreated", eventName(PairCreated));
}

test "eventName honors an explicit name override" {
    try std.testing.expectEqualStrings("Erc721Transfer", eventName(RenamedTransfer));
}

test "eventTopic0 hashes the signature" {
    // Hash at comptime to dodge an alignment bug in eth.zig's runtime xkcp
    // backend on aarch64. The SDK only ever calls this at comptime anyway.
    const expected = comptime eth.keccak.hash("Transfer(address,address,uint256)");
    try std.testing.expectEqualSlices(u8, &expected, &eventTopic0(Transfer));
}

test "validateManifest walks contracts and factories" {
    const m = Manifest{
        .name = "test",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{
            .name = "rETH",
            .address = [_]u8{0xAE} ** 20,
            .events = &.{ Transfer, Approval },
        }},
        .factories = &.{.{
            .name = "UniV2",
            .address = [_]u8{0x5C} ** 20,
            .create_event = PairCreated,
            .address_param = .{ .data = 0 },
            .child_events = &.{Sync},
        }},
    };
    validateManifest(m);
}

test "allEvents flattens contracts plus factories without duplicates" {
    const m = Manifest{
        .name = "test",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{
            .{ .name = "A", .address = [_]u8{1} ** 20, .events = &.{Transfer} },
            .{ .name = "B", .address = [_]u8{2} ** 20, .events = &.{ Transfer, Approval } },
        },
        .factories = &.{},
    };
    const events = comptime allEvents(m);
    try std.testing.expectEqual(@as(usize, 2), events.len);
}

test "extractAddress reads indexed topic" {
    var topics: [4][32]u8 = std.mem.zeroes([4][32]u8);
    topics[1][12..32].* = [_]u8{0xAB} ** 20;
    const got = extractAddress(&topics, &.{}, .{ .indexed = 0 });
    try std.testing.expectEqualSlices(u8, &([_]u8{0xAB} ** 20), &got);
}

test "extractAddress reads data field at the right offset" {
    var data: [64]u8 = std.mem.zeroes([64]u8);
    data[32 + 12 ..][0..20].* = [_]u8{0xCD} ** 20;
    const got = extractAddress(&.{}, &data, .{ .data = 1 });
    try std.testing.expectEqualSlices(u8, &([_]u8{0xCD} ** 20), &got);
}
