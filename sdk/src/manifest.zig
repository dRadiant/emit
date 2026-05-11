/// User-facing manifest types for `sdk.run` / `sdk.init`.
///
/// Each event type declares `pub const signature` in either form:
///
///     "Transfer(address,address,uint256)"                                  // bare
///     "Transfer(address indexed from, address indexed to, uint256 value)"  // full
///
/// `abi_parse` produces a canonical form (names + `indexed` stripped,
/// aliases resolved) which is what gets keccak-hashed for topic0. Both
/// forms above hash identically, matching `solc`'s event selector.
///
/// The full form unlocks named arg lookup (`log.argAddress(E, "from")`)
/// and named factory spawn args (`spawn_arg = "pair"`).
const std = @import("std");

const eth = @import("eth");

const abi_parse = @import("abi_parse.zig");

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

pub const FactoryDef = struct {
    name: []const u8,
    address: [20]u8,
    create_event: type,
    /// Name of the arg in `create_event` that carries the spawned address.
    /// Must reference a named `address` arg in the signature — comptime
    /// validation fires `@compileError` listing available args otherwise.
    spawn_arg: []const u8,
    child_events: []const type,
};

/// Parse the event's signature at comptime. Cached per type by the
/// compiler's memoization of comptime calls.
pub fn parsedEvent(comptime E: type) abi_parse.ParsedEvent {
    return comptime abi_parse.parseEvent(E.signature);
}

/// Comptime-only: eth.zig's runtime xkcp backend does unaligned u64 loads
/// that crash on aarch64, so we route every call through the stdlib keccak
/// path by forcing the body to comptime. We hash the canonical form so
/// `"Transfer(address indexed from, …)"` and `"Transfer(address,…)"` both
/// produce the same selector, matching `solc`.
pub fn eventTopic0(comptime E: type) [32]u8 {
    return comptime blk: {
        @setEvalBranchQuota(200_000);
        break :blk eth.keccak.hash(parsedEvent(E).canonical);
    };
}

pub fn eventName(comptime E: type) []const u8 {
    return comptime blk: {
        if (@hasDecl(E, "name")) break :blk E.name;
        break :blk parsedEvent(E).name;
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
        validateSpawnArg(f);
        inline for (f.child_events) |E| validateEvent(E);
    }
}

/// Comptime check that `f.spawn_arg` references a named `address` arg on
/// `f.create_event`'s signature.
fn validateSpawnArg(comptime f: FactoryDef) void {
    comptime {
        const parsed = parsedEvent(f.create_event);
        const p = abi_parse.paramByName(parsed, f.spawn_arg);
        if (!std.mem.eql(u8, p.type_str, "address")) @compileError(
            "manifest: factory `" ++ f.name ++ "` spawn_arg `" ++ f.spawn_arg ++ "` is type `" ++ p.type_str ++ "`, expected `address`",
        );
    }
}

/// Extract the spawned address from a factory log. The slot (topic vs
/// data offset) is comptime-resolved from `f.create_event` and `f.spawn_arg`;
/// the runtime cost is one slice read plus a 20-byte copy.
pub fn extractFactoryAddress(comptime f: FactoryDef, topics: []const [32]u8, data: []const u8) [20]u8 {
    const parsed = comptime parsedEvent(f.create_event);
    const p = comptime abi_parse.paramByName(parsed, f.spawn_arg);
    const word: [32]u8 = switch (comptime p.slot_kind) {
        .topic => topics[comptime p.slot_index],
        .data => data[comptime p.slot_index..][0..32].*,
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
    pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair, uint256 allPairsLength)";
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
            .spawn_arg = "pair",
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

test "extractFactoryAddress reads indexed topic" {
    const Spawned = struct {
        pub const signature = "Spawned(address indexed creator, address child)";
    };
    const f: FactoryDef = .{
        .name = "F",
        .address = [_]u8{0} ** 20,
        .create_event = Spawned,
        .spawn_arg = "creator",
        .child_events = &.{},
    };
    var topics: [4][32]u8 = std.mem.zeroes([4][32]u8);
    topics[1][12..32].* = [_]u8{0xAB} ** 20;
    const got = extractFactoryAddress(f, &topics, &.{});
    try std.testing.expectEqualSlices(u8, &([_]u8{0xAB} ** 20), &got);
}

test "extractFactoryAddress reads non-indexed data slot" {
    const f: FactoryDef = .{
        .name = "UniV2",
        .address = [_]u8{0} ** 20,
        .create_event = PairCreated,
        .spawn_arg = "pair",
        .child_events = &.{},
    };
    var data: [64]u8 = std.mem.zeroes([64]u8);
    data[12..32].* = [_]u8{0xCD} ** 20; // pair is the first data word
    const got = extractFactoryAddress(f, &.{}, &data);
    try std.testing.expectEqualSlices(u8, &([_]u8{0xCD} ** 20), &got);
}

test "topic0 is identical for bare and full-form signatures" {
    const Bare = struct {
        pub const signature = "Transfer(address,address,uint256)";
    };
    const Full = struct {
        pub const signature = "Transfer(address indexed from, address indexed to, uint256 value)";
    };
    const a = comptime eventTopic0(Bare);
    const b = comptime eventTopic0(Full);
    try std.testing.expectEqualSlices(u8, &a, &b);
}
