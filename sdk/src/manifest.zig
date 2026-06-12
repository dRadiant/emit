/// User-facing manifest types for `sdk.run` / `sdk.init`.
///
/// Each event type declares `pub const signature` in either form:
///
///     "Transfer(address,address,uint256)"                                  // bare
///     "Transfer(address indexed from, address indexed to, uint256 value)"  // full
///
/// `abi_parse` strips names + `indexed` and resolves aliases to a canonical
/// form. That form is keccak-hashed for topic0, so both spellings produce
/// the same selector as `solc`.
///
/// Full form unlocks named parameter lookup (`log.param(E, "from")`) and
/// named factory spawn parameters (`spawn_param = "pair"`).
const std = @import("std");

const eth = @import("eth");

const abi_parse = @import("abi_parse.zig");

pub const Manifest = struct {
    name: []const u8,
    chain_id: u64,
    start_block: u64,
    /// Optional inclusive upper bound on the scan range. `null` scans to the
    /// flat store's `latest_block`. Pins a run to a fixed window for
    /// byte-for-byte comparison against an external reference.
    end_block: ?u64 = null,
    contracts: []const ContractDef = &.{},
    factories: []const FactoryDef = &.{},
    /// Event-driven eth_call prefetch. Per matching log, queues the declared
    /// `(address, method)` pairs and batches them through Multicall3.
    /// Handlers read cached results via `ctx.ethCall(T, addr, "method()")`.
    prefetch: []const PrefetchDef = &.{},
    /// Address-driven eth_call independent of any event. For canonical
    /// contracts whose metadata handlers reference regardless of log flow
    /// (WETH decimals, a router's factory pointer). One `(address, method)`
    /// pair per entry.
    static_prefetch: []const StaticCall = &.{},
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
    /// Name of the `create_event` parameter carrying the spawned address.
    /// Must reference a named `address` parameter. Comptime validation
    /// fires `@compileError` listing available parameters otherwise.
    spawn_param: []const u8,
    child_events: []const type,
};

/// Per-log target-address source for a `PrefetchCall`. `.log` selects the
/// emitter address. `.param: "name"` resolves a named event parameter via
/// `abi_parse.paramByName` (same machinery as `FactoryDef.spawn_param`).
/// Comptime validation rejects names absent from the signature or not of
/// type `address`.
pub const AddressSource = union(enum) {
    log,
    param: []const u8,
};

/// One declared eth_call. Target address resolves per-log via `address`.
/// `method` is the no-argument Solidity signature whose first four keccak
/// bytes form the call selector.
pub const PrefetchCall = struct {
    address: AddressSource,
    method: []const u8,
};

pub const PrefetchDef = struct {
    on_event: type,
    calls: []const PrefetchCall,
};

/// One eth_call against a fixed address, independent of any event. `method`
/// is the no-argument Solidity signature whose first four keccak bytes form
/// the selector. The same string appears at the handler call site
/// (`ctx.ethCall(T, address, "method()")`) so cache key derivation is
/// unambiguous.
pub const StaticCall = struct {
    address: [20]u8,
    method: []const u8,
};

/// Parse the event's signature at comptime. Memoized per type by the compiler.
pub fn parsedEvent(comptime E: type) abi_parse.ParsedEvent {
    return comptime abi_parse.parseEvent(E.signature);
}

/// Comptime-only. eth.zig's runtime xkcp backend does unaligned u64 loads
/// that crash on aarch64, so forcing the body to comptime routes through the
/// stdlib keccak path. Hashes the canonical form, so
/// `"Transfer(address indexed from, …)"` and `"Transfer(address,…)"` yield
/// the same selector as `solc`.
pub fn eventTopic0(comptime E: type) [32]u8 {
    return comptime blk: {
        // Headroom for callers that resolve many topic0 hashes in one comptime
        // evaluation (the dispatcher gate). @setEvalBranchQuota only raises.
        @setEvalBranchQuota(1_000_000);
        break :blk eth.keccak.hash(parsedEvent(E).canonical);
    };
}

pub fn eventName(comptime E: type) []const u8 {
    return comptime blk: {
        if (@hasDecl(E, "name")) break :blk E.name;
        break :blk parsedEvent(E).name;
    };
}

/// True when the event declares `pub const tx_fields = true;`, opting its
/// handler into `log.tx` (the owning transaction's from/to/value, ADR-006).
/// Declared per event, like an entity's `storage`, so the field exists only
/// where a handler reads it and the compiler rejects the read elsewhere.
pub fn eventWantsTx(comptime E: type) bool {
    return @hasDecl(E, "tx_fields") and E.tx_fields;
}

/// True when any declared event wants tx fields. Turns on the whole carry:
/// the filtered index's tx subtables, the fail-loud `txs.{dat,idx}` coverage
/// check at init, and the remote REGISTER flag.
pub fn wantsTxFields(comptime m: Manifest) bool {
    comptime {
        for (m.contracts) |c| for (c.events) |E| {
            if (eventWantsTx(E)) return true;
        };
        for (m.factories) |f| {
            if (eventWantsTx(f.create_event)) return true;
            for (f.child_events) |E| if (eventWantsTx(E)) return true;
        }
        return false;
    }
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
        validateSpawnParam(f);
        inline for (f.child_events) |E| validateEvent(E);
    }
    inline for (m.prefetch) |d| validatePrefetch(d);
    inline for (m.static_prefetch) |c| validateStaticCall(c);
    validateFactoryDispatch(m);
}

/// Comptime: dispatch routes by topic0 with no per-child factory provenance, so
/// two factories declaring different child events would mis-dispatch a child of
/// one emitting the other's event. Require identical child-event sets. Split
/// distinct protocols into separate manifests instead.
fn validateFactoryDispatch(comptime m: Manifest) void {
    if (comptime m.factories.len < 2) return;
    const first = m.factories[0].child_events;
    inline for (m.factories[1..]) |f| {
        if (comptime !sameTopicSet(first, f.child_events)) @compileError(
            "manifest: factories must declare identical child_events. Split distinct protocols into separate manifests.",
        );
    }
}

/// Comptime: do `a` and `b` cover the same set of event topic0s?
fn sameTopicSet(comptime a: []const type, comptime b: []const type) bool {
    comptime {
        if (a.len != b.len) return false;
        for (a) |E| if (!eventListHas(b, E)) return false;
        return true;
    }
}

fn validateStaticCall(comptime c: StaticCall) void {
    comptime if (c.method.len == 0) @compileError(
        "manifest: static_prefetch entry has an empty method string",
    );
}

/// Comptime check: every `PrefetchCall` in `d` has a non-empty method, and
/// any `.param: name` source resolves to an `address` parameter on
/// `d.on_event`'s signature.
fn validatePrefetch(comptime d: PrefetchDef) void {
    comptime {
        validateEvent(d.on_event);
        for (d.calls) |c| {
            if (c.method.len == 0) @compileError(
                "manifest: prefetch on `" ++ @typeName(d.on_event) ++ "` has an empty method string",
            );
            switch (c.address) {
                .log => {},
                .param => |name| {
                    const parsed = parsedEvent(d.on_event);
                    const p = abi_parse.paramByName(parsed, name);
                    if (!std.mem.eql(u8, p.type_str, "address")) @compileError(
                        "manifest: prefetch on `" ++ @typeName(d.on_event) ++ "` references parameter `" ++ name ++ "` of type `" ++ p.type_str ++ "`, expected `address`",
                    );
                },
            }
        }
    }
}

/// Comptime check: `f.spawn_param` references a named `address` parameter on
/// `f.create_event`'s signature.
fn validateSpawnParam(comptime f: FactoryDef) void {
    comptime {
        const parsed = parsedEvent(f.create_event);
        const p = abi_parse.paramByName(parsed, f.spawn_param);
        if (!std.mem.eql(u8, p.type_str, "address")) @compileError(
            "manifest: factory `" ++ f.name ++ "` spawn_param `" ++ f.spawn_param ++ "` is type `" ++ p.type_str ++ "`, expected `address`",
        );
    }
}

/// Extract the spawned address from a factory log. Thin wrapper over
/// `extractAddress(.param)`, kept for call-site clarity at factory pre-pass.
pub fn extractFactoryAddress(comptime f: FactoryDef, topics: []const [32]u8, data: []const u8) [20]u8 {
    return extractAddress(f.create_event, .{ .param = f.spawn_param }, [_]u8{0} ** 20, topics, data);
}

/// Resolve a `PrefetchCall`'s target address against a matching log.
/// `.log` returns `log_address` directly. `.param: name` resolves the named
/// parameter at comptime via `abi_parse.paramByName`, reads from
/// `topics[slot_index]` or `data[slot_index..][0..32]` per the parser's
/// `slot_kind`, and returns the trailing 20 bytes.
pub fn extractAddress(
    comptime E: type,
    comptime src: AddressSource,
    log_address: [20]u8,
    topics: []const [32]u8,
    data: []const u8,
) [20]u8 {
    return switch (comptime src) {
        .log => log_address,
        .param => |name| blk: {
            const parsed = comptime parsedEvent(E);
            const p = comptime abi_parse.paramByName(parsed, name);
            const word = abi_parse.wordAt(p, topics, data);
            break :blk word[12..32].*;
        },
    };
}

/// Statically known emitter addresses: every contract plus every factory.
/// The historical filter keeps these logs and the live path admits them
/// without consulting the runtime child set. Factory children are discovered
/// at runtime and tracked separately.
pub fn knownAddresses(comptime m: Manifest) []const [20]u8 {
    comptime {
        var out: []const [20]u8 = &.{};
        for (m.contracts) |c| out = out ++ &[_][20]u8{c.address};
        for (m.factories) |f| out = out ++ &[_][20]u8{f.address};
        return out;
    }
}

/// Topic0-deduplicated flat list of every event referenced by `m`. Drives
/// the comptime dispatch table.
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

/// Comptime: does `events` list one whose topic0 equals `E`'s?
fn eventListHas(comptime events: []const type, comptime E: type) bool {
    comptime {
        const t = eventTopic0(E);
        for (events) |X| {
            if (std.mem.eql(u8, &eventTopic0(X), &t)) return true;
        }
        return false;
    }
}

/// Comptime: can a factory legitimately emit `E` (its create-event or a child
/// event)? Then runtime children emit it, so the dispatcher cannot gate it by a
/// comptime address set.
fn factoryEmits(comptime m: Manifest, comptime E: type) bool {
    comptime {
        const t = eventTopic0(E);
        for (m.factories) |f| {
            if (std.mem.eql(u8, &eventTopic0(f.create_event), &t)) return true;
            if (eventListHas(f.child_events, E)) return true;
        }
        return false;
    }
}

/// Comptime: must the dispatcher gate `E` by emitter address? Gate when a
/// statically-known address could reach dispatch for `E` without legitimately
/// emitting it: a static contract that did not declare `E` (it emits `E`
/// cross-product), or, for a static-only event, a factory child (children are
/// arbitrary contracts that can emit any topic, e.g. an LP-token pair emitting
/// Transfer). False for the degenerate manifests (single contract, every
/// contract declaring `E`, or pure factory) so dispatch compiles identically to
/// the un-gated topic0 switch. The flagship ERC20 and single-factory paths pay
/// nothing. `isLegitEmitter` is the runtime predicate.
pub fn dispatchGateNeeded(comptime m: Manifest, comptime E: type) bool {
    comptime {
        var declarers: usize = 0;
        for (m.contracts) |c| {
            if (eventListHas(c.events, E)) declarers += 1;
        }
        if (factoryEmits(m, E)) {
            // Factory event. Children legitimately emit it (the strict
            // multi-factory check keeps child events identical) and pass the
            // gate as non-static addresses. Gate only to reject a static
            // contract that emits it without declaring it.
            return declarers < m.contracts.len;
        }
        // Static-only event. Children never legitimately emit it. Gate when a
        // known static is not a declarer, or any factory exists (a child could
        // cross-emit). `isLegitEmitter` excludes both.
        if (declarers == 0) return false;
        return declarers < m.contracts.len or m.factories.len > 0;
    }
}

/// Did a static contract at `addr` declare `E`? A building block of
/// `isLegitEmitter`, also used directly by the dispatcher gate tests.
pub fn contractDeclaredEvent(comptime m: Manifest, comptime E: type, addr: [20]u8) bool {
    inline for (m.contracts) |c| {
        if (comptime eventListHas(c.events, E)) {
            if (std.mem.eql(u8, &addr, &c.address)) return true;
        }
    }
    return false;
}

/// Comptime: is `addr` a statically-known emitter (a contract or factory
/// address)? Any other address reaching dispatch is a runtime child, since the
/// address pre-filter admits only known statics and discovered children.
fn isKnownStatic(comptime m: Manifest, addr: [20]u8) bool {
    inline for (m.contracts) |c| if (std.mem.eql(u8, &addr, &c.address)) return true;
    inline for (m.factories) |f| if (std.mem.eql(u8, &addr, &f.address)) return true;
    return false;
}

/// Comptime: does any factory list `E` among its child events?
fn factoryHasChildEvent(comptime m: Manifest, comptime E: type) bool {
    comptime {
        for (m.factories) |f| if (eventListHas(f.child_events, E)) return true;
        return false;
    }
}

/// Does `addr` legitimately emit `E`? The dispatcher gate, evaluated only when
/// `dispatchGateNeeded(m, E)`. Legitimate when `addr` is a static contract that
/// declared `E`, the factory whose create-event is `E`, or (for a factory child
/// event) a runtime child, i.e. any address not statically known. Rejects a
/// known static that did not declare `E` (a Swap-less contract emitting Swap)
/// while passing children, with no per-child provenance, because the strict
/// multi-factory check keeps every factory's child events identical.
pub fn isLegitEmitter(comptime m: Manifest, comptime E: type, addr: [20]u8) bool {
    if (contractDeclaredEvent(m, E, addr)) return true;
    inline for (m.factories) |f| {
        if (comptime eventListHas(&[_]type{f.create_event}, E)) {
            if (std.mem.eql(u8, &addr, &f.address)) return true;
        }
    }
    if (comptime factoryHasChildEvent(m, E)) return !isKnownStatic(m, addr);
    return false;
}

/// SHA-256 over fields that affect filtered-index content. Persisted next to
/// the filter dir, triggering a rebuild on mismatch. Without it, manifest
/// edits between runs would replay stale data into new handlers.
pub fn fingerprint(comptime m: Manifest) [32]u8 {
    return comptime blk: {
        @setEvalBranchQuota(200_000);
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hasher.update(m.name);
        hasher.update(std.mem.asBytes(&m.chain_id));
        hasher.update(std.mem.asBytes(&m.start_block));
        const end: u64 = m.end_block orelse std.math.maxInt(u64);
        hasher.update(std.mem.asBytes(&end));
        // Flipping the tx carry changes the filtered-entry layout (tx subtable
        // after the lz4 payload), so it must force a rebuild.
        const wants_tx = wantsTxFields(m);
        hasher.update(std.mem.asBytes(&wants_tx));
        for (m.contracts) |c| {
            hasher.update(c.name);
            hasher.update(&c.address);
            for (c.events) |E| hasher.update(&eventTopic0(E));
        }
        for (m.factories) |f| {
            hasher.update(f.name);
            hasher.update(&f.address);
            hasher.update(&eventTopic0(f.create_event));
            hasher.update(f.spawn_param);
            for (f.child_events) |E| hasher.update(&eventTopic0(E));
        }
        for (m.prefetch) |p| {
            hasher.update(&eventTopic0(p.on_event));
            for (p.calls) |call| {
                hasher.update(call.method);
                switch (call.address) {
                    .log => hasher.update("log"),
                    .param => |name| {
                        hasher.update("param:");
                        hasher.update(name);
                    },
                }
            }
        }
        for (m.static_prefetch) |s| {
            hasher.update(&s.address);
            hasher.update(s.method);
        }
        var out: [32]u8 = undefined;
        hasher.final(&out);
        break :blk out;
    };
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
    // Comptime hash dodges an alignment bug in eth.zig's runtime xkcp
    // backend on aarch64. SDK only ever calls this at comptime anyway.
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
            .spawn_param = "pair",
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
        .spawn_param = "creator",
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
        .spawn_param = "pair",
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

const NamedTransfer = struct {
    pub const signature = "Transfer(address indexed from, address indexed to, uint256 value)";
};

test "validateManifest accepts prefetch and static_prefetch" {
    const m = Manifest{
        .name = "test",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{
            .name = "rETH",
            .address = [_]u8{0xAE} ** 20,
            .events = &.{NamedTransfer},
        }},
        .prefetch = &.{.{
            .on_event = NamedTransfer,
            .calls = &.{
                .{ .address = .log, .method = "decimals()" },
                .{ .address = .{ .param = "from" }, .method = "balanceOf()" },
            },
        }},
        .static_prefetch = &.{
            .{ .address = [_]u8{0xC0} ** 20, .method = "decimals()" },
            .{ .address = [_]u8{0xC0} ** 20, .method = "symbol()" },
            .{ .address = [_]u8{0xC0} ** 20, .method = "name()" },
        },
    };
    validateManifest(m);
}

test "extractAddress returns the emitter for .log" {
    const EMIT = [_]u8{0xDE} ** 20;
    const got = extractAddress(NamedTransfer, .log, EMIT, &.{}, &.{});
    try std.testing.expectEqualSlices(u8, &EMIT, &got);
}

test "extractAddress resolves a named indexed-topic parameter" {
    const FROM = [_]u8{0x11} ** 20;
    var topics: [4][32]u8 = std.mem.zeroes([4][32]u8);
    @memcpy(topics[1][12..32], &FROM);
    const got = extractAddress(NamedTransfer, .{ .param = "from" }, [_]u8{0} ** 20, &topics, &.{});
    try std.testing.expectEqualSlices(u8, &FROM, &got);
}

test "extractAddress resolves a named non-indexed data parameter" {
    const PAIR = [_]u8{0xCD} ** 20;
    var data: [64]u8 = std.mem.zeroes([64]u8);
    @memcpy(data[12..32], &PAIR);
    const got = extractAddress(PairCreated, .{ .param = "pair" }, [_]u8{0} ** 20, &.{}, &data);
    try std.testing.expectEqualSlices(u8, &PAIR, &got);
}

test "StaticCall expresses one (address, method) pair" {
    const c: StaticCall = .{ .address = [_]u8{0xC0} ** 20, .method = "decimals()" };
    try std.testing.expectEqual(@as(usize, 20), c.address.len);
    try std.testing.expectEqualStrings("decimals()", c.method);
}
