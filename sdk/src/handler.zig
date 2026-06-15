/// Handler-side log shape and comptime topic0 dispatch.
///
/// ## Determinism contract
///
/// Handlers must be pure functions of `(log, prior state)`. The SDK
/// re-dispatches blocks on reorg recovery and on restart against an existing
/// entity store. Both rely on a handler producing the same mutations for the
/// same inputs.
///
/// Forbidden inside a handler:
/// - `std.time` / wall-clock reads
/// - randomness (`std.crypto.random`, `std.rand`, anything entropic)
/// - direct HTTP / RPC calls
/// - reads from the engine's pending file or any external state outside
///   `log`, `ctx.stores.*.load*`, and `ctx.ethCall` (a strict cache read
///   against pre-fetched immutable metadata)
///
/// Block time, if needed, comes from `ctx.timestamp` (derived from
/// `block_number` via `humanize.blockTimestamp`, not the system clock).
/// Anything else breaks replay.
const std = @import("std");

const core = @import("core");

const abi_parse = @import("abi_parse.zig");
const manifest = @import("manifest.zig");

/// View of a single log presented to a handler. `data` borrows from the
/// scanner's per-block decompression buffer, valid only for the duration of the
/// handler invocation.
///
/// Two decoder layers:
///
/// 1. **Slot-positional**: `indexedAddress(i)`, `dataU256(word)`, etc.
///    For one-off ad-hoc reads or signatures without parameter names.
/// 2. **Name-resolved**: `param(E, "from")`, return type comptime-resolved from
///    `E.signature`. `address`→`[20]u8`, `uintN`→`uN`, `intN`→`iN`,
///    `boolean`→`bool`, `bytesN`→`[N]u8`. Wrong parameter name or unsupported
///    type is a `@compileError`.
pub const DecodedLog = struct {
    block_number: u64,
    tx_index: u16,
    log_index: u16,
    tx_hash: [32]u8,
    address: [20]u8,
    topics: [4][32]u8,
    topic_count: u8,
    data: []const u8,
    /// Dispatch plumbing: the owning transaction's storage record, resolved
    /// by the replay/live loop. Non-null for every log when the manifest's
    /// events want tx fields (resolution fails loud otherwise), always null
    /// for tx-blind manifests. `Log(E)` decodes it into `Tx` for events
    /// declaring `tx_fields`. Borrowed for the dispatch only.
    tx: ?*const core.txs.TxRecord = null,

    pub fn fromRawLog(raw: core.RawLog) DecodedLog {
        return .{
            .block_number = raw.block_number,
            .tx_index = raw.tx_index,
            .log_index = raw.log_index,
            .tx_hash = raw.tx_hash,
            .address = raw.address,
            .topics = raw.topics,
            .topic_count = raw.topic_count,
            .data = raw.data,
        };
    }

    /// Read the i-th indexed event parameter as an address. Indexed parameters
    /// live in `topics[i + 1]` (topic0 is the event selector). EVM addresses are
    /// right-padded inside their 32-byte word, so the trailing 20 bytes are the
    /// address.
    pub fn indexedAddress(self: DecodedLog, i: u8) [20]u8 {
        return self.topics[i + 1][12..32].*;
    }

    /// Read the `word`-th 32-byte slot of `data` as a big-endian u256.
    /// Most ERC20-class events pack each non-indexed parameter in one word.
    /// `dataU256(0)` reads the value from a Transfer or Approval log.
    pub fn dataU256(self: DecodedLog, word: u8) u256 {
        const start = @as(usize, word) * 32;
        return std.mem.readInt(u256, self.data[start..][0..32], .big);
    }

    /// Read the `word`-th 32-byte slot of `data` as an address. EVM addresses
    /// are right-padded inside their 32-byte word, so the trailing 20 bytes are
    /// the address. Symmetric with `indexedAddress` for the non-indexed case
    /// (e.g. Uniswap V2 `PairCreated`'s `pair`).
    pub fn dataAddress(self: DecodedLog, word: u8) [20]u8 {
        const start = @as(usize, word) * 32;
        return self.data[start + 12 ..][0..20].*;
    }

    /// Read a named parameter from `E.signature`. Return type derived at
    /// comptime: `address`→`[20]u8`, `uintN`→`uN`, `intN`→`iN`, `bool`→`bool`,
    /// `bytesN`→`[N]u8`, dynamic `bytes`/`string`→`[]const u8` borrowing
    /// `log.data`, `T[N]`→`[N]…`, `(t1,…)`→a Zig tuple, `T[]`→a zero-copy
    /// `Array(T)` view borrowing `log.data`. Wrong names list the available
    /// params. An indexed array/tuple/`bytes`/`string` stores `keccak(value)`,
    /// not the value, so those error with a pointer at the raw `log.topics[i]`.
    pub fn param(self: DecodedLog, comptime E: type, comptime param_name: []const u8) TypeFor(resolveParam(E, param_name).type_str) {
        const p = comptime resolveParam(E, param_name);
        if (comptime p.slot_kind == .topic) {
            if (comptime !isTopicPrimitive(p.type_str)) @compileError("DecodedLog.param: indexed `" ++ p.type_str ++ "` stores keccak(value), not the value — read `log.topics[" ++ std.fmt.comptimePrint("{d}", .{p.slot_index}) ++ "]` directly");
            const word = abi_parse.wordAt(p, &self.topics, self.data);
            return decodeWord(TypeFor(p.type_str), p.type_str, &word);
        }
        return decodeValue(p.type_str, self.data, p.slot_index);
    }

    /// Decode every named parameter of `E.signature` into one struct.
    /// `ParamsOf(E)` has one field per named parameter with the right Zig type
    /// (see `param`). Unnamed parameters are skipped. Data slots beyond
    /// `self.data.len` are zero-filled (ABI zero-padding, also guards against
    /// malformed RPC responses or short test fixtures). Errors if any named
    /// param is an indexed non-primitive.
    pub fn decode(self: DecodedLog, comptime E: type) ParamsOf(E) {
        var out: ParamsOf(E) = undefined;
        inline for (comptime manifest.parsedEvent(E).params) |p| {
            if (comptime p.name.len == 0) continue;
            if (comptime p.slot_kind == .topic) {
                if (comptime !isTopicPrimitive(p.type_str)) @compileError("DecodedLog.decode: indexed `" ++ p.type_str ++ "` stores keccak(value); read `log.topics` for it or leave it unnamed");
                const word = abi_parse.wordAt(p, &self.topics, self.data);
                @field(out, p.name) = decodeWord(TypeFor(p.type_str), p.type_str, &word);
            } else {
                @field(out, p.name) = decodeValue(p.type_str, self.data, p.slot_index);
            }
        }
        return out;
    }

    /// Canonical 16-byte event id. Equivalent to `EventId.pack` over this log's
    /// `(block_number, tx_index, log_index)`. See `EventId` for the byte layout
    /// and the inverse `unpack`.
    pub fn eventId(self: DecodedLog) [16]u8 {
        return (EventId{
            .block_number = self.block_number,
            .tx_index = self.tx_index,
            .log_index = self.log_index,
        }).pack();
    }
};

/// Structured view of an event's 16-byte storage key. `pack` and `unpack` are
/// inverses, so `EventId.unpack(log.eventId())` round-trips. The packed form is
/// big-endian, so byte-order sort equals chronological `(block, tx, log)`
/// order, which lets `ImmutableStore.save` append in monotonic-key order.
///
/// `pack` also builds a key from components, making the immutable by-key read
/// usable as `ctx.read(MyEvent, (EventId{ ... }).pack())`.
///
/// Layout: `block_number(BE u64) ++ tx_index(BE u32) ++ log_index(BE u32)`.
/// A log's `tx_index`/`log_index` are u16-ranged at the source and widen into
/// the u32 fields, so the top 16 bits of each are always zero.
pub const EventId = struct {
    block_number: u64,
    tx_index: u32,
    log_index: u32,

    pub fn pack(self: EventId) [16]u8 {
        var id: [16]u8 = undefined;
        std.mem.writeInt(u64, id[0..8], self.block_number, .big);
        std.mem.writeInt(u32, id[8..12], self.tx_index, .big);
        std.mem.writeInt(u32, id[12..16], self.log_index, .big);
        return id;
    }

    pub fn unpack(id: [16]u8) EventId {
        return .{
            .block_number = std.mem.readInt(u64, id[0..8], .big),
            .tx_index = std.mem.readInt(u32, id[8..12], .big),
            .log_index = std.mem.readInt(u32, id[12..16], .big),
        };
    }
};

// ── Comptime parameter resolution ────────────────────────────────────────

fn resolveParam(comptime E: type, comptime param_name: []const u8) abi_parse.ParsedParam {
    return comptime abi_parse.paramByName(manifest.parsedEvent(E), param_name);
}

/// Zig type the decoder returns for an ABI type string. Recurses over the
/// `abi_parse.typeShape` structure: `T[N]`→`[N]TypeFor(T)`, `(t1,…)`→a Zig
/// tuple, `T[]`→an `Array(T)` view. Primitives map via `PrimType`.
fn TypeFor(comptime t: []const u8) type {
    const s = abi_parse.typeShape(t);
    return switch (s.tag) {
        .primitive => PrimType(t),
        .fixed_array => [s.len]TypeFor(s.elem),
        .dynamic_array => Array(s.elem),
        .tuple => TupleType(s.components),
    };
}

/// Primitive Solidity type → Zig type. `address`→`[20]u8`, `uintN`→`uN`,
/// `intN`→`iN`, `bytesN`→`[N]u8`, `bool`→`bool`, dynamic `bytes`/`string`→a
/// `[]const u8` borrowing `log.data` (ADR-005). The borrow is valid for the
/// source log's lifetime; copy it (or `save` it into a blob entity) to keep it.
fn PrimType(comptime t: []const u8) type {
    if (comptime std.mem.eql(u8, t, "address")) return [20]u8;
    if (comptime std.mem.eql(u8, t, "bool")) return bool;
    if (comptime std.mem.eql(u8, t, "bytes") or std.mem.eql(u8, t, "string")) return []const u8;
    if (comptime std.mem.startsWith(u8, t, "uint")) return std.meta.Int(.unsigned, parseBits(t["uint".len..]));
    if (comptime std.mem.startsWith(u8, t, "int")) return std.meta.Int(.signed, parseBits(t["int".len..]));
    if (comptime std.mem.startsWith(u8, t, "bytes") and t.len > "bytes".len) {
        return [parseBits(t["bytes".len..])]u8;
    }
    @compileError("DecodedLog: type `" ++ t ++ "` not auto-decodable. Use slot-positional helpers.");
}

fn TupleType(comptime components: []const []const u8) type {
    comptime var types: [components.len]type = undefined;
    inline for (components, 0..) |c, i| types[i] = TypeFor(c);
    return std.meta.Tuple(&types);
}

/// Zero-copy view over a dynamic array `elem[]` in `log.data`. Borrows the log
/// data, valid only while the source `DecodedLog`/`Log(E)` is. Iterate with
/// `arr.len` and `arr.at(i)`. Element `i` is decoded on access.
pub fn Array(comptime elem: []const u8) type {
    return struct {
        const Self = @This();
        pub const Elem = TypeFor(elem);
        /// The full `log.data` the offsets are relative to.
        data: []const u8,
        /// Byte offset of the array's length word within `data`.
        tail: usize,
        len: usize,

        pub fn at(self: Self, i: usize) Elem {
            const stride = comptime abi_parse.headWords(elem) * 32;
            return decodeValue(elem, self.data, self.tail + 32 + i * stride);
        }
    };
}

/// True if `t` is a static primitive, the only thing an indexed (topic) slot
/// can hold as its actual value. Indexed arrays/tuples/`bytes`/`string` hold
/// `keccak(value)` instead.
fn isTopicPrimitive(comptime t: []const u8) bool {
    return abi_parse.typeShape(t).tag == .primitive and !abi_parse.isDynamicType(t);
}

/// Recursively decode an ABI value of canonical type `t` from `data` whose head
/// word starts at byte `head_off`. Supports any fully-static type (nested
/// arrays/tuples/primitives) and a top-level dynamic array of a static element.
/// Deeper dynamic nesting and `bytes`/`string` values are `@compileError`.
fn decodeValue(comptime t: []const u8, data: []const u8, head_off: usize) TypeFor(t) {
    const s = comptime abi_parse.typeShape(t);
    switch (comptime s.tag) {
        .primitive => {
            if (comptime abi_parse.isDynamicType(t)) {
                // `bytes`/`string`: the head word holds the tail offset; at the
                // tail a length word, then the bytes. The returned slice borrows
                // `data` (`log.data`). A short or malformed payload yields an
                // empty slice rather than reading out of bounds.
                const tail = readOffset(data, head_off);
                const len = readOffset(data, tail);
                const start = tail + 32;
                if (data.len < start + len) return data[0..0];
                return data[start..][0..len];
            }
            const w = wordOf(data, head_off);
            return decodeWord(PrimType(t), t, &w);
        },
        .fixed_array => {
            if (comptime abi_parse.isDynamicType(t)) @compileError("DecodedLog: static array of dynamic elements (`" ++ t ++ "`) not yet supported");
            var out: TypeFor(t) = undefined;
            const stride = comptime abi_parse.headWords(s.elem) * 32;
            inline for (0..s.len) |i| out[i] = decodeValue(s.elem, data, head_off + i * stride);
            return out;
        },
        .tuple => {
            if (comptime abi_parse.isDynamicType(t)) @compileError("DecodedLog: `" ++ t ++ "` is a tuple with a dynamic component; nested dynamics are not yet supported"); // You can read it slot-positionally
            var out: TypeFor(t) = undefined;
            comptime var off: usize = 0;
            inline for (s.components, 0..) |c, i| {
                out[i] = decodeValue(c, data, head_off + off);
                off += comptime abi_parse.headWords(c) * 32;
            }
            return out;
        },
        .dynamic_array => {
            if (comptime abi_parse.isDynamicType(s.elem)) @compileError("DecodedLog: dynamic array of dynamic elements (`" ++ t ++ "`) not yet supported");
            // Head word holds the tail offset (relative to `data`). The word at
            // that offset is the length, then the elements follow.
            const tail = readOffset(data, head_off);
            return TypeFor(t){ .data = data, .tail = tail, .len = readOffset(data, tail) };
        },
    }
}

/// 32-byte word at `off`, zero-filled when `data` is short (ABI zero-padding
/// and the guard against truncated/malformed payloads).
fn wordOf(data: []const u8, off: usize) [32]u8 {
    return if (data.len < off + 32) std.mem.zeroes([32]u8) else data[off..][0..32].*;
}

/// Read the big-endian word at `off` as a byte offset/length (truncated to usize).
fn readOffset(data: []const u8, off: usize) usize {
    const w = wordOf(data, off);
    return @truncate(std.mem.readInt(u256, &w, .big));
}

/// Comptime struct synthesized from `E.signature`: one field per named
/// parameter with the right Zig type (see `TypeFor`). Unnamed parameters are
/// skipped. For fully-positional reads, use `log.param` / `log.dataU256`.
pub fn ParamsOf(comptime E: type) type {
    return comptime blk: {
        var fields: []const std.builtin.Type.StructField = &.{};
        for (manifest.parsedEvent(E).params) |p| {
            if (p.name.len == 0) continue;
            const T = TypeFor(p.type_str);
            // StructField.name needs a sentinel. The parser's slice into the
            // signature has none, so reformat at comptime.
            fields = fields ++ &[_]std.builtin.Type.StructField{.{
                .name = std.fmt.comptimePrint("{s}", .{p.name}),
                .type = T,
                .default_value_ptr = null,
                .is_comptime = false,
                .alignment = @alignOf(T),
            }};
        }
        break :blk @Type(.{ .@"struct" = .{
            .layout = .auto,
            .fields = fields,
            .decls = &.{},
            .is_tuple = false,
        } });
    };
}

fn decodeWord(comptime T: type, comptime type_str: []const u8, word: *const [32]u8) T {
    if (comptime std.mem.eql(u8, type_str, "address")) return word[12..32].*;
    if (comptime std.mem.eql(u8, type_str, "bool")) return word[31] != 0;
    if (comptime std.mem.startsWith(u8, type_str, "uint")) {
        return @truncate(std.mem.readInt(u256, word, .big));
    }
    if (comptime std.mem.startsWith(u8, type_str, "int")) {
        const signed: i256 = @bitCast(std.mem.readInt(u256, word, .big));
        return @truncate(signed);
    }
    if (comptime std.mem.startsWith(u8, type_str, "bytes")) {
        // `bytesN` is left-aligned in the 32-byte word (Solidity ABI §4.1).
        const len = @typeInfo(T).array.len;
        return word[0..len].*;
    }
    unreachable;
}

fn parseBits(comptime s: []const u8) comptime_int {
    var n: comptime_int = 0;
    for (s) |c| n = n * 10 + @as(comptime_int, c - '0');
    return n;
}

/// The owning transaction's fields (ADR-006), decoded for handler reads.
/// Behind `log.tx` on events declaring `pub const tx_fields = true;`.
pub const Tx = struct {
    /// Sender. The zero address marks the rare source-store anomaly where no
    /// sender was stored or recoverable (zero observed on full mainnet). A
    /// real zero-address sender cannot exist, it has no key.
    from: [20]u8,
    /// Null for contract creations.
    to: ?[20]u8,
    value: u256,
    tx_type: u8,

    fn fromRecord(r: *const core.txs.TxRecord) Tx {
        return .{
            .from = r.from,
            .to = if (r.flags & core.txs.FLAG_TO_ABSENT != 0) null else r.to,
            .value = r.valueU256(),
            .tx_type = r.tx_type,
        };
    }
};

/// Typed log handed to handlers by the dispatcher. Same meta shape as
/// `DecodedLog`, plus a comptime-decoded `params: ParamsOf(E)` so handlers read
/// named fields directly (`log.params.from`) instead of routing every access
/// through `log.param(E, "from")`.
///
/// `tx` exists only for events declaring `pub const tx_fields = true;`. The
/// declaration is what turns on the manifest's tx carry, so the field is
/// never null where it compiles. Reading it elsewhere is a compile error
/// (`tx` is `void` there).
pub fn Log(comptime E: type) type {
    return struct {
        block_number: u64,
        tx_index: u16,
        log_index: u16,
        tx_hash: [32]u8,
        address: [20]u8,
        topics: [4][32]u8,
        topic_count: u8,
        data: []const u8,
        tx: if (manifest.eventWantsTx(E)) Tx else void,
        params: ParamsOf(E),

        pub fn fromDecoded(d: DecodedLog) @This() {
            return .{
                .block_number = d.block_number,
                .tx_index = d.tx_index,
                .log_index = d.log_index,
                .tx_hash = d.tx_hash,
                .address = d.address,
                .topics = d.topics,
                .topic_count = d.topic_count,
                .data = d.data,
                // The unwrap is guarded by the carry chain: the declaration
                // implies `wantsTxFields`, so the dispatching loop resolved
                // the record or failed loud before reaching here.
                .tx = if (comptime manifest.eventWantsTx(E)) Tx.fromRecord(d.tx.?) else {},
                .params = d.decode(E),
            };
        }

        pub fn eventId(self: @This()) [16]u8 {
            return (EventId{
                .block_number = self.block_number,
                .tx_index = self.tx_index,
                .log_index = self.log_index,
            }).pack();
        }
    };
}

/// Decode a `RawLog` and route it through the manifest's comptime dispatcher
/// in one step. Backfill (`scanner.replay`) and live mode share this single
/// per-log path.
///
/// Routes by topic0, then applies the comptime per-contract gate so a known
/// static address that did not declare the event never reaches its handler
/// (`dispatchGateNeeded`). The gate refines which declared handler fires, it
/// does not admit addresses. Callers feeding *untrusted* logs (the live path's
/// raw pending blocks) MUST still pre-filter by address. Backfill is safe
/// because `filter_builder` already pruned non-matching addresses before the
/// filtered store was written.
pub fn dispatchLog(
    comptime m: manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    raw_log: core.RawLog,
    tx: ?*const core.txs.TxRecord,
) !void {
    var decoded = DecodedLog.fromRawLog(raw_log);
    decoded.tx = tx;
    return dispatcherFor(m).dispatch(Handler, decoded, ctx);
}

/// Build the comptime dispatch table for a manifest. Returns a type that
/// switches on `log.topics[0]` against each declared event's topic0, applies
/// the per-contract emitter gate (`isLegitEmitter`, comptime-elided unless the
/// manifest needs it), and invokes `Handler.handle ++ event.name`. Logs whose
/// topic0 matches no declared event, or whose emitter the gate rejects, are
/// silently skipped.
///
/// `validateHandler(Handler, m)` runs at comptime. Any required handler method
/// missing from `Handler` produces a `@compileError` listing the method name
/// and the event signature.
pub fn dispatcherFor(comptime m: manifest.Manifest) type {
    return struct {
        pub fn dispatch(comptime Handler: type, log: DecodedLog, ctx: anytype) !void {
            if (log.topic_count == 0) return;
            const events = comptime manifest.allEvents(m);
            inline for (events) |E| {
                const topic = comptime manifest.eventTopic0(E);
                if (std.mem.eql(u8, &log.topics[0], &topic)) {
                    // Per-contract gate, comptime-elided unless a known static
                    // address could reach `E` without declaring it. Degenerate
                    // manifests and both flagship paths (single contract, single
                    // factory) compile to the un-gated switch.
                    if (comptime manifest.dispatchGateNeeded(m, E)) {
                        if (!manifest.isLegitEmitter(m, E, log.address)) return;
                    }
                    const method_name = comptime "handle" ++ manifest.eventName(E);
                    return @field(Handler, method_name)(Log(E).fromDecoded(log), ctx);
                }
            }
        }

        pub fn validateHandler(comptime Handler: type) void {
            const events = comptime manifest.allEvents(m);
            inline for (events) |E| {
                const method_name = comptime "handle" ++ manifest.eventName(E);
                if (!@hasDecl(Handler, method_name)) @compileError(
                    "handler: type '" ++ @typeName(Handler) ++ "' is missing method `" ++ method_name ++ "` for event `" ++ E.signature ++ "`. Add `pub fn " ++ method_name ++ "(log: sdk.Log(@This()), ctx: *Ctx) !void { ... }`.",
                );
            }
        }
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const Transfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
};

const Approval = struct {
    pub const signature = "Approval(address,address,uint256)";
};

const TestManifest: manifest.Manifest = .{
    .name = "test",
    .chain_id = 1,
    .start_block = 0,
    .contracts = &.{.{
        .name = "rETH",
        .address = [_]u8{0xAE} ** 20,
        .events = &.{ Transfer, Approval },
    }},
};

const Counter = struct {
    transfers: u32 = 0,
    approvals: u32 = 0,

    pub fn handleTransfer(_: Log(Transfer), self: *Counter) !void {
        self.transfers += 1;
    }

    pub fn handleApproval(_: Log(Approval), self: *Counter) !void {
        self.approvals += 1;
    }
};

fn makeLog(topic0: [32]u8) DecodedLog {
    var topics: [4][32]u8 = std.mem.zeroes([4][32]u8);
    topics[0] = topic0;
    return .{
        .block_number = 1,
        .tx_index = 0,
        .log_index = 0,
        .tx_hash = [_]u8{0} ** 32,
        .address = [_]u8{0} ** 20,
        .topics = topics,
        .topic_count = 1,
        .data = &.{},
    };
}

fn makeLogFrom(topic0: [32]u8, address: [20]u8) DecodedLog {
    var d = makeLog(topic0);
    d.address = address;
    return d;
}

test "Tx.fromRecord decodes value and maps a creation to null `to`" {
    const plain = core.txs.TxRecord{
        .tx_index = 1,
        .tx_type = 2,
        .flags = 0,
        .from = [_]u8{0x10} ** 20,
        .to = [_]u8{0x20} ** 20,
        .value = [_]u8{ 0xFF, 0x01 } ++ [_]u8{0} ** 30,
    };
    const tx = Tx.fromRecord(&plain);
    try std.testing.expectEqualSlices(u8, &([_]u8{0x10} ** 20), &tx.from);
    try std.testing.expectEqualSlices(u8, &([_]u8{0x20} ** 20), &tx.to.?);
    try std.testing.expectEqual(@as(u256, 0x01FF), tx.value);
    try std.testing.expectEqual(@as(u8, 2), tx.tx_type);

    var creation = plain;
    creation.flags = core.txs.FLAG_TO_ABSENT;
    creation.to = [_]u8{0} ** 20;
    try std.testing.expectEqual(@as(?[20]u8, null), Tx.fromRecord(&creation).to);
}

test "Log(E).tx exists only for events declaring tx_fields" {
    const Declared = struct {
        pub const signature = "Transfer(address,address,uint256)";
        pub const tx_fields = true;
    };
    try std.testing.expectEqual(Tx, @FieldType(Log(Declared), "tx"));
    try std.testing.expectEqual(void, @FieldType(Log(Transfer), "tx"));
}

test "dispatch gates a cross-emitted event to its declaring contract" {
    // Disjoint per-contract events: A declares Transfer, B declares Approval.
    // B emitting a Transfer (an LP token under a Swap-only entry, say) must not
    // reach handleTransfer. Single-contract and homogeneous manifests skip the
    // gate (compiled identically), so this disjoint manifest is the only one
    // that pays for it.
    const A: [20]u8 = [_]u8{0xA1} ** 20;
    const B: [20]u8 = [_]u8{0xB2} ** 20;
    const HetManifest: manifest.Manifest = .{
        .name = "het",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{
            .{ .name = "A", .address = A, .events = &.{Transfer} },
            .{ .name = "B", .address = B, .events = &.{Approval} },
        },
    };
    // Degenerate manifests carry no gate, the disjoint one gates both events.
    try std.testing.expect(comptime !manifest.dispatchGateNeeded(TestManifest, Transfer));
    try std.testing.expect(comptime manifest.dispatchGateNeeded(HetManifest, Transfer));
    try std.testing.expect(comptime manifest.dispatchGateNeeded(HetManifest, Approval));

    const D = dispatcherFor(HetManifest);
    comptime D.validateHandler(Counter);
    const tt = comptime manifest.eventTopic0(Transfer);
    const at = comptime manifest.eventTopic0(Approval);

    var c = Counter{};
    try D.dispatch(Counter, makeLogFrom(tt, A), &c); // A's Transfer, dispatched
    try D.dispatch(Counter, makeLogFrom(tt, B), &c); // B's Transfer, gated out
    try D.dispatch(Counter, makeLogFrom(at, B), &c); // B's Approval, dispatched
    try D.dispatch(Counter, makeLogFrom(at, A), &c); // A's Approval, gated out

    try std.testing.expectEqual(@as(u32, 1), c.transfers);
    try std.testing.expectEqual(@as(u32, 1), c.approvals);
}

test "dispatch passes every declarer of a shared event through the gate" {
    // A and B both declare Transfer, C does not: the gate compiles for
    // Transfer with two declarers and both must pass its unrolled compare
    // chain. The single-declarer tests never run the second compare.
    const A: [20]u8 = [_]u8{0xA1} ** 20;
    const B: [20]u8 = [_]u8{0xB2} ** 20;
    const C: [20]u8 = [_]u8{0xC3} ** 20;
    const TriManifest: manifest.Manifest = .{
        .name = "tri",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{
            .{ .name = "A", .address = A, .events = &.{Transfer} },
            .{ .name = "B", .address = B, .events = &.{Transfer} },
            .{ .name = "C", .address = C, .events = &.{Approval} },
        },
    };
    try std.testing.expect(comptime manifest.dispatchGateNeeded(TriManifest, Transfer));

    const D = dispatcherFor(TriManifest);
    comptime D.validateHandler(Counter);
    const tt = comptime manifest.eventTopic0(Transfer);

    var c = Counter{};
    try D.dispatch(Counter, makeLogFrom(tt, A), &c); // first declarer, dispatched
    try D.dispatch(Counter, makeLogFrom(tt, B), &c); // second declarer, dispatched
    try D.dispatch(Counter, makeLogFrom(tt, C), &c); // non-declarer, gated out

    try std.testing.expectEqual(@as(u32, 2), c.transfers);
}

test "a factory child cannot cross-emit a static-only event" {
    // A token declares Transfer; a factory spawns children. A child is an
    // arbitrary contract that can emit Transfer (an LP-token pair), but no
    // factory declared Transfer, so the dispatcher gates it: the comptime
    // static-declarer check excludes every non-token address, children included.
    const X: [20]u8 = [_]u8{0x11} ** 20;
    const PairCreated = struct {
        pub const signature = "PairCreated(address,address,address,uint256)";
    };
    const Swap = struct {
        pub const signature = "Swap(address,uint256,uint256,uint256,uint256,address)";
    };
    const FacManifest: manifest.Manifest = .{
        .name = "fac",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "X", .address = X, .events = &.{Transfer} }},
        .factories = &.{.{ .name = "F", .address = [_]u8{0x33} ** 20, .create_event = PairCreated, .spawn_param = "pair", .child_events = &.{Swap} }},
    };
    // Transfer (static-only, factory present) gates; the token passes, a child does not.
    try std.testing.expect(comptime manifest.dispatchGateNeeded(FacManifest, Transfer));
    try std.testing.expect(comptime manifest.contractDeclaredEvent(FacManifest, Transfer, X));
    try std.testing.expect(comptime !manifest.contractDeclaredEvent(FacManifest, Transfer, [_]u8{0x22} ** 20));
    // Swap is a factory child event, but a static contract present didn't
    // declare it, so it gates too: the contract is rejected, children pass.
    try std.testing.expect(comptime manifest.dispatchGateNeeded(FacManifest, Swap));
    try std.testing.expect(comptime !manifest.isLegitEmitter(FacManifest, Swap, X));
    try std.testing.expect(comptime manifest.isLegitEmitter(FacManifest, Swap, [_]u8{0x22} ** 20));

    // End to end: the token's stray Swap is gated out, a child's Swap dispatches.
    const HandlerS = struct {
        swaps: u32 = 0,
        pub fn handleTransfer(_: Log(Transfer), _: *@This()) !void {}
        pub fn handleSwap(_: Log(Swap), self: *@This()) !void {
            self.swaps += 1;
        }
        pub fn handlePairCreated(_: Log(PairCreated), _: *@This()) !void {}
    };
    const D = dispatcherFor(FacManifest);
    const st = comptime manifest.eventTopic0(Swap);
    var h = HandlerS{};
    try D.dispatch(HandlerS, makeLogFrom(st, X), &h); // token's Swap, gated out
    try D.dispatch(HandlerS, makeLogFrom(st, [_]u8{0x22} ** 20), &h); // child's Swap, dispatched
    try std.testing.expectEqual(@as(u32, 1), h.swaps);
}

test "dispatch routes by topic0" {
    const D = dispatcherFor(TestManifest);
    comptime D.validateHandler(Counter);

    var counter = Counter{};
    try D.dispatch(Counter, makeLog(comptime manifest.eventTopic0(Transfer)), &counter);
    try D.dispatch(Counter, makeLog(comptime manifest.eventTopic0(Transfer)), &counter);
    try D.dispatch(Counter, makeLog(comptime manifest.eventTopic0(Approval)), &counter);

    try std.testing.expectEqual(@as(u32, 2), counter.transfers);
    try std.testing.expectEqual(@as(u32, 1), counter.approvals);
}

test "dispatch silently skips unknown topic0" {
    const D = dispatcherFor(TestManifest);
    var counter = Counter{};
    const unknown_topic = [_]u8{0xFF} ** 32;
    try D.dispatch(Counter, makeLog(unknown_topic), &counter);
    try std.testing.expectEqual(@as(u32, 0), counter.transfers);
    try std.testing.expectEqual(@as(u32, 0), counter.approvals);
}

test "dispatch skips logs with no topics" {
    const D = dispatcherFor(TestManifest);
    var counter = Counter{};
    var log = makeLog([_]u8{0} ** 32);
    log.topic_count = 0;
    try D.dispatch(Counter, log, &counter);
    try std.testing.expectEqual(@as(u32, 0), counter.transfers);
}

test "dispatch propagates handler errors" {
    const Failing = struct {
        pub fn handleTransfer(_: Log(Transfer), _: *@This()) !void {
            return error.HandlerFailed;
        }
        pub fn handleApproval(_: Log(Approval), _: *@This()) !void {}
    };
    const D = dispatcherFor(TestManifest);
    var f = Failing{};
    const res = D.dispatch(Failing, makeLog(comptime manifest.eventTopic0(Transfer)), &f);
    try std.testing.expectError(error.HandlerFailed, res);
}

test "DecodedLog decoder helpers extract addresses and u256 from data" {
    // Mimic Uniswap V2 PairCreated layout: token0/token1 indexed in topics,
    // pair non-indexed in data[0..32], a uint256 count in data[32..64].
    const TOKEN0 = [_]u8{0xAA} ** 20;
    const TOKEN1 = [_]u8{0xBB} ** 20;
    const PAIR = [_]u8{0xCC} ** 20;

    var topics: [4][32]u8 = std.mem.zeroes([4][32]u8);
    @memcpy(topics[1][12..32], &TOKEN0);
    @memcpy(topics[2][12..32], &TOKEN1);

    var data_buf: [64]u8 = std.mem.zeroes([64]u8);
    @memcpy(data_buf[12..32], &PAIR);
    std.mem.writeInt(u256, data_buf[32..64], 12345, .big);

    const log: DecodedLog = .{
        .block_number = 1,
        .tx_index = 0,
        .log_index = 0,
        .tx_hash = [_]u8{0} ** 32,
        .address = [_]u8{0} ** 20,
        .topics = topics,
        .topic_count = 3,
        .data = &data_buf,
    };

    try std.testing.expectEqualSlices(u8, &TOKEN0, &log.indexedAddress(0));
    try std.testing.expectEqualSlices(u8, &TOKEN1, &log.indexedAddress(1));
    try std.testing.expectEqualSlices(u8, &PAIR, &log.dataAddress(0));
    try std.testing.expectEqual(@as(u256, 12345), log.dataU256(1));
}

test "param resolves indexed addresses and uint256 from data" {
    const NamedTransfer = struct {
        pub const signature = "Transfer(address indexed from, address indexed to, uint256 value)";
    };
    const FROM = [_]u8{0x11} ** 20;
    const TO = [_]u8{0x22} ** 20;

    var topics: [4][32]u8 = std.mem.zeroes([4][32]u8);
    @memcpy(topics[1][12..32], &FROM);
    @memcpy(topics[2][12..32], &TO);

    var data_buf: [32]u8 = std.mem.zeroes([32]u8);
    std.mem.writeInt(u256, &data_buf, 0xdead_beef, .big);

    const log: DecodedLog = .{
        .block_number = 1,
        .tx_index = 0,
        .log_index = 0,
        .tx_hash = [_]u8{0} ** 32,
        .address = [_]u8{0} ** 20,
        .topics = topics,
        .topic_count = 3,
        .data = &data_buf,
    };

    const from = log.param(NamedTransfer, "from");
    const to = log.param(NamedTransfer, "to");
    const value = log.param(NamedTransfer, "value");
    try std.testing.expectEqual([20]u8, @TypeOf(from));
    try std.testing.expectEqual(u256, @TypeOf(value));
    try std.testing.expectEqualSlices(u8, &FROM, &from);
    try std.testing.expectEqualSlices(u8, &TO, &to);
    try std.testing.expectEqual(@as(u256, 0xdead_beef), value);
}

test "param returns the right narrow integer type for sub-256 uintN" {
    // Sync(uint112,uint112) verifies param returns u112, not u256.
    const Sync = struct {
        pub const signature = "Sync(uint112 reserve0, uint112 reserve1)";
    };
    var data_buf: [64]u8 = std.mem.zeroes([64]u8);
    std.mem.writeInt(u256, data_buf[0..32], 0x1234, .big);
    std.mem.writeInt(u256, data_buf[32..64], 0xabcd, .big);

    const log: DecodedLog = .{
        .block_number = 1,
        .tx_index = 0,
        .log_index = 0,
        .tx_hash = [_]u8{0} ** 32,
        .address = [_]u8{0} ** 20,
        .topics = std.mem.zeroes([4][32]u8),
        .topic_count = 1,
        .data = &data_buf,
    };

    const r0 = log.param(Sync, "reserve0");
    const r1 = log.param(Sync, "reserve1");
    try std.testing.expectEqual(u112, @TypeOf(r0));
    try std.testing.expectEqual(u112, @TypeOf(r1));
    try std.testing.expectEqual(@as(u112, 0x1234), r0);
    try std.testing.expectEqual(@as(u112, 0xabcd), r1);
}

test "decode extracts a dynamic string param as a []const u8 borrow (ADR-005)" {
    const NameReg = struct {
        pub const signature = "NameRegistered(string name, uint256 cost)";
    };
    const name = "vitalik.eth";
    // ABI: head[0] = offset to the string tail (past the 2-word head = 0x40),
    // head[1] = cost. Tail: length word, then the bytes (32-byte padded).
    var data_buf: [128]u8 = std.mem.zeroes([128]u8);
    std.mem.writeInt(u256, data_buf[0..32], 0x40, .big);
    std.mem.writeInt(u256, data_buf[32..64], 42, .big);
    std.mem.writeInt(u256, data_buf[64..96], name.len, .big);
    @memcpy(data_buf[96..][0..name.len], name);

    const log: DecodedLog = .{
        .block_number = 1,
        .tx_index = 0,
        .log_index = 0,
        .tx_hash = [_]u8{0} ** 32,
        .address = [_]u8{0} ** 20,
        .topics = std.mem.zeroes([4][32]u8),
        .topic_count = 1,
        .data = &data_buf,
    };

    const d = log.decode(NameReg);
    try std.testing.expectEqual([]const u8, @TypeOf(d.name));
    try std.testing.expectEqualSlices(u8, name, d.name);
    try std.testing.expectEqual(@as(u256, 42), d.cost);
    // The slot-positional helper resolves the same bytes.
    try std.testing.expectEqualSlices(u8, name, log.param(NameReg, "name"));
}

test "decode extracts dynamic bytes and tolerates a truncated tail" {
    const Blob = struct {
        pub const signature = "Blob(bytes payload)";
    };
    // Well-formed: 3-byte payload.
    {
        var data_buf: [96]u8 = std.mem.zeroes([96]u8);
        std.mem.writeInt(u256, data_buf[0..32], 0x20, .big); // offset
        std.mem.writeInt(u256, data_buf[32..64], 3, .big); // length
        data_buf[64..67].* = [_]u8{ 0xDE, 0xAD, 0xBE };
        const log: DecodedLog = .{ .block_number = 0, .tx_index = 0, .log_index = 0, .tx_hash = [_]u8{0} ** 32, .address = [_]u8{0} ** 20, .topics = std.mem.zeroes([4][32]u8), .topic_count = 1, .data = &data_buf };
        try std.testing.expectEqualSlices(u8, &[_]u8{ 0xDE, 0xAD, 0xBE }, log.decode(Blob).payload);
    }
    // Malformed: length claims 99 bytes but data is short. Yields empty, no OOB.
    {
        var data_buf: [64]u8 = std.mem.zeroes([64]u8);
        std.mem.writeInt(u256, data_buf[0..32], 0x20, .big);
        std.mem.writeInt(u256, data_buf[32..64], 99, .big);
        const log: DecodedLog = .{ .block_number = 0, .tx_index = 0, .log_index = 0, .tx_hash = [_]u8{0} ** 32, .address = [_]u8{0} ** 20, .topics = std.mem.zeroes([4][32]u8), .topic_count = 1, .data = &data_buf };
        try std.testing.expectEqual(@as(usize, 0), log.decode(Blob).payload.len);
    }
}

test "decode returns a struct with one field per named parameter, correct types" {
    const Sync = struct {
        pub const signature = "Sync(uint112 reserve0, uint112 reserve1)";
    };
    var data_buf: [64]u8 = std.mem.zeroes([64]u8);
    std.mem.writeInt(u256, data_buf[0..32], 100, .big);
    std.mem.writeInt(u256, data_buf[32..64], 200, .big);

    const log: DecodedLog = .{
        .block_number = 1,
        .tx_index = 0,
        .log_index = 0,
        .tx_hash = [_]u8{0} ** 32,
        .address = [_]u8{0} ** 20,
        .topics = std.mem.zeroes([4][32]u8),
        .topic_count = 1,
        .data = &data_buf,
    };

    const a = log.decode(Sync);
    try std.testing.expectEqual(u112, @TypeOf(a.reserve0));
    try std.testing.expectEqual(u112, @TypeOf(a.reserve1));
    try std.testing.expectEqual(@as(u112, 100), a.reserve0);
    try std.testing.expectEqual(@as(u112, 200), a.reserve1);
}

test "param returns bool and bytesN with the right Zig types" {
    const E = struct {
        pub const signature = "E(bool indexed flag, bytes4 selector, bytes32 hash)";
    };
    var topics: [4][32]u8 = std.mem.zeroes([4][32]u8);
    topics[1][31] = 1;

    var data_buf: [64]u8 = std.mem.zeroes([64]u8);
    data_buf[0..4].* = [_]u8{ 0xAA, 0xBB, 0xCC, 0xDD };
    @memset(data_buf[32..64], 0x77);

    const log: DecodedLog = .{
        .block_number = 1,
        .tx_index = 0,
        .log_index = 0,
        .tx_hash = [_]u8{0} ** 32,
        .address = [_]u8{0} ** 20,
        .topics = topics,
        .topic_count = 2,
        .data = &data_buf,
    };

    const flag = log.param(E, "flag");
    const sel = log.param(E, "selector");
    const hash = log.param(E, "hash");
    try std.testing.expectEqual(bool, @TypeOf(flag));
    try std.testing.expectEqual([4]u8, @TypeOf(sel));
    try std.testing.expectEqual([32]u8, @TypeOf(hash));
    try std.testing.expectEqual(true, flag);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xAA, 0xBB, 0xCC, 0xDD }, &sel);
    try std.testing.expectEqualSlices(u8, &([_]u8{0x77} ** 32), &hash);
}

test "DecodedLog.fromRawLog preserves all fields" {
    const raw = core.RawLog{
        .block_number = 100,
        .tx_index = 5,
        .log_index = 3,
        .tx_hash = [_]u8{0xAA} ** 32,
        .address = [_]u8{0xBB} ** 20,
        .topic_count = 2,
        .topics = .{ [_]u8{0xCC} ** 32, [_]u8{0xDD} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &[_]u8{0xEE} ** 16,
    };
    const log = DecodedLog.fromRawLog(raw);
    try std.testing.expectEqual(@as(u64, 100), log.block_number);
    try std.testing.expectEqual(raw.tx_index, log.tx_index);
    try std.testing.expectEqualSlices(u8, &raw.address, &log.address);
    try std.testing.expectEqualSlices(u8, &raw.topics[0], &log.topics[0]);
    try std.testing.expectEqualSlices(u8, raw.data, log.data);
}

test "EventId pack/unpack round-trips and lays out big-endian" {
    const id = (EventId{ .block_number = 0x0102030405060708, .tx_index = 7, .log_index = 3 }).pack();
    // block_number is the high 8 bytes, big-endian. tx/log are the next two u32 fields.
    try std.testing.expectEqual(@as(u8, 0x01), id[0]);
    try std.testing.expectEqual(@as(u8, 0x08), id[7]);
    try std.testing.expectEqual(@as(u8, 7), id[11]);
    try std.testing.expectEqual(@as(u8, 3), id[15]);

    const back = EventId.unpack(id);
    try std.testing.expectEqual(@as(u64, 0x0102030405060708), back.block_number);
    try std.testing.expectEqual(@as(u32, 7), back.tx_index);
    try std.testing.expectEqual(@as(u32, 3), back.log_index);

    // DecodedLog.eventId() equals packing the log's coordinates.
    const raw = core.RawLog{
        .block_number = 100,
        .tx_index = 5,
        .log_index = 9,
        .tx_hash = [_]u8{0} ** 32,
        .address = [_]u8{0} ** 20,
        .topic_count = 0,
        .topics = .{ [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
    };
    const ev = EventId.unpack(DecodedLog.fromRawLog(raw).eventId());
    try std.testing.expectEqual(@as(u64, 100), ev.block_number);
    try std.testing.expectEqual(@as(u32, 5), ev.tx_index);
    try std.testing.expectEqual(@as(u32, 9), ev.log_index);
}

// ── Array / tuple decode tests ───────────────────────────────────────────

fn dataLog(data: []const u8) DecodedLog {
    return .{
        .block_number = 1,
        .tx_index = 0,
        .log_index = 0,
        .tx_hash = [_]u8{0} ** 32,
        .address = [_]u8{0} ** 20,
        .topics = std.mem.zeroes([4][32]u8),
        .topic_count = 1,
        .data = data,
    };
}

fn wU256(buf: *[32]u8, v: u256) void {
    std.mem.writeInt(u256, buf, v, .big);
}

test "decode a static tuple of (address, uint256)" {
    const E = struct {
        pub const signature = "E((address,uint256) pair)";
    };
    var data: [64]u8 = std.mem.zeroes([64]u8);
    const ADDR = [_]u8{0xAB} ** 20;
    @memcpy(data[12..32], &ADDR); // address left-padded in word 0
    wU256(data[32..64], 0xDEAD);
    const log = dataLog(&data);

    const pair = log.param(E, "pair");
    try std.testing.expectEqualSlices(u8, &ADDR, &pair[0]);
    try std.testing.expectEqual(@as(u256, 0xDEAD), pair[1]);
}

test "decode a static array uint256[3]" {
    const E = struct {
        pub const signature = "E(uint256[3] arr)";
    };
    var data: [96]u8 = std.mem.zeroes([96]u8);
    wU256(data[0..32], 10);
    wU256(data[32..64], 20);
    wU256(data[64..96], 30);

    const arr = dataLog(&data).param(E, "arr");
    try std.testing.expectEqual([3]u256{ 10, 20, 30 }, arr);
}

test "decode a dynamic array uint256[] via the Array view" {
    const E = struct {
        pub const signature = "E(uint256[] amounts)";
    };
    // head: offset 0x20 → tail. tail: len=3, then 3 elements.
    var data: [160]u8 = std.mem.zeroes([160]u8);
    wU256(data[0..32], 0x20);
    wU256(data[32..64], 3);
    wU256(data[64..96], 100);
    wU256(data[96..128], 200);
    wU256(data[128..160], 300);

    const amounts = dataLog(&data).param(E, "amounts");
    try std.testing.expectEqual(@as(usize, 3), amounts.len);
    try std.testing.expectEqual(@as(u256, 100), amounts.at(0));
    try std.testing.expectEqual(@as(u256, 200), amounts.at(1));
    try std.testing.expectEqual(@as(u256, 300), amounts.at(2));
}

test "decode a dynamic array address[] (governance-style)" {
    const E = struct {
        pub const signature = "E(address[] voters)";
    };
    const A1 = [_]u8{0x11} ** 20;
    const A2 = [_]u8{0x22} ** 20;
    var data: [128]u8 = std.mem.zeroes([128]u8);
    wU256(data[0..32], 0x20);
    wU256(data[32..64], 2);
    @memcpy(data[64 + 12 .. 64 + 32], &A1);
    @memcpy(data[96 + 12 .. 96 + 32], &A2);

    const voters = dataLog(&data).param(E, "voters");
    try std.testing.expectEqual(@as(usize, 2), voters.len);
    try std.testing.expectEqualSlices(u8, &A1, &voters.at(0));
    try std.testing.expectEqualSlices(u8, &A2, &voters.at(1));
}

test "mixed: primitive, dynamic array, primitive — slot offsets + tail" {
    const E = struct {
        pub const signature = "E(uint256 a, uint256[] arr, uint256 b)";
    };
    // head: a (off 0), arr offset = 0x60 (off 32), b (off 64). tail at 0x60.
    var data: [192]u8 = std.mem.zeroes([192]u8);
    wU256(data[0..32], 11);
    wU256(data[32..64], 0x60);
    wU256(data[64..96], 22);
    wU256(data[96..128], 2); // arr length
    wU256(data[128..160], 7);
    wU256(data[160..192], 8);

    const d = dataLog(&data).decode(E);
    try std.testing.expectEqual(@as(u256, 11), d.a);
    try std.testing.expectEqual(@as(u256, 22), d.b);
    try std.testing.expectEqual(@as(usize, 2), d.arr.len);
    try std.testing.expectEqual(@as(u256, 7), d.arr.at(0));
    try std.testing.expectEqual(@as(u256, 8), d.arr.at(1));
}
