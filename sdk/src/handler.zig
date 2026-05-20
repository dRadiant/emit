/// Handler-side log shape and comptime topic0 dispatch.
///
/// ## Determinism contract
///
/// Handlers must be pure functions of `(log, prior state)`. The SDK
/// re-dispatches blocks on reorg recovery and on restart against an
/// existing entity store; both rely on a handler producing the same
/// mutations for the same inputs.
///
/// Forbidden inside a handler:
/// - `std.time` / wall-clock reads
/// - randomness (`std.crypto.random`, `std.rand`, anything entropic)
/// - direct HTTP / RPC calls
/// - reads from the engine's pending file or any external state outside
///   `log`, `ctx.stores.*.load*`, and `ctx.ethCall` (which is itself a
///   strict cache read against pre-fetched immutable metadata)
///
/// Block time, if needed, comes from `ctx.timestamp` (derived from
/// `block_number` via `humanize.blockTimestamp` — the 12 s consensus
/// invariant, not the system clock). Anything else breaks replay.
const std = @import("std");

const core = @import("core");

const abi_parse = @import("abi_parse.zig");
const manifest = @import("manifest.zig");

/// View of a single log presented to a handler. `data` borrows from the
/// scanner's per-block decompression buffer and is valid only for the
/// duration of the handler invocation.
///
/// Two decoder layers:
///
/// 1. **Slot-positional**: `indexedAddress(i)`, `dataU256(word)`, etc.
///    For one-off ad-hoc reads or signatures without parameter names.
/// 2. **Name-resolved**: `param(E, "from")` — single helper whose return
///    type is comptime-resolved from `E.signature`. `address` → `[20]u8`,
///    `uintN` → `uN`, `intN` → `iN`, `boolean` → `bool`, `bytesN` → `[N]u8`.
///    Wrong parameter name or unsupported type is a `@compileError`.
pub const DecodedLog = struct {
    block_number: u64,
    tx_index: u16,
    log_index: u16,
    tx_hash: [32]u8,
    address: [20]u8,
    topics: [4][32]u8,
    topic_count: u8,
    data: []const u8,

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

    /// Read the i-th indexed event parameter as an address. Indexed
    /// parameters live in `topics[i + 1]` (topic0 is the event selector).
    /// EVM addresses are right-padded inside their 32-byte word; the
    /// trailing 20 bytes are the address.
    pub fn indexedAddress(self: DecodedLog, i: u8) [20]u8 {
        return self.topics[i + 1][12..32].*;
    }

    /// Read the `word`-th 32-byte slot of `data` as a big-endian u256.
    /// Most ERC20-class events pack each non-indexed parameter in one
    /// word; `dataU256(0)` reads the value from a Transfer or Approval log.
    pub fn dataU256(self: DecodedLog, word: u8) u256 {
        const start = @as(usize, word) * 32;
        return std.mem.readInt(u256, self.data[start..][0..32], .big);
    }

    /// Read the `word`-th 32-byte slot of `data` as an address. EVM
    /// addresses are right-padded inside their 32-byte word; the trailing
    /// 20 bytes are the address. Symmetric with `indexedAddress` for the
    /// non-indexed case (e.g., Uniswap V2 `PairCreated`'s `pair`).
    pub fn dataAddress(self: DecodedLog, word: u8) [20]u8 {
        const start = @as(usize, word) * 32;
        return self.data[start + 12 ..][0..20].*;
    }

    /// Read a named parameter from `E.signature`. The return type is
    /// derived at comptime from the parsed type — `address` → `[20]u8`,
    /// `uintN` → `uN`, `intN` → `iN`, `bool` → `bool`, `bytesN` → `[N]u8`.
    /// Wrong parameter name lists the available params; dynamic types
    /// (`bytes`, `string`) are rejected with a pointer at the slot-positional
    /// helpers.
    pub fn param(self: DecodedLog, comptime E: type, comptime param_name: []const u8) TypeFor(resolveParam(E, param_name).type_str) {
        const p = comptime resolveParam(E, param_name);
        const word: [32]u8 = switch (comptime p.slot_kind) {
            .topic => self.topics[comptime p.slot_index],
            .data => self.data[comptime p.slot_index..][0..32].*,
        };
        return decodeWord(TypeFor(p.type_str), p.type_str, &word);
    }

    /// Decode every named parameter of `E.signature` into one struct.
    /// `ParamsOf(E)` has one field per named parameter with the right Zig
    /// type (see `param` for the type mapping). Unnamed parameters are
    /// skipped. Data slots beyond `self.data.len` are zero-filled (matches
    /// the ABI's zero-padding convention; keeps the dispatcher safe against
    /// malformed RPC responses or test fixtures with short payloads).
    pub fn decode(self: DecodedLog, comptime E: type) ParamsOf(E) {
        var out: ParamsOf(E) = undefined;
        inline for (comptime manifest.parsedEvent(E).params) |p| {
            if (comptime p.name.len == 0) continue;
            const word: [32]u8 = switch (comptime p.slot_kind) {
                .topic => self.topics[comptime p.slot_index],
                .data => if (self.data.len < comptime p.slot_index + 32)
                    std.mem.zeroes([32]u8)
                else
                    self.data[comptime p.slot_index..][0..32].*,
            };
            @field(out, p.name) = decodeWord(TypeFor(p.type_str), p.type_str, &word);
        }
        return out;
    }

    /// Canonical 16-byte event id: `block_number(BE u64) ++ tx_index(BE u32) ++ log_index(BE u32)`.
    /// Big-endian so MDBX byte order matches dispatch order, satisfying
    /// `MDBX_APPEND` for immutable event entities. The same construction
    /// underpins envio's `${block.number}-${logIndex}` string id without
    /// the runtime concat.
    pub fn eventId(self: DecodedLog) [16]u8 {
        return encodeEventId(self.block_number, self.tx_index, self.log_index);
    }
};

fn encodeEventId(block_number: u64, tx_index: u16, log_index: u16) [16]u8 {
    var id: [16]u8 = undefined;
    std.mem.writeInt(u64, id[0..8], block_number, .big);
    std.mem.writeInt(u32, id[8..12], tx_index, .big);
    std.mem.writeInt(u32, id[12..16], log_index, .big);
    return id;
}

// ── Comptime parameter resolution ────────────────────────────────────────

fn resolveParam(comptime E: type, comptime param_name: []const u8) abi_parse.ParsedParam {
    return comptime abi_parse.paramByName(manifest.parsedEvent(E), param_name);
}

/// Map a Solidity type string to the Zig type the decoder returns:
/// `address`→`[20]u8`, `uintN`→`uN`, `intN`→`iN`, `bytesN`→`[N]u8`,
/// `bool`→`bool`. Dynamic types (`bytes`, `string`) are rejected.
fn TypeFor(comptime t: []const u8) type {
    if (comptime std.mem.eql(u8, t, "address")) return [20]u8;
    if (comptime std.mem.eql(u8, t, "bool")) return bool;
    if (comptime std.mem.startsWith(u8, t, "uint")) return std.meta.Int(.unsigned, parseBits(t["uint".len..]));
    if (comptime std.mem.startsWith(u8, t, "int")) return std.meta.Int(.signed, parseBits(t["int".len..]));
    if (comptime std.mem.startsWith(u8, t, "bytes") and t.len > "bytes".len) {
        return [parseBits(t["bytes".len..])]u8;
    }
    @compileError("DecodedLog: type `" ++ t ++ "` not supported by auto-decoder. Use slot-positional helpers.");
}

/// Comptime struct synthesized from `E.signature`: one field per named
/// parameter with the right Zig type (see `TypeFor`). Unnamed parameters
/// are skipped — for fully-positional reads, use `log.param` / `log.dataU256`.
pub fn ParamsOf(comptime E: type) type {
    return comptime blk: {
        var fields: []const std.builtin.Type.StructField = &.{};
        for (manifest.parsedEvent(E).params) |p| {
            if (p.name.len == 0) continue;
            const T = TypeFor(p.type_str);
            // StructField.name needs a sentinel; the parser's slice into the
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

/// Typed log handed to handlers by the dispatcher: same meta shape as
/// `DecodedLog`, plus a comptime-decoded `params: ParamsOf(E)` so handlers
/// read named fields directly (`log.params.from`) instead of routing every
/// access through `log.param(E, "from")`.
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
                .params = d.decode(E),
            };
        }

        pub fn eventId(self: @This()) [16]u8 {
            return encodeEventId(self.block_number, self.tx_index, self.log_index);
        }
    };
}

/// Decode a `RawLog` and route it through the manifest's comptime dispatcher
/// in one step. Backfill (`scanner.replay`) and live mode share this — keeping
/// the per-log path in one place means a future change (e.g. metrics, tracing)
/// lands once.
pub fn dispatchLog(
    comptime m: manifest.Manifest,
    comptime Handler: type,
    ctx: anytype,
    raw_log: core.RawLog,
) !void {
    return dispatcherFor(m).dispatch(Handler, DecodedLog.fromRawLog(raw_log), ctx);
}

/// Build the comptime dispatch table for a manifest. Returns a function
/// type that switches on `log.topics[0]` against each declared event's
/// topic0 and invokes `Handler.handle ++ event.name`. Logs whose topic0
/// matches no declared event are silently skipped.
///
/// `validateHandler(Handler, m)` runs at comptime: any required handler
/// method missing from `Handler` produces a `@compileError` listing the
/// method name and the event signature.
pub fn dispatcherFor(comptime m: manifest.Manifest) type {
    return struct {
        pub fn dispatch(comptime Handler: type, log: DecodedLog, ctx: anytype) !void {
            if (log.topic_count == 0) return;
            const events = comptime manifest.allEvents(m);
            inline for (events) |E| {
                const topic = comptime manifest.eventTopic0(E);
                if (std.mem.eql(u8, &log.topics[0], &topic)) {
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
