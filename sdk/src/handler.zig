/// Handler-side log shape and comptime topic0 dispatch.
const std = @import("std");
const core = @import("core");
const manifest = @import("manifest.zig");

/// View of a single log presented to a handler. `data` borrows from the
/// scanner's per-block decompression buffer and is valid only for the
/// duration of the handler invocation.
///
/// TODO: higher-level decoder helpers (`indexedAddress(i)`,
/// `dataU256(offset)`, comptime-derived per-event typed decoder) live here.
/// concrete usage patterns from examples/erc20 will
/// inform the right shape.
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
};

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
                    return @field(Handler, method_name)(log, ctx);
                }
            }
        }

        pub fn validateHandler(comptime Handler: type) void {
            const events = comptime manifest.allEvents(m);
            inline for (events) |E| {
                const method_name = comptime "handle" ++ manifest.eventName(E);
                if (!@hasDecl(Handler, method_name)) @compileError(
                    "handler: type '" ++ @typeName(Handler) ++ "' is missing method `" ++ method_name ++ "` for event `" ++ E.signature ++ "`. Add `pub fn " ++ method_name ++ "(log: sdk.DecodedLog, ctx: *Ctx) !void { ... }`.",
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

    pub fn handleTransfer(_: DecodedLog, self: *Counter) !void {
        self.transfers += 1;
    }

    pub fn handleApproval(_: DecodedLog, self: *Counter) !void {
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
        pub fn handleTransfer(_: DecodedLog, _: *@This()) !void {
            return error.HandlerFailed;
        }
        pub fn handleApproval(_: DecodedLog, _: *@This()) !void {}
    };
    const D = dispatcherFor(TestManifest);
    var f = Failing{};
    const res = D.dispatch(Failing, makeLog(comptime manifest.eventTopic0(Transfer)), &f);
    try std.testing.expectError(error.HandlerFailed, res);
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
