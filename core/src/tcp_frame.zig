//! Remote-engine streaming wire protocol
//!
//! Pure bytes↔structs: the socket read/write loop lives in the engine
//! server and the SDK client, so this module has no I/O dependency
//!
//! Frame on the wire: `[type u8][length u32 LE][payload, length bytes]`.
//! Decoders that return slices (`Register.addresses`, `Push.lz4_entry`, …) are
//! views into the payload buffer - keep alive for the struct's lifetime.
const std = @import("std");

pub const PROTOCOL_VERSION: u32 = 1;
pub const HEADER_SIZE: usize = 5;
/// Upper bound on a single payload, so a hostile or corrupt length word can't
/// drive an unbounded allocation. A backfill `PUSH` is one block's filtered
/// logs
pub const MAX_PAYLOAD: u32 = 16 * 1024 * 1024;

pub const FrameType = enum(u8) {
    register = 1, // client→engine: filter (addresses, topics) + cursor
    push = 2, // engine→client: one server-side-filtered matched block
    add_address = 3, // client→engine: add a live-discovered factory child + mini-backfill from its creation block
    reorg = 4, // engine→client: fork point; client truncates its pending snapshot
    heartbeat = 5, // bidirectional: cursor, tip, last_finalized
    goaway = 6, // engine→client: graceful close / version mismatch / overload
    // Exhaustive: an unknown type is a protocol error (versions are negotiated
    // via REGISTER + GOAWAY, so peers never send each other unknown frames).
};

pub const Error = error{ InvalidFrameType, PayloadTooLarge, Truncated };

/// 5-byte frame header for a payload of `len`. The caller writes the header
/// then the payload to the socket (two `writeAll`s — no concatenation).
pub fn header(t: FrameType, len: u32) [HEADER_SIZE]u8 {
    var h: [HEADER_SIZE]u8 = undefined;
    h[0] = @intFromEnum(t);
    std.mem.writeInt(u32, h[1..5], len, .little);
    return h;
}

pub const Header = struct { type: FrameType, len: u32 };

/// Parse a header, rejecting unknown types and oversized lengths so a bad
/// stream fails before the payload is allocated.
pub fn parseHeader(buf: []const u8) Error!Header {
    if (buf.len < HEADER_SIZE) return error.Truncated;
    const len = std.mem.readInt(u32, buf[1..5], .little);
    if (len > MAX_PAYLOAD) return error.PayloadTooLarge;
    const t = std.meta.intToEnum(FrameType, buf[0]) catch return error.InvalidFrameType;
    return .{ .type = t, .len = len };
}

// ── REGISTER (client → engine) ───────────────────────────────────────────────
// version(u32 LE) ‖ token_len(u16 LE) ‖ token ‖ cursor(u64 LE)
//   ‖ addr_count(u32 LE) ‖ addresses(20×N) ‖ topic_count(u32 LE) ‖ topics(32×M)

pub const Register = struct {
    version: u32 = PROTOCOL_VERSION,
    /// Reserved auth slot
    /// Currently empty
    /// Will be used in the future
    token: []const u8 = &.{},
    cursor: u64,
    addresses: []const [20]u8 = &.{}, // borrowed from the payload
    topics: []const [32]u8 = &.{}, // borrowed from the payload

    pub fn encode(self: Register, alloc: std.mem.Allocator) ![]u8 {
        const size = 4 + 2 + self.token.len + 8 + 4 + self.addresses.len * 20 + 4 + self.topics.len * 32;
        const buf = try alloc.alloc(u8, size);
        errdefer alloc.free(buf);
        var p: usize = 0;
        std.mem.writeInt(u32, buf[p..][0..4], self.version, .little);
        p += 4;
        std.mem.writeInt(u16, buf[p..][0..2], @intCast(self.token.len), .little);
        p += 2;
        @memcpy(buf[p..][0..self.token.len], self.token);
        p += self.token.len;
        std.mem.writeInt(u64, buf[p..][0..8], self.cursor, .little);
        p += 8;
        std.mem.writeInt(u32, buf[p..][0..4], @intCast(self.addresses.len), .little);
        p += 4;
        for (self.addresses) |a| {
            @memcpy(buf[p..][0..20], &a);
            p += 20;
        }
        std.mem.writeInt(u32, buf[p..][0..4], @intCast(self.topics.len), .little);
        p += 4;
        for (self.topics) |t| {
            @memcpy(buf[p..][0..32], &t);
            p += 32;
        }
        return buf;
    }

    pub fn decode(payload: []const u8) Error!Register {
        if (payload.len < 6) return error.Truncated;
        const version = std.mem.readInt(u32, payload[0..4], .little);
        const token_len = std.mem.readInt(u16, payload[4..6], .little);
        var p: usize = 6;
        if (payload.len < p + token_len + 8 + 4) return error.Truncated;
        const token = payload[p .. p + token_len];
        p += token_len;
        const cursor = std.mem.readInt(u64, payload[p..][0..8], .little);
        p += 8;
        const addr_bytes = @as(usize, std.mem.readInt(u32, payload[p..][0..4], .little)) * 20;
        p += 4;
        if (payload.len < p + addr_bytes + 4) return error.Truncated;
        const addresses = std.mem.bytesAsSlice([20]u8, payload[p .. p + addr_bytes]);
        p += addr_bytes;
        const topic_bytes = @as(usize, std.mem.readInt(u32, payload[p..][0..4], .little)) * 32;
        p += 4;
        if (payload.len < p + topic_bytes) return error.Truncated;
        const topics = std.mem.bytesAsSlice([32]u8, payload[p .. p + topic_bytes]);
        return .{ .version = version, .token = token, .cursor = cursor, .addresses = addresses, .topics = topics };
    }
};

// ── PUSH (engine → client) ───────────────────────────────────────────────────
// block_number(u64 LE) ‖ timestamp(u32 LE) ‖ lz4_entry. The lz4 is a
// `primary.dat` entry verbatim; the timestamp rides in the entry the client
// persists (its FilteredStore), so `ctx.timestamp` is exact off-engine.
// Used for both backfill and live (live blocks are pending until `last_finalized`
// passes them; a reorg is signalled out-of-band via REORG).

pub const Push = struct {
    pub const PREFIX = 8 + 4;
    block_number: u64,
    timestamp: u32,
    lz4_entry: []const u8, // borrowed from the payload

    /// The fixed 12-byte head of a PUSH payload (block ‖ timestamp), written
    /// before the lz4 entry. Lets a streaming server emit `header ‖ prefix ‖
    /// entry` straight from its block buffer — no copy into a payload buffer.
    pub fn prefix(block_number: u64, timestamp: u32) [PREFIX]u8 {
        var b: [PREFIX]u8 = undefined;
        std.mem.writeInt(u64, b[0..8], block_number, .little);
        std.mem.writeInt(u32, b[8..12], timestamp, .little);
        return b;
    }

    pub fn encode(self: Push, alloc: std.mem.Allocator) ![]u8 {
        const buf = try alloc.alloc(u8, PREFIX + self.lz4_entry.len);
        errdefer alloc.free(buf);
        @memcpy(buf[0..PREFIX], &prefix(self.block_number, self.timestamp));
        @memcpy(buf[PREFIX..], self.lz4_entry);
        return buf;
    }

    pub fn decode(payload: []const u8) Error!Push {
        if (payload.len < PREFIX) return error.Truncated;
        return .{
            .block_number = std.mem.readInt(u64, payload[0..8], .little),
            .timestamp = std.mem.readInt(u32, payload[8..12], .little),
            .lz4_entry = payload[12..],
        };
    }
};

// ── ADD_ADDRESS (client → engine) ────────────────────────────────────────────
// address(20) ‖ from_block(u64 LE). A live-discovered factory child: the engine
// mini-backfills [from_block, tip] for this address (catching same-block-as-
// creation events) and adds it to the client's live filter. Keeps the engine
// dumb — the client does the discovery, the engine just adds + backfills.

pub const AddAddress = struct {
    const SIZE = 20 + 8;
    address: [20]u8,
    from_block: u64,

    pub fn encode(self: AddAddress) [SIZE]u8 {
        var b: [SIZE]u8 = undefined;
        @memcpy(b[0..20], &self.address);
        std.mem.writeInt(u64, b[20..28], self.from_block, .little);
        return b;
    }

    pub fn decode(payload: []const u8) Error!AddAddress {
        if (payload.len < SIZE) return error.Truncated;
        return .{ .address = payload[0..20].*, .from_block = std.mem.readInt(u64, payload[20..28], .little) };
    }
};

// ── REORG (engine → client) ──────────────────────────────────────────────────

pub const Reorg = struct {
    fork_point: u64,

    pub fn encode(self: Reorg) [8]u8 {
        var b: [8]u8 = undefined;
        std.mem.writeInt(u64, &b, self.fork_point, .little);
        return b;
    }

    pub fn decode(payload: []const u8) Error!Reorg {
        if (payload.len < 8) return error.Truncated;
        return .{ .fork_point = std.mem.readInt(u64, payload[0..8], .little) };
    }
};

// ── HEARTBEAT (bidirectional) ────────────────────────────────────────────────

pub const Heartbeat = struct {
    pub const SIZE = 24;
    cursor: u64,
    tip: u64,
    last_finalized: u64,

    pub fn encode(self: Heartbeat) [SIZE]u8 {
        var b: [SIZE]u8 = undefined;
        std.mem.writeInt(u64, b[0..8], self.cursor, .little);
        std.mem.writeInt(u64, b[8..16], self.tip, .little);
        std.mem.writeInt(u64, b[16..24], self.last_finalized, .little);
        return b;
    }

    pub fn decode(payload: []const u8) Error!Heartbeat {
        if (payload.len < SIZE) return error.Truncated;
        return .{
            .cursor = std.mem.readInt(u64, payload[0..8], .little),
            .tip = std.mem.readInt(u64, payload[8..16], .little),
            .last_finalized = std.mem.readInt(u64, payload[16..24], .little),
        };
    }
};

// ── GOAWAY (engine → client) ─────────────────────────────────────────────────

pub const GoawayCode = enum(u8) {
    shutdown = 0,
    version_mismatch = 1,
    overloaded = 2,
    slow_client = 3,
    _,
};

pub const Goaway = struct {
    code: GoawayCode,
    reason: []const u8 = &.{}, // borrowed from the payload

    pub fn encode(self: Goaway, alloc: std.mem.Allocator) ![]u8 {
        const buf = try alloc.alloc(u8, 1 + self.reason.len);
        errdefer alloc.free(buf);
        buf[0] = @intFromEnum(self.code);
        @memcpy(buf[1..], self.reason);
        return buf;
    }

    pub fn decode(payload: []const u8) Error!Goaway {
        if (payload.len < 1) return error.Truncated;
        return .{ .code = @enumFromInt(payload[0]), .reason = payload[1..] };
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "header round-trips and rejects bad type / oversize" {
    const h = header(.push, 1234);
    const parsed = try parseHeader(&h);
    try testing.expectEqual(FrameType.push, parsed.type);
    try testing.expectEqual(@as(u32, 1234), parsed.len);

    var bad = header(.push, 5);
    bad[0] = 99; // unknown type
    try testing.expectError(error.InvalidFrameType, parseHeader(&bad));

    var big = header(.push, 0);
    std.mem.writeInt(u32, big[1..5], MAX_PAYLOAD + 1, .little);
    try testing.expectError(error.PayloadTooLarge, parseHeader(&big));

    try testing.expectError(error.Truncated, parseHeader(&[_]u8{ 1, 2 }));
}

test "REGISTER round-trips addresses, topics, cursor" {
    const addrs = [_][20]u8{ [_]u8{0xAA} ** 20, [_]u8{0xBB} ** 20 };
    const tops = [_][32]u8{[_]u8{0xCC} ** 32};
    const reg = Register{ .cursor = 18_600_000, .addresses = &addrs, .topics = &tops };

    const payload = try reg.encode(testing.allocator);
    defer testing.allocator.free(payload);

    const got = try Register.decode(payload);
    try testing.expectEqual(PROTOCOL_VERSION, got.version);
    try testing.expectEqual(@as(u64, 18_600_000), got.cursor);
    try testing.expectEqual(@as(usize, 0), got.token.len);
    try testing.expectEqual(@as(usize, 2), got.addresses.len);
    try testing.expectEqualSlices(u8, &addrs[1], &got.addresses[1]);
    try testing.expectEqual(@as(usize, 1), got.topics.len);
    try testing.expectEqualSlices(u8, &tops[0], &got.topics[0]);
}

test "REGISTER with empty filter and a reserved token" {
    const reg = Register{ .cursor = 0, .token = "secret" };
    const payload = try reg.encode(testing.allocator);
    defer testing.allocator.free(payload);
    const got = try Register.decode(payload);
    try testing.expectEqualStrings("secret", got.token);
    try testing.expectEqual(@as(usize, 0), got.addresses.len);
    try testing.expectEqual(@as(usize, 0), got.topics.len);
}

test "REGISTER decode rejects a truncated payload" {
    const addrs = [_][20]u8{[_]u8{1} ** 20};
    const reg = Register{ .cursor = 1, .addresses = &addrs };
    const payload = try reg.encode(testing.allocator);
    defer testing.allocator.free(payload);
    try testing.expectError(error.Truncated, Register.decode(payload[0 .. payload.len - 5]));
}

test "PUSH round-trips block number, timestamp, and lz4 entry" {
    const lz4 = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
    const payload = try (Push{ .block_number = 100, .timestamp = 1_700_000_000, .lz4_entry = &lz4 }).encode(testing.allocator);
    defer testing.allocator.free(payload);
    const got = try Push.decode(payload);
    try testing.expectEqual(@as(u64, 100), got.block_number);
    try testing.expectEqual(@as(u32, 1_700_000_000), got.timestamp);
    try testing.expectEqualSlices(u8, &lz4, got.lz4_entry);
    try testing.expectError(error.Truncated, Push.decode(&[_]u8{ 1, 2, 3 }));
}

test "ADD_ADDRESS round-trips child address and creation block" {
    const addr = [_]u8{0x9A} ** 20;
    const got = try AddAddress.decode(&(AddAddress{ .address = addr, .from_block = 25_000_001 }).encode());
    try testing.expectEqualSlices(u8, &addr, &got.address);
    try testing.expectEqual(@as(u64, 25_000_001), got.from_block);
}

test "REORG and HEARTBEAT round-trip" {
    const r = try Reorg.decode(&(Reorg{ .fork_point = 25_000_042 }).encode());
    try testing.expectEqual(@as(u64, 25_000_042), r.fork_point);

    const hb = try Heartbeat.decode(&(Heartbeat{ .cursor = 10, .tip = 20, .last_finalized = 15 }).encode());
    try testing.expectEqual(@as(u64, 10), hb.cursor);
    try testing.expectEqual(@as(u64, 20), hb.tip);
    try testing.expectEqual(@as(u64, 15), hb.last_finalized);
}

test "GOAWAY round-trips code and reason" {
    const payload = try (Goaway{ .code = .version_mismatch, .reason = "need v1" }).encode(testing.allocator);
    defer testing.allocator.free(payload);
    const got = try Goaway.decode(payload);
    try testing.expectEqual(GoawayCode.version_mismatch, got.code);
    try testing.expectEqualStrings("need v1", got.reason);
}
