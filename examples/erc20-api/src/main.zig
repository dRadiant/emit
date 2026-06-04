/// erc20-api: the rETH indexer of `examples/erc20`, served over HTTP from the
/// same process. `sdk.spawn` backfills, then runs the live follow loop on a
/// background thread and hands back a `*Context`; the HTTP handlers read that
/// live, tip-inclusive, reorg-aware state through the SDK's in-process read
/// surface (`ctx.read` / `ctx.count` / `ctx.range` / `ctx.cursor`), each of
/// which takes the Context lock internally — the API code never touches a mutex.
///
/// This is the traditional indexer model: the indexing service answers its
/// own queries, at the head. Endpoints:
///
///   GET /health                       cursor (last indexed block) + liveness
///   GET /account/:addr                balance (mutable point read)
///   GET /allowance/:owner/:spender    allowance (mutable point read)
///   GET /transfers?limit=&offset=     newest-first transfer page (immutable range)
const std = @import("std");
const sdk = @import("sdk");
const httpz = @import("httpz");
const cli = @import("cli");

const e = @import("entities.zig");
const m = @import("manifest.zig");
const h = @import("handlers.zig");

/// Module form so this Context type matches the one `sdk.spawn` returns and
/// the one `handlers.zig` derives. All three resolve to `Context(entities-module)`.
const Ctx = sdk.Context(e);

const App = struct { ctx: *Ctx };

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Reuse the shared example flag parser for the standard dirs + node RPC,
    // then pull the API-only `--port` (default 8080) from argv separately.
    const args = try cli.parseStandardArgs(allocator, "erc20-api");
    defer allocator.free(args.engine_data_dir);
    defer allocator.free(args.data_dir);
    defer if (args.node_rpc) |s| allocator.free(s);
    const port = try parsePort(allocator);

    // Backfill, then follow on a background thread; `ctx` is the live handle.
    const ctx = try sdk.spawn(m.config, h, e, .{
        .engine_data_dir = args.engine_data_dir,
        .data_dir = args.data_dir,
        .node_rpc = args.node_rpc,
    }, allocator);
    defer ctx.deinit(); // signals + joins the follow thread

    var app = App{ .ctx = ctx };
    var server = try httpz.Server(*App).init(allocator, .{ .address = .localhost(port) }, &app);
    defer server.deinit();

    var router = try server.router(.{});
    router.get("/health", health, .{});
    router.get("/account/:addr", account, .{});
    router.get("/allowance/:owner/:spender", allowance, .{});
    router.get("/transfers", transfers, .{});

    std.debug.print("erc20-api serving on http://127.0.0.1:{d}\n", .{port});
    try server.listen();
}

// ── Endpoints ──────────────────────────────────────────────────────────────

/// Indexing progress + liveness. `cursor()` is the last fully-dispatched
/// block; `followError()` is null while the follow thread is healthy.
fn health(app: *App, _: *httpz.Request, res: *httpz.Response) !void {
    try res.json(.{
        .last_indexed_block = app.ctx.cursor(),
        .following = app.ctx.followError() == null,
        .transfers = app.ctx.count(e.Transfer),
        .approvals = app.ctx.count(e.Approval),
    }, .{});
}

/// Balance for a holder. An address the indexer has never seen has an implicit
/// zero balance (ERC20 semantics), so this is a 200 with "0", not a 404.
fn account(app: *App, req: *httpz.Request, res: *httpz.Response) !void {
    const addr = parseAddr(req.param("addr")) orelse return badRequest(res, "invalid address");
    const acct = try app.ctx.read(e.Account, addr);
    try res.json(.{
        .address = req.param("addr").?,
        .balance = try dec(res.arena, if (acct) |a| a.balance else 0),
    }, .{});
}

/// Allowance for an (owner, spender) pair. Key is `owner ++ spender` (40 bytes),
/// the same composite the handler writes.
fn allowance(app: *App, req: *httpz.Request, res: *httpz.Response) !void {
    const owner = parseAddr(req.param("owner")) orelse return badRequest(res, "invalid owner");
    const spender = parseAddr(req.param("spender")) orelse return badRequest(res, "invalid spender");
    const al = try app.ctx.read(e.Allowance, sdk.concat(.{ owner, spender }));
    try res.json(.{
        .owner = req.param("owner").?,
        .spender = req.param("spender").?,
        .value = try dec(res.arena, if (al) |a| a.value else 0),
    }, .{});
}

/// Newest-first page of transfers. `?limit` (default 20, capped 100) and
/// `?offset` page backwards from the tip. Transfers are an append-only log, so
/// `count()` + `range()` are the natural reads; we read an ascending window and
/// reverse it for newest-first presentation.
fn transfers(app: *App, req: *httpz.Request, res: *httpz.Response) !void {
    const q = try req.query();
    const limit = @min(parseU64(q.get("limit")) orelse 20, 100);
    const offset = parseU64(q.get("offset")) orelse 0;

    const total = app.ctx.count(e.Transfer);
    const remaining = if (offset < total) total - offset else 0;
    const n: usize = @intCast(@min(limit, remaining));

    var buf: [100]e.Transfer = undefined;
    const start = total - offset - n; // ascending index of the window's oldest
    const window = try app.ctx.range(e.Transfer, start, buf[0..n]);

    // Reverse the ascending window into newest-first presentation views.
    // `EventId.unpack` decodes the packed key back into its coordinates.
    const views = try res.arena.alloc(TransferView, window.len);
    for (window, 0..) |t, i| {
        views[window.len - 1 - i] = .{
            .block = sdk.EventId.unpack(t.id).block_number,
            .from = try hexAddr(res.arena, t.from),
            .to = try hexAddr(res.arena, t.to),
            .value = try dec(res.arena, t.value),
        };
    }

    try res.json(.{ .total = total, .count = views.len, .transfers = views }, .{});
}

const TransferView = struct {
    block: u64,
    from: []const u8,
    to: []const u8,
    value: []const u8,
};

// ── Helpers ──────────────────────────────────────────────────────────────

fn badRequest(res: *httpz.Response, msg: []const u8) !void {
    res.status = 400;
    try res.json(.{ .@"error" = msg }, .{});
}

/// Parse an optional "0x"-prefixed 40-hex string into a 20-byte address.
fn parseAddr(s: ?[]const u8) ?[20]u8 {
    const str = s orelse return null;
    const hex = if (std.mem.startsWith(u8, str, "0x")) str[2..] else str;
    if (hex.len != 40) return null;
    var out: [20]u8 = undefined;
    _ = std.fmt.hexToBytes(&out, hex) catch return null;
    return out;
}

fn parseU64(s: ?[]const u8) ?u64 {
    const str = s orelse return null;
    return std.fmt.parseInt(u64, str, 10) catch null;
}

/// `--port N` from argv, default 8080. Kept out of the shared parser since it
/// is API-only.
fn parsePort(allocator: std.mem.Allocator) !u16 {
    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);
    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        if (std.mem.eql(u8, argv[i], "--port") and i + 1 < argv.len) {
            return std.fmt.parseInt(u16, argv[i + 1], 10) catch 8080;
        }
    }
    return 8080;
}

const hex_chars = "0123456789abcdef";

/// "0x" + 40 lowercase hex chars, allocated in the per-request arena.
fn hexAddr(arena: std.mem.Allocator, addr: [20]u8) ![]const u8 {
    const out = try arena.alloc(u8, 42);
    out[0] = '0';
    out[1] = 'x';
    for (addr, 0..) |b, i| {
        out[2 + i * 2] = hex_chars[b >> 4];
        out[2 + i * 2 + 1] = hex_chars[b & 0x0f];
    }
    return out;
}

/// u256 as a decimal string (JSON numbers can't hold 256 bits safely).
fn dec(arena: std.mem.Allocator, value: u256) ![]const u8 {
    return std.fmt.allocPrint(arena, "{d}", .{value});
}

// ── Tests ────────────────────────────────────────────────────────────────

test "manifest validates and handlers expose required methods" {
    sdk.validateHandler(m.config, h);
}

test "Context type instantiates from the entities module" {
    _ = sdk.Context(e);
}

test "parseAddr round-trips a 0x address and rejects malformed input" {
    const got = parseAddr("0x000000000000000000000000000000000000beef").?;
    try std.testing.expectEqual(@as(u8, 0xbe), got[18]);
    try std.testing.expectEqual(@as(u8, 0xef), got[19]);
    try std.testing.expectEqual(@as(?[20]u8, null), parseAddr("0xzz"));
    try std.testing.expectEqual(@as(?[20]u8, null), parseAddr(null));
}
