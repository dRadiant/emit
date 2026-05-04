/// Minimal JSON-RPC client over HTTP for Ethereum node communication.
/// Wraps std.http.Client with JSON-RPC 2.0 request/response framing.
/// Used by rpc_import (bulk eth_getLogs) and head_follower (polling).
const std = @import("std");

pub const RpcClient = struct {
    url: []const u8,
    http: std.http.Client,
    alloc: std.mem.Allocator,
    next_id: u64 = 1,

    pub fn init(alloc: std.mem.Allocator, url: []const u8) RpcClient {
        return .{
            .url = url,
            .http = std.http.Client{ .allocator = alloc },
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *RpcClient) void {
        self.http.deinit();
    }

    /// Send a JSON-RPC request and return the raw response body.
    /// Caller owns the returned slice.
    pub fn call(self: *RpcClient, method: []const u8, params: []const u8) ![]u8 {
        const id = self.next_id;
        self.next_id += 1;

        var body_buf = std.ArrayList(u8).init(self.alloc);
        defer body_buf.deinit();
        try body_buf.writer().print(
            \\{{"jsonrpc":"2.0","id":{d},"method":"{s}","params":{s}}}
        , .{ id, method, params });

        // Collect response into a heap-allocated list
        var response_body = std.ArrayList(u8).init(self.alloc);
        errdefer response_body.deinit();

        const result = try self.http.fetch(.{
            .location = .{ .url = self.url },
            .method = .POST,
            .payload = body_buf.items,
            .headers = .{ .content_type = .{ .override = "application/json" } },
            .response_writer = .{ .any = response_body.writer().any() },
        });

        if (result.status != .ok) {
            response_body.deinit();
            return error.HttpError;
        }

        return try response_body.toOwnedSlice();
    }
};
