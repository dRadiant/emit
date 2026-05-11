// expected: has no parameter named `frmo`. Available: from, to, value
//
// param() with a typo'd name lists the available parameters.

const std = @import("std");
const sdk = @import("sdk");

const Transfer = struct {
    pub const signature = "Transfer(address indexed from, address indexed to, uint256 value)";
};

comptime {
    const log: sdk.DecodedLog = .{
        .block_number = 0,
        .tx_index = 0,
        .log_index = 0,
        .tx_hash = [_]u8{0} ** 32,
        .address = [_]u8{0} ** 20,
        .topics = std.mem.zeroes([4][32]u8),
        .topic_count = 3,
        .data = &.{},
    };
    _ = log.param(Transfer, "frmo");
}
