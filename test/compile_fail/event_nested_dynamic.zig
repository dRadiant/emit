// expected: is a tuple with a dynamic component; nested dynamics are not yet supported

// Nested dynamics — a dynamic type inside a tuple or array — are not yet
// decodable (they need head/tail offset resolution relative to the enclosing
// dynamic region). The auto-decoder rejects them at compile time rather than
// returning wrong values. Top-level dynamic arrays of static elements, static
// arrays, and static tuples are supported; this is one level too deep.

const sdk = @import("sdk");

const E = struct {
    pub const signature = "E((address,uint256[]) memo)";
};

comptime {
    const log: sdk.DecodedLog = .{
        .block_number = 0,
        .tx_index = 0,
        .log_index = 0,
        .tx_hash = [_]u8{0} ** 32,
        .address = [_]u8{0} ** 20,
        .topics = [_][32]u8{[_]u8{0} ** 32} ** 4,
        .topic_count = 0,
        .data = &.{},
    };
    _ = log.decode(E);
}
