// expected: factories must declare identical child_events. Split distinct protocols into separate manifests.
//
// validateManifest rejects two factories with differing child events. Dispatch
// routes by topic0 with no per-child factory provenance, so a child of one
// factory emitting the other's event would mis-dispatch.

const sdk = @import("sdk");

const PairCreated = struct {
    pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair, uint256 n)";
};
const PoolCreated = struct {
    pub const signature = "PoolCreated(address indexed token0, address indexed token1, address pool, uint256 n)";
};
const Swap = struct {
    pub const signature = "Swap(address sender, uint256 a0In, uint256 a1In, uint256 a0Out, uint256 a1Out, address to)";
};
const Burn = struct {
    pub const signature = "Burn(address indexed sender, uint256 a0, uint256 a1, address indexed to)";
};

comptime {
    sdk.manifest.validateManifest(.{
        .name = "x",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{
            .{ .name = "F1", .address = [_]u8{1} ** 20, .create_event = PairCreated, .spawn_param = "pair", .child_events = &.{Swap} },
            .{ .name = "F2", .address = [_]u8{2} ** 20, .create_event = PoolCreated, .spawn_param = "pool", .child_events = &.{Burn} },
        },
    });
}
