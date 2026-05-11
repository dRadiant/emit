// expected: spawn_param `allPairsLength` is type `uint256`, expected `address`
//
// validateManifest catches spawn_param pointing at a non-address parameter.

const sdk = @import("sdk");

const PairCreated = struct {
    pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair, uint256 allPairsLength)";
};

const Sync = struct {
    pub const signature = "Sync(uint112 reserve0, uint112 reserve1)";
};

comptime {
    sdk.manifest.validateManifest(.{
        .name = "x",
        .chain_id = 1,
        .start_block = 0,
        .factories = &.{.{
            .name = "F",
            .address = [_]u8{0} ** 20,
            .create_event = PairCreated,
            .spawn_param = "allPairsLength",
            .child_events = &.{Sync},
        }},
    });
}
