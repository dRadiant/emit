// expected: has no parameter named `piar`. Available: token0, token1, pair, allPairsLength
//
// validateManifest catches spawn_param referencing a non-existent parameter.

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
            .spawn_param = "piar",
            .child_events = &.{Sync},
        }},
    });
}
