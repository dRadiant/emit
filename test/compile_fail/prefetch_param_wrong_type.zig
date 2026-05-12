// expected: references parameter `allPairsLength` of type `uint256`, expected `address`
//
// validateManifest catches a prefetch .param source pointing at a non-address
// parameter (analogous to factory_spawn_param_wrong_type but for prefetch).

const sdk = @import("sdk");

const PairCreated = struct {
    pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair, uint256 allPairsLength)";
};

comptime {
    sdk.manifest.validateManifest(.{
        .name = "x",
        .chain_id = 1,
        .start_block = 0,
        .prefetch = &.{.{
            .on_event = PairCreated,
            .calls = &.{
                .{ .address = .{ .param = "allPairsLength" }, .method = "decimals()" },
            },
        }},
    });
}
