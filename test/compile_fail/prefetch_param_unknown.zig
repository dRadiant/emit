// expected: has no parameter named `tken0`. Available: token0, token1, pair, allPairsLength
//
// validateManifest catches a prefetch .param source referencing a parameter
// that doesn't exist on the on_event signature.

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
                .{ .address = .{ .param = "tken0" }, .method = "decimals()" },
            },
        }},
    });
}
