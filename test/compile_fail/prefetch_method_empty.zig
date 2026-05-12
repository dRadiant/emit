// expected: has an empty method string
//
// validateManifest catches a prefetch entry with an empty method signature.

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
                .{ .address = .log, .method = "" },
            },
        }},
    });
}
