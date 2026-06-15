// expected: but no earlier call declares it.
//
// validateManifest rejects a chained `.of` whose producer method is not
// declared by any earlier call in the same prefetch def.

const sdk = @import("sdk");

const Swap = struct {
    pub const signature = "Swap(address indexed sender, uint256 amount0, uint256 amount1, address indexed to)";
};

comptime {
    sdk.manifest.validateManifest(.{
        .name = "x",
        .chain_id = 1,
        .start_block = 0,
        .prefetch = &.{.{
            .on_event = Swap,
            .calls = &.{
                // References token0(), but no earlier call declares it.
                .{ .address = .{ .of = "token0()" }, .method = "decimals()" },
            },
        }},
    });
}
