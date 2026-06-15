// expected: but multiple earlier calls declare that method.
//
// validateManifest rejects a chained `.of` when more than one earlier call
// declares the referenced method, so the producer would be ambiguous.

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
                .{ .address = .log, .method = "factory()" },
                .{ .address = .{ .param = "to" }, .method = "factory()" }, // same method, different target
                .{ .address = .{ .of = "factory()" }, .method = "decimals()" },
            },
        }},
    });
}
