// expected: static_prefetch entry has an empty method string
//
// validateManifest catches a static_prefetch entry with an empty method.

const sdk = @import("sdk");

comptime {
    sdk.manifest.validateManifest(.{
        .name = "x",
        .chain_id = 1,
        .start_block = 0,
        .static_prefetch = &.{
            .{ .address = [_]u8{0} ** 20, .method = "" },
        },
    });
}
