/// ERC20 example indexer (rETH on mainnet). All glue lives in
/// `examples/utils/cli.zig`; this file just wires the manifest, handlers,
/// and entities together.
const sdk = @import("sdk");
const cli = @import("cli");

pub fn main() !void {
    try cli.run(
        @import("manifest.zig").config,
        @import("handlers.zig"),
        @import("entities.zig"),
    );
}

// ── Tests ────────────────────────────────────────────────────────────────

test "manifest validates and handlers expose required methods" {
    const manifest = @import("manifest.zig");
    const handlers = @import("handlers.zig");
    sdk.validateHandler(manifest.config, handlers);
}

test "Context type instantiates from the entities module" {
    _ = sdk.Context(@import("entities.zig"));
}
