/// ENS example indexer. Stores each registration's name as a blob field
/// Wires manifest, handlers, and entities through the shared CLI.
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
    sdk.validateHandler(@import("manifest.zig").config, @import("handlers.zig"));
}

test "Context type instantiates from the entities module" {
    _ = sdk.Context(@import("entities.zig"));
}
