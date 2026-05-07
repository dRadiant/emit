// expected: is missing method `handleTransfer` for event `Transfer(address,address,uint256)`. Add `pub fn handleTransfer(log: sdk.DecodedLog, ctx: *Ctx) !void { ... }`.
//
// dispatcher's validateHandler rejects handlers missing required methods.

const sdk = @import("sdk");

const Transfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
};

const TestManifest: sdk.Manifest = .{
    .name = "test",
    .chain_id = 1,
    .start_block = 0,
    .contracts = &.{.{
        .name = "C",
        .address = [_]u8{0} ** 20,
        .events = &.{Transfer},
    }},
};

const Bad = struct {};

comptime {
    const D = sdk.handler.dispatcherFor(TestManifest);
    D.validateHandler(Bad);
}
