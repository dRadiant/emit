/// ENS event handlers. Demonstrates the variable-length field path: the
/// `string name` param decodes to a `[]const u8` borrowing the log data, and
/// `save` copies it into the entity's blob store. No marker types, no special
/// handling at the call site.
const sdk = @import("sdk");

const m = @import("manifest.zig");

const Ctx = sdk.Context(@import("entities.zig"));

pub fn handleNameRegistered(log: sdk.Log(m.NameRegistered), ctx: *Ctx) !void {
    try ctx.stores.registrations.save(.{
        .id = log.params.label,
        .owner = log.params.owner,
        .expires = log.params.expires,
        .name = log.params.name,
    });
}
