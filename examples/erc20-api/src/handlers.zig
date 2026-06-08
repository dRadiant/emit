/// ERC20 event handlers. The comptime topic0 dispatcher invokes
/// `handle<EventName>(log, ctx)` per matching log. Method names mirror the
/// event types declared in `manifest.zig`.
const sdk = @import("sdk");

const m = @import("manifest.zig");

/// Derived from the entities module. `sdk.Context` introspects its pub decls
/// for any struct declaring `pub const storage`. Add or remove an entity in
/// `entities.zig` and both `Ctx` here and the tuple in `main.zig` pick it up.
const Ctx = sdk.Context(@import("entities.zig"));

/// Wrapping arithmetic on balances. A non-genesis start block would otherwise
/// trip integer-overflow safety checks. Balances reconcile by head.
pub fn handleTransfer(log: sdk.Log(m.Transfer), ctx: *Ctx) !void {
    var sender = try ctx.stores.accounts.loadOrInit(log.params.from);
    var receiver = try ctx.stores.accounts.loadOrInit(log.params.to);
    sender.balance -%= log.params.value;
    receiver.balance +%= log.params.value;
    try ctx.stores.accounts.save(sender);
    try ctx.stores.accounts.save(receiver);

    try ctx.stores.transfers.save(.{
        .id = log.eventId(),
        .from = log.params.from,
        .to = log.params.to,
        .value = log.params.value,
    });
}

/// Sets the allowance to `value` (overwrites previous).
pub fn handleApproval(log: sdk.Log(m.Approval), ctx: *Ctx) !void {
    const owner = log.params.owner;
    const spender = log.params.spender;
    const value = log.params.value;

    try ctx.stores.allowances.save(.{
        .id = sdk.concat(.{ owner, spender }),
        .value = value,
    });

    try ctx.stores.approvals.save(.{
        .id = log.eventId(),
        .owner = owner,
        .spender = spender,
        .value = value,
    });
}
