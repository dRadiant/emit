/// Uniswap V2 event handlers.
///
/// The factory pre-pass (Phase 2) scans `PairCreated` logs and feeds the
/// discovered pair addresses into Phase 3's filter. By the time the main
/// replay invokes `handlePairCreated`, the pair address has already been
/// wired into the SDK's child-address set; the handler's only job is to
/// persist the `Pair` entity row.
///
/// `handleMint` and `handleBurn` are no-ops — the example doesn't model
/// LP-token issuance/burn, but the manifest declares them so the factory
/// pre-pass admits all four child events through the BLOCKS_CHILDREN
/// filter (verifying the dispatcher round-trips them all).
const std = @import("std");

const sdk = @import("sdk");

const m = @import("manifest.zig");

const Ctx = sdk.Context(@import("entities.zig"));

pub fn handlePairCreated(log: sdk.Log(m.PairCreated), ctx: *Ctx) !void {
    try ctx.stores.pairs.save(.{
        .id = log.params.pair,
        .token0 = log.params.token0,
        .token1 = log.params.token1,
        .reserve0 = 0,
        .reserve1 = 0,
    });
}

pub fn handleSync(log: sdk.Log(m.Sync), ctx: *Ctx) !void {
    var pair = try ctx.stores.pairs.loadOrInit(log.address);
    // Sync.reserve0/1 are uint112; widen to u256 for the entity store.
    pair.reserve0 = log.params.reserve0;
    pair.reserve1 = log.params.reserve1;
    try ctx.stores.pairs.save(pair);
}

pub fn handleSwap(log: sdk.Log(m.Swap), ctx: *Ctx) !void {
    try ctx.stores.swapEvents.save(.{
        .id = log.eventId(),
        .pair = log.address,
        .amount0_in = log.params.amount0In,
        .amount1_in = log.params.amount1In,
        .amount0_out = log.params.amount0Out,
        .amount1_out = log.params.amount1Out,
    });
}

pub fn handleMint(_: sdk.Log(m.Mint), _: *Ctx) !void {}
pub fn handleBurn(_: sdk.Log(m.Burn), _: *Ctx) !void {}
