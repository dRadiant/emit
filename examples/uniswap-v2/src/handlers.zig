/// Uniswap V2 event handlers.
///
/// `handlePairCreated` persists the `Pair` row for pairs created in-window
/// (token addresses straight off the event). `handleSwap` enriches the pair
/// from the chained prefetch (`token0()`/`token1()` then `decimals()`/
/// `symbol()`), which also covers pairs created before the scan window.
///
/// `handleMint`/`handleBurn` are no-ops: the example doesn't model LP-token
/// issuance/burn. The manifest declares them so the pre-pass admits all four
/// child events through the BLOCKS_CHILDREN filter.
const std = @import("std");

const sdk = @import("sdk");

const m = @import("manifest.zig");

const Ctx = sdk.Context(@import("entities.zig"));

/// ERC20 default when a token's `decimals()` is uncached (no RPC) or reverts.
const DEFAULT_DECIMALS: u8 = 18;

pub fn handlePairCreated(log: sdk.Log(m.PairCreated), ctx: *Ctx) !void {
    try ctx.stores.pairs.save(.{
        .id = log.params.pair,
        .token0 = log.params.token0,
        .token1 = log.params.token1,
        .token0_decimals = DEFAULT_DECIMALS,
        .token1_decimals = DEFAULT_DECIMALS,
        .reserve0 = 0,
        .reserve1 = 0,
    });
}

pub fn handleSync(log: sdk.Log(m.Sync), ctx: *Ctx) !void {
    var pair = try ctx.stores.pairs.loadOrInit(log.address);
    // Sync.reserve0/1 are uint112, widened to u256 for the entity store.
    pair.reserve0 = log.params.reserve0;
    pair.reserve1 = log.params.reserve1;
    try ctx.stores.pairs.save(pair);
}

pub fn handleSwap(log: sdk.Log(m.Swap), ctx: *Ctx) !void {
    var pair = try ctx.stores.pairs.loadOrInit(log.address);

    // A Swap log exposes only the pair address. The chained prefetch resolved
    // the token addresses and their decimals, so even pairs created before the
    // scan window enrich here. `catch` keeps the prior value when a call is
    // uncached (no RPC) or reverts.
    pair.token0 = ctx.ethCall([20]u8, log.address, "token0()") catch pair.token0;
    pair.token1 = ctx.ethCall([20]u8, log.address, "token1()") catch pair.token1;
    pair.token0_decimals = ctx.ethCall(u8, pair.token0, "decimals()") catch DEFAULT_DECIMALS;
    pair.token1_decimals = ctx.ethCall(u8, pair.token1, "decimals()") catch DEFAULT_DECIMALS;
    try ctx.stores.pairs.save(pair);

    // `symbol()` returns a dynamic `string`, resolved through the same chain
    // and read as `[]const u8` borrowing the ethcall cache for this call.
    const sym0 = ctx.ethCall([]const u8, pair.token0, "symbol()") catch "?";
    const sym1 = ctx.ethCall([]const u8, pair.token1, "symbol()") catch "?";
    std.log.debug("swap {s}/{s} pair={x} in0={f} in1={f} out0={f} out1={f}", .{
        sym0,
        sym1,
        log.address,
        sdk.amount(log.params.amount0In, pair.token0_decimals),
        sdk.amount(log.params.amount1In, pair.token1_decimals),
        sdk.amount(log.params.amount0Out, pair.token0_decimals),
        sdk.amount(log.params.amount1Out, pair.token1_decimals),
    });

    try ctx.stores.swapEvents.save(.{
        .id = log.eventId(),
        .pair = log.address,
        .trader = log.tx.from,
        .amount0_in = log.params.amount0In,
        .amount1_in = log.params.amount1In,
        .amount0_out = log.params.amount0Out,
        .amount1_out = log.params.amount1Out,
    });
}

pub fn handleMint(_: sdk.Log(m.Mint), _: *Ctx) !void {}
pub fn handleBurn(_: sdk.Log(m.Burn), _: *Ctx) !void {}
