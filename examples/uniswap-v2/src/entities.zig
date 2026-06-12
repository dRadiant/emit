/// Uniswap V2 entities. Factory pre-pass discovers `Pair` addresses from
/// `PairCreated` events. Main scan dispatches `Sync` and `Swap` against them.
const sdk = @import("sdk");

/// Mutable. One row per pair contract. Latest reserves, updated on every Sync.
pub const Pair = struct {
    pub const storage: sdk.StorageMode = .mutable;
    id: [20]u8,
    token0: [20]u8,
    token1: [20]u8,
    reserve0: u256,
    reserve1: u256,
};

/// Immutable. One row per Swap log. Amounts in/out suffice for OHLC
/// reconstruction. `trader` is the transaction sender (`log.tx.from`), the
/// EOA behind the swap. The log's own `sender` param is just the router.
pub const SwapEvent = struct {
    pub const storage: sdk.StorageMode = .immutable;
    id: [16]u8,
    pair: [20]u8,
    trader: [20]u8,
    amount0_in: u256,
    amount1_in: u256,
    amount0_out: u256,
    amount1_out: u256,
};
