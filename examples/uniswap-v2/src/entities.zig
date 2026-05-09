/// Uniswap V2 entities. The factory pre-pass discovers `Pair` addresses
/// from `PairCreated` events; the main scan dispatches `Sync` and `Swap`
/// against those discovered addresses.
const sdk = @import("sdk");

/// Mutable. One row per pair contract; tracks the latest reserves
/// (updated on every Sync).
pub const Pair = struct {
    pub const storage: sdk.StorageMode = .mutable;
    id: [20]u8,
    token0: [20]u8,
    token1: [20]u8,
    reserve0: u256,
    reserve1: u256,
};

/// Immutable. One row per Swap log. Records what flowed in/out — enough
/// for OHLC reconstruction without needing tx_hash or sender/to.
pub const SwapEvent = struct {
    pub const storage: sdk.StorageMode = .immutable;
    id: [16]u8,
    pair: [20]u8,
    amount0_in: u256,
    amount1_in: u256,
    amount0_out: u256,
    amount1_out: u256,
};
