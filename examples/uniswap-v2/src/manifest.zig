/// Uniswap V2 manifest. Child events fire on every pair the factory spawned.
/// The factory pre-pass (`scanCreations`) discovers pair addresses from
/// `PairCreated` and feeds them into the main scan as the BLOCKS_CHILDREN set.
const sdk = @import("sdk");

pub const PairCreated = struct {
    pub const signature = "PairCreated(address indexed token0, address indexed token1, address pair, uint256 allPairsLength)";
};

pub const Mint = struct {
    pub const signature = "Mint(address indexed sender, uint256 amount0, uint256 amount1)";
};

pub const Burn = struct {
    pub const signature = "Burn(address indexed sender, uint256 amount0, uint256 amount1, address indexed to)";
};

pub const Swap = struct {
    pub const signature = "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)";
    /// Expose `log.tx`. `params.sender` is the router contract,
    /// the actual trader is the transaction sender.
    pub const tx_fields = true;
};

pub const Sync = struct {
    pub const signature = "Sync(uint112 reserve0, uint112 reserve1)";
};

pub const config: sdk.Manifest = .{
    .name = "uniswap-v2",
    .chain_id = 1,
    // 10k-block benchmark window. Drop start_block to 0 and raise end_block
    // for a full-chain backfill.
    .start_block = 19_000_000,
    .end_block = 19_010_000,
    .factories = &.{.{
        .name = "UniswapV2Factory",
        .address = sdk.address("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"),
        .create_event = PairCreated,
        .spawn_param = "pair",
        .child_events = &.{ Mint, Burn, Swap, Sync },
    }},
    // Chained prefetch. A `Swap` log carries only the pair address, so token
    // metadata is two hops away: `pair.token0()`, then `token0.decimals()` /
    // `symbol()`. `.of "token0()"` targets the address the earlier `token0()`
    // call returned, resolved one prefetch round later. Works for pairs created
    // before the scan window, where no `PairCreated` is seen so token addresses
    // can't come from an event param. `symbol()` returns a dynamic `string`,
    // read as `[]const u8`.
    .prefetch = &.{.{
        .on_event = Swap,
        .calls = &.{
            .{ .address = .log, .method = "token0()" },
            .{ .address = .log, .method = "token1()" },
            .{ .address = .{ .of = "token0()" }, .method = "decimals()" },
            .{ .address = .{ .of = "token0()" }, .method = "symbol()" },
            .{ .address = .{ .of = "token1()" }, .method = "decimals()" },
            .{ .address = .{ .of = "token1()" }, .method = "symbol()" },
        },
    }},
};
