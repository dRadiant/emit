/// Uniswap V2 manifest. The factory address is canonical; child events
/// fire on every pair contract the factory spawned. The factory pre-pass
/// (`scanCreations`) discovers pair addresses from `PairCreated` and
/// feeds them into the main scan as the BLOCKS_CHILDREN address set.
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
};

pub const Sync = struct {
    pub const signature = "Sync(uint112 reserve0, uint112 reserve1)";
};

pub const config: sdk.Manifest = .{
    .name = "uniswap-v2",
    .chain_id = 1,
    // 10k-block window matching the open-indexer-benchmark fixture. Drop
    // start_block to 0 and raise end_block for a full-chain backfill.
    .start_block = 19_000_000,
    .end_block = 19_010_000,
    .factories = &.{.{
        .name = "UniswapV2Factory",
        .address = sdk.address("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"),
        .create_event = PairCreated,
        .spawn_param = "pair",
        .child_events = &.{ Mint, Burn, Swap, Sync },
    }},
};
