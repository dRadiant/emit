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

/// Expands `addrs` into ERC20-shaped `StaticCall` triples at comptime.
/// Lives in user code so adapting to other shapes (ERC721 `tokenURI`,
/// ERC4626 `asset`) is a copy-paste edit.
fn erc20Metadata(comptime addrs: []const [20]u8) []const sdk.StaticCall {
    comptime {
        var out: []const sdk.StaticCall = &.{};
        for (addrs) |a| {
            out = out ++ &[_]sdk.StaticCall{
                .{ .address = a, .method = "decimals()" },
                .{ .address = a, .method = "symbol()" },
                .{ .address = a, .method = "name()" },
            };
        }
        return out;
    }
}

const WETH = sdk.address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
const USDC = sdk.address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");

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
    // Per-pair token decimals fetched once at creation. `symbol()`/`name()`
    // would fit here too but their dynamic-string returns are deferred.
    .prefetch = &.{.{
        .on_event = PairCreated,
        .calls = &.{
            .{ .address = .{ .param = "token0" }, .method = "decimals()" },
            .{ .address = .{ .param = "token1" }, .method = "decimals()" },
        },
    }},
    .static_prefetch = erc20Metadata(&.{ WETH, USDC }),
};
