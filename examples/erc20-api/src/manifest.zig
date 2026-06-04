/// ERC20 manifest for rETH. Each event type declares its `signature`; the
/// SDK derives `topic0` (keccak-256) and the dispatch method name
/// (`handle<EventName>`) at comptime. Mirrors `examples/erc20`
const sdk = @import("sdk");

pub const Transfer = struct {
    pub const signature = "Transfer(address indexed from, address indexed to, uint256 value)";
};

pub const Approval = struct {
    pub const signature = "Approval(address indexed owner, address indexed spender, uint256 value)";
};

pub const config: sdk.Manifest = .{
    .name = "erc20-api",
    .chain_id = 1,
    .start_block = 0,
    .contracts = &.{.{
        .name = "rETH",
        .address = sdk.address("0xae78736Cd615f374D3085123A210448E74Fc6393"),
        .events = &.{ Transfer, Approval },
    }},
};
