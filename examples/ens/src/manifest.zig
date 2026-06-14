/// ENS manifest. Indexes the ETHRegistrarController's `NameRegistered`, whose
/// `name` is a top-level dynamic `string`. topic0 is derived from
/// the signature at comptime.
const sdk = @import("sdk");

pub const NameRegistered = struct {
    pub const signature = "NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires)";
};

pub const config: sdk.Manifest = .{
    .name = "ens",
    .chain_id = 1,
    .start_block = 0,
    .contracts = &.{.{
        .name = "ETHRegistrarController",
        .address = sdk.address("0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5"),
        .events = &.{NameRegistered},
    }},
};
