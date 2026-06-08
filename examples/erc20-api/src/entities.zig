/// ERC20 entities. Each declares its storage mode via
/// `pub const storage: sdk.StorageMode = .mutable | .immutable;`.
///
/// First field is the primary key (fixed-size). Field types restricted to
/// integers and fixed-size arrays by the SDK comptime serializer.
/// Identical to `examples/erc20`. The API in `main.zig` reads these stores
/// in-process via `ctx.read` / `ctx.count` / `ctx.range`.
const sdk = @import("sdk");

/// Mutable. Keyed by the holder's 20-byte address.
pub const Account = struct {
    pub const storage: sdk.StorageMode = .mutable;
    id: [20]u8,
    balance: u256,
};

/// Mutable. Keyed by `owner ++ spender` (40 bytes).
pub const Allowance = struct {
    pub const storage: sdk.StorageMode = .mutable;
    id: [40]u8,
    value: u256,
};

/// Immutable. Keyed by `block_number(BE u64) ++ tx_index(BE u32) ++ log_index(BE u32)`.
/// Monotonic in (block, tx, log). Required by `ImmutableStore`'s append-only contract.
pub const Transfer = struct {
    pub const storage: sdk.StorageMode = .immutable;
    id: [16]u8,
    from: [20]u8,
    to: [20]u8,
    value: u256,
};

/// Immutable. Same key construction as Transfer.
pub const Approval = struct {
    pub const storage: sdk.StorageMode = .immutable;
    id: [16]u8,
    owner: [20]u8,
    spender: [20]u8,
    value: u256,
};
