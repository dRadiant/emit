// expected: must declare `pub const storage: sdk.StorageMode = .mutable;` or `.immutable;`. The choice is per-entity and intentional.
//
// storeFor rejects entity types missing the storage decl.

const sdk = @import("sdk");

const Account = struct {
    id: [20]u8,
    balance: u256,
};

comptime {
    _ = sdk.Context(.{Account});
}
