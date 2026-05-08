// expected: is not a struct. Entities must be plain data structs whose first field is the primary key.
//
// storeFor rejects non-struct entity types.

const sdk = @import("sdk");

comptime {
    _ = sdk.storeFor(u64);
}
