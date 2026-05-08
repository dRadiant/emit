// expected: has no fields. The first field must be the primary key.
//
// storeFor rejects empty entity structs.

const sdk = @import("sdk");

const Empty = struct {};

comptime {
    _ = sdk.storeFor(Empty);
}
