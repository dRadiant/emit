// expected: is not a struct. Entities must be plain data structs whose first field is the primary key.
//
// validateEntity rejects non-struct entity types.

const sdk = @import("sdk");

comptime {
    _ = sdk.mutable(u64);
}
