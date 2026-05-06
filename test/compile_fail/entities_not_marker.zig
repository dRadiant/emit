// expected: is not a marker produced by sdk.mutable() or sdk.appendOnly()
//
// validateEntityTuple rejects tuple elements that are not markers.

const sdk = @import("sdk");

const Account = struct { id: [20]u8, balance: u256 };

comptime {
    // Passing the bare entity type instead of `sdk.mutable(Account)`.
    sdk.validateEntityTuple(.{Account});
}
