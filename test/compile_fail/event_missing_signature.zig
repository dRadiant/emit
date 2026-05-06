// expected: must declare `pub const signature = "Name(types,...)";`. The SDK derives topic0 and name from it.
//
// validateEvent rejects event types without signature.

const sdk = @import("sdk");

const Bad = struct {};

comptime {
    sdk.manifest.validateEvent(Bad);
}
