/// emit sdk — library for user indexer projects.
///
/// Built on top of core (flat-store reading) and lmdbx (entity stores +
/// filtered index).
const std = @import("std");

test {
    std.testing.refAllDecls(@This());
}
