/// emit sdk — library for user indexer projects.
///
/// Built on top of core (flat-store reading) and lmdbx (entity stores +
/// filtered index).
pub const append_store = @import("append_store.zig");
pub const AppendStore = append_store.AppendStore;
pub const AppendError = append_store.AppendError;

test {
    _ = append_store;
}
