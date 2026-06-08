//! Internal module surface for cross-package integration tests. Not part of the
//! user-facing sdk API. Exposes the remote-client and builder pieces an
//! end-to-end test drives directly, without widening `root.zig`.
pub const tcp_client = @import("tcp_client.zig");
pub const filter_builder = @import("filter_builder.zig");
pub const filtered_store = @import("filtered_store.zig");
pub const manifest = @import("manifest.zig");
