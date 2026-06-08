//! Cross-package integration tests. The engine and sdk never import each other,
//! so end-to-end checks that drive both live here, in a target that imports the
//! source files directly.
test {
    _ = @import("tcp_loopback.zig");
}
