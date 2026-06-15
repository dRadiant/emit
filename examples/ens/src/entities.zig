/// ENS registration entities. `name` is a variable-length blob field
/// a `[]const u8` backed by `<entity>.blobs.dat`, declared like
/// any other field with no marker type.
const sdk = @import("sdk");

/// Latest registration per label, keyed by the 32-byte label hash. Mutable so
/// a re-registration of the same label overwrites in place. `name` is the
/// registered label string (a blob field).
pub const Registration = struct {
    pub const storage: sdk.StorageMode = .mutable;
    id: [32]u8,
    owner: [20]u8,
    expires: u256,
    name: []const u8,
};

/// Append-only log of every registration, keyed by the 16-byte event id
/// (`block ++ tx ++ log`, monotonic). Demonstrates a blob field on the
/// immutable store path; `name` is the same variable-length string.
pub const RegistrationLog = struct {
    pub const storage: sdk.StorageMode = .immutable;
    id: [16]u8,
    owner: [20]u8,
    name: []const u8,
};
