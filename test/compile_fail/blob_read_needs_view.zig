// expected: copy any blob bytes out before the view is released.
//
// A blob field borrows the store mmap, so the auto-locking `ctx.read` (which
// releases the lock on return) is rejected for blob entities. They must be
// read through `ctx.readView()`, which holds the lock across the borrow.

const sdk = @import("sdk");

const NameEntity = struct {
    pub const storage: sdk.StorageMode = .mutable;
    id: [20]u8,
    label: []const u8,
};

const Entities = struct {
    pub const Name = NameEntity;
};

const Ctx = sdk.Context(Entities);

export fn probe(ctx: *Ctx) void {
    _ = ctx.read(NameEntity, [_]u8{0} ** 20) catch {};
}
