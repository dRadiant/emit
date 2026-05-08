// expected: ImmutableStore.load is not supported: cannot load immutable entities during backfill
//
// load() on the store backing an immutable entity is a MutableStore /
// ImmutableStore mixup. The SDK catches it at compile time.

const sdk = @import("sdk");

const Event = struct {
    pub const storage: sdk.StorageMode = .immutable;
    id: [8]u8,
    value: u64,
};

comptime {
    const StoreT = sdk.storeFor(Event);
    const store: StoreT = undefined;
    _ = store.load([_]u8{0} ** 8) catch {};
}
