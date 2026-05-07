// expected: AppendStore.load is not supported: cannot load append-only entities during backfill
//
// load() on the store backing an `sdk.appendOnly(T)` entity is a
// CachedStore/AppendStore mixup. The SDK catches it at compile time.

const sdk = @import("sdk");

const Event = struct {
    id: [8]u8,
    value: u64,
};

comptime {
    const Marker = sdk.appendOnly(Event);
    const store: Marker.Store = undefined;
    _ = store.load([_]u8{0} ** 8) catch {};
}
