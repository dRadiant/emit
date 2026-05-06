// expected: AppendStore.load is not supported: cannot load append-only entities during backfill
//
// load() on an AppendStore is a CachedStore/AppendStore mixup. The SDK
// catches it at compile time.

const sdk = @import("sdk");

const Event = struct {
    id: [8]u8,
    value: u64,
};

comptime {
    const S = sdk.AppendStore(Event);
    const store: S = undefined;
    _ = store.load(undefined, [_]u8{0} ** 8) catch {};
}
