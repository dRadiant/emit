// expected: both derive store field name 'items'. Rename one of the entity types.
//
// sdk.Context rejects two entity types whose basenames lowercase-collide.

const sdk = @import("sdk");

const Holder1 = struct {
    pub const Item = struct {
        pub const storage: sdk.StorageMode = .mutable;
        id: [20]u8,
        value: u64,
    };
};

const Holder2 = struct {
    pub const Item = struct {
        pub const storage: sdk.StorageMode = .mutable;
        id: [8]u8,
        count: u32,
    };
};

comptime {
    _ = sdk.Context(.{ Holder1.Item, Holder2.Item });
}
