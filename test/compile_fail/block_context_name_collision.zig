// expected: both derive store field name 'items'. Rename one of the entity types.
//
// BlockContext rejects two entity types whose basenames lowercase-collide.

const sdk = @import("sdk");

const Holder1 = struct {
    pub const Item = struct { id: [20]u8, value: u64 };
};

const Holder2 = struct {
    pub const Item = struct { id: [8]u8, count: u32 };
};

comptime {
    _ = sdk.BlockContext(.{
        sdk.mutable(Holder1.Item),
        sdk.mutable(Holder2.Item),
    });
}
