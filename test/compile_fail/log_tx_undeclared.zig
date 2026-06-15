// expected: type 'void' does not support field access
//
// `log.tx` exists only for events declaring `pub const tx_fields = true;`.
// Reading it on an undeclared event is a compile error, never a runtime null.

const sdk = @import("sdk");

const Transfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
};

export fn probe() u8 {
    var log: sdk.Log(Transfer) = undefined;
    _ = &log;
    return log.tx.from[0];
}
