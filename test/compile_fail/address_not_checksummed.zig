// expected: is not EIP-55 checksummed. Use '0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045'.
//
// A manifest address must carry its EIP-55 checksum so a typo fails the build
// instead of silently matching no logs. All-lowercase is rejected; the error
// hands back the checksummed form to paste.

const sdk = @import("sdk");

export fn probe() void {
    _ = sdk.address("0xd8da6bf26964af9d7eed9e03e53415d37aa96045");
}
