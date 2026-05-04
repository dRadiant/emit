/// emit engine — imports EVM logs into the flat log store and follows chain head.
///
/// Standalone binary that runs alongside an Ethereum node. Reads receipts
/// (via RocksDB direct or eth_getLogs), writes blocks.dat/blooms.bin/blocks.idx/meta.bin.
/// Handles reorgs via a 64-block pending ring buffer. Knows nothing about
/// handlers, entities, or manifests — that's the sdk's domain.
pub const flat_writer = @import("flat_writer.zig");
pub const rlp = @import("rlp.zig");
pub const pending_ring = @import("pending_ring.zig");

test {
    _ = flat_writer;
    _ = rlp;
    _ = pending_ring;
}
