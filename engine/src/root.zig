/// emit engine — imports EVM logs into the flat log store and follows chain head.
///
/// Standalone binary that runs alongside an Ethereum node. Reads receipts
/// (via RocksDB direct or eth_getLogs), writes blocks.dat/blooms.bin/blocks.idx/meta.bin.
/// Handles reorgs via pending.bin (atomic rewrite, see ADR-001).
/// Knows nothing about handlers, entities, or manifests — that's the sdk's domain.
pub const flat_writer = @import("flat_writer.zig");
pub const rlp = @import("rlp.zig");
pub const pending_ring = @import("pending_ring.zig");
pub const receipt_decoder = @import("receipt_decoder.zig");
pub const head_follower = @import("head_follower.zig");
pub const rpc_import = @import("rpc_import.zig");

// rocksdb_import is not included here — it depends on the rocksdb lazy dep
// and is compiled separately via `zig build import`. Tests for its decode
// logic live in receipt_decoder.zig (no rocksdb dependency).

test {
    _ = flat_writer;
    _ = rlp;
    _ = pending_ring;
    _ = receipt_decoder;
    _ = head_follower;
    _ = rpc_import;
}
