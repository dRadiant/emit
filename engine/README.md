# emit-engine

Standalone binary that imports historical receipts from an Ethereum execution client and follows chain head into a flat log store on disk.

## What it does

`emit-engine` is the producer side of EMIT. It owns the flat log store at `<data-dir>/` and is the only process that ever writes to it. SDK indexers read from the store; the engine is blind to them.

Two operations:

- **Import** historical logs from a Nethermind RocksDB (direct, ~13 min full chain) or a JSON-RPC endpoint (`eth_getLogs` paginator — currently experimental).
- **Follow** the chain head via WebSocket (`eth_subscribe(newHeads)`) with HTTP `eth_getLogs` for catch-up and reorg recovery.

Module index:

| File | Role |
|---|---|
| `main.zig` | CLI: `import`, `follow`, `status` |
| `rocksdb_import.zig` | Nethermind RocksDB → flat store, 7 parallel decode workers |
| `receipt_decoder.zig` | RLP-encoded receipt → `core.RawLog` |
| `rlp.zig` | Minimal RLP walker (~150 lines) |
| `flat_writer.zig` | Append to `blocks.dat` / `blooms.bin` / `blocks.idx` / `meta.bin` |
| `head_follower.zig` | WS newHeads + gap-fill + reorg detection + finalization |
| `pending_ring.zig` | 64-block `pending.bin` ring buffer (atomic tmp + rename per write) |

## CLI

```
Usage: emit-engine <command> [options]

Commands:
  import --rocksdb <path> --data-dir <path>   Bulk import from Nethermind
  import --rpc <url> --data-dir <path>        Import via eth_getLogs (v2)
  follow --rpc <url> [--ws <url>] --data-dir <path> [--catch-up-rpc]
                                              Follow chain head
  status --data-dir <path>                    Print store status
```

Build:

```sh
zig build engine -Doptimize=ReleaseFast
./zig-out/bin/emit-engine status --data-dir /var/lib/emit-engine
```

## Node requirements

Nethermind is recommended for the RocksDB direct-import path. Required sync flags:

```
--Sync.DownloadBodiesInFastSync true
--Sync.DownloadReceiptsInFastSync true
--Sync.NonValidatorNode true
```

Bodies are required because receipts depend on them. Receipts are required for log reads. `NonValidatorNode` reduces overhead.

For `follow`, any execution client serving `eth_subscribe(newHeads)` + `eth_getLogs` works — Geth, Reth, Erigon, Nethermind. Pair with any consensus client (Lighthouse recommended at ~200 GB storage).

## Output layout

Five files written to `--data-dir`:

| File | Purpose | Mutability |
|---|---|---|
| `blocks.dat` | LZ4-compressed log entries, append-only | Immutable after write |
| `blooms.bin` | Per-block topic + address blooms (1280 B/block) | Immutable after write |
| `blocks.idx` | `block_number → (offset u64, length u32)` dense array | Immutable after write |
| `meta.bin` | Checkpoint: `(first_block, latest_block, total_logs, file_sizes, crc32)` | Atomic rename per commit |
| `pending.bin` | Last 64 pre-finality blocks, reorg buffer | Atomic rename per write |

Total at mainnet chain tip: ~249 GB.

## What engine does NOT do

- No handlers, no entity stores, no manifest parsing.
- No API serving, no query interface, no GraphQL.
- No SDK linkage. **Engine never imports SDK.**

Indexers built with `sdk` read the flat store via `core.FlatStoreReader` and watch `pending.bin` via inotify. The engine is unaware of them.

## Performance

| Operation | Time | Notes |
|---|---|---|
| Import (RocksDB direct) | 13 min | full mainnet, 7 parallel decode workers |
| Head-follow latency | <1s | local WebSocket, gap-fill via HTTP `eth_getLogs` |
| Reorg recovery | <1s | bounded by `pending.bin` walk depth (64 blocks max) |

Measured on Hetzner i7-8700, 64 GB DDR4, Gen3 NVMe RAID0. The import path is bound by Nethermind RocksDB throughput plus `core/io_pipeline.zig`'s io_uring batch reads — Linux-only for the fast path.

## License

AGPL-3.0. See [LICENSE](../LICENSE).
