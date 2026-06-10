# emit-engine

Standalone binary that imports historical receipts from an Ethereum execution client, follows chain head into a flat log store on disk, and streams server-side-filtered blocks to remote indexers.

## What it does

`emit-engine` is the producer side of EMIT. It owns the flat log store at `<data-dir>/` and is the only process that ever writes to it. SDK indexers read from the store; the Engine is blind to them.

Three operations:

- **Import** historical logs from a Nethermind RocksDB (direct, ~13 min full chain) or a JSON-RPC endpoint (`eth_getLogs` paginator — currently experimental).
- **Follow** the chain head via WebSocket (`eth_subscribe(newHeads)`) with HTTP `eth_getLogs` for catch-up and reorg recovery.
- **Serve** server-side-filtered blocks to off-host indexers over TCP, so an indexer need not be collocated with the Engine (v1.1).

Module index:

| File | Role |
|---|---|
| `main.zig` | CLI: `import`, `follow`, `serve`, `status` |
| `rocksdb_import.zig` | Nethermind RocksDB → flat store, 7 parallel decode workers |
| `receipt_decoder.zig` | RLP-encoded receipt → `core.RawLog` |
| `rlp.zig` | Minimal RLP walker (~150 lines) |
| `flat_writer.zig` | Append to `blocks.dat` / `blooms.bin` / `blocks.idx` / `meta.bin` |
| `head_follower.zig` | WS newHeads + gap-fill + reorg detection + finalization |
| `pending_ring.zig` | 64-block `pending.bin` ring buffer (atomic tmp + rename per write) |
| `tcp_server.zig` | TCP `serve`: REGISTER → filtered backfill + live stream, bounded worker pool (v1.1) |

## CLI

```
Usage: emit-engine <command> [options]

Commands:
  import --rocksdb <path> --data-dir <path> [--rpc <url>]
                                              Bulk import from Nethermind
  import --rpc <url> --data-dir <path>        Import via eth_getLogs (v2)
  follow --rpc <url> [--ws <url>] --data-dir <path> [--catch-up-rpc]
                                              Follow chain head
  serve [--listen <host:port>] [--max-connections <n>] --data-dir <path>
                                              Stream filtered blocks to remote indexers
  status --data-dir <path>                    Print store status
```

Pass `--rpc` to the RocksDB import so it can resolve the canonical receipt row
for blocks that carry reorg-history duplicates. Nethermind keys receipts by
`blockNumber + blockHash` and keeps the orphaned rows of reorged-out blocks even
past finality; the importer needs the canonical hash (one `eth_getBlockByNumber`
per duplicate, rare) to keep the right one. Without `--rpc` it fast-paths
single-row blocks and fails loud on a duplicate rather than guessing.

`serve` opens a TCP listener (default `127.0.0.1:9090`) and streams server-side-
filtered blocks to SDK indexers running off-host. It binds localhost by design —
remote clients reach it through an SSH tunnel; `--listen 0.0.0.0:9090` is opt-in.
A bounded worker pool (`--max-connections`, default 16) serves indexers
concurrently. A streamed indexer is byte-for-byte identical to a collocated one;
the client side is configured with the SDK's `--remote-engine host:port`. The
process is independent of `import`/`follow` — point it at the same `--data-dir`.

Build:

```sh
zig build -Doptimize=ReleaseFast
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

## Container deployment

A multi-stage `Dockerfile` at the repo root builds a static `emit-engine` binary (musl, scratch runtime, ~11 MB image). Two compose files split the lifecycles:

| File | Role |
|---|---|
| [`compose.node.yml`](../compose.node.yml) | Nethermind + Lighthouse, always-on. Mirrors the production Hetzner setup. |
| [`compose.emit.yml`](../compose.emit.yml) | One-shot `emit-engine-import` (gated by the `import` profile), long-running `emit-engine` follower, and an opt-in `emit-engine-serve` TCP server for remote indexers (gated by the `serve` profile). |

Operator flow:

```sh
# 1. Start the node stack. Wait for Nethermind to fully sync before continuing.
docker compose -f compose.node.yml up -d
curl -s -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}' \
  http://localhost:8545
# repeat until result is `false`

# 2. One-time historical import (~13 min on the reference Hetzner box).
docker compose -f compose.emit.yml --profile import up emit-engine-import

# 3. Start the long-running follower.
docker compose -f compose.emit.yml up -d emit-engine

# 4. (Optional, v1.1) Serve remote indexers. Binds the host's localhost;
#    reach it from another machine with `ssh -N -L 9090:127.0.0.1:9090 <host>`.
docker compose -f compose.emit.yml --profile serve up -d emit-engine-serve
```

Environment overrides:

- `NETHERMIND_DATA_DIR` (default `./data/nethermind`) — host path mounted into Nethermind at `/data` and into the importer at `/nethermind-data:ro`.
- `LIGHTHOUSE_DATA_DIR` (default `./data/lighthouse`) — host path mounted into Lighthouse at `/root/.lighthouse`.
- `EMIT_DATA_DIR` (default `./data/emit-engine`) — host path mounted into the engine at `/var/lib/emit-engine`. SDK indexers on the host read this same path.

## Output layout

Five files written to `--data-dir`:

| File | Purpose | Mutability |
|---|---|---|
| `blocks.dat` | LZ4-compressed log entries, append-only | Immutable after write |
| `blooms.bin` | Per-block topic + address blooms (1280 B/block) | Immutable after write |
| `blocks.idx` | `block_number → (offset u64, length u32)` dense array | Immutable after write |
| `meta.bin` | Checkpoint: `(first_block, latest_block, total_logs, file_sizes, crc32)` | Atomic rename per commit |
| `pending.bin` | Last 64 pre-finality blocks + per-block timestamp, reorg buffer (`EMITPEND` magic) | Atomic rename per write |
| `timestamps.bin` | Exact per-block Unix timestamp (`u32 LE`, dense by block), backfilled from the `headers` DB during import and extended by the follower on finalization | Advisory; absent ⇒ formula fallback |

Total at mainnet chain tip: ~249 GB.

## What engine does NOT do

- No handlers, no entity stores, no manifest parsing.
- No API serving, no query interface, no GraphQL.
- No SDK linkage. **Engine never imports SDK.**

Indexers built with `sdk` read the flat store via `core.FlatStoreReader` and watch `pending.bin` via inotify. The Engine is unaware of them.

## Performance

| Operation | Time | Notes |
|---|---|---|
| Import (RocksDB direct) | 13 min | full mainnet, 7 parallel decode workers |
| Head-follow latency | <1s | local WebSocket, gap-fill via HTTP `eth_getLogs` |
| Reorg recovery | <1s | bounded by `pending.bin` walk depth (64 blocks max) |

Measured on Hetzner i7-8700, 64 GB DDR4, Gen3 NVMe RAID0. The import path is bound by Nethermind RocksDB throughput plus `core/io_pipeline.zig`'s io_uring batch reads — Linux-only for the fast path.

## License

AGPL-3.0. See [LICENSE](../LICENSE).
