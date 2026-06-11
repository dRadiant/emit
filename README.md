# EMIT

Event Materialization & Indexing Toolkit

![EMIT building a filtered index over rETH's full history, then replaying 1.25M logs in under 3s](docs/assets/demo.gif)

*Full rETH backfill over the Engine's flat store (~15s, the Merge to chain tip), then a warm handler re-run replaying 1.25M logs in under 3s. Real timings, recorded against a live mainnet node.*

## What is EMIT?

EMIT is an EVM indexer rooted in a no compromise ethos of free software. Achieving frontier performance, without compromising on freedom.

EMIT operates under the principle of owning and managing your own infrastructure, and by doing so, reaches unprecedented levels of performance, reliability, and efficiency.

Self-hosted, node-agnostic, written in Zig with no managed dependencies. 

EMIT has two components: the **Engine** and the **Indexer**.

The EMIT Engine is responsible for materializing historical logs into a central flat store after ingesting them from an Ethereum execution client, and then following chain head. 

EMIT Indexers then read that store and produce whatever entities your application needs.

## Why EMIT?

- **Self-hosted.** No API keys, no rate limits, own your data. Single static binary plus a Zig library.
- **Fast.** According to our benchmarks, EMIT is currently the fastest EVM indexer, and by a wide margin. It also has the smallest footprint.
- **Events-first.** All time-varying state derives from events. No archive node, no historical `eth_call`. Balances, prices, positions all reconstruct from logs. The lack of historical `eth_call` is intentional as it constrains the developer into following indexer [best practices](https://thegraph.com/docs/en/subgraphs/best-practices/avoid-eth-calls/) while also making the system lighter.
- **Iterate quickly.** Build a filtered index over the Engine's flat store once (~10s for rETH), then iterate handlers against the filtered index (<1s per re-run). Most development cycles become instant and changes are visible in seconds, not hours.
- **Node-agnostic.** Anything that serves `eth_subscribe(newHeads)` and `eth_getLogs` works. Nethermind is recommended for the RocksDB direct-import path; Geth, Reth, and Erigon work via RPC.
- **Multi-chain by composition.** One engine per chain, one indexer binary per chain. Cross-chain joins are application-level.
- **You own the API.** EMIT fills entity stores. Your choice is to serve them via REST, GraphQL, WebSocket, or raw memory map.

Currently requires the Execution Client, Engine, and Indexer to be collocated on the same machine. This will change with the introduction of remote engine TCP streaming in v1.1.0, allowing indexers to be remote.

## Quickstart

Prerequisites: a synced Nethermind execution client with bodies + receipts (see [engine/README.md](engine/README.md) for required flags), plus Zig 0.15.2. 

It is also recommended to host EMIT on Linux for optimal performance (io_uring, fadvise, inotify).

```sh
# 1. Clone and build the engine.
git clone https://github.com/dradiant/emit
cd emit
zig build -Doptimize=ReleaseFast

# 2. Import historical logs from a local Nethermind RocksDB (~13 min full chain).
#    --rpc lets the importer resolve the canonical receipt row for the rare
#    blocks that kept reorg-orphan duplicates (see engine/README.md).
./zig-out/bin/emit-engine import \
  --rocksdb /var/lib/nethermind/nethermind_db/mainnet/receipts \
  --data-dir /var/lib/emit-engine \
  --rpc http://localhost:8545

# 3. Follow the chain head into the same store.
./zig-out/bin/emit-engine follow \
  --rpc http://localhost:8545 \
  --ws ws://localhost:8545 \
  --data-dir /var/lib/emit-engine

# 4. In another shell, run the ERC20 example against rETH.
cd examples/erc20
zig build run -Doptimize=ReleaseFast -- \
  --engine-data-dir /var/lib/emit-engine \
  --data-dir ./data \
  --follow
```

Expected output from step 4: a per-cycle line like `scan: 124,500 blocks / filtered: 8,213 / handler: 1,082,996 events`, then once caught up, a steady tick as new blocks arrive. See [examples/erc20/README.md](examples/erc20/README.md) for the entity layout and [examples/uniswap-v2/README.md](examples/uniswap-v2/README.md) for the factory-contract walkthrough.

For a fully featured example with a REST API, see [examples/erc20-api/README.md](examples/erc20-api/README.md).

If you prefer Docker, see [engine/README.md](engine/README.md#container-deployment) for the `compose.node.yml` + `compose.emit.yml` operator flow.

### Remote indexing (v1.1)

To run an indexer on a different machine from the Engine, serve the flat store over TCP instead of reading it from disk:

```sh
# On the Engine host: stream server-side-filtered blocks. Binds localhost by
# design — tunnel in from elsewhere; --listen 0.0.0.0:9090 is opt-in.
./zig-out/bin/emit-engine serve \
  --listen 127.0.0.1:9090 \
  --data-dir /var/lib/emit-engine

# On the indexer host: forward the port over SSH, then point the indexer at it.
ssh -N -L 9090:127.0.0.1:9090 engine-host &
cd examples/erc20
zig build run -Doptimize=ReleaseFast -- \
  --remote-engine 127.0.0.1:9090 \
  --data-dir ./data \
  --follow
```

See [engine/README.md](engine/README.md) for `serve` options such as `--max-connections`.

## Benchmarks

A major deciding factor of choosing your infrastructure tooling is performance. Which is why EMIT was designed for efficiency. The goal is maximal performance, with the smallest footprint possible.

EMIT is designed to run on modest hardware. The minimum requirements are 2 TB of SSD (preferably NVMe), 32 GB of DDR4 RAM, and a processor with at least 12 threads. Since EMIT operates close to hardware limits, it also benefits from better hardware (Gen 4/5 NVMe, DDR5, etc.).

Below is listed the measured benchmarks from EMIT deployed on varying Hetzner dedicated server configurations.

$50/month Hetzner dedicated server (i7-8700, 64GB DDR4, 2x 1TB Gen3 NVMe + Software RAID0):

**Operations**

| Operation                    | Time           | Notes                                  |
|------------------------------|----------------|----------------------------------------|
| One-time Import (RocksDB direct)      | 13 min         | full (post-merge) mainnet, 7 parallel workers       |
| Filter build (rETH)          | 10s            | 7 io_uring workers, QD=112             |
| Wall-to-wall backfill (rETH) | 11s            | filter + handler                       |
| Wall-to-wall backfill (LBTC) | 7s             | Transfer-only                          |
| Handler re-run               | <1s            | filtered index hot in page cache       |
| Handler throughput           | 2M events/s    | peak, ERC20                            |
| Head-follow latency          | <1s            | local WebSocket + inotify              |

**vs Envio**

| Workload       | EMIT  | Envio   | Speedup    |
|----------------|-------|---------|------------|
| rETH backfill  | 11s   | 51s     | 4.7×       |
| LBTC backfill  | 7s    | 119s    | 17×        |
| Handler re-run | <1s   | 50-100s | 50-100×    |

$170/month Hetzner dedicated server (i9-13900, 128GB DDR5 ECC, 2x 2TB Gen4 U.2 NVMe + Software RAID0):

**TBD**

> Measured warm: sync && echo 3 > /proc/sys/vm/drop_caches → one cold run to warm the page cache → mean of three timed warm runs. ReleaseFast build.

> Execution + consensus client sync time is not included in the benchmark; expect 4-12 hours depending on hardware and network.

## Status

**v1.1.0** EMIT is not yet fully production ready, and is still under active development. Use at your own risk, and expect breaking changes.

## Packages

| Package | Role | Imports |
|---|---|---|
| [`core/`](core/README.md) | Shared primitives: types, bloom filter, flat-store reader, io_uring pipeline | (none) |
| [`engine/`](engine/README.md) | Standalone binary: import historical receipts, follow chain head, serve filtered streams to remote indexers | `core` |
| [`sdk/`](sdk/README.md) | Zig library: manifest validation, filtered index, handler dispatch, entity stores | `core` |
| [`examples/`](examples/README.md) | A collection of example indexers (ERC20, Uniswap V2) | `sdk` |

**Engine never imports SDK; SDK never imports Engine.** They share data via the flat log store on disk, read through `core`.

## Architecture

Four independent processes connected only via the filesystem:

```
Nethermind + Lighthouse (Execution + Consensus Clients)  →  emit-engine  →  Indexer A, Indexer B, ...
                                                  (flat log store + pending ring)
```

The execution client owns receipts. The Engine owns the flat log store (immutable post-finality). Each Indexer owns its entity store. Any one can be stopped, replaced, or scaled without touching the others. Read-only API replicas spawn by opening the entity store at a path; concurrent readers are native.

As of v1.1, Indexers need not be collocated with the Engine. `emit-engine serve` streams server-side-filtered blocks over TCP, so an Indexer can run on a different machine — reached through an SSH tunnel — and stay byte-for-byte identical to a collocated one (backfill, live follow, and reorg recovery all run over the wire).

Full architectural detail in `docs/`. Decision records in [docs/adr/](docs/adr/).

## Migrating from The Graph, Envio, Ponder, or Subsquid

If you have a subgraph manifest or a TypeScript indexer config, the mapping to EMIT is direct: your manifest becomes `manifest.zig`, your event handlers become methods on a Zig struct in `handlers.zig`, your schema becomes Zig struct definitions in `entities.zig`. The two-stage backfill replaces the streaming model — you write the same handler logic, but you can re-run it in under a second against the local filtered index instead of replaying from a remote source.

A dedicated migration guide is planned for the future. Until then, the example indexers ([erc20](examples/erc20/), [uniswap-v2](examples/uniswap-v2/)) are the canonical reference for what an EMIT indexer looks like.

## Documentation

- `docs/` — contains documentation, architecture, and technical documents. (TBA)
- [`docs/adr/`](docs/adr/) — architectural decision records (pending block storage, filtered-index format, SDK storage, ethcall strictness)
- [`core/README.md`](core/README.md), [`engine/README.md`](engine/README.md), [`sdk/README.md`](sdk/README.md) — per-package orientation
- [`examples/`](examples/) — runnable reference indexers with their own READMEs
- [`CHANGELOG.md`](CHANGELOG.md) — release history

## License

This repository is licensed under **AGPL-3.0.** We believe in free and open source software, and any such derivative work must be made available under the same license.

Proprietary software is not cypherpunk.

## Contributing

Issue reports, feature discussions, and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, commit conventions, and review norms.

## Acknowledgements

`StrobeLabs/eth.zig` - Ethereum client library

`jedisct1/zig-lz4` - Implementation of LZ4

`Syndica/rocksdb-zig` - Zig RocksDB Bindings used in the importer tool.

`Nethermind` - Stable and Well Documented Execution Client

`open-indexer-benchmark` - Reference Indexer Implementations and Benchmarks

`TheGraph` - The original Ethereum Indexer

`Envio` - Their proprietary HyperSync data lake and previously industry-leading speeds set a reference point to beat.
