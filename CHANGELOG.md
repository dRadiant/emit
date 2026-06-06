# Changelog

All notable changes to EMIT are documented here.

EMIT follows semantic versioning. Major bumps (1.x → 2.x) signal breaking changes. Minor bumps (1.0 → 1.1) add features in a backward-compatible way, but may contain breaking changes. Patch bumps (1.0.0 → 1.0.1) are bug fixes only.

## EMIT 1.1.0 (unreleased)

### SDK

- Symmetric in-process reads: the `Context` returned by `sdk.spawn`/`init` can now read both store kinds, so an in-process API serves the live, tip-inclusive, reorg-aware state. `Context.read(T, key)` does a point read for either kind — a `MutableStore`'s tip-fresh state, or an `ImmutableStore`'s live log (finalized records plus the pending overlay). Previously only mutable point reads were exposed; immutable stores were unreadable in-process despite the overlay living in memory.
- `ImmutableStore(T)` gains overlay-aware ordered reads — `count()`, `get(key)`, and `range(start, out)` — surfaced on the Context as `Context.count(T)` and `Context.range(T, start, out)`. `range` orders the bounded overlay once per call (not per record) and spans the finalized log and the live overlay; build a newest-first page with `start = count(T) - n`. These are native to the append-only log; `Context.count`/`range` are a compile error for mutable entities, which are point-read by key via `read`.
- `Context.cursor()` returns the highest fully-dispatched (committed) block for an honest health/progress readout.
- `sdk.EventId` is the canonical (un)packer for the 16-byte event key: `EventId.unpack(id)` decodes a stored key into `(block_number, tx_index, log_index)`, and `EventId{ ... }.pack()` builds a key from components. `log.eventId()` is now defined as `EventId{ ... }.pack()`, so encode and decode live in one type.
- All read accessors take the Context mutex internally, so API code never manages the lock. They must not be called from handlers, which already hold it.
- Exact block timestamps, historical and live. `ctx.timestamp` (and `humanize.timestampOf`) read the engine's exact per-block time, falling back to the `MERGE_TS + n*12` formula only when a timestamp is unknown. The formula assumes one block per slot; missed slots make it drift by the accumulated miss count — ~10 days at the current chain tip. Historical blocks come from `timestamps.bin`; **live (head-followed) blocks** read the exact header time the engine carries in `pending.bin`. The split is necessary because the SDK mmaps `timestamps.bin` at a fixed size on init and so cannot see blocks the follower appends past the import cutoff — `pending.bin` delivers the exact time for that pre-finalization window. Result: `ctx.timestamp` is exact (0 s error) across both the backfill and the live tip.

### Engine

- `emit-engine import --rpc <url>` is now implemented (was a stub). Unfiltered `eth_getLogs` over adaptively-sized block ranges (grow ×2 on success, halve on a node result/range cap, down to one block with transient-error backoff) decodes into the same `RawLog` shape as the RocksDB path and feeds the existing `flat_writer`. Logs are grouped by block — including empty blocks, so `blocks.idx` stays dense — and out-of-order/out-of-range logs fail loud rather than landing a misgrouped block. Resumes from the next dense block (`--from`/`--to` bound the range; default upper bound is `tip − FINALITY_DEPTH`). Timestamps are populated by a second, **strict** pass: a batched `eth_getBlockByNumber` fills `timestamps.bin`, every block in a chunk must return a timestamp, and a persistent failure **aborts the import loudly** rather than silently degrading to the L1-only formula — essential for L2s, whose block time the formula does not model. The pass has its own resume cursor (the `timestamps.bin` count), so a re-run backfills exactly the missing range and timestamps can be added to an existing logs-only store after the fact. `--no-timestamps` opts out explicitly. Verified against live mainnet: a 1,000-block range (586,486 logs) byte-matches the RocksDB store on every non-empty block, its timestamps are exact (identical to a RocksDB-headers store and to `eth_getBlockByNumber`), and it additionally populates `tx_hash` (which the RocksDB path leaves zeroed) and writes proper dense empty-block entries.
- `emit-engine import --rocksdb` now backfills exact per-block timestamps into `timestamps.bin`, in tandem with the receipts decode. A concurrent pass reads Nethermind's sibling `headers` DB — whose keys are `block_number(8 BE) ++ block_hash(32)`, so it iterates in block order like the receipts CF — and extracts the RLP `timestamp` field (header field 11). Fully offline (no `--rpc`), runs even when the log import is already caught up (so existing stores get backfilled), and resumes from the first un-backfilled block. ~9.7 M timestamps in ~15 s on the reference box.
- The follower keeps timestamps exact past the import. It carries each block's exact header time in `pending.bin` for the live SDK, and mirrors it into `timestamps.bin` on finalization — resuming the importer's file keyed off the flat store's `first_block` — so cold re-backfills over blocks finalized after the import stay exact instead of drifting to the formula.
- `pending.bin` gains an `EMITPEND` magic header and a per-block `u32` timestamp. The magic makes the format change self-healing: an unrecognized `pending.bin` (e.g. one written by an older engine) reads as empty, so the engine re-baselines from `meta.bin` and rewrites it in the current format, and the SDK treats it as no pending blocks. A genuinely truncated current-format file still fails loud.
- `timestamps.bin`: a dense `u32 LE` array indexed by `block - first_block` (~39 MB at the tip; `u32` epoch-seconds is exact until 2106). Advisory — a missing or partial file degrades to the formula, so old stores keep working.

### Examples

- New `examples/erc20-api`: the rETH indexer served over HTTP from the same process via `sdk.spawn` + the Context read surface, using [http.zig](https://github.com/karlseguin/http.zig) (pinned to its `zig-0.15` branch). `GET /account/:addr`, `/allowance/:owner/:spender`, `/transfers?limit=&offset=` (paginated, newest-first), and `/health` (cursor + liveness). The worked demonstration of EMIT's in-process serving model.

## EMIT 1.0.0

Released: 2026-05-29

First release. EMIT is a self-hosted EVM event indexer in Zig with zero managed dependencies, designed to run on modest dedicated hardware and exceed the performance of every comparable indexer.

### Engine

- `emit-engine import` — bulk historical import from a Nethermind RocksDB receipts column family. Seven parallel decode workers, ~13 minutes for full mainnet on the reference benchmark hardware. Pass `--rpc` to resolve the canonical receipt row for blocks that retain reorg-orphan duplicates (Nethermind keys receipts by number+hash and keeps orphans past finality). Every block, including no-log blocks, is appended so the reader's dense block index stays aligned.
- `emit-engine follow` — chain-head follower over WebSocket (`eth_subscribe(newHeads)`) with HTTP `eth_getLogs` gap-fill and reorg recovery. Sub-second head-follow latency against a local node.
- `emit-engine status` — print flat log store metadata.
- 64-block pending ring (`pending.bin`) with atomic tmp + rename writes for crash safety and reorg recovery.
- Minimal pure-Zig RLP walker (~150 lines) for receipt decoding; no RLP library dependency.
- RocksDB import is a separate binary (`zig build import`) so the Engine itself never links the ~20 MB RocksDB C dependency.

### SDK

- `sdk.run` / `sdk.init` / `sdk.spawn` entry points. Comptime-validated entity tuple, handler struct, and manifest. `run` blocks (backfill + follow); `init` returns a caught-up context; `spawn` runs the live loop on a background thread and returns a context for in-process queries.
- Manifest types: `Manifest`, `ContractDef`, `FactoryDef`, `PrefetchDef`, `PrefetchCall`, `StaticCall`. Static contracts, factory pre-pass for dynamically discovered child addresses, and event-driven + address-driven prefetch declarations.
- Two-stage backfill pipeline: filter build (matching blocks via bloom scan, ~10 s for rETH) followed by handler replay (<1 s for warm re-runs). Most development cycles become instant.
- `MutableStore(T)` with HashMap + sorted-slab cold-load binary search; `ImmutableStore(T)` append-only with comptime-rejected `load()`.
- `state.snap` as the atomic commit point — cursor plus all entity-store state in one file, rename-promoted on each commit.
- Strict `ethCall` cache (cache-or-error, per ADR-004). Pre-fetched and batched through Multicall3 during the prefetch phase between filter build and handler replay. Zero RPC calls on re-backfill once warmed.
- Comptime topic0 dispatch — handlers are named `handle<EventName>` and routed by topic without runtime indirection.
- ABI signature parser for canonical event signatures, named-parameter resolution, and `paramByName` lookup.
- Live head-following loop with Linux `inotify` wakeup on `pending.bin` rename events. Sub-second tick latency to the Engine's flat store + pending ring updates. Live dispatch is address-gated — only logs from manifest contracts and runtime-discovered factory children are handled, matching the historical filter.
- In-process serving: `sdk.spawn` runs the follow loop on a background thread; query the live, tip-inclusive (pending-overlay), reorg-aware state via `ctx.read(T, key)`. A Context mutex serializes reads against the loop, so handler code stays lock-free. External read-only replicas on the finalized `state.snap` remain the horizontal read scale-out path.
- Pure-Zig storage throughout — the SDK ships with zero external KV dependency.

### Core

- Bloom filter primitives: `Bloom(SIZE)` and `AddrBloom` comptime generics.
- `FlatStoreReader`: mmap-backed access to `blocks.idx` + `blooms.bin`, `pread` on `blocks.dat`.
- `scanBloomsParallel`: dual bloom check (topic + address) over the entire flat store; rejects 92%+ of blocks before any block read.
- `ReadPipeline(QUEUE_DEPTH)`: io_uring batch reads on Linux with up to 16 outstanding reads per worker (112 total across the SDK's 7-worker filter build).
- `fadvise(WILLNEED)` prefetch hints on matching blocks during the bloom scan; ~30% faster warm runs.
- LZ4 log serialization wrappers; packed wire format for log entries.
- `core.atomic_file` (tmp + fsync + rename) and `core.flat_format` (magic header helpers) as the shared primitives consumed by Engine and SDK alike.
- Cross-thread parallel scaffolding (`run`, `chunkRanges`).
- Linux-only paths gate cleanly on non-Linux hosts; the SDK degrades to plain `pread` and a polling live-mode loop without modification.

### Examples

- `examples/erc20` — reference ERC20 indexer (default target: rETH on mainnet). Tracks per-holder `Account` balances and per-pair `Allowance`; writes immutable `Transfer` and `Approval` event entities.
- `examples/uniswap-v2` — reference factory-contract indexer. Discovers `Pair` addresses via the canonical V2 factory's `PairCreated` events, then tracks `Pair` reserves (via `Sync`) and `SwapEvent` rows. Exercises factory pre-pass, dynamic-address registration, static + per-log prefetch, and Multicall3 batching.
- `examples/readers/{python,c,zig}` — pure std, reference readers that decode `state.snap` and `<entity>.events.dat` without any EMIT dependency. Format spec at `examples/readers/entity-format.md`.

### Documentation

- Root `README.md` plus per-package `README.md` for `core/`, `engine/`, `sdk/`, and per-example READMEs for `examples/erc20/` and `examples/uniswap-v2/`. Each per-package README restates the import boundary: Engine never imports SDK; SDK never imports Engine; both share data via the flat log store through Core.
- `docs/adr/` — four architectural decision records (pending block storage, filtered-index format, SDK storage substrate, ethCall strict prefetch).

### Deployment

- Multi-stage `Dockerfile` cross-compiles `emit-engine` to `x86_64-linux-musl`. Final image: 10.9 MB on `scratch`.
- `compose.node.yml` orchestrates Nethermind + Lighthouse with host networking, shared JWT bootstrap, and the perf-tuning flags that hold up on a 64 GB dedicated server.
- `compose.emit.yml` ships two services: a one-shot `emit-engine-import` (gated by the `import` profile) and a long-running `emit-engine` follower.

### Internals

- Comptime entity serialization — BE encoding for the primary key, LE for everything else. Fixed-size fields only; comptime-rejected variable-length types until v2.
- Compile-fail test harness with 15 cases covering manifest validation, handler dispatch, entity storage-mode declarations, and ABI parameter resolution.
- Cross-language flat-store interop verified: Python, C, and Zig reference readers decode entity files byte-for-byte without an EMIT dependency.
