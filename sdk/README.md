# emit-sdk

Zig library for writing typed event indexers against an emit-engine flat log store.

## What it does

`emit-sdk` is the consumer side of EMIT. It reads the engine's flat log store, materializes a per-manifest filtered index, runs comptime-dispatched handlers against the filtered logs, and persists entities to a pure-Zig flat store. The two-stage pipeline (filter build then handler replay) is why sub-second handler re-runs are possible.

Module index:

| File | Role |
|---|---|
| `root.zig` | Public re-exports, `address()`, `concat()`, `storeFor()` helpers |
| `entry.zig` | `sdk.run` / `sdk.init`, `Context`, `Options`, `RunStats`, commit cycle |
| `manifest.zig` | `Manifest`, `ContractDef`, `FactoryDef`, `PrefetchDef`, `StaticCall` |
| `abi_parse.zig` | Parse Solidity signatures → param table, named-arg lookups |
| `entity_serial.zig` | Comptime serialize/deserialize for entity structs (BE key + LE fields) |
| `filter_builder.zig` | Two-bucket filtered index over `core.scanBloomsParallel` |
| `filtered_store.zig` | Paired `.dat` (LZ4) + `.idx` (dense `(block, offset, length)`) flat files |
| `scanner.zig` | Cursor walk over filtered index, comptime topic0 dispatch |
| `handler.zig` | `DecodedLog`, `Log(E)`, dispatcher generation |
| `mutable_store.zig` | `MutableStore(T)`: HashMap + sorted slab + binary search |
| `immutable_store.zig` | `ImmutableStore(T)`: append-only with `load()` as `@compileError` |
| `event_log.zig` | `EventLog(T)` backing file for ImmutableStore |
| `state_snap.zig` | `state.snap` — atomic commit point for cursor + all stores |
| `ethcall.zig` | Strict `eth_call` cache, comptime-keyed by method signature |
| `prefetch.zig` | Per-event Multicall3 prefetch (static + per-log + per-block) |
| `humanize.zig` | `Amount` formatter, `blockTimestamp()` derivation |
| `live.zig` | Live head-following loop, inotify wakeup, reorg classification |
| `fake_engine.zig` | Test fixture: pure-Zig `pending.bin` + `meta.bin` writer |

## Usage

```zig
const sdk = @import("emit-sdk");
const e = @import("entities.zig");

pub fn main() !void {
    try sdk.run(
        @import("manifest.zig").config,
        @import("handlers.zig"),
        .{ e.Account, e.Allowance, e.Transfer, e.Approval },
        .{
            .engine_data_dir = "/var/lib/emit-engine",
            .data_dir = "./data",
            .node_rpc = "http://localhost:8545",
        },
    );
}
```

Four arguments: manifest (concrete types), handler (a struct exposing `handleTransfer`, `handleApproval`, etc.), entities tuple (each declares `pub const storage: sdk.StorageMode = .mutable | .immutable`), runtime options. Comptime validation rejects missing handler methods, malformed manifests, and entity types without a declared storage mode.

## Public surface

Re-exported from `root.zig`:

- **Entry:** `run`, `init`, `Context`, `Options`, `RunStats`, `StorageMode`
- **Manifest:** `Manifest`, `ContractDef`, `FactoryDef`, `PrefetchDef`, `PrefetchCall`, `StaticCall`, `AddressSource`
- **Handler:** `DecodedLog`, `Log`
- **Helpers:** `address` (comptime hex → `[20]u8` with EIP-55 check), `concat`, `storeFor`, `Amount`, `amount`
- **Validation:** `validateHandler`, `validateManifest`
- **Constants:** `DEFAULT_BATCH_SIZE`

See [examples/erc20/](../examples/erc20/) for a working four-file indexer and [examples/uniswap-v2/](../examples/uniswap-v2/) for the factory pre-pass pattern.

## What sdk does NOT do

- No log import — that's the engine's job. SDK reads the engine's output via `core.FlatStoreReader`.
- No API serving — entity stores are flat files; serve them however you want.
- No historical `eth_call` — `Context.ethCall` is strict cache-only, populated by the prefetch declared in the manifest. Events-first by design.
- No engine linkage. **SDK never imports engine.**

## Events-first contract

All time-varying state must derive from events. `Context.ethCall` returns immutable metadata only (`decimals()`, `symbol()`, `name()`, `factory()`, etc.) — cached permanently on first read. This is not a limitation; it's a design choice to avoid requiring an archive node, and also increasing throughput through barring poor indexer design. ERC20 balances reconstruct from `Transfer` events; Uniswap V2 prices reconstruct from `Swap` events; stETH balances reconstruct from `TransferShares` + `TokenRebased`.

## Build and test

```sh
zig build test-sdk --summary all
```

176 tests covering manifest validation, ABI parsing, entity serialization, store round-trips, filter build, scanner, prefetch, live head-following.

## Performance

| Operation | Number | Notes |
|---|---|---|
| Handler throughput | 2M+ events/s | peak, ERC20 |
| Filter build (rETH) | 10s | 7 io_uring workers via `core` |
| Handler re-run | <1s | filtered index hot in page cache |
| Head-follow tick latency | <1s | local engine, inotify wakeup on `pending.bin` |

The live head-follower (`live.zig`) uses Linux inotify on the engine's data directory to wake on `pending.bin` rename events. On non-Linux hosts the inotify path stubs to no-op and live mode degrades to a periodic re-check loop.

## License

AGPL-3.0. See [LICENSE](../LICENSE).
