# uniswap-v2 — reference factory indexer

Tracks Uniswap V2 `Pair` reserves and `Swap` events across every pair the canonical factory ever spawned. Demonstrates the factory pre-pass pattern: a one-pass scan discovers child addresses from `PairCreated` events, then feeds them into the main scan as the dynamic address set.

Also demonstrates transaction fields: `Swap` declares `pub const tx_fields = true;`, so its handler reads `log.tx.from`. This is the tx-sender, and usually the trader. Requires the engine store's `txs.{dat,idx}` (written by default on import and follow); init fails loud if missing.

## Run

```sh
cd examples/uniswap-v2
zig build run -Doptimize=ReleaseFast -- \
  --engine-data-dir /var/lib/emit-engine \
  --data-dir ./data \
  --node-rpc http://localhost:8545 \
  --follow
```

`--node-rpc` is required for the static prefetch of token metadata (`decimals()`, `symbol()`, `name()` on WETH and USDC) — the SDK batches these through Multicall3 once during Phase 4 and caches them permanently. Handlers then read via `ctx.ethCall(...)` with zero subsequent network calls.

## Entities produced

| Entity | Storage | Key | Updated on |
|---|---|---|---|
| `Pair` | mutable | `[20]u8` (pair contract address) | every `Sync` (latest reserves) |
| `SwapEvent` | immutable | `[16]u8` (block ++ tx ++ log index) | every `Swap` log, with `trader = log.tx.from` |

Intentionally minimal — `PairCreated` is used only for factory discovery (registers child addresses, no entity row), and `Mint` / `Burn` handlers are no-op stubs present for SDK completeness. Adapt the example to materialize additional events by adding the corresponding entity types and filling in the stub handlers.

## Files

- [`src/manifest.zig`](src/manifest.zig) — factory address, child events, static prefetch declarations
- [`src/handlers.zig`](src/handlers.zig) — `handlePairCreated`, `handleSync`, `handleSwap`, plus no-op `handleMint` / `handleBurn`
- [`src/entities.zig`](src/entities.zig) — `Pair`, `SwapEvent`
- [`src/main.zig`](src/main.zig) — wires the four pieces into `sdk.run(...)`

## Performance

| Phase | Time | Notes |
|---|---|---|
| Factory pre-pass | ~1s | one-pass scan for `PairCreated`, discovers ~400K pairs |
| Filter build | ~25s | wider address set than ERC20; ~400K child addresses |
| Handler replay | ~1s | per-pair reserve updates + per-Swap append |
| Wall-to-wall backfill | ~30s | factory pre-pass + filter + handler |

Ran on the same Hetzner i7-8700 instance with prefetch enabled (`prefetch_ns == 0` on warm re-runs — every `ctx.ethCall` hits the cache). The factory-contract case is the stress test for the SDK's two-stage pipeline: it exercises dynamic address registration, prefetch batching, and per-pair mutable updates simultaneously.
