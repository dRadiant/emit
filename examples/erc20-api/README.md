# erc20-api

The [`erc20`](../erc20) indexer (rETH on mainnet), **served over HTTP from the
same process**. This is a full end-to-end example of EMIT's in-process serving model, similar to
the traditional indexer (Ponder, Envio, Subsquid, etc.) shape, where the indexing service answers its own
queries at the chain head, reorg-aware.

It demonstrates the SDK's in-process read surface on the `Context` returned by
`sdk.spawn`:

| Accessor | Used for | Store kind |
|---|---|---|
| `ctx.read(T, key)` | balance, allowance | mutable point read |
| `ctx.count(T)` / `ctx.range(T, start, out)` | transfer pagination | immutable event log |
| `ctx.cursor()` | last indexed block | progress |

Every accessor takes the Context lock internally, so this example never touches
a mutex. Reads see the live, tip-inclusive (pending-overlay), reorg-aware state.

## How it works

`sdk.spawn` backfills the rETH manifest, then runs the live follow loop on a
background thread and returns a `*Context`. The HTTP layer [http.zig](https://github.com/karlseguin/http.zig), reads that Context per request. `manifest.zig`,
`handlers.zig`, and `entities.zig` are identical to `examples/erc20`; `main.zig` is the new API layer.

## Endpoints

| Method | Path | Returns |
|---|---|---|
| GET | `/health` | `{ last_indexed_block, following, transfers, approvals }` |
| GET | `/account/:addr` | `{ address, balance }` — unknown address is balance `"0"` |
| GET | `/allowance/:owner/:spender` | `{ owner, spender, value }` |
| GET | `/transfers?limit=&offset=` | newest-first page `{ total, count, transfers: [{ block, from, to, value }] }` |

`limit` defaults to 20 (capped at 100); `offset` pages backward from the tip.
`u256` values are JSON strings (decimal); addresses are `0x`-hex. The block
number is decoded from the packed event key via `sdk.EventId.unpack`.

## Run

```bash
zig build -Doptimize=ReleaseFast run -- \
  --engine-data-dir /var/lib/emit-engine \
  --data-dir ./data \
  --node-rpc http://localhost:8545 \
  --port 8080
```

`--engine-data-dir`, `--data-dir`, and `--node-rpc` match the other examples;
`--port` (default 8080) is API-only. Then:

```bash
curl localhost:8080/health
curl localhost:8080/account/0xae78736Cd615f374D3085123A210448E74Fc6393
curl 'localhost:8080/transfers?limit=10'
```

## Why in-process (vs. an external reader)

EMIT supports two serving topologies. This example is the **in-process** one:
tip-fresh, reorg-aware, the indexer answers its own queries. The alternative is
an **external read-only replica** that opens the committed `state.snap` +
`events.dat` (finalized, ~64 blocks behind) — see
[`examples/readers`](../readers). Use in-process for head-fresh APIs; use
external replicas in scenarios that don't require tip-freshness.
