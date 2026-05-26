# erc20 — reference ERC20 indexer

Tracks `Account` balances and per-pair `Allowance` over `Transfer` and `Approval` events. Targets rETH (Rocket Pool) on mainnet by default; retargetable to any ERC20 by changing the address in `manifest.zig`.

## Run

```sh
cd examples/erc20
zig build run -Doptimize=ReleaseFast -- \
  --engine-data-dir /var/lib/emit-engine \
  --data-dir ./data \
  --follow
```

Expected output: a per-cycle line summarizing the backfill, then a steady tick once caught up to the engine's flat store and pending ring:

```
scan: 124,500 blocks / filtered: 8,213 / handler: 1,082,996 events / commit: 29 ms
```

`--follow` keeps the indexer running and applies new blocks as they finalize. Drop the flag for a one-shot backfill.

## Entities produced

| Entity | Storage | Key | Updated on |
|---|---|---|---|
| `Account` | mutable | `[20]u8` (holder address) | every `Transfer` |
| `Allowance` | mutable | `[40]u8` (owner ++ spender) | every `Approval` |
| `Transfer` | immutable | `[16]u8` (block ++ tx ++ log index) | every `Transfer` log |
| `Approval` | immutable | `[16]u8` (block ++ tx ++ log index) | every `Approval` log |

Mutable entities live in `data/state.snap` (atomic-commit slab). Immutable entities live in `data/<EntityName>.events.dat` (append-only). See [`examples/readers/entity-format.md`](../readers/entity-format.md) for the byte layout and [`examples/readers/`](../readers/) for stdlib-only decoders in Python, C, and Zig.

## Files

- [`src/manifest.zig`](src/manifest.zig) — contract address, event signatures
- [`src/handlers.zig`](src/handlers.zig) — `handleTransfer`, `handleApproval`
- [`src/entities.zig`](src/entities.zig) — `Account`, `Allowance`, `Transfer`, `Approval`
- [`src/main.zig`](src/main.zig) — wires the four pieces into `sdk.run(...)` via the shared CLI helper

## Performance

| Phase | Time | Notes |
|---|---|---|
| Filter build (rETH, warm) | 10s | `core` io_uring pipeline, 7 workers |
| Handler replay | <1s | sorted-slab `MutableStore`, batched commits |
| Wall-to-wall backfill | 11s | filter + handler + commit |
| Handler re-run | <1s | filtered index hot in page cache |

Measured on Hetzner i7-8700, 64 GB DDR4, Gen3 NVMe RAID0, ReleaseFast. Same numbers documented in [root README's Benchmarks section](../../README.md#benchmarks).

Iteration loop: edit `handlers.zig`, delete `./data`, re-run. The filtered index survives between runs; only handler replay re-executes. <2s for this contract.
