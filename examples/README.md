# examples

Reference indexers built on [emit-sdk](../sdk/README.md). Each lives in its own directory with `manifest.zig`, `handlers.zig`, `entities.zig`, and `main.zig`, plus a per-example README.

| Example | Contracts indexed | Entities | Demonstrates |
|---|---|---|---|
| [erc20](erc20/README.md) | rETH (single ERC20, retargetable) | `Account`, `Allowance`, `Transfer`, `Approval` | Mutable + immutable stores, single-address indexing |
| [uniswap-v2](uniswap-v2/README.md) | Uniswap V2 factory + every spawned pair | `Pair`, `SwapEvent` | Factory pre-pass, static + dynamic prefetch, Multicall3 batching |

Shared CLI scaffolding lives in [`utils/cli.zig`](utils/cli.zig) so every example reuses the same `--engine-data-dir`, `--data-dir`, `--node-rpc`, `--follow` flag parsing.

Reference readers that decode entity files from outside EMIT. Pure implementations. Python, C, and Zig implementations live in [`readers/`](readers/).
