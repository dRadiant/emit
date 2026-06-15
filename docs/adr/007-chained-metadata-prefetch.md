# ADR-007: Chained Metadata Prefetch (Multi-Round Fixpoint)

**Status**: Accepted
**Date**: 2026-06-15
**Supersedes**: ADR-004's reserved lazy escape hatch (`Cache.warm` / `ethCallLazy`), already withdrawn in its 2026-06-05 amendment.
**Context**: Strict `ethCall` (ADR-004) batches every `(address, calldata)` the indexer issues in a prefetch phase before dispatch, keeping the hot loop network-free and replay-deterministic. One real case escapes that model: a call whose *target* depends on a prior call's result (`event → pool.token0() → token0.decimals()`). A single Multicall3 batch cannot resolve it — the second target is unknown until the first returns.

## Problem

Prefetch gathers calls from the manifest and the matching logs, then batches them once. A chained call's target is not knowable at gather time: `decimals()` runs on whatever address `token0()` returns. ADR-004 rejected the lazy alternative (inline `eth_call` on miss) because it reintroduces per-call latency and nondeterminism into the 2M-events/s loop. The question is how to resolve chains while keeping every call resolved *before* dispatch.

## Decision

A **bounded multi-round fixpoint** over the existing prefetch pipeline, plus a manifest surface to declare the chain.

### Surface: reference by method

`AddressSource` gains two variants:

- `of: "method()"` — target is the first return value (a bare `address`, or return value 0 of a tuple) of an earlier call in the same `PrefetchDef`, named by its method.
- `of_return: .{ .call, .index }` — return value `index`, for an address that is not the first return value (`getReserveTokensAddresses() -> (aToken, stable, …)`).

```zig
.calls = &.{
    .{ .address = .log,                .method = "token0()" },
    .{ .address = .{ .of = "token0()" }, .method = "decimals()" },
    .{ .address = .{ .of = "token0()" }, .method = "symbol()"   },
}
```

Chosen over a positional index, a `*const PrefetchCall` pointer, an invented label, or an inline hop-path to optimize author DX: it reuses a string already written (`"token0()"`), mirrors the handler's own chaining (`ethCall(decimals, ethCall(token0, pool))`), and adds no identifier to invent or maintain. It mirrors the existing `.param = "name"` idiom one level over. Comptime `producerIndex` resolves the method to the unique earlier call; absent or ambiguous is a compile error, and "earlier-only" (`producer index < self`) makes a cycle unconstructable.

### Resolution: re-gather to a fixpoint

The existing pass — gather → dedupe → drop-cached → preload — already converges if re-run with the cache available for chain resolution. `resolveTarget` resolves `.of`/`.of_return` from the cache, returning `null` (the call is dropped this round) when the producer is uncached or reverted:

- **Round 0**: `token0()`/`token1()` (`.log` targets) gather and preload. The chained `decimals()` cannot resolve its target yet, so it is dropped.
- **Round 1**: `token0()` is cached, so `decimals()` resolves its target and preloads.
- **Round k**: `missing == 0` → converged.

The loop is `while (round < prefetchMaxDepth(m))`, the cap being the comptime-computed deepest chain — **1 for every unchained manifest, so the loop runs once with zero added overhead**. A per-round arena reset frees each round's gather before the next. Termination is guaranteed: every gathered call is cached (status 0 or reverted) each round, so it cannot reappear; new calls only surface as deeper producers cache, bounded by the finite chain depth. Backfill (`runPhase4`) and live (`maybePrefetchBlock`) share the shape.

This preserves the load-bearing properties of ADR-004: every call resolves before dispatch, the hot loop stays network-free and replay-deterministic, and Multicall3 batching is the only fetch path.

## Options rejected

- **Lazy inline call** (ADR-004's withdrawn hatch): reintroduces per-call latency and nondeterminism for the one case this covers cleanly. Its only unique niche — a truly unpredictable mid-handler target — is an events-first anti-pattern.
- **Dependency graph with deferred resolution**: a DAG of producer→consumer edges resolved in topological order. Correct, but more machinery than re-gathering — which converges for free because dedupe and drop-cached already idempotently collapse re-emitted producers.
- **Surface alternatives** to reference-by-method: positional index (fragile to reorder, opaque), `*const PrefetchCall` pointer (needs a named const before the literal, mutual type recursion), invented label (an extra identifier to coin and keep unique), inline via-path (repeats the hop, intermediate not a first-class cached call). All lose on author friction.

## Consequences

- Chained immutable metadata is declarable and resolves before dispatch. `decimals()`/`symbol()` on a token reached via `pool.token0()` need no event carrying the token address, so pairs created before the scan window enrich too.
- `symbol()`/`name()` dynamic-string returns flow through the chain, decoded as `[]const u8`.
- `examples/uniswap-v2` is the documented idiom (`Swap → token0()/token1() → decimals()/symbol()`). Verified on the reference box over blocks 19.0M–19.01M: 850 chained calls resolved through the depth-2 chain, 0 reverts, including 213 depth-2 `symbol()` strings decoded to real tokens, in 527 ms.
