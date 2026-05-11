# ADR-004: ethCall is Strict Prefetch-Only

**Status**: Accepted
**Date**: 2026-05-11
**Context**: M3 fills the M2-stubbed `BlockContext.ethCall`. The decision is whether handlers may issue ad-hoc network calls (lazy mode) or only read pre-declared prefetched results (strict mode).

## Problem

M2 shipped `BlockContext.ethCall(to, calldata) ![]const u8` returning `error.NotYetImplemented`. M3 implements it backed by a Multicall3-batched MDBX cache populated in a Phase 4 prefetch step between filter build and handler replay.

The semantic decision is not how the cache works, but what `ethCall` does on a cache miss.

**Lazy mode**: on miss, issue a one-off HTTP call, cache the result, return it. Handlers can call `ethCall(addr, calldata)` for anything at any time and the SDK transparently fetches and remembers.

**Strict mode**: on miss, return `error.NotPrefetched`. The full set of `(address, calldata)` pairs the indexer will ever issue must be declarable from the manifest, so that Phase 4 can batch them via Multicall3. Handlers never trigger HTTP I/O.

Both are implementable. The choice shapes user ergonomics, replay determinism, throughput guarantees, and the operational story for re-runs.

## Options

### A: Lazy mode (cache-or-fetch)

`ethCall` checks the cache; on miss, performs a single eth_call against the configured provider, writes the result to the cache, returns the bytes. Handlers behave like clients with an automatic durable cache.

**Pros**
- Familiar pattern from JavaScript/TypeScript indexers (Envio, Ponder, Subgraph all permit ad-hoc fetches).
- Users can prototype handlers without declaring prefetch upfront.
- Robust to surprise: a contract event the user did not anticipate at design time can still be handled with a follow-up `ethCall`.

**Cons**
- First handler invocation that hits an uncached call stalls on HTTP. Tail latency depends on RPC provider behavior. Spec §9 commits to ≥2M events/s replay; one cache miss in the wrong block tanks per-block throughput by four orders of magnitude.
- Defeats Multicall3 batching. A miss is one RTT per call, not 500 calls per RTT. The whole architectural premise of M3 collapses if handlers can bypass the batch.
- Breaks spec §M3 exit criterion ("zero RPC calls on re-backfill"). Any newly added handler with an undeclared call leaks RPC on every run.
- Handlers become impure functions of `(log, cache, entity-state, network)`. Replay is no longer deterministic over durable inputs. Reorg replays, forensic replays, and CI replays diverge from production.
- Errors are silent and late: a typo'd selector or wrong address gets cached as a reverted call and continues to "work" without telling the user.

### B: Strict mode (cache-or-error)

`ethCall` returns cached bytes for prefetched pairs, `error.CallReverted` for cached entries with status = 1, and `error.NotPrefetched` for anything else. Phase 4 is the only place that talks to the network. Handlers are pure.

**Pros**
- Replay is a pure function over the filtered index plus the ethcall cache. Both are durable. Both are reproducible.
- Throughput is deterministic. Replay does zero network I/O, period.
- Multicall3 batching is the only fetch path, so the architectural win is preserved.
- Spec §M3's zero-RPC re-backfill exit criterion becomes a structural property, not a hope.
- Missing prefetch declarations fail loudly on the first handler invocation that needs them. Address + 4-byte selector show up in the error path. Triage is immediate.
- The full set of network calls the indexer ever makes is enumerable at comptime from the manifest. Useful for audits, ops budgets, and regulated deployments.
- Operationalizes the events-first thesis from spec §"Events-first design": all relevant `eth_call`s are for immutable metadata, all immutable metadata is knowable from the manifest.

**Cons**
- Less forgiving for users coming from lazy-mode indexers. A forgotten prefetch declaration produces a runtime error, not a slow run.
- Requires users to think about prefetch up front, even for trivial cases. Mitigated by `known_tokens` for the most common case (token metadata) and by `.log` address-source on broad events (Transfer-class) that catches all emitters.
- Unanticipated mid-handler data needs are not directly supported. The events-first thesis says they should not exist (if a contract does not emit enough data, that is a contract design flaw, not an SDK design flaw), but a user trying to model an under-emitting protocol will feel friction.

### C: Hybrid (strict default, lazy opt-in per call)

Default `ethCall` is strict. Expose a second method `ethCallLazy(addr, calldata)` that does cache-or-fetch. Users opt into ad-hoc semantics per call.

**Pros**
- Strict for the common path; lazy for the escape hatch.
- The opt-in nature makes the semantic break visible at the call site.

**Cons**
- Two methods is one more concept than necessary in M3. No real use case in the spec or examples needs lazy mode today.
- The escape hatch can be added later in ~20 LOC (a `Cache.warm(provider, addr, calldata)` plus a `BlockContext.ethCallLazy` thin wrapper) without breaking strict callers. Reversible. There is no reason to ship it pre-emptively.

## Considerations

**Where unanticipated calls actually come from.** In production indexers, unanticipated `eth_call` needs almost always reduce to "I see a new token I did not know about, I want `decimals()`/`symbol()`/`name()`." Strict mode handles this cleanly via `prefetch` with `.log` address-source on a broad event (e.g., Transfer). Every emitter that ever appears in a matching log triggers the prefetch. The set of "new tokens" is bounded by what the filtered index admits, which is bounded by the manifest's address set. No surprises.

**Where lazy mode would genuinely help.** Conditional fetches where the calldata depends on a parsed log field (e.g., "if the event names a contract address, fetch that contract's owner"). These can be modeled in strict mode by over-fetching unconditionally (Multicall3 amortizes; the extra entries cost ~50 bytes each in the cache) or by structuring the manifest so that the conditional event becomes its own prefetch trigger. Both work without lazy mode.

**Operational story for re-runs.** Strict mode means: "delete entities, re-run, zero RPC." The cache survives because it lives in its own MDBX env per `<entity_data_dir>/ethcall.mdbx`. Lazy mode means: "delete entities, re-run, RPC bill is whatever is uncached." Strict makes the re-run cost a structural property of the manifest; lazy makes it a function of historical luck.

**Replay determinism.** Strict mode makes the replay a function of two durable inputs (filtered index + ethcall cache). Lazy mode makes it depend on the network and on whatever the RPC provider returned at the time of the first call. Both can produce correct results for immutable metadata, but only strict produces *bit-identical* results across replays. For regulated indexers and chain forensics, this matters.

**Throughput.** The spec commits ≥2M events/s on replay (§9). Lazy mode admits any network stall into the hot path. Strict mode does not. The choice is between a guarantee and an aspiration.

## Decision

**Adopt option B (strict mode).** Implement `ethCall` as cache-or-error. Reserve `Cache.warm` + `ethCallLazy` as a future escape hatch (~20 LOC, additive, decided when a real use case appears).

Rationale:

- Replay determinism and throughput are the load-bearing M3 properties. Lazy mode trades both away for a small ergonomic gain on a use case the events-first thesis says should not exist.
- The ergonomic cost of strict mode is mostly absorbed by `known_tokens` and `.log` prefetch sources, which together cover the common case of "fetch metadata for everything that ever emits an event I care about."
- Failure mode is the right shape: missing prefetch declarations are surfaced immediately at the first handler call, with the address and selector in the error. Lazy mode buries the same mistake as a quiet RPC bill or a slow first run.
- Reversibility is preserved. If a future use case forces lazy mode, it lands as an additive method with no impact on existing strict callers.

## Consequences

**Spec.** `BlockContext.ethCall` signature is unchanged from M2 (`![]const u8`). Error union gains `error.NotPrefetched` and `error.CallReverted`. Spec scenarios pin the strict semantics.

**Examples.** `examples/uniswap-v2/` demonstrates `known_tokens` for canonical WETH/USDC and `prefetch` with `.log` source for opportunistic token metadata. The pattern is the recommended idiom in user docs.

**Error reporting.** `error.NotPrefetched` debug-prints the address (hex) and the 4-byte selector at the call site so triage is one grep. Done in the BlockContext error path, not in user code.

**Future flexibility.** A `Cache.warm(provider, addr, calldata) !void` method plus a `BlockContext.ethCallLazy` variant adds lazy semantics in ~20 LOC, fully additive. Decision to ship gated on a real use case, not speculation.
