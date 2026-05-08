# ADR-003: SDK Storage — Pure Zig Snapshot Files

**Status**: Proposed
**Date**: 2026-05-07
**Context**: Entity stores and the filtered index ship on MDBX via lmdbx-zig. We are evaluating whether to replace one or both with pure-Zig storage in a future change.

## Problem

The SDK has two storage surfaces with different requirements.

**Filtered index** is an internal artifact. It is rebuildable from the engine's flat store, append-only during build, read-only during scan, has no durability requirement, and is never accessed by the user. The current design stores it in `filtered_index.mdbx` because the prototype did.

**Entity stores** are the user-facing output. They hold mutable state (`Account`, `Allowance`) and append-only event records (`Transfer`, `Approval`). They need durability, point lookups during indexing, and (for the user's API server) point lookups, range scans, and concurrent reads after indexing. The current design stores them in MDBX behind `MutableStore(T)` and `ImmutableStore(T)`.

Three forces motivate revisiting this:

1. **lmdbx-zig wrapper bugs.** Known issues exist in `cursor_del` and `dbi_stat`. Earlier work has had to drop to raw C API calls in some paths, and future work in the entity-store and filtered-index paths is likely to encounter more. Each workaround is documented overhead.
2. **Cross-language friction.** A user wanting to read entity stores from Python, Rust, or JavaScript must install MDBX bindings. Coverage is uneven across ecosystems. SQLite is more universal but slower. A documented binary format readable with `struct.unpack` is the lowest-friction option.
3. **Dependency footprint.** Pure Zig means fewer C libraries, fewer transitive dependencies, faster builds, easier auditing.

The MutableStore pattern moves the indexer's hot path entirely into a HashMap. The storage backend matters only at commit boundaries (every 100K events) and during cold reads. This shrinks the performance gap between MDBX and any alternative.

## Options

### A: Status quo. MDBX behind lmdbx-zig

Both filtered index and entity stores use MDBX. Wrapper bugs are worked around case by case.

**Pros**
- Proven by the prototype.
- Best raw KV performance.
- ACID transactions, MVCC, concurrent readers.
- Range scans via cursor.
- Bindings exist in many languages (Python `libmdbx`, Rust `mdbx-rs`).

**Cons**
- lmdbx-zig wrapper bugs require ongoing raw-C-API mitigations.
- C dependency, ~50 KLoC of foreign code.
- MDBX language bindings are narrower than SQLite's.
- B-tree page management is overhead our access pattern does not need.

### B: Status quo, but bypass lmdbx-zig

Drop the lmdbx-zig wrapper. Use `@cImport("mdbx.h")` directly for the dozen functions the SDK needs. Eliminates wrapper bug surface.

**Pros**
- Eliminates wrapper bugs entirely.
- Stable C API.
- Same performance as option A.

**Cons**
- Still a C dependency.
- Cross-language story unchanged.
- More verbose call sites (raw C API, no Zig sugar).

### C: SQLite for entity stores, flat files for filtered index

Filtered index becomes pure-Zig flat files matching the engine's `blocks.dat` / `blocks.idx` pattern. Entity stores move to SQLite via a thin Zig wrapper or `@cImport`.

**Pros**
- SQLite is the most universally supported embedded database. Every major language has first-class bindings.
- ACID, range scans, indexes, joins, foreign keys.
- File format is the most stable embedded format in existence.
- Eliminates lmdbx-zig.
- Filtered-index path becomes pure Zig.

**Cons**
- SQLite is C, not Zig. Drops one C dep, adds another.
- ~3-5x slower than MDBX on raw KV. MutableStore mitigates for the hot path; flush cost is the open question.
- For very large entity counts (~5M+), SQLite write amplification becomes a real cost.

### D: Pure Zig sorted snapshot files for entity stores, flat files for filtered index

**Filtered index** uses the engine's flat-store pattern: `filtered.dat` (LZ4 entries) + `filtered.idx` (dense `(block_number, offset, length)` array, mmap'd). Internal artifact, no user access required.

**Entity stores** use a single sorted binary file per entity type:

```
File layout (one file per entity type):
  [magic 8 bytes "EMITSTOR"]
  [version u32 LE]
  [reserved u32 LE]
  [count u64 LE]
  [records: count × (key bytes ++ value bytes), sorted ascending by key]

Key and value sizes are comptime-known per entity type.
Records are fixed-size, no length prefix needed.
```

Indexer access pattern:

- `load(key)` is a HashMap lookup with cold-fault read from the mmap'd snapshot via binary search.
- `save(key, value)` is a HashMap put with dirty flag.
- `flush()` collects dirty entries, sorts the full set by key, writes via tmp + fsync + rename.
- `close()` is a no-op.

External (other-language) access pattern:

- Open and mmap the file.
- Read header (24 bytes).
- Binary search the sorted records for point lookup (`O(log N)`).
- Binary search to find a range start, sequential read for range scans on primary key (`O(log N + K)` for K results).
- Full scan reads sequentially.

No library required. A Python reader is ~12 lines using `struct.unpack` and `bisect`. A C reader is ~30 lines.

**Pros**
- Pure Zig. Zero C dependencies for the SDK's core storage.
- Lowest LoC. Roughly 200 lines for both `MutableStore` and `ImmutableStore` combined, including the comptime serializer carried from the prototype.
- Eliminates lmdbx-zig entirely from the SDK.
- File format is documented and stable. A user with a hex editor can decode any record.
- Beats MDBX on commit latency at the expected scales because sequential `pwrite` of contiguous bytes is faster than B-tree page modification + WAL.
- Cross-language access requires no library, just bytes and a spec.
- Time complexity is bounded everywhere: `O(1)` hot path, `O(log N)` cold lookup, `O(N log N)` flush.

**Cons**
- Full snapshot rewrite per commit. `O(N)` not `O(dirty)`. Acceptable up to ~5M entities at typical commit cadence; beyond that, an incremental log + compaction pattern is needed (~50 more LoC).
- No secondary indexes. Range scans on non-primary fields require either a denormalized entity type with the desired primary key or a sidecar that loads the snapshot into PostgreSQL/SQLite for SQL access.
- No concurrent writer. Atomic rename gives readers a consistent snapshot but a brief window during rename can stall a reader for milliseconds. Acceptable for typical API server usage; not acceptable for high-frequency mutating workloads (which an indexer is not).
- No ACID across multiple entity types. Each file is atomic individually. If an indexer needs cross-store transactional guarantees, this design does not provide them. The current entity-storage-spec does not require cross-store atomicity.

## Considerations

**Performance at the expected workloads.** With MutableStore fronting both MDBX and pure-Zig snapshot, the hot path is identical (HashMap operations). The difference is at flush boundaries.

| Entity count | Sort cost | Snapshot write | MDBX commit (estimate) |
|---|---|---|---|
| 58K (rETH) | ~3 ms | ~1 ms | ~5 to 10 ms |
| 150K (Uniswap V2) | ~8 ms | ~5 ms | ~15 to 30 ms |
| 500K (Polymarket) | ~30 ms | ~30 ms | ~50 to 100 ms |
| 2M (All ERC20) | ~150 ms | ~120 ms | ~50 to 200 ms |

Pure-Zig wins or ties on commit latency up through ~2M entities. At ~5M+ the full-rewrite cost dominates and an incremental scheme becomes necessary.

**Range-scan complexity.** Sorted by primary key. Point lookup is binary search (`O(log N)`). Range scans on the primary key are binary search to start + sequential read until end (`O(log N + K)`). For ImmutableStore (event records keyed by `block_number || log_index`), this gives efficient block-range queries naturally. For MutableStore (state keyed by entity identity), it gives prefix scans.

**Secondary indexes.** Not provided. Three honest options for users who need them:

1. Define an additional entity type with the desired primary key, denormalizing the data. The SDK serializes each independently. Disk usage doubles, but query patterns become `O(log N + K)`.
2. The user's API server loads the snapshot into memory once and builds whatever indexes it wants in its own language. Works up to a few million entities.
3. Sidecar process reads the snapshot file and pushes records into PostgreSQL or SQLite. The user runs SQL queries with arbitrary indexes. Adds operational complexity but lifts the SDK's responsibility.

**Reorg recovery.** Per `entity-storage-spec.md`, the reorg path is "wipe entities and re-backfill". Pure-Zig snapshot supports this trivially: delete the entity files, re-run. No range-delete needed.

**Cross-store atomicity.** The current spec does not require it. Each entity type's file is atomic via tmp + rename. If a future feature needs multi-file atomicity, group flushes can use a write-ahead log of intended renames, replayed on startup. Not in this proposal.

**Ecosystem.** SQLite is the most portable choice for cross-language access in absolute terms. The pure-Zig snapshot format with documented spec is the most portable in *zero-dependency* terms. For a user already pulling Python or Rust into their stack, SQLite via existing bindings is cheaper. For a user wanting to write a 30-line C reader on an embedded target, the pure-Zig format wins.

## Decision

**Defer. Ship option A (MDBX via lmdbx-zig with raw-C-API workarounds).** Revisit this ADR after live verification has produced measured numbers.

Rationale:

- The current SDK has a working, well-specified plan against MDBX. Re-orienting now adds churn and design risk before any code is written.
- The prototype validated the architecture against MDBX. We have measured numbers to compare against.
- The pure-Zig design needs validation: can we actually beat MDBX commit latency at the expected scales, or are the estimates above optimistic?
- The user's API surface (`load`, `save`, `flush`, comptime markers) is identical between options A and D. Migrating storage backends later does not affect handler code.

Triggers for adopting option D in a follow-up change:

- Live verification surfaces more lmdbx-zig wrapper bugs than the two we already know about, and raw-C-API workarounds become onerous.
- A user explicitly needs cross-language access from a language without good MDBX bindings.
- We benchmark the pure-Zig snapshot design at production scales and confirm it ties or beats MDBX.
- The dependency reduction becomes valuable for distribution (e.g., publishing the SDK on a registry where C deps add friction).

## Consequences

**Adopting D later (likely):**

- The handler API and entity-storage-spec contract stay the same.
- `MutableStore` and `ImmutableStore` get new internal implementations, swapped via build flag or migrated wholesale.
- lmdbx-zig is removed from the SDK's dependencies.
- New deliverable: `docs/entity-format.md` documenting the binary file format.
- New deliverable: reference C and Python readers in `examples/readers/`.
- Migration tool: one-shot scan of an MDBX entity store, write out as snapshot files. ~50 LoC.

**Staying with A indefinitely:**

- Continue documenting raw-C-API workarounds for each lmdbx-zig wrapper bug encountered.
- Cross-language users install MDBX bindings.
- Performance and feature set match the prototype. No surprises.

**Not chosen (option B, raw mdbx C API):**

- Removes wrapper bug surface immediately but does not address cross-language friction or dependency reduction. The migration cost (touch every MDBX call site to replace wrapper calls with raw C) is comparable to the migration to option D, but the destination is less compelling.

**Not chosen (option C, SQLite):**

- Best cross-language ecosystem but slower than MDBX and not pure Zig. Useful as an opt-in backend for users who want SQL access, not as the default.
