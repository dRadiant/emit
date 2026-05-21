# ADR-003: SDK Storage — Pure Zig Snapshot Files

**Status**: Accepted
**Date**: 2026-05-07 (proposed) / 2026-05-21 (accepted)
**Context**: Entity stores and the filtered index ship on MDBX via lmdbx-zig. This ADR captures the evaluation of pure-Zig alternatives and the decision to adopt one.

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

### D: Pure-Zig storage. Combined state file for mutable entities, append-only flat files for immutable entities

**Filtered index** uses the engine's flat-store pattern: `filtered.dat` (LZ4 entries) + `filtered.idx` (dense `(block_number, offset, length)` array, mmap'd). Internal artifact, no user access required.

**Mutable entity stores** colocate inside a single `state.snap` file. That file's atomic rename is the only primitive needed to commit cursor + every MutableStore consistently.

```
state.snap layout:
  magic              [8]u8       "EMITSTAT"
  version            u32 LE
  cursor             u64 LE      last fully-dispatched block whose state is durable
  mutable_bytes      [N]u64 LE   slab byte length per MutableStore type, tuple order
  immutable_counts   [M]u64 LE   authoritative record count per ImmutableStore type
  [body: N MutableStore slabs concatenated in tuple order, each sorted ascending by primary key]
```

The entities tuple is comptime-known. The arrays index by tuple position, so no in-file name strings, record-size fields, or descriptor table are needed.

**Immutable entity stores** use append-only flat files mirroring the engine's `blocks.dat`. One file per immutable entity type; filename derived at comptime from the entity type (e.g. `Transfer` → `transfer.events.dat`, following the SDK's lowercase-first-plus-`s` rule).

```
<entity>.events.dat layout:
  magic            [8]u8       "EMITEVTS"
  records          [count × record_size]   fixed-size, sorted by (block_number, log_index)
```

`count` is *not* stored in the file. The authoritative count lives in `state.snap.immutable_counts`. The on-disk file may transiently hold orphan trailing records from a crashed commit; those bytes are invisible to readers (bounded by the count) and are overwritten by the next append.

Indexer access pattern:

- `MutableStore.load(key)`: HashMap lookup; cold-fault binary-searches the mmap'd slab.
- `MutableStore.save(key, value)`: HashMap put with dirty flag.
- `ImmutableStore.save(record)`: sequential append to `<entity>.events.dat`.
- `flush()`: append any new ImmutableStore records and fsync; serialize the new `state.snap` (cursor + mutable_bytes + immutable_counts + slabs) to `state.snap.tmp`; fsync; rename over `state.snap`; fsync the directory.
- `close()`: no-op.

External (other-language) access pattern:

- Open and mmap `state.snap`. Read the fixed-size header and the two u64 arrays (sizes are known from the schema spec).
- Mutable slabs: slice the body at offsets computed from `mutable_bytes`. Binary search the sorted records for point lookup (`O(log N)`); binary search + sequential read for range scans on primary key (`O(log N + K)`).
- Immutable records: open `<entity>.events.dat`, read `count` from `state.snap.immutable_counts`, slice `count × record_size` bytes. Binary search on the composite `(block_number, log_index)` key.

No library required. A Python reader is ~12 lines using `struct.unpack` and `bisect`. A C reader is ~30 lines.

**Pros**
- Pure Zig. Zero C dependencies for the SDK's core storage.
- Lowest LoC. Roughly 150 lines for both `MutableStore` and `ImmutableStore` combined, including the comptime serializer carried from the prototype.
- Eliminates lmdbx-zig entirely from the SDK.
- File format is documented and stable. A user with a hex editor can decode any record.
- Cursor and every MutableStore commit atomically via a single rename of `state.snap` — no WAL, no generation table, no orphan GC.
- ImmutableStore growth is handled natively by append-only flat files; per-commit cost is `O(dirty)`, not `O(total)`.
- Cross-language access requires no library, just bytes and a spec.
- Time complexity is bounded everywhere: `O(1)` hot path, `O(log N)` cold lookup, `O(N log N)` MutableStore flush, `O(dirty)` ImmutableStore append.

**Cons**
- MutableStore is rewritten in full on every commit. `O(N)` not `O(dirty)` *for MutableStore only*. Acceptable up to ~5M entities per type at typical commit cadence; beyond that, an incremental scheme would be needed. ImmutableStore is not subject to this — its growth is the unbounded chain-history dimension and is handled by append.
- No secondary indexes. Range scans on non-primary fields require either a denormalized entity type with the desired primary key or a sidecar that loads the snapshot into PostgreSQL/SQLite for SQL access.
- No concurrent writer. Atomic rename gives readers a consistent snapshot but a brief window during rename can stall a reader for milliseconds. Acceptable for typical API server usage; not acceptable for high-frequency mutating workloads (which an indexer is not).

## Considerations

**Performance at the expected workloads.** With MutableStore fronting both MDBX and pure-Zig snapshot, the hot path is identical (HashMap operations). The difference is at flush boundaries.

| Entity count | Sort cost | Snapshot write | MDBX commit (estimate) |
|---|---|---|---|
| 58K (rETH) | ~3 ms | ~1 ms | ~5 to 10 ms |
| 150K (Uniswap V2) | ~8 ms | ~5 ms | ~15 to 30 ms |
| 500K (Polymarket) | ~30 ms | ~30 ms | ~50 to 100 ms |
| 2M (All ERC20) | ~150 ms | ~120 ms | ~50 to 200 ms |

Pure-Zig wins or ties on MutableStore commit latency up through ~2M entities per type. At ~5M+ per type the full-rewrite cost dominates and an incremental scheme would become necessary. ImmutableStore is not subject to this curve — append-only writes are `O(dirty)` regardless of total record count.

**Range-scan complexity.** Sorted by primary key. Point lookup is binary search (`O(log N)`). Range scans on the primary key are binary search to start + sequential read until end (`O(log N + K)`). For ImmutableStore (event records keyed by `block_number || log_index`), this gives efficient block-range queries naturally. For MutableStore (state keyed by entity identity), it gives prefix scans.

**Secondary indexes.** Not provided. Three honest options for users who need them:

1. Define an additional entity type with the desired primary key, denormalizing the data. The SDK serializes each independently. Disk usage doubles, but query patterns become `O(log N + K)`.
2. The user's API server loads the snapshot into memory once and builds whatever indexes it wants in its own language. Works up to a few million entities.
3. Sidecar process reads the snapshot file and pushes records into PostgreSQL or SQLite. The user runs SQL queries with arbitrary indexes. Adds operational complexity but lifts the SDK's responsibility.

**Reorg recovery.** Per `entity-storage-spec.md`, the reorg path is "wipe entities and re-backfill". Pure-Zig snapshot supports this trivially: delete the entity files, re-run. No range-delete needed.

**Cross-store atomicity.** Provided. The `state.snap` rename is one primitive that commits the cursor and every MutableStore atomically. ImmutableStore appends ride alongside via the authoritative count field in `state.snap` — bytes past that count are invisible until the next `state.snap` rename publishes them.

**Ecosystem.** SQLite is the most portable choice for cross-language access in absolute terms. The pure-Zig snapshot format with documented spec is the most portable in *zero-dependency* terms. For a user already pulling Python or Rust into their stack, SQLite via existing bindings is cheaper. For a user wanting to write a 30-line C reader on an embedded target, the pure-Zig format wins.

## Decision

**Adopt option D.** Pure-Zig sorted snapshot files for entity stores; flat files for the filtered index; flat KV file for the ethcall cache. Remove lmdbx-zig from the SDK.

Rationale:

- Live verification surfaced wrapper friction. Raw-C-API workarounds already exist for `cursor_del` and `dbi_stat`; each new path is another opportunity for the wrapper to misbehave under load.
- The commit-latency analysis in the considerations section holds at the entity counts the SDK targets (≤ ~5M per type per commit). Sort-and-rewrite ties or beats MDBX's B-tree + WAL path at those scales.
- The handler API (`load`, `save`, `flush`, comptime markers) is unchanged. This is a backend swap; user code is untouched.
- Dependency reduction is a first-class deliverable for SDK distribution. A pure-Zig core with no transitive C dependencies is easier to package, audit, and consume from a Zig package registry.
- Cross-language readers (C, Python) become trivial. The binary format is documented and small enough that a hex editor decodes a record.

## Consequences

- The handler API and entity-storage-spec contract are unchanged.
- `MutableStore` and `ImmutableStore` get new internal implementations.
- lmdbx-zig is removed from the SDK's dependencies.
- New deliverables: `docs/entity-format.md` documenting the binary file format; reference C and Python readers in `examples/readers/`. No migration tool — operators delete the data dir and re-backfill (the architectural pitch is that re-backfill is fast).

### Cursor location and atomicity

The SDK's resume cursor previously rode the entity MDBX transaction at `_meta.cursor` so the cursor write committed atomically with the entity flush — giving "crash any time, cursor and entity state are byte-atomic." A standalone `cursor.bin` written out-of-band was rejected because the file-write vs entity-commit gap could silently double-count `MutableStore` mutations on restart. That invariant must survive the storage migration.

**Mechanism: `state.snap` as the single atomic commit point.**

The cursor lives in the `state.snap` header alongside the MutableStore slabs and the authoritative ImmutableStore record counts. One file rename publishes all three simultaneously.

Commit sequence on each flush boundary:

```
1. For each ImmutableStore T with new records:
     append records to T.events.dat at offset count[T] × record_size[T]
     fsync(T.events.dat)
2. Build new state.snap contents in memory:
     cursor = new_cursor
     mutable_bytes = current sorted slab sizes
     immutable_counts = updated counts
     body = serialized mutable slabs
3. Write to state.snap.tmp → fsync → rename(state.snap.tmp, state.snap)
4. fsync(dir)
```

The atomic primitive is step 3's rename. Before it lands, `state.snap` still references the previous cursor, slab contents, and immutable counts — any newly-appended bytes in `events.dat` past the previous count are invisible to readers. After it lands, the new cursor, new mutable slabs, and new immutable counts are all visible together. There is no intermediate state.

Crash recovery:

- Crash between 1 and 3: `events.dat` files hold orphan trailing records, but `state.snap` still has the previous `immutable_counts`. Readers bounded by the count see nothing extra. The next commit's append seeks to `count × record_size`, overwriting the orphan bytes. No truncation needed.
- Crash during 3 (between write and rename): `state.snap.tmp` exists; startup unlinks it. `state.snap` is unchanged.
- Crash after 3: complete commit. Routine startup.

`writeCursorIn` becomes "set cursor field in the in-flight `state.snap` struct"; `readCursorIn` becomes "read `state.snap` header on startup, return cursor field." No out-of-band cursor write is ever introduced — the previously rejected failure mode is structurally impossible under this design.

### Options not chosen

**Option A — status quo with raw-C-API workarounds.** Wrapper bug surface keeps growing. Cross-language story unchanged. Dependency footprint unchanged.

**Option B — raw mdbx C API.** Eliminates wrapper bugs immediately but does not address cross-language friction or dependency reduction. Migration cost comparable to option D for a less compelling destination.

**Option C — SQLite for entity stores.** Best cross-language ecosystem but slower than MDBX and not pure Zig. Useful as an opt-in backend for users who want SQL access, not as the default.
