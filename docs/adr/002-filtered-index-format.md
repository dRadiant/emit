# ADR-002: Filtered Index Format

**Status**: Accepted
**Date**: 2026-05-07
**Context**: The SDK ships `sdk/src/filter_builder.zig`, which materializes a per-manifest subset of the engine's flat log store into a scanner-friendly artifact at `<entity_data_dir>/filtered_index.mdbx`. The chosen format affects build cost, scan cost, the SDK's MDBX surface, and what future features (multi-manifest serving, pure-Zig storage) cost to add.

For factory manifests the build runs in two passes (build static-and-factory blocks → pre-pass to discover child addresses → build child blocks). The format must accommodate either one bucket of blocks (no factories) or two (factories with discovered children) with no semantic difference at scan time.

## Problem

The filter builder produces an artifact that the scanner walks in block order, decompresses, and dispatches per log. The artifact must:

- Hold every log whose `(address, topic0)` matches the manifest's static contracts, plus every log whose `topic0` matches a factory child event (the address of a factory child is unknown until the scanner's pre-pass discovers it).
- Support a forward block-ordered cursor walk for the handler-replay phase.
- Survive crash mid-build without corrupting an in-progress build (the next run rebuilds from scratch).

It does not need:

- Random point lookups by block number.
- Concurrent writers.
- Cross-store atomicity.
- User-facing access (the artifact is internal; users read entity stores, not the filtered index).

## Options

### A: One MDBX env with `BLOCKS_PRIMARY` plus optional `BLOCKS_CHILDREN`

Single env at `<entity_data_dir>/filtered_index.mdbx` containing one or two DBIs:

- `BLOCKS_PRIMARY` (always present): keyed by `block_number` as u64 big-endian (8 bytes). Value = `lz4_len(u32 LE) || lz4_data`, identical to the engine's `blocks.dat` entry format. Holds logs from the manifest's static contracts and factory addresses (creation events live here).
- `BLOCKS_CHILDREN` (created only when the factory pre-pass discovers at least one child contract): same key/value shape. Holds logs from factory-discovered child addresses, with the per-log filter excluding addresses already covered by `BLOCKS_PRIMARY` so the same log never appears in both DBIs.

Empty blocks (matched the bloom but had no qualifying logs after per-log filtering) are not written. Both DBIs use `MDBX_APPEND` since their writers feed keys in ascending order.

**Pros**
- Cursor walk by block order is `cursor.goToFirst` then `goToNext` until `null`. ~3 lines per DBI.
- `MDBX_APPEND` skips B-tree traversal — about 5x faster than `MDBX_UPSERT` per insert.
- Crash-safe partial builds via transaction commit cadence (every 10K block writes).
- Scanner needs zero metadata: each DBI carries first/last via cursor extremes.
- Format is stable across rebuilds (decoded contents are identical run-to-run).
- Two DBIs in one env keeps the scanner simple: single open call, single `max_dbs = 2` config, k-way merge across two cursors under one read transaction.
- Factory and non-factory manifests share the same code path. Scanner conditionally opens `BLOCKS_CHILDREN`; nothing else changes.

**Cons**
- One MDBX consumer in the SDK. Pure-Zig users pay one C dependency they would not otherwise need.
- B-tree overhead at scan time, even though we only ever do sequential reads.

### B: Two MDBX DBIs (the prototype's layout)

`BLOCKS` keyed by block number plus a `TOPIC_INDEX` DBI keyed by block number, value = the per-block topic bloom (256 bytes). Plus a `META` DBI with `(first_block, latest_block, total_logs)`.

**Pros**
- Scanner can re-check the topic bloom before decompressing the BLOCKS value, dropping bloom false positives without paying LZ4 cost.
- The META DBI gives the scanner upfront totals for progress reporting.

**Cons**
- The TOPIC_INDEX bloom is redundant: every block in the filtered index has already passed `core.block_filter.scanBloomsParallel` against the engine's `blooms.bin`. The downstream bloom check would only refuse blocks that the upstream bloom already accepted but whose entries we already decided to write. The builder never writes a block with no qualifying logs, so a re-check has nothing to gain.
- The scanner already topic-filters per log (cheap byte compare on the unpacked entry's topic0). Adding a per-block bloom step in front saves time only when the LZ4 decompress is very expensive relative to the filtered logs' cardinality, which is not the workload here.
- Adds ~5 MB per filtered index (bloom rows) and one cursor walk per block during scan.
- Three DBIs = three cursors to manage during the scan, plus three commits per batch boundary.

### C: Pure-Zig flat files (`filtered.dat` + `filtered.idx`)

Mirror the engine's `blocks.dat` / `blocks.idx` layout: an append-only LZ4 file plus a dense `(block_number, offset, length)` index. No MDBX.

**Pros**
- One less MDBX consumer in the SDK. Aligned with ADR-003's deferred direction (move SDK storage off MDBX over time).
- mmap-friendly index, sequential read of the data file is the fastest path on NVMe.
- Format is hex-readable; debug tooling reduces to `xxd | less`.
- Eliminates the lmdbx-zig wrapper-bug surface for this particular file.

**Cons**
- New writer code for crash-safe append (tmp + rename per commit, or a journal of intended renames). MDBX's transaction boundary is free; flat-file equivalent is ~50 lines.
- New cursor abstraction in the scanner. MDBX's cursor API is reusable; flat-file walk needs its own iterator.
- The SDK already uses lmdbx-zig for entity stores. Adding a second storage path now means two backends to maintain, one of them brand new, before the existing path has measured numbers from production workloads.
- ADR-003 explicitly defers this direction until measured. Pulling it forward into this decision violates ADR-003's stated trigger conditions.

## Considerations

**Scan-side cost.** The handler-replay hot path is decompress + dispatch. MDBX cursor advancement is sub-microsecond. The B-tree-versus-flat-file distinction at scan time is not in the critical path for the throughput targets.

**Build-side cost.** `MDBX_APPEND` is comparable to a flat-file `pwrite` plus an index-array append. Both are dominated by the LZ4 recompress and disk bandwidth, not the storage backend.

**Pure-Zig direction.** ADR-003 names the filtered index as a candidate for early migration to pure-Zig flat files (the artifact is internal, so no cross-language pressure, and the access pattern is the simplest possible). This ADR picks A; ADR-003's evaluation can pull C forward as a focused refactor without affecting handler code or entity stores.

**Multi-manifest serving.** A future feature could serve several manifests off a single shared filtered index by re-checking topics inside the index. That use case wants the option-B layout. This format does not pay that cost; if the feature lands, gate option B's `TOPIC_INDEX` behind a comptime flag in `filter_builder.zig`. The change is ~20 lines and does not invalidate option A's data files.

**Compatibility with engine's `blocks.dat`.** Option A's value format is identical to the engine's per-block entry. `core.log_serial.compressEntry` and `decompressEntry` work unchanged on both. A future reader that needs to ingest either source pays no format-translation cost.

## Decision

**A — single env, `BLOCKS_PRIMARY` always present plus `BLOCKS_CHILDREN` when factories discover children. Both DBIs keyed by `block_number` as u64 big-endian, value = LZ4 entry matching `blocks.dat`'s per-block format.**

Drop `TOPIC_INDEX` and `META` from the prototype's layout. The scanner reads first/last via cursor extremes and topic-filters per log inline.

## Consequences

- `filter_builder.zig` opens `filtered_index.mdbx` with `max_dbs = 2`. `build()` opens `BLOCKS_PRIMARY` with `.create = true` and writes via `set(key, entry, .Append)`. `appendChildren()` opens `BLOCKS_CHILDREN` similarly, writes a second pass, and is invoked only when the pre-pass discovers at least one child address. Both commit every 10K block writes.
- The handler-replay scanner opens the same env read-only and conditionally opens `BLOCKS_CHILDREN` based on whether the manifest declares factories. With one DBI it walks one cursor; with two DBIs it k-way merges by `(block_number, tx_index, log_index)`.
- The idempotency test asserts decoded-content equality across rebuilds, not byte-equality (MDBX page metadata varies run-to-run; the data semantics do not).
- If a future change adopts ADR-003 option D for the filtered index, the migration touches only `filter_builder.zig` and the scanner. The handler API and entity-storage contract are unaffected. Build flag or wholesale swap, both viable. The two-bucket layout maps cleanly to two pure-Zig flat files (`filtered_primary.dat` + `filtered_children.dat`) if D lands.
- If multi-manifest serving lands, gate `TOPIC_INDEX` behind a comptime flag in `filter_builder.zig`. Format A's data files are forward-compatible — the new layout adds a sibling DBI without touching BLOCKS_PRIMARY or BLOCKS_CHILDREN.
