# ADR-001: Pending Block Storage

**Status**: Accepted
**Date**: 2026-05-03
**Context**: Engine must hold the last ~64 blocks (pre-finality) and handle reorgs. How do we store them?

## Problem

Flat files (blocks.dat, blooms.bin, blocks.idx) are append-only and contain finalized data. Blocks arrive from the chain head before finalization (64 confirmations on Ethereum). Reorgs require deleting the invalidated tail and re-inserting canonical blocks. We need a storage strategy for these pre-finality blocks.

## Options

### A: MDBX pending ring (current implementation)

Separate `pending.mdbx` (~5 MB) holds last 64 blocks. Key = block_number (u64 BE), value = hash + blooms + lz4_entry. Flat files are never mutated — only finalized blocks are appended. On finalization: read oldest from ring → append to flat store → delete from ring. On reorg: truncate ring entries ≥ fork point.

**Pros**:
- Flat files are truly immutable after write — clean invariant
- Crash-safe (MDBX ACID transactions)
- SDK reads pending ring via MDBX MVCC — safe concurrent access, no coordination
- Simple operations: put, get, cursor first/last, delete

**Cons**:
- MDBX C dependency in engine solely for 64 entries
- lmdbx-zig v0.3.0 has wrapper bugs (cursor_del, dbi_stat) — requires raw C API workarounds
- Data movement on finalization (read from MDBX → write to flat files → delete from MDBX)
- Two data sources for SDK to read (flat files + pending.mdbx)

### B: Append to flat files, ftruncate on reorg

Append blocks directly to flat files immediately (pre-finality). Track `finalized_block` alongside `last_block` in meta.bin. "Pending" state is implicit: blocks between finalized_block and last_block. Add `hashes.bin` (~2 KB for 64 blocks) for reorg detection. On reorg: ftruncate all files to fork point, re-append canonical.

**Pros**:
- No MDBX dependency in engine — pure flat files
- No data movement on finalization (just advance finalized_block in meta)
- Single data source for SDK (flat files only)
- Simpler engine with fewer moving parts

**Cons**:
- Flat files become mutable (ftruncate on reorg) — breaks immutability invariant
- SDK has blooms.bin/blocks.idx mmap'd — ftruncate during concurrent read causes SIGBUS
  - Mitigation: SDK mmaps only up to finalized_block, uses pread for pending tail
  - Mitigation: reorgs are rare (~1/day mainnet, 1 block deep), collision window is microseconds
- Adds complexity to SDK reader (split mmap/pread strategy)
- New file (hashes.bin) and meta field (finalized_block)
- Crash during ftruncate + re-append sequence needs careful recovery logic

### C: Directory of files (one per pending block)

Write each pending block as `pending/000100.bin`. Delete on finalization or reorg. SDK reads individual files.

**Pros**:
- Zero dependencies — filesystem ops only
- Atomic writes via tmp + rename
- Human-debuggable (`ls pending/`)

**Cons**:
- 64 inodes + directory entries
- Oldest/latest requires sorted readdir or separate index
- Concurrent read/write coordination between engine and SDK
- More code than either A or B

### D: Single pending.bin, atomic rewrite on block arrival

All pending blocks held in memory as an `ArrayList(PendingEntry)`. Persisted to a single `pending.bin` via tmp + rename on every mutation (new block, finalization, reorg). Flat files remain 100% immutable. On finalization: pop oldest from in-memory list, append to flat store, rewrite `pending.bin`.

```
# Immutable (never mutated after write)
blocks.dat, blooms.bin, blocks.idx, meta.bin

# Atomic rewrite (tmp + rename, triggered by block arrival)
pending.bin  — serialized pending list, ~1.6 MB typical, 12.8 MB worst
```

**Pros**:
- Flat files are truly immutable — invariant preserved
- No MDBX dependency in engine
- Crash-safe — rename is atomic, either old or new version exists, both valid
- No mmap/SIGBUS risk — SDK reads the whole file (small enough for a single `read()`)
- Concurrent reads safe — atomic rename guarantees SDK always sees a complete snapshot
- Simplest implementation — ArrayList in memory, serialize, tmp + rename
- Single data source for SDK to parse (one file, known format)

**Cons**:
- O(n) full rewrite per block vs O(1) MDBX put. n ≤ 64, ~1.6 MB — sub-millisecond on NVMe
- Data copy on finalization — pending.bin → flat files (~25 KB per block, negligible)
- All pending blocks in memory (~1.6 MB typical, 12.8 MB worst). Negligible but nonzero
- Worst case 12.8 MB rewrite (64 × 200 KB max). Fine on NVMe, could matter on slow storage
- O(n) lookup by block number (no key index). n ≤ 64, linear scan is fine
- Binary file — less debuggable than directory listing, more than MDBX
- One corrupt write affects all pending blocks (single file, no fault isolation per entry)

## Considerations

- MDBX is already a monorepo dependency (sdk entity stores). Adding it to engine doesn't increase the total dep count, only the coupling.
- Pending blocks are re-fetchable from the node. Crash-safety of the pending store is nice-to-have, not critical — on restart, re-fetch last 64 blocks from the node in seconds.
- Cross-store atomicity (pending → flat files) is impossible regardless of approach. Both A and B need idempotent finalization (detect if a block is already in flat files before re-writing).
- Option B's ftruncate concern is specific to concurrent SDK reads during reorgs — a rare event with a microsecond collision window. Defensive mitigation (split mmap) adds ~20 lines to SDK reader.

## Decision

**D — atomic rewrite of `pending.bin`**. Preserves flat file immutability, no MDBX in engine, simplest implementation. The O(n) rewrite cost is irrelevant at n ≤ 64 on NVMe with 12s block intervals.

## Consequences

- **If A (MDBX)**: Engine depends on lmdbx-zig. SDK reads two data sources. Finalization involves data movement. Clean immutability invariant on flat files.
- **If B (flat + ftruncate)**: Engine is pure flat files. SDK reader has split mmap/pread logic. No data movement on finalization. Flat files are mutable on reorg (rare).
- **If C (directory)**: No deps but most code to write and maintain.
- **If D (atomic rewrite)**: Engine is pure flat files + one small mutable file. Flat file immutability preserved. Sub-millisecond rewrite per block. SDK parses one extra small file. Simplest code path.
