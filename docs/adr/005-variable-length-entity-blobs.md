# ADR-005: Variable-Length Entity Fields — Out-of-Line Blobs

**Status**: Implemented
**Date**: 2026-06-05 (proposed)
**Context**: ADR-003 established the SDK's pure-Zig storage: fixed-stride sorted slabs in `state.snap` for `MutableStore`, append-only `<entity>.events.dat` for `ImmutableStore`, both serialized by the comptime `entity_serial.zig`. That serializer accepts only integers and fixed-size `[N]u8` arrays — every record has a comptime-constant width, which is what makes the slab binary-searchable (`base + i·stride`) and the records stack-copyable. This ADR decides how to add **variable-length fields** (Solidity `string`, dynamic `bytes`, flat dynamic arrays) without giving up any of those properties.

## Problem

Entity fields are fixed-size only. There is no way to store a value whose length is not known at comptime — an NFT name or token URI, an ENS name, a governance proposal description, a market label.

The naive fix to let a record grow to inline the bytes is a non-starter: variable-width records break `base + i·stride` binary search, the dense slab, the stack-copyable record, and the simple `mutable_bytes` accounting. The design space is "how do we store variable bytes while keeping the record fixed-width."

The fundamentals from ADR-003 that any solution **must preserve**:

1. **Fixed-stride sorted slab** → O(log N) binary search by primary key.
2. **Single atomic commit** (`state.snap` rename) for cursor + all stores, crash-safe.
3. **Zero-copy reads** — mmap/stack slices, no hot-loop allocation.
4. **Comptime serializer** generated from the struct type.
5. **Flat files only** — no B-tree, no Postgres, cross-language-readable.
6. **Numeric path zero-regression** — the 2M-events/s ERC20 case must be byte-for-byte unchanged.

## Options

### A: Out-of-line blobs — `BlobRef` in the fixed record, bytes in an append-only `<entity>.blobs.dat`

The record stays **fixed-width**. Each variable field serializes to a `BlobRef` (a packed `offset || len`); the bytes live in a per-type append-only `<entity>.blobs.dat`, mmap'd for reads. This is exactly the offset-addressing the engine's flat store already uses — `blocks.dat` holds variable-length LZ4 payloads keyed by `(offset, length)` from `blocks.idx` — applied one level up, at the entity field.

```
BlobRef (8 bytes, packed LE):
  offset : u40   byte offset into <entity>.blobs.dat   (1 TB addressable)
  len    : u24   byte length                            (16 MB max field)
  (0, 0) = empty / unset → empty slice

<entity>.blobs.dat:
  magic  [8]u8   "EMITBLOB"
  [body: concatenated blob payloads, append-only, NEVER mutated]
```

The authoritative valid length of each `blobs.dat` lives in `state.snap` — a new `blob_bytes[N] u64 LE` array, parallel to ADR-003's `mutable_bytes`/`immutable_counts`. Bytes past that length are orphan tail from a crashed commit: invisible (no committed record references them) and overwritten by the next append — identical to how `events.dat` already handles its uncommitted tail.

**Pros**
- Record width stays comptime-constant → binary search, the HashMap front, and `mutable_bytes` accounting are untouched.
- Reuses ADR-003's exact crash-consistency discipline (append-only data file + authoritative length in `state.snap`); no new commit protocol.
- Reads are zero-copy mmap slices. Blobs ride the existing per-block overlay (arena for pending blocks, dropped on reorg, flushed on finalize), so the live path reuses existing machinery.
- Numeric-only entities get no `blobs.dat` and compile to today's exact layout — zero regression.
- Cross-language readers gain one indirection (read the 8-byte `BlobRef`, slice `blobs.dat`), still minimal implementation cost.

**Cons**
- Mutating a blob field orphans the old bytes (append-only) → garbage that needs occasional compaction (see Considerations). Absent for write-once / immutable usage.
- Introduces a borrowed-slice lifetime contract: blob reads borrow the mmap, valid under `ctx.lock()` / the store's lifetime.
- A second file per blob-bearing entity type.

### B: Variable-length records + a `(key, offset, len)` index

Abandon the fixed-stride slab. Store whole records (fixed *and* variable fields) in a `.dat`, with a parallel fixed-stride index of `(key, offset, len)`. Binary search the index; the record itself is variable.

**Pros**
- One file per store, no separate blob file.
- Natural for `ImmutableStore` (append-only, monotonic key — the index *is* `events.dat` + an offset table, like `blocks.idx`).

**Cons**
- Variabilizes the *whole* record, so the fixed fields lose their stack-copyable, dense, directly-binary-searchable form — complexity pushed into the common numeric path for no gain over A.
- `MutableStore` can't update a grown record in place → append-new + reindex, the same garbage problem as A but now for *every* field change, not just blob fields.
- Two indirections to read a fixed field (index → record → field) where A has none.

### C: Inline fixed-cap (`bytesN`) — the status quo

Cap variable fields at a fixed width (`bytes32`, `bytes64`) and pad. Available today.

**Pros**
- Zero new machinery.

**Cons**
- Does not solve the problem: wastes space for short values and *fails outright* for anything longer than the cap (URIs, proposal text). It's fixed-size by another name.

### D: Dictionary / interning

Intern repeated values: a `dictionary.dat` maps value → `u32` id; records store the id.

**Pros**
- Excellent for **low-cardinality, high-repetition** values (enums, a handful of symbols) — collapses a string field to a `u32` + a tiny dictionary.

**Cons**
- For **high-cardinality unique** values (every NFT's distinct URI), the dictionary *is* the blob store with extra lookup overhead — no win.
- Not a general solution; it's an optimization that layers *on top of* A for the low-cardinality case.

### E: External store (Postgres/SQLite sidecar for variable data)

Keep fixed fields in the flat slab; push variable fields to an embedded SQL store.

**Pros**
- Variable length, indexes, and SQL come free.

**Cons**
- Reintroduces exactly the C dependency, the cross-language friction, and the commit-latency cost ADR-003 removed — for a subset of fields. Splits the entity across two storage engines with two consistency domains. Directly contradicts the pure-Zig, flat-file, single-atomic-commit thesis.

## Considerations

**Crash consistency.** The invariant: a committed `state.snap` references only blob bytes durable in `blobs.dat`. The commit sequence extends ADR-003's by one step:

```
1. For each store T with new blobs this cycle:
     append blob arena bytes to T.blobs.dat at offset blob_bytes[T]
     fsync(T.blobs.dat)
   (events.dat appends happen here too, per ADR-003)
2. Build state.snap: cursor, mutable_bytes, immutable_counts, blob_bytes (NEW), body
   — every BlobRef embedded in the slab/events records now points at an offset < blob_bytes[T]
3. write state.snap.tmp → fsync → rename over state.snap → fsync(dir)
```

The atomic primitive is still step 3's rename. Crash before it: `blobs.dat` holds orphan tail bytes (≥ the previous `blob_bytes[T]`), and the previous `state.snap` references none of them — invisible, overwritten next append. Crash during/after: identical to ADR-003. No truncation, no GC, no WAL.

**Reorg / overlay integration.** Blobs ride the existing per-block overlay. While a block is pending, its blob bytes sit in the overlay's arena with arena-relative offsets and tentative `BlobRef`s; reads resolve against the arena. On reorg the arena is dropped — `blobs.dat` never saw the bytes. On finalize the arena flushes to `blobs.dat` and offsets are rebased to `blob_bytes[T] + arena_offset`. This is the same drop-on-reorg / flush-on-finalize lifecycle the entity overlay already implements; blobs add a byte-arena beside the per-block submaps, nothing more.

**The mutable-blob garbage trade-off.** Append-only means an *updated* blob field orphans its old bytes. Quantified:
- `ImmutableStore` (append-only, never mutated) → `blobs.dat` is always garbage-free.
- Write-once fields — the overwhelming majority of variable entity data is immutable metadata (token name/symbol, NFT URI, ENS name) set at creation → zero garbage.
- Churning mutable blob fields → an **offline compaction**: walk live records, copy their referenced blobs into a fresh `blobs.dat` (reassigning offsets), rewrite the slab with the new `BlobRef`s, atomic-rename both — the same shape as a `state.snap` rewrite. It's a **space** concern, deferrable indefinitely (dead bytes are simply unreferenced), never a correctness one.

**Read lifetime.** A blob read returns `blobs_map[offset..][0..len]` — a slice borrowing the mmap (or the overlay arena for pending blocks). Valid under `ctx.lock()` / for the store's lifetime. Code that must outlive the lock copies, exactly as it would for any borrowed field. This is the same slice-lifetime contract the array/tuple ABI work needs (§6) and the events-first "borrow from `log.data`" idiom already in the codebase.

**Serializer split.** `entity_serial.zig`'s comptime `inline for` gains one field-kind: a blob-typed field has `fixedSize = sizeof(BlobRef) = 8` and (de)serializes the 8-byte `BlobRef`. The slice ↔ `BlobRef` resolution (copy bytes to the arena on write; slice `blobs.dat` on read) lives in the store layer (`MutableStore`/`ImmutableStore`), which owns the arena and the mmap — `entity_serial` stays a stateless byte-packer that simply treats a blob field as a fixed 8-byte slot. The handler sees a blob field as `[]const u8` (via a marker type, see Open Questions).

**Primary-key constraint.** A blob field cannot be the primary key (`field[0]`): the key must stay fixed-width for the sorted slab and binary search. Enforced with `@compileError`. Keys are always addresses/ids/block numbers — fixed — so this costs nothing real.

**Performance.** Numeric-only entities are untouched (no `blobs.dat`, no `BlobRef`, byte-identical layout). A blob-bearing entity pays: one memcpy per blob *write* (slice → arena), one slice computation per *read*, and one extra `fsync` per *commit cycle* (amortized over the 100K commit interval → noise). The hot 2M-events/s path for fixed entities sees zero of this.

**Scope for v1.2.** Strings and dynamic `bytes` first (the demanded cases). Flat dynamic arrays `[]T` of fixed-size `T` fold into the same `BlobRef` mechanism (`count·sizeof(T)` bytes) and can ship alongside or immediately after. Nested dynamic (array-of-strings, `string[]`) needs recursive offsets and is explicitly deferred.

## Decision

**Adopt option A.** A fixed-width `BlobRef` in the record; bytes in a per-type append-only `<entity>.blobs.dat`; the valid length in a new `state.snap.blob_bytes[N]` array; reads as zero-copy mmap slices through the existing overlay-then-durable path.

Rationale:

- It is the only option that preserves **all six** ADR-003 fundamentals. B variabilizes the fixed path; C doesn't solve the problem; E abandons the thesis; D is an optimization, not a base.
- It is not a new mechanism — it is `blocks.dat`'s proven `(offset, len)` addressing and `events.dat`'s proven append-only / authoritative-length / orphan-tail-safe commit discipline, reused at the entity layer. Low conceptual cost, low risk.
- It rides the existing per-block overlay lifecycle, so reorg/commit correctness comes for free.
- The single real cost (mutable-blob garbage) is bounded, deferrable, offline, and absent in the common write-once / immutable case.

## Consequences

- **Closes the one real Envio entity-model gap** (NFT/ENS/governance/labels) while keeping the pure-Zig, flat-file, single-atomic-commit, zero-copy storage intact.
- `entity_serial.zig` gains a blob field-kind; `MutableStore`/`ImmutableStore` gain arena-write / mmap-read blob resolution and an overlay blob-arena.
- `state.snap` header gains `blob_bytes[N]` (a version bump). ADR-003's documented `state.snap` layout and `docs/entity-format.md` must be updated; the reference Python/C readers gain a `BlobRef` resolve step (still trivial).
- New file `<entity>.blobs.dat` (`"EMITBLOB"`) for blob-bearing types; blobless types are unaffected and produce no such file.
- An offline `blobs.dat` compaction utility (deferrable — only needed for heavy mutable-blob churn).
- No migration tool — consistent with ADR-003, operators delete the entity dir and re-backfill.

### Open questions — resolved at implementation

- **Marker type — plain `[]const u8`, no newtype.** The `sdk.Text`/`sdk.Bytes` newtypes were dropped. A field's type *is* the declaration (`label: []const u8`), read like any other field, no `.bytes` accessor. The feared ambiguity with "future genuine `[]const u8`" does not exist (an entity slice field has no other meaning), and plain slices extend to `[]const T` arrays for free. `entity_serial.fieldKind` classifies a `[]const T` (fixed-size `T`) field as a blob; any other slice type is a loud compile error.
- **Array support — shipped with strings.** `[]const u8` (string/bytes) and `[]const T` for a fixed-size `T` (`[]const u64`, `[]const [20]u8`) both land. The writer aligns the blob offset to `@alignOf(T)`, so a `[]const T` read is a zero-copy aligned cast (host order == on-disk LE on x86-64). Nested dynamics (`[]const []const u8`) stay a compile error.
- **Null vs empty.** Kept: `(offset=0, len=0)` is the empty slice; no "absent" distinction.
- **Compaction trigger.** Kept: manual offline utility only, deferred until a churning-blob workload appears.

### Read lifetime (an addition not in the original design)

A blob field read borrows the store mmap, and `Context.read`/`count`/`range` release the lock on return — so a value handed past the lock would dangle on a concurrent commit (live mode). Resolution: those auto-locking accessors are a **compile error** for blob entities, which are read through `ctx.readView()` — a guard that holds the lock across the borrow's use (the caller copies out before `deinit`). Handlers are unaffected (they already run under the lock).

## Implementation & verification (2026-06-14)

`MutableStore` blob path (backfill + live overlay with per-block arenas, reorg-safe), `state.snap` conditional v2 header, the `blob_log.zig` append-stage-mmap module with crash-tail safety, the `readView` guard, and top-level `string`/`bytes` ABI decode all landed; the numeric path is byte-identical (the pre-blob test suite passes unchanged). End-to-end on the reference box: an ENS indexer (`examples/ens`) storing each registration's `name` as a `[]const u8` blob backfilled **799,333 registrations** (`state.snap` v2, ~9 MB `registrations.blobs.dat`); a sample of 400 stored names matched an independent `eth_getLogs` ABI decode 400/400, and the reference `blob_reader.py` re-derives the same names straight from the documented format. **ImmutableStore blobs remain pending** (the append-only variant of the same machinery); blob entities must be `mutable` until then.

### Options not chosen

**B — variable-length records + offset index.** Pushes variability into the fixed-field path; `MutableStore` in-place updates become append+reindex for every field. Rejected: more complexity in the common path than A, for no benefit A doesn't already give.

**C — inline fixed-cap.** The status quo; fails for any value longer than the cap. Not a solution.

**D — dictionary/interning.** A future optimization layerable on A for low-cardinality repeated values; no win for high-cardinality unique values. Not a base mechanism.

**E — external SQL store.** Reintroduces the C dependency, cross-language friction, and commit cost ADR-003 removed, and splits the entity across two consistency domains. Contradicts the storage thesis.
