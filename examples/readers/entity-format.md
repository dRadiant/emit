# entity-format

Binary layout of the files the SDK writes under `<data_dir>/`. Every file is
plain bytes, decodable with any language that has `struct.unpack` and a
filesystem. No library required.

Endianness convention:
- u32 / u64 sizes and counts are **little-endian**.
- Primary keys (first field of every entity) are **big-endian** so sorted-by-bytes
  equals sorted-by-numeric.
- All other entity fields are little-endian.

## Directory layout

```
<data_dir>/
├── entity/
│   ├── state.snap              cursor + every MutableStore slab + every ImmutableStore count + blob lengths
│   ├── <entity>.events.dat     append-only records, one file per ImmutableStore entity type
│   └── <entity>.blobs.dat      variable-length field payloads, one file per blob-bearing entity type
├── filter/
│   ├── primary.dat             filtered-index payloads (internal; rebuildable)
│   ├── primary.idx             filtered-index offsets (internal; rebuildable)
│   ├── children.dat            filtered-index payloads for factory children (internal; rebuildable)
│   ├── children.idx            filtered-index offsets for factory children (internal; rebuildable)
│   └── manifest.fingerprint    32-byte manifest hash; a mismatch triggers a rebuild
└── ethcall/
    └── ethcall.dat             eth_call result cache (advisory; deletable)
```

The `<entity>` filename component is the entity type's basename with its
first letter lowercased and `s` appended (e.g. `Transfer` →
`transfers.events.dat`, `SwapEvent` → `swapEvents.events.dat`). An entity
may override this via `pub const store_name = "events";` on the entity
struct.

## state.snap

The single atomicity-bearing file. Every commit writes a new `state.snap`
via tmp + fsync + rename. Cursor advance, MutableStore updates,
ImmutableStore count advances, and blob-file lengths are all published by
this one rename.

```
offset  size                            field
0       8                               magic "EMITSTAT"
8       4                               version (u32 LE; 1 with no blob stores, 2 with any)
12      8                               cursor (u64 LE; last fully-dispatched block)
20      mutable_count × 8               mutable_bytes[i] (u64 LE; slab byte length per MutableStore slot)
+       immutable_count × 8             immutable_counts[i] (u64 LE; authoritative record count per ImmutableStore slot)
+       blob_count × 8                  blob_bytes[i] (u64 LE; committed payload length per `<entity>.blobs.dat`) — version 2 only
+       Σ mutable_bytes[i]              body: MutableStore slabs concatenated in slot order
```

`mutable_count`, `immutable_count`, and `blob_count` are *not stored* in the
file — they are comptime-known from the indexer's entities tuple. Readers
know them from the schema spec they were written against. A schema with no
blob-bearing entity writes version 1 with no `blob_bytes` array, byte-
identical to the pre-blob layout. `blob_count` is the number of entities
(either kind) with a variable-length field, in entity-declaration order.

### Slab layout (one per MutableStore type)

A MutableStore slab is a dense, sorted array of fixed-size records. Records
are sorted ascending by primary key (the first field, big-endian encoded).

Record size and field layout are entity-specific and comptime-known. Each
entity is serialized field-by-field in declaration order:

- Field 0 (the primary key): big-endian.
- Fields 1..N: little-endian for integers; raw bytes for `[N]u8` arrays.
- A variable-length field (`[]const u8`, or `[]const T` for a fixed-size `T`)
  serializes to an 8-byte `BlobRef`, not the payload. The bytes live in
  `<entity>.blobs.dat` (see below). The record stays fixed-width, so binary
  search is unaffected.

Binary search on the slab is `O(log N)` using `std.mem.order(u8, ...)` on
the first `key_size` bytes of each record.

### BlobRef (8 bytes, variable-length fields)

```
A BlobRef is a u64 LE with two packed fields:
  bits 0..40   offset (absolute byte offset into <entity>.blobs.dat)
  bits 40..64  len    (payload byte length)
  (0, 0) = empty / unset → empty slice
```

Read it as `ref = u64_le(record[field_off..field_off+8])`, then
`offset = ref & 0xFFFFFFFFFF`, `len = ref >> 40`. The payload is
`blobs[offset : offset+len]` in the matching `<entity>.blobs.dat`. For a
`[]const u8`/`string`/`bytes` field that is the value directly; for a
`[]const T` field it is `len / sizeof(T)` little-endian elements (the writer
aligns the offset to `T`'s alignment).

## `<entity>.events.dat`

Append-only flat record file per ImmutableStore entity type. The
authoritative record count lives in `state.snap.immutable_counts[slot]` —
not in this file.

```
offset                     size                    field
0                          8                       magic "EMITEVTS"
8                          count × record_size     records (fixed-size, sorted by primary key)
```

A crashed prior commit may leave bytes past `8 + count × record_size`.
Those bytes are invisible to readers (bounded by the count from
`state.snap`) and are overwritten by the next append.

Records share the same serialization rule as MutableStore slabs:
big-endian primary key, little-endian everything else, and an 8-byte
`BlobRef` for any variable-length field (resolved against the entity's
`<entity>.blobs.dat`).

## `<entity>.blobs.dat`

Append-only payload file for one blob-bearing entity type's variable-length
fields (ADR-005). The fixed slab/events record holds a `BlobRef`
(`offset`, `len`); the bytes live here. Never mutated in place — an updated
field appends fresh bytes and orphans the old (garbage, not corruption).

```
offset                     size                    field
0                          8                       magic "EMITBLOB"
8                          variable                concatenated payloads, element-aligned
```

The authoritative valid length is `state.snap.blob_bytes[slot]`, not this
file's size. Bytes past it are orphan tail from a crashed commit (the commit
order is blobs.dat append + fsync, then the `state.snap` rename), invisible
to readers and overwritten by the next append — the same discipline as
`events.dat`. A `BlobRef.offset` is an absolute file offset (so it includes
the 8-byte header); resolve a payload as `blobs[offset : offset+len]`.

## primary.dat / primary.idx (filtered index, internal)

The filtered index is regenerable from the engine's flat store, so this
file pair has no durability requirement. Readers that delete it will see
the SDK rebuild it on the next run.

There are two pairs of files: `primary.{dat,idx}` and `children.{dat,idx}`.
The split exists because the children pair is appended after factory
discovery, when block numbers from the primary pair may already be present.

### primary.dat / children.dat

```
offset                   size              field
0                        8                 magic "EMITFDAT"
8                        variable          per-block entries, concatenated

Each entry:
  offset 0         4          lz4_len (u32 LE)
  offset 4         lz4_len    LZ4-compressed packed-log payload
  offset 4+lz4_len variable   tx subtable (present only for manifests whose
                              events declare `tx_fields`; absent otherwise)
```

Each LZ4 payload decodes to a packed-log block payload — same format as the
engine's `blocks.dat`, but filtered to only the logs matching the manifest.
Block-payload format is documented in `core/src/log_serial.zig`.

The tx subtable holds the kept logs' transaction fields (ADR-006), bounded
by the index entry's `length`. Readers that don't want it can ignore every
byte past `4 + lz4_len`:

```
offset  size         field
0       4            record_count (u32 LE)
4       count × 76   TxRecords, sorted ascending by tx_index

Each TxRecord (76 bytes):
  offset 0   2    tx_index (u16 LE)
  offset 2   1    tx_type (u8; 0x00 legacy … 0x04 EIP-7702)
  offset 3   1    flags (u8; bit0 = to absent / contract creation,
                  bit1 = sender unrecovered in source)
  offset 4   20   from
  offset 24  20   to (zero when bit0 set)
  offset 44  32   value (u256 LE)
```

### primary.idx / children.idx

```
offset                   size              field
0                        8                 magic "EMITFIDX"
8                        count × 24        index entries (count is implicit from idx file size)

Each index entry (24 bytes):
  offset 0   8   block_number (u64 BE)
  offset 8   4   timestamp (u32 LE, exact block time; 0 = unknown)
  offset 12  8   dat_offset (u64 LE, byte offset into the matching .dat)
  offset 20  4   length (u32 LE, full entry length incl. any tx subtable)
```

Block numbers are strictly increasing.

## ethcall.dat

Advisory cache for eth_call results. Invalidation is "delete this file" —
the next prefetch phase rebuilds the relevant entries from RPC.

```
offset                   size                       field
0                        8                          magic "EMITCALL"
8                        variable                   records, length-prefixed

Each record:
  offset 0    20         target (EVM address)
  offset 20   32         calldata_hash (keccak256 of the calldata, full 32 bytes)
  offset 52   1          status (0 = success, 1 = reverted)
  offset 53   4          value_len (u32 LE)
  offset 57   value_len  value (raw ABI-encoded return payload; empty if status = 1)
```

The cache key for a lookup is the 52-byte concatenation of `target` and
`calldata_hash`. Duplicate keys are allowed in the file (later records
overwrite earlier ones in the in-memory map built on open).

A truncated last record is detected on open and truncated away before any
further writes — the file is self-healing in this respect.

## Schema evolution

The `version` field in `state.snap` selects the layout: **1** for a schema
with no blob-bearing entity (the original layout), **2** when any entity has
a variable-length field (adds the `blob_bytes` array). A reader keys off the
version to decide whether to parse `blob_bytes`.

If the SDK detects a version it does not expect for the schema it was built
for, it raises a loud error directing the operator to delete the data
directory and re-backfill. There is no in-place migration — re-backfill is
fast enough that maintenance cost beats the savings.

## Reference readers

Working examples that decode entity files in other languages:

- `examples/readers/python/reader.py` — opens `state.snap`, walks the
  mutable slab descriptors, decodes a MutableStore's records into a list
  of tuples.
- `examples/readers/c/reader.c` — mmaps `state.snap` and prints cursor +
  per-store record counts.
- `examples/readers/zig/reader.zig` — sdk-free Zig decode of the same.
- `examples/readers/python/blob_reader.py` — decodes a blob-bearing entity
  (the ENS `Registration`), resolving each `BlobRef` against
  `<entity>.blobs.dat` to recover the variable-length field.

The readers consume the format documented above. The first three target the
blobless ERC20 schema (version 1); `blob_reader.py` shows the version-2 path.
