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
├── state.snap                  cursor + every MutableStore slab + every ImmutableStore count
├── <entity>.events.dat         append-only records, one file per ImmutableStore entity type
├── primary.dat                 filtered-index payloads (internal; rebuildable)
├── primary.idx                 filtered-index offsets (internal; rebuildable)
├── children.dat                filtered-index payloads for factory children (internal; rebuildable)
├── children.idx                filtered-index offsets for factory children (internal; rebuildable)
└── ethcall.dat                 eth_call result cache (advisory; deletable)
```

The `<entity>` filename component is the entity type's basename lowercased
with `s` appended (e.g. `Transfer` → `transfer.events.dat`). An entity may
override this via `pub const store_name = "events";` on the entity struct.

## state.snap

The single atomicity-bearing file. Every commit writes a new `state.snap`
via tmp + fsync + rename. Cursor advance, MutableStore updates, and
ImmutableStore count advances are all published by this one rename.

```
offset  size                            field
0       8                               magic "EMITSTAT"
8       4                               version (u32 LE; currently 1)
12      8                               cursor (u64 LE; last fully-dispatched block)
20      mutable_count × 8               mutable_bytes[i] (u64 LE; slab byte length per MutableStore slot)
+       immutable_count × 8             immutable_counts[i] (u64 LE; authoritative record count per ImmutableStore slot)
+       Σ mutable_bytes[i]              body: MutableStore slabs concatenated in slot order
```

`mutable_count` and `immutable_count` are *not stored* in the file — they
are comptime-known from the indexer's entities tuple. Readers know N and M
from the schema spec they were written against.

### Slab layout (one per MutableStore type)

A MutableStore slab is a dense, sorted array of fixed-size records. Records
are sorted ascending by primary key (the first field, big-endian encoded).

Record size and field layout are entity-specific and comptime-known. Each
entity is serialized field-by-field in declaration order:

- Field 0 (the primary key): big-endian.
- Fields 1..N: little-endian for integers; raw bytes for `[N]u8` arrays.

Binary search on the slab is `O(log N)` using `std.mem.order(u8, ...)` on
the first `key_size` bytes of each record.

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
big-endian primary key, little-endian everything else.

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
8                        variable          LZ4-compressed log entries, concatenated
```

Each LZ4 entry decodes to a packed-log block payload — same format as the
engine's `blocks.dat`, but filtered to only the logs matching the manifest.
Block-payload format is documented in `core/src/log_serial.zig`.

### primary.idx / children.idx

```
offset                   size              field
0                        8                 magic "EMITFIDX"
8                        count × 20        index entries (count is implicit from idx file size)

Each index entry (20 bytes):
  offset 0   8   block_number (u64 BE)
  offset 8   8   dat_offset (u64 LE, byte offset into the matching .dat)
  offset 16  4   length (u32 LE, compressed payload length)
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

The `version` field in `state.snap` is reserved for future format bumps.
The current shipping version is **1**.

If the SDK detects an unrecognized version on open, it raises a loud error
directing the operator to delete the data directory and re-backfill. There
is no in-place migration — re-backfill is fast enough that maintenance
cost beats the savings.

## Reference readers

Working examples that decode entity files in other languages:

- `examples/readers/python/reader.py` — opens `state.snap`, walks the
  mutable slab descriptors, decodes a MutableStore's records into a list
  of tuples.
- `examples/readers/c/reader.c` — mmaps `state.snap` and prints cursor +
  per-store record counts.

Both readers consume the format documented above and stay in sync with
the schema as long as `version == 1` is unchanged.
