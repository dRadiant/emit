# emit-core

Shared infrastructure: types, bloom filter, flat-store reader, log serializer, block filter, io_uring pipeline, atomic file helpers.

## What it does

`core` is the foundation imported by `engine` and `sdk`. It owns the on-disk format for the flat log store (`blocks.dat`, `blooms.bin`, `blocks.idx`, `meta.bin`, `pending.bin`) and the read-side primitives both consumers need: bloom scan, LZ4-compressed log decode, parallel block reads via io_uring, atomic file writes.

Module index:

| File | Role |
|---|---|
| `types.zig` | `RawLog`, `BLOCK_BUF_SIZE`, `MAX_LOGS_PER_BLOCK`, `FINALITY_DEPTH` |
| `bloom.zig` | `Bloom(SIZE)` + `AddrBloom` comptime generics |
| `flat_reader.zig` | `FlatStoreReader`: mmap `blocks.idx` + `blooms.bin`, pread `blocks.dat` |
| `block_filter.zig` | `scanBloomsParallel`: dual bloom check, matching block list |
| `log_serial.zig` | Pack/unpack log entries, LZ4 compress/decompress wrappers |
| `io_pipeline.zig` | `ReadPipeline(QD)`: io_uring batch reads from `blocks.dat` |
| `parallel.zig` | Thread pool: `run()`, `chunkRanges()` |
| `pending_format.zig` | `pending.bin` wire format (single source of truth) |
| `atomic_file.zig` | tmp + fsync + rename primitive used by every commit point |
| `flat_format.zig` | Magic header helpers for all flat-file pairs |

## Public surface

Re-exported from `root.zig`:

- `RawLog`, `Bloom`, `AddrBloom`, `FlatStoreReader`, `Meta`
- Submodules `types`, `bloom`, `flat_reader`, `block_filter`, `log_serial`, `io_pipeline`, `parallel`, `pending_format`, `atomic_file`, `flat_format`

The full module set is intentionally public.

## Dependencies

- One external dependency: `zig-lz4` (compress/decompress).

`engine` and `sdk` import `core`. **Nothing imports `engine` or `sdk` from `core`.**

## Build and test

```sh
zig build test --summary all
```

Tests are in-memory where possible. mmap-backed structs use page-aligned heap buffers via `testReader()` / `buildTestBlooms()` helpers in `flat_reader.zig` and `block_filter.zig` rather than tmp directories.

## Performance notes

`core` owns the read hot path. Two Linux-specific optimizations live here:

- **io_uring (`io_pipeline.zig`).** Queue depth 16 per worker × 7 workers in the SDK's filter build = 112 concurrent NVMe reads in flight. Prototype measured mmap was 4.5× slower for the historical block-read workload. On non-Linux, `io_pipeline.supported` is `false` and consumers fall back to plain `pread`.
- **`fadvise(POSIX_FADV_WILLNEED)` (`block_filter.zig`).** Issued on matching blocks during the bloom scan before io_uring workers begin reading. The kernel starts async NVMe DMA in parallel. ~30% faster warm runs. Gated by a comptime `prefetch` flag plus the Linux check.

Both are no-ops on non-Linux hosts; functionality degrades gracefully but the speed claims in the root README assume Linux.

## License

AGPL-3.0. See [LICENSE](../LICENSE).
