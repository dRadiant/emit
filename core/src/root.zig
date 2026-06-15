//! emit core: shared infrastructure for the flat log store.
//!
//! Read-only access to blocks.dat/blooms.bin/blocks.idx, bloom filter ops,
//! parallel block filtering by address, packed log serialization, io_uring read pipeline.
//!
//! Imported by engine (import + head follow) and sdk (filtered index build).
pub const types = @import("types.zig");
pub const bloom = @import("bloom.zig");
pub const flat_reader = @import("flat_reader.zig");
pub const block_filter = @import("block_filter.zig");
pub const filter = @import("filter.zig");
pub const head_watch = @import("head_watch.zig");
pub const log_serial = @import("log_serial.zig");
pub const io_pipeline = @import("io_pipeline.zig");
pub const parallel = @import("parallel.zig");
pub const parallel_filter = @import("parallel_filter.zig");
pub const log = @import("log.zig");
pub const pending_format = @import("pending_format.zig");
pub const atomic_file = @import("atomic_file.zig");
pub const flat_format = @import("flat_format.zig");
pub const timestamps = @import("timestamps.zig");
pub const txs = @import("txs.zig");
pub const tcp_frame = @import("tcp_frame.zig");

// Top-level re-exports of commonly used types.
pub const RawLog = types.RawLog;
pub const Bloom = bloom.Bloom;
pub const AddrBloom = bloom.AddrBloom;
pub const FlatStoreReader = flat_reader.FlatStoreReader;
pub const Meta = flat_reader.Meta;
pub const TimestampReader = timestamps.TimestampReader;

test {
    _ = types;
    _ = bloom;
    _ = flat_reader;
    _ = block_filter;
    _ = filter;
    _ = head_watch;
    _ = log_serial;
    _ = io_pipeline;
    _ = parallel;
    _ = parallel_filter;
    _ = log;
    _ = pending_format;
    _ = atomic_file;
    _ = flat_format;
    _ = timestamps;
    _ = txs;
    _ = tcp_frame;
}
