//! Per-block transaction tables: `txs.dat` + `txs.idx` (ADR-006).
//!
//! One LZ4 entry per block holding the block's log-producing transactions'
//! `from`/`to`/`value`, dense by block number. Advisory like `timestamps.bin`:
//! an absent or partially-covered pair degrades gracefully, the engine
//! backfills it into an existing store with a resume cursor, and only
//! manifests opting into `tx_fields` ever read it.
//!
//! txs.dat layout:
//!   0  8        magic "EMITTXSD"
//!   [LZ4 entries, append-only: lz4_len(u32 LE) ‖ lz4_data]
//!   entry payload: count(u16 LE) ‖ [count × TxRecord], sorted by tx_index
//!
//! txs.idx layout:
//!   0   8       magic "EMITTXSI"
//!   8   8       first_block (u64 LE)
//!   16  8       count (u64 LE), published last on `sync`
//!   24  count×12  (offset u64 LE, len u32 LE) per block
//!
//! Crash ordering: dat entry, then idx entry, then count. Bytes past the
//! published count are an orphan tail, invisible to readers and overwritten
//! by the next append after a resume.
const std = @import("std");

const flat_format = @import("flat_format.zig");
const log_serial = @import("log_serial.zig");

pub const DAT_MAGIC: flat_format.Magic = "EMITTXSD".*;
pub const IDX_MAGIC: flat_format.Magic = "EMITTXSI".*;
pub const DAT_NAME = "txs.dat";
pub const IDX_NAME = "txs.idx";

pub const IDX_HEADER_SIZE: usize = 24; // magic(8) + first_block(8) + count(8)
pub const IDX_ENTRY_SIZE: usize = 12; // offset u64 LE + len u32 LE
pub const RECORD_SIZE: usize = 76;

pub const FLAG_TO_ABSENT: u8 = 1; // contract creation, `to` is zero
pub const FLAG_FROM_UNRECOVERED: u8 = 2; // sender missing in source, `from` is zero

/// One log-producing transaction's fields. `value` is u256 LE bytes so the
/// record serializes with plain copies. Fixed 76-byte stride.
pub const TxRecord = struct {
    tx_index: u16,
    tx_type: u8,
    flags: u8,
    from: [20]u8,
    to: [20]u8,
    value: [32]u8,

    pub fn valueU256(self: *const TxRecord) u256 {
        return std.mem.readInt(u256, &self.value, .little);
    }
};

/// Locate `tx_index` in a block's table. Records are sorted ascending and
/// blocks hold at most a few hundred, so a linear scan suffices.
pub fn find(records: []const TxRecord, tx_index: u16) ?*const TxRecord {
    for (records) |*r| {
        if (r.tx_index == tx_index) return r;
        if (r.tx_index > tx_index) return null;
    }
    return null;
}

/// Serialize a block's table into `buf`. Returns bytes written.
pub fn serializeRecords(records: []const TxRecord, buf: []u8) usize {
    std.mem.writeInt(u16, buf[0..2], @intCast(records.len), .little);
    var pos: usize = 2;
    for (records) |r| {
        std.mem.writeInt(u16, buf[pos..][0..2], r.tx_index, .little);
        buf[pos + 2] = r.tx_type;
        buf[pos + 3] = r.flags;
        @memcpy(buf[pos + 4 ..][0..20], &r.from);
        @memcpy(buf[pos + 24 ..][0..20], &r.to);
        @memcpy(buf[pos + 44 ..][0..32], &r.value);
        pos += RECORD_SIZE;
    }
    return pos;
}

/// Parse a decompressed entry payload into `out`. Returns the filled slice.
pub fn deserializeRecords(payload: []const u8, out: []TxRecord) ![]TxRecord {
    if (payload.len < 2) return error.Truncated;
    const count: usize = std.mem.readInt(u16, payload[0..2], .little);
    if (count > out.len) return error.TooManyRecords;
    if (payload.len < 2 + count * RECORD_SIZE) return error.Truncated;
    var pos: usize = 2;
    for (out[0..count]) |*r| {
        r.tx_index = std.mem.readInt(u16, payload[pos..][0..2], .little);
        r.tx_type = payload[pos + 2];
        r.flags = payload[pos + 3];
        r.from = payload[pos + 4 ..][0..20].*;
        r.to = payload[pos + 24 ..][0..20].*;
        r.value = payload[pos + 44 ..][0..32].*;
        pos += RECORD_SIZE;
    }
    return out[0..count];
}

const MmapSlice = []align(std.heap.page_size_min) const u8;

/// Read-only handle: idx mmap'd for O(1) lookups, dat read via pread.
/// Null `open` when the pair is absent, like an absent timestamps.bin.
pub const TxsReader = struct {
    dat: std.fs.File,
    idx_map: MmapSlice,
    first_block: u64,
    count: u64,

    pub fn open(dir: std.fs.Dir) !?TxsReader {
        const idx_file = dir.openFile(IDX_NAME, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer idx_file.close();

        const size = (try idx_file.stat()).size;
        if (size < IDX_HEADER_SIZE) return null;

        const map = try std.posix.mmap(null, size, std.posix.PROT.READ, .{ .TYPE = .SHARED }, idx_file.handle, 0);
        errdefer std.posix.munmap(map);
        try flat_format.validateMagic(map, IDX_MAGIC);

        const first_block = std.mem.readInt(u64, map[8..16], .little);
        // Clamp the live-writer race like timestamps: entries land before the
        // count publishes, so count×entry ≤ mapped size always holds.
        const count = @min(
            std.mem.readInt(u64, map[16..24], .little),
            (size - IDX_HEADER_SIZE) / IDX_ENTRY_SIZE,
        );

        const dat = dir.openFile(DAT_NAME, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        errdefer dat.close();
        var magic: [8]u8 = undefined;
        if ((try dat.preadAll(&magic, 0)) != 8) return error.Truncated;
        try flat_format.validateMagic(&magic, DAT_MAGIC);

        return .{ .dat = dat, .idx_map = map, .first_block = first_block, .count = count };
    }

    pub fn deinit(self: *TxsReader) void {
        self.dat.close();
        std.posix.munmap(self.idx_map);
    }

    /// True when `[from, to]` lies inside the covered range, the fail-loud
    /// check for manifests requiring tx fields.
    pub fn covers(self: *const TxsReader, from: u64, to: u64) bool {
        return from >= self.first_block and to < self.first_block + self.count;
    }

    /// Read one block's table. Null outside coverage. `payload_buf` holds the
    /// raw entry, `decompress_buf` the decompressed payload.
    pub fn readBlock(
        self: *const TxsReader,
        block: u64,
        payload_buf: []u8,
        decompress_buf: []u8,
        out: []TxRecord,
    ) !?[]TxRecord {
        if (block < self.first_block) return null;
        const i = block - self.first_block;
        if (i >= self.count) return null;

        const e = self.idx_map[IDX_HEADER_SIZE + i * IDX_ENTRY_SIZE ..][0..IDX_ENTRY_SIZE];
        const offset = std.mem.readInt(u64, e[0..8], .little);
        const len: usize = std.mem.readInt(u32, e[8..12], .little);
        if (len > payload_buf.len) return error.BufferTooSmall;

        if ((try self.dat.preadAll(payload_buf[0..len], offset)) != len) return error.Truncated;
        const payload = try log_serial.decompressEntry(payload_buf[0..len], decompress_buf);
        return try deserializeRecords(payload, out);
    }
};

/// Dense appender with the timestamps resume discipline: reopen a matching
/// pair and continue from the published count, recreate on any mismatch.
pub const TxsWriter = struct {
    dat: std.fs.File,
    idx: std.fs.File,
    first_block: u64,
    count: u64,
    dat_size: u64,

    pub fn open(dir: std.fs.Dir, first_block: u64) !TxsWriter {
        if (try resume_(dir, first_block)) |w| return w;

        const dat = try dir.createFile(DAT_NAME, .{ .read = true, .truncate = true });
        errdefer dat.close();
        try dat.writeAll(&DAT_MAGIC);

        const idx = try dir.createFile(IDX_NAME, .{ .read = true, .truncate = true });
        errdefer idx.close();
        var hdr: [IDX_HEADER_SIZE]u8 = undefined;
        @memcpy(hdr[0..8], &IDX_MAGIC);
        std.mem.writeInt(u64, hdr[8..16], first_block, .little);
        std.mem.writeInt(u64, hdr[16..24], 0, .little);
        try idx.writeAll(&hdr);

        return .{ .dat = dat, .idx = idx, .first_block = first_block, .count = 0, .dat_size = 8 };
    }

    fn resume_(dir: std.fs.Dir, first_block: u64) !?TxsWriter {
        const idx = dir.openFile(IDX_NAME, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        errdefer idx.close();

        var hdr: [IDX_HEADER_SIZE]u8 = undefined;
        const n = idx.preadAll(&hdr, 0) catch 0;
        if (n != IDX_HEADER_SIZE or !std.mem.eql(u8, hdr[0..8], &IDX_MAGIC) or
            std.mem.readInt(u64, hdr[8..16], .little) != first_block)
        {
            idx.close();
            return null;
        }
        const count = std.mem.readInt(u64, hdr[16..24], .little);

        const dat = dir.openFile(DAT_NAME, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => {
                idx.close();
                return null;
            },
            else => return err,
        };
        errdefer dat.close();

        // Next append position from the last PUBLISHED entry, so an orphan dat
        // tail from a crash before `sync` is overwritten, not extended.
        var dat_size: u64 = 8;
        if (count > 0) {
            var e: [IDX_ENTRY_SIZE]u8 = undefined;
            const en = idx.preadAll(&e, IDX_HEADER_SIZE + (count - 1) * IDX_ENTRY_SIZE) catch 0;
            if (en != IDX_ENTRY_SIZE) {
                idx.close();
                dat.close();
                return null;
            }
            dat_size = std.mem.readInt(u64, e[0..8], .little) + std.mem.readInt(u32, e[8..12], .little);
        }

        return .{ .dat = dat, .idx = idx, .first_block = first_block, .count = count, .dat_size = dat_size };
    }

    /// Append one block's table. Blocks must arrive dense from
    /// `first_block + count`. `serialize_buf`/`compress_buf` are caller
    /// scratch sized for the largest block (a few hundred × 76 B).
    pub fn append(
        self: *TxsWriter,
        block: u64,
        records: []const TxRecord,
        serialize_buf: []u8,
        compress_buf: []u8,
    ) !void {
        if (block != self.first_block + self.count) return error.NonDenseAppend;

        const written = serializeRecords(records, serialize_buf);
        const entry_len = try log_serial.compressEntry(serialize_buf[0..written], compress_buf);
        try self.dat.pwriteAll(compress_buf[0..entry_len], self.dat_size);

        var e: [IDX_ENTRY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, e[0..8], self.dat_size, .little);
        std.mem.writeInt(u32, e[8..12], @intCast(entry_len), .little);
        try self.idx.pwriteAll(&e, IDX_HEADER_SIZE + self.count * IDX_ENTRY_SIZE);

        self.dat_size += entry_len;
        self.count += 1;
    }

    /// Publish the count so readers see the appended range.
    pub fn sync(self: *TxsWriter) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, self.count, .little);
        try self.idx.pwriteAll(&buf, 16);
        try self.dat.sync();
        try self.idx.sync();
    }

    pub fn deinit(self: *TxsWriter) void {
        self.sync() catch {};
        self.dat.close();
        self.idx.close();
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

fn rec(tx_index: u16, seed: u8) TxRecord {
    return .{
        .tx_index = tx_index,
        .tx_type = 2,
        .flags = 0,
        .from = [_]u8{seed} ** 20,
        .to = [_]u8{seed +% 1} ** 20,
        .value = [_]u8{ seed, 0, 0 } ++ [_]u8{0} ** 29,
    };
}

test "writer/reader round-trip across blocks, including an empty one" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var sbuf: [4096]u8 = undefined;
    var cbuf: [4096]u8 = undefined;

    {
        var w = try TxsWriter.open(tmp.dir, 100);
        defer w.deinit();
        try w.append(100, &.{ rec(0, 0x11), rec(3, 0x22) }, &sbuf, &cbuf);
        try w.append(101, &.{}, &sbuf, &cbuf); // no log-producing txs
        try w.append(102, &.{rec(7, 0x33)}, &sbuf, &cbuf);
        try testing.expectError(error.NonDenseAppend, w.append(105, &.{}, &sbuf, &cbuf));
    }

    var r = (try TxsReader.open(tmp.dir)).?;
    defer r.deinit();
    try testing.expectEqual(@as(u64, 3), r.count);
    try testing.expect(r.covers(100, 102));
    try testing.expect(!r.covers(100, 103));

    var pbuf: [4096]u8 = undefined;
    var dbuf: [4096]u8 = undefined;
    var out: [16]TxRecord = undefined;

    const t100 = (try r.readBlock(100, &pbuf, &dbuf, &out)).?;
    try testing.expectEqual(@as(usize, 2), t100.len);
    try testing.expectEqual(@as(u16, 3), t100[1].tx_index);
    try testing.expectEqualSlices(u8, &([_]u8{0x22} ** 20), &t100[1].from);
    try testing.expectEqual(@as(u256, 0x22), t100[1].valueU256());

    try testing.expectEqual(@as(usize, 0), (try r.readBlock(101, &pbuf, &dbuf, &out)).?.len);
    try testing.expectEqual(@as(usize, 1), (try r.readBlock(102, &pbuf, &dbuf, &out)).?.len);
    try testing.expectEqual(@as(?[]TxRecord, null), try r.readBlock(99, &pbuf, &dbuf, &out));
    try testing.expectEqual(@as(?[]TxRecord, null), try r.readBlock(103, &pbuf, &dbuf, &out));
}

test "find locates records in a sorted table" {
    const table = [_]TxRecord{ rec(2, 1), rec(5, 2), rec(9, 3) };
    try testing.expectEqual(@as(u16, 5), find(&table, 5).?.tx_index);
    try testing.expectEqual(@as(?*const TxRecord, null), find(&table, 4));
    try testing.expectEqual(@as(?*const TxRecord, null), find(&table, 10));
}

test "writer resumes from the published count and overwrites an orphan tail" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var sbuf: [4096]u8 = undefined;
    var cbuf: [4096]u8 = undefined;

    {
        var w = try TxsWriter.open(tmp.dir, 100);
        try w.append(100, &.{rec(0, 0xAA)}, &sbuf, &cbuf);
        try w.sync();
        // Crash simulation: a second append lands in dat + idx, but the count
        // never publishes (no sync, no deinit).
        try w.append(101, &.{rec(1, 0xBB)}, &sbuf, &cbuf);
        w.dat.close();
        w.idx.close();
    }

    {
        var r = (try TxsReader.open(tmp.dir)).?;
        defer r.deinit();
        try testing.expectEqual(@as(u64, 1), r.count); // orphan invisible
    }

    {
        var w = try TxsWriter.open(tmp.dir, 100); // resume at count=1
        defer w.deinit();
        try testing.expectEqual(@as(u64, 1), w.count);
        try w.append(101, &.{rec(1, 0xCC)}, &sbuf, &cbuf); // overwrites the orphan
    }

    var r = (try TxsReader.open(tmp.dir)).?;
    defer r.deinit();
    var pbuf: [4096]u8 = undefined;
    var dbuf: [4096]u8 = undefined;
    var out: [4]TxRecord = undefined;
    const t = (try r.readBlock(101, &pbuf, &dbuf, &out)).?;
    try testing.expectEqualSlices(u8, &([_]u8{0xCC} ** 20), &t[0].from);
}

test "absent pair opens as null, mismatched first_block recreates" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try testing.expectEqual(@as(?TxsReader, null), try TxsReader.open(tmp.dir));

    var sbuf: [256]u8 = undefined;
    var cbuf: [256]u8 = undefined;
    {
        var w = try TxsWriter.open(tmp.dir, 100);
        defer w.deinit();
        try w.append(100, &.{rec(0, 1)}, &sbuf, &cbuf);
    }
    {
        // Store rebuilt from a different first block: the pair is recreated so
        // dense indexing stays aligned.
        var w = try TxsWriter.open(tmp.dir, 200);
        defer w.deinit();
        try testing.expectEqual(@as(u64, 0), w.count);
        try w.append(200, &.{rec(0, 2)}, &sbuf, &cbuf);
    }
    var r = (try TxsReader.open(tmp.dir)).?;
    defer r.deinit();
    try testing.expectEqual(@as(u64, 200), r.first_block);
    try testing.expectEqual(@as(u64, 1), r.count);
}

test "wrong idx magic is rejected loudly" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var f = try tmp.dir.createFile(IDX_NAME, .{});
    try f.writeAll("NOTTHIS!" ++ ([_]u8{0} ** 16));
    f.close();
    try testing.expectError(error.InvalidMagic, TxsReader.open(tmp.dir));
}
