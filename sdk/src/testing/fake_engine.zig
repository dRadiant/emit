/// Test fixture: pure-Zig stand-in for the engine that writes `pending.bin`
/// and `meta.bin` to a directory on demand. Lets SDK tests script
/// ingest / reorg / finalize sequences without an EVM node. Not re-exported
/// from `root.zig`.
const std = @import("std");

const core = @import("core");

const bloom = core.bloom;
const flat_reader = core.flat_reader;
const log_serial = core.log_serial;
const pending_format = core.pending_format;
const types = core.types;

const PENDING_FILE = "pending.bin";
const PENDING_TMP = "pending.bin.tmp";
const META_FILE = "meta.bin";
const META_TMP = "meta.bin.tmp";

pub const FakeEngine = struct {
    dir: std.fs.Dir,
    alloc: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .{},
    last_finalized: u64 = 0,

    pub const Entry = struct {
        block_number: u64,
        /// Exact block time (epoch seconds). 0 = unknown — mirrors the engine
        /// follower carrying `header.timestamp` into `pending.bin`.
        timestamp: u32 = 0,
        hash: [32]u8,
        topic_bloom: [bloom.BLOOM_SIZE]u8,
        addr_bloom: [bloom.ADDR_BLOOM_SIZE]u8,
        /// Owned by `FakeEngine.alloc`. LZ4-compressed packed-log payload.
        lz4_entry: []u8,
    };

    pub fn init(dir: std.fs.Dir, alloc: std.mem.Allocator) FakeEngine {
        return .{ .dir = dir, .alloc = alloc };
    }

    pub fn deinit(self: *FakeEngine) void {
        for (self.entries.items) |e| self.alloc.free(e.lz4_entry);
        self.entries.deinit(self.alloc);
    }

    /// Append a block to pending and atomically rewrite `pending.bin`.
    /// Timestamp is left unknown (0); use `ingestAt` to drive the exact path.
    pub fn ingest(
        self: *FakeEngine,
        block_number: u64,
        hash: [32]u8,
        logs: []const core.RawLog,
    ) !void {
        return self.ingestAt(block_number, 0, hash, logs);
    }

    /// Like `ingest` but carries an exact block timestamp, matching the
    /// follower writing `header.timestamp` into the pending ring.
    pub fn ingestAt(
        self: *FakeEngine,
        block_number: u64,
        timestamp: u32,
        hash: [32]u8,
        logs: []const core.RawLog,
    ) !void {
        const serialize_buf = try self.alloc.alloc(u8, types.BLOCK_BUF_SIZE);
        defer self.alloc.free(serialize_buf);
        const serialized_len = log_serial.serializeLogs(logs, serialize_buf);

        const compress_buf = try self.alloc.alloc(u8, types.BLOCK_BUF_SIZE);
        defer self.alloc.free(compress_buf);
        const entry_len = try log_serial.compressEntry(serialize_buf[0..serialized_len], compress_buf);

        const owned = try self.alloc.dupe(u8, compress_buf[0..entry_len]);

        const tb = log_serial.buildTopicBloom(logs);
        const ab = log_serial.buildAddrBloom(logs);

        try self.entries.append(self.alloc, .{
            .block_number = block_number,
            .timestamp = timestamp,
            .hash = hash,
            .topic_bloom = tb.bits,
            .addr_bloom = ab.bits,
            .lz4_entry = owned,
        });
        try self.persistPending();
    }

    /// Drop the oldest pending block and advance `meta.last_finalized_block`
    /// to `block`. Returns `error.InvalidFinalize` when `block` isn't the
    /// oldest entry — engine semantics only finalize in order.
    pub fn finalize(self: *FakeEngine, block: u64) !void {
        if (self.entries.items.len == 0 or self.entries.items[0].block_number != block)
            return error.InvalidFinalize;
        const oldest = self.entries.orderedRemove(0);
        self.alloc.free(oldest.lz4_entry);
        self.last_finalized = block;
        try self.persistPending();
        try self.persistMeta();
    }

    /// Truncate pending entries with `block_number >= fork`. Mirrors the
    /// engine's `resolveReorg.truncateFrom`; meta is unchanged so the SDK
    /// can disambiguate truncation from finalization.
    pub fn reorg(self: *FakeEngine, fork: u64) !void {
        while (self.entries.items.len > 0) {
            const last = self.entries.items[self.entries.items.len - 1];
            if (last.block_number < fork) break;
            self.alloc.free(last.lz4_entry);
            _ = self.entries.pop();
        }
        try self.persistPending();
    }

    // ── Internal: file I/O ──────────────────────────────────────────────────

    fn persistPending(self: *FakeEngine) !void {
        const buf = try pending_format.serialize(self.alloc, self.entries.items);
        defer self.alloc.free(buf);
        try core.atomic_file.write(self.dir, PENDING_TMP, PENDING_FILE, buf);
    }

    fn persistMeta(self: *FakeEngine) !void {
        // `core.Meta.serialize` writes a valid checksum, which `readMeta`
        // validates via the same `deserialize` path.
        const meta = flat_reader.Meta{
            .last_finalized_block = self.last_finalized,
            .blocks_dat_size = 0,
            .blocks_idx_count = 0,
            .blooms_count = 0,
            .checksum = 0,
        };
        var buf: [flat_reader.META_SIZE]u8 = undefined;
        meta.serialize(&buf);
        try core.atomic_file.write(self.dir, META_TMP, META_FILE, &buf);
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

fn dummyLog(block: u64) core.RawLog {
    return .{
        .block_number = block,
        .tx_index = 0,
        .log_index = 0,
        .address = [_]u8{0xAA} ** 20,
        .topic_count = 1,
        .topics = .{ [_]u8{0xCC} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32, [_]u8{0} ** 32 },
        .data = &.{},
        .tx_hash = [_]u8{0xFE} ** 32,
    };
}

fn readPending(dir: std.fs.Dir, alloc: std.mem.Allocator) ![]u8 {
    const file = try dir.openFile(PENDING_FILE, .{});
    defer file.close();
    const stat = try file.stat();
    const buf = try alloc.alloc(u8, stat.size);
    _ = try file.readAll(buf);
    return buf;
}

test "ingest produces a parseable pending.bin" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fake = FakeEngine.init(tmp.dir, testing.allocator);
    defer fake.deinit();

    const hash_a = [_]u8{0xAA} ** 32;
    try fake.ingest(100, hash_a, &.{dummyLog(100)});
    try fake.ingest(101, hash_a, &.{dummyLog(101)});

    const buf = try readPending(tmp.dir, testing.allocator);
    defer testing.allocator.free(buf);
    const parsed = try pending_format.parse(testing.allocator, buf);
    defer testing.allocator.free(parsed);

    try testing.expectEqual(@as(usize, 2), parsed.len);
    try testing.expectEqual(@as(u64, 100), parsed[0].block_number);
    try testing.expectEqual(@as(u64, 101), parsed[1].block_number);
    try testing.expectEqualSlices(u8, &hash_a, &parsed[0].hash);
}

test "reorg truncates pending but leaves meta alone" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fake = FakeEngine.init(tmp.dir, testing.allocator);
    defer fake.deinit();

    const hash_a = [_]u8{0xAA} ** 32;
    for (100..106) |i| try fake.ingest(i, hash_a, &.{dummyLog(i)});
    // meta.bin doesn't exist yet — finalize hasn't fired.
    try testing.expectError(error.FileNotFound, tmp.dir.openFile(META_FILE, .{}));

    try fake.reorg(103);

    // Pending truncated to 100..102.
    const buf = try readPending(tmp.dir, testing.allocator);
    defer testing.allocator.free(buf);
    const parsed = try pending_format.parse(testing.allocator, buf);
    defer testing.allocator.free(parsed);
    try testing.expectEqual(@as(usize, 3), parsed.len);
    try testing.expectEqual(@as(u64, 102), parsed[parsed.len - 1].block_number);

    // Meta still absent — reorg-truncation must not touch meta.
    try testing.expectError(error.FileNotFound, tmp.dir.openFile(META_FILE, .{}));

    // Re-ingest canonical chain with a different hash; ring stays dense.
    const hash_b = [_]u8{0xBB} ** 32;
    for (103..106) |i| try fake.ingest(i, hash_b, &.{dummyLog(i)});

    const buf2 = try readPending(tmp.dir, testing.allocator);
    defer testing.allocator.free(buf2);
    const parsed2 = try pending_format.parse(testing.allocator, buf2);
    defer testing.allocator.free(parsed2);
    try testing.expectEqual(@as(usize, 6), parsed2.len);
    try testing.expectEqualSlices(u8, &hash_b, &parsed2[3].hash);
    try testing.expectEqual(@as(u64, 105), parsed2[5].block_number);
}

test "finalize drops oldest and advances meta.last_finalized_block" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fake = FakeEngine.init(tmp.dir, testing.allocator);
    defer fake.deinit();

    const hash_a = [_]u8{0xAA} ** 32;
    try fake.ingest(100, hash_a, &.{dummyLog(100)});
    try fake.ingest(101, hash_a, &.{dummyLog(101)});

    try fake.finalize(100);

    // Pending: only block 101 remains.
    const buf = try readPending(tmp.dir, testing.allocator);
    defer testing.allocator.free(buf);
    const parsed = try pending_format.parse(testing.allocator, buf);
    defer testing.allocator.free(parsed);
    try testing.expectEqual(@as(usize, 1), parsed.len);
    try testing.expectEqual(@as(u64, 101), parsed[0].block_number);

    // Meta: last_finalized_block = 100, checksum validates.
    const meta_file = try tmp.dir.openFile(META_FILE, .{});
    defer meta_file.close();
    var meta_buf: [flat_reader.META_SIZE]u8 = undefined;
    _ = try meta_file.readAll(&meta_buf);
    const m = flat_reader.Meta.deserialize(&meta_buf) orelse return error.MetaCorrupt;
    try testing.expectEqual(@as(u64, 100), m.last_finalized_block);
}

test "finalize rejects out-of-order block" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fake = FakeEngine.init(tmp.dir, testing.allocator);
    defer fake.deinit();

    const hash_a = [_]u8{0xAA} ** 32;
    try fake.ingest(100, hash_a, &.{dummyLog(100)});
    try fake.ingest(101, hash_a, &.{dummyLog(101)});

    // Block 101 isn't the oldest — engine semantics require finalize on the
    // oldest pending block. The fake catches the mismatch.
    try testing.expectError(error.InvalidFinalize, fake.finalize(101));
}
