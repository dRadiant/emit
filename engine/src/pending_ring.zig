/// Pre-finality block buffer persisted as a single file via atomic rewrite.
///
/// Holds the last ≤64 blocks before they're finalized to the immutable flat
/// store. All mutations operate on an in-memory ArrayList; after each mutation
/// the list is serialized to pending.bin via tmp + rename (crash-safe).
///
/// Flat files are NEVER mutated — this is the only mutable state in the engine.
/// See ADR-001 for the decision rationale.
///
/// File format:
///   count(u32 LE)
///   [count × Entry]:
///     block_number(u64 BE)
///     hash(32)
///     topic_bloom(256)
///     addr_bloom(1024)
///     lz4_len(u32 LE)
///     lz4_data(lz4_len)
const std = @import("std");

const core = @import("core");

const bloom = core.bloom;
const pending_format = core.pending_format;

pub const FINALITY_DEPTH = pending_format.FINALITY_DEPTH;

const HASH_SIZE = pending_format.HASH_SIZE;

/// Same shape as `core.pending_format.Entry` but owns `lz4_entry` —
/// the engine writes and frees these bytes.
pub const Entry = struct {
    block_number: u64,
    hash: [HASH_SIZE]u8,
    topic_bloom: [bloom.BLOOM_SIZE]u8,
    addr_bloom: [bloom.ADDR_BLOOM_SIZE]u8,
    lz4_entry: []u8,
};

pub const PendingRing = struct {
    entries: std.ArrayListUnmanaged(Entry),
    dir: std.fs.Dir,
    alloc: std.mem.Allocator,

    /// Open or create a pending ring in the given directory.
    /// Loads existing pending.bin if present.
    pub fn open(dir: std.fs.Dir, alloc: std.mem.Allocator) !PendingRing {
        var ring = PendingRing{
            .entries = .{},
            .dir = dir,
            .alloc = alloc,
        };
        ring.load() catch {};
        return ring;
    }

    pub fn deinit(self: *PendingRing) void {
        for (self.entries.items) |e| self.alloc.free(e.lz4_entry);
        self.entries.deinit(self.alloc);
    }

    /// Append a block. Persists immediately.
    pub fn insert(
        self: *PendingRing,
        block_number: u64,
        hash: [HASH_SIZE]u8,
        topic_bloom: *const [bloom.BLOOM_SIZE]u8,
        addr_bloom: *const [bloom.ADDR_BLOOM_SIZE]u8,
        lz4_entry: []const u8,
    ) !void {
        const owned = try self.alloc.alloc(u8, lz4_entry.len);
        @memcpy(owned, lz4_entry);
        try self.entries.append(self.alloc, .{
            .block_number = block_number,
            .hash = hash,
            .topic_bloom = topic_bloom.*,
            .addr_bloom = addr_bloom.*,
            .lz4_entry = owned,
        });
        try self.persist();
    }

    /// Get the block hash for reorg detection. O(1) via index arithmetic
    /// since entries are dense (sequential block numbers, no gaps).
    pub fn getHash(self: *const PendingRing, block_number: u64) ?[HASH_SIZE]u8 {
        const oldest = self.oldestBlock() orelse return null;
        if (block_number < oldest) return null;
        const idx = block_number - oldest;
        if (idx >= self.entries.items.len) return null;
        return self.entries.items[idx].hash;
    }

    pub fn oldestBlock(self: *const PendingRing) ?u64 {
        if (self.entries.items.len == 0) return null;
        return self.entries.items[0].block_number;
    }

    pub fn latestBlock(self: *const PendingRing) ?u64 {
        if (self.entries.items.len == 0) return null;
        return self.entries.items[self.entries.items.len - 1].block_number;
    }

    pub fn count(self: *const PendingRing) usize {
        return self.entries.items.len;
    }

    /// True if the oldest block has 64+ confirmations.
    pub fn canFinalize(self: *const PendingRing, current_head: u64) bool {
        const oldest = self.oldestBlock() orelse return false;
        return current_head >= oldest + FINALITY_DEPTH;
    }

    /// Remove and return the oldest entry. Does not persist.
    /// Caller must call flush() after batch operations.
    pub fn popOldest(self: *PendingRing) ?Entry {
        if (self.entries.items.len == 0) return null;
        return self.entries.orderedRemove(0);
    }

    /// Persist current state to disk. Call after batch mutations.
    pub fn flush(self: *PendingRing) !void {
        try self.persist();
    }

    /// Walk backwards from `from` comparing stored hashes against `canonical`.
    /// Returns the first block number where they diverge (the fork point).
    /// `canonical[0]` is the hash for `from - 1`, `canonical[1]` for `from - 2`, etc.
    pub fn findForkPoint(self: *const PendingRing, from: u64, canonical: []const [32]u8) u64 {
        const oldest = self.oldestBlock() orelse return from;
        var fork = from;
        for (canonical) |hash| {
            if (fork <= oldest) break;
            const stored = self.getHash(fork - 1) orelse break;
            if (std.mem.eql(u8, &stored, &hash)) break;
            fork -= 1;
        }
        return fork;
    }

    /// Delete all entries with block_number >= from_block. Returns count deleted.
    pub fn truncateFrom(self: *PendingRing, from_block: u64) !u64 {
        var deleted: u64 = 0;
        while (self.entries.items.len > 0) {
            const last = self.entries.items[self.entries.items.len - 1];
            if (last.block_number < from_block) break;
            self.alloc.free(last.lz4_entry);
            _ = self.entries.pop();
            deleted += 1;
        }
        if (deleted > 0) try self.persist();
        return deleted;
    }

    // ── Persistence ──────────────────────────────────────────────────────

    fn persist(self: *PendingRing) !void {
        const buf = try pending_format.serialize(self.alloc, self.entries.items);
        defer self.alloc.free(buf);
        try core.writeAtomicFile(self.dir, "pending.bin.tmp", "pending.bin", buf);
    }

    /// Load pending.bin on startup via `core.pending_format.parse`, then
    /// dupe each `lz4_entry` into ring-owned memory.
    fn load(self: *PendingRing) !void {
        const file = try self.dir.openFile("pending.bin", .{});
        defer file.close();
        const stat = try file.stat();
        if (stat.size == 0) return;

        const buf = try self.alloc.alloc(u8, stat.size);
        defer self.alloc.free(buf);
        const n = try file.readAll(buf);

        const parsed = try pending_format.parse(self.alloc, buf[0..n]);
        defer self.alloc.free(parsed);

        try self.entries.ensureUnusedCapacity(self.alloc, parsed.len);
        for (parsed) |p| {
            const owned = try self.alloc.dupe(u8, p.lz4_entry);
            self.entries.appendAssumeCapacity(.{
                .block_number = p.block_number,
                .hash = p.hash,
                .topic_bloom = p.topic_bloom,
                .addr_bloom = p.addr_bloom,
                .lz4_entry = owned,
            });
        }
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;
const dummy_hash = [_]u8{0xAA} ** 32;
const dummy_topic = [_]u8{0} ** bloom.BLOOM_SIZE;
const dummy_addr = [_]u8{0} ** bloom.ADDR_BLOOM_SIZE;
const dummy_entry = [_]u8{ 1, 0, 0, 0, 0x42 };


test "insert and read back hash" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    try ring.insert(100, dummy_hash, &dummy_topic, &dummy_addr, &dummy_entry);

    const hash = ring.getHash(100).?;
    try testing.expectEqualSlices(u8, &dummy_hash, &hash);
    try testing.expect(ring.getHash(999) == null);
}

test "oldest and latest track correctly" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    try testing.expect(ring.oldestBlock() == null);
    try testing.expect(ring.latestBlock() == null);

    try ring.insert(100, dummy_hash, &dummy_topic, &dummy_addr, &dummy_entry);
    try ring.insert(101, dummy_hash, &dummy_topic, &dummy_addr, &dummy_entry);
    try ring.insert(102, dummy_hash, &dummy_topic, &dummy_addr, &dummy_entry);

    try testing.expectEqual(@as(u64, 100), ring.oldestBlock().?);
    try testing.expectEqual(@as(u64, 102), ring.latestBlock().?);
    try testing.expectEqual(@as(usize, 3), ring.count());
}

test "popOldest removes and returns first entry" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    try ring.insert(100, dummy_hash, &dummy_topic, &dummy_addr, &dummy_entry);
    try ring.insert(101, dummy_hash, &dummy_topic, &dummy_addr, &dummy_entry);

    const oldest = (ring.popOldest()).?;
    defer ring.alloc.free(oldest.lz4_entry);
    try testing.expectEqual(@as(u64, 100), oldest.block_number);
    try testing.expectEqual(@as(u64, 101), ring.oldestBlock().?);
    try testing.expectEqual(@as(usize, 1), ring.count());

    const last = (ring.popOldest()).?;
    defer ring.alloc.free(last.lz4_entry);
    try testing.expectEqual(@as(usize, 0), ring.count());
    try testing.expect(ring.oldestBlock() == null);
}

test "truncateFrom removes blocks at and above fork point" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    for (100..110) |i| {
        try ring.insert(i, dummy_hash, &dummy_topic, &dummy_addr, &dummy_entry);
    }
    try testing.expectEqual(@as(usize, 10), ring.count());

    const deleted = try ring.truncateFrom(107);
    try testing.expectEqual(@as(u64, 3), deleted);
    try testing.expectEqual(@as(usize, 7), ring.count());
    try testing.expectEqual(@as(u64, 106), ring.latestBlock().?);
    try testing.expect(ring.getHash(107) == null);
    try testing.expect(ring.getHash(106) != null);
}

test "canFinalize respects finality depth" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    try ring.insert(100, dummy_hash, &dummy_topic, &dummy_addr, &dummy_entry);

    try testing.expect(!ring.canFinalize(163)); // 63 confirmations
    try testing.expect(ring.canFinalize(164)); // 64 confirmations
}

test "persists across reopen" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    // Write
    {
        var ring = try PendingRing.open(tmp.dir, testing.allocator);
        defer ring.deinit();
        const hash = [_]u8{0xBB} ** 32;
        const entry = [_]u8{ 3, 0, 0, 0, 0xDE, 0xAD, 0xBE };
        try ring.insert(42, hash, &dummy_topic, &dummy_addr, &entry);
        try ring.insert(43, dummy_hash, &dummy_topic, &dummy_addr, &dummy_entry);
    }

    // Reopen and verify
    {
        var ring = try PendingRing.open(tmp.dir, testing.allocator);
        defer ring.deinit();
        try testing.expectEqual(@as(usize, 2), ring.count());
        try testing.expectEqual(@as(u64, 42), ring.oldestBlock().?);
        const hash = ring.getHash(42).?;
        try testing.expectEqual(@as(u8, 0xBB), hash[0]);
    }
}

test "reorg scenario: insert, truncate, re-insert" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    const hash_a = [_]u8{0xAA} ** 32;
    for (100..110) |i| {
        try ring.insert(i, hash_a, &dummy_topic, &dummy_addr, &dummy_entry);
    }

    // Reorg at 107
    _ = try ring.truncateFrom(107);

    // Re-insert canonical
    const hash_b = [_]u8{0xBB} ** 32;
    for (107..110) |i| {
        try ring.insert(i, hash_b, &dummy_topic, &dummy_addr, &dummy_entry);
    }

    try testing.expectEqual(@as(usize, 10), ring.count());
    // 100-106: original hash
    try testing.expectEqualSlices(u8, &hash_a, &ring.getHash(106).?);
    // 107-109: new hash
    try testing.expectEqualSlices(u8, &hash_b, &ring.getHash(107).?);
}

test "truncate everything leaves empty ring" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    try ring.insert(100, dummy_hash, &dummy_topic, &dummy_addr, &dummy_entry);
    try ring.insert(101, dummy_hash, &dummy_topic, &dummy_addr, &dummy_entry);

    _ = try ring.truncateFrom(100);
    try testing.expectEqual(@as(usize, 0), ring.count());
    try testing.expect(ring.oldestBlock() == null);
}

test "findForkPoint walks back to matching hash" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    const hash_a = [_]u8{0xAA} ** 32;
    for (100..110) |i| {
        try ring.insert(i, hash_a, &dummy_topic, &dummy_addr, &dummy_entry);
    }

    // Canonical matches at 105, diverges above
    const hash_b = [_]u8{0xBB} ** 32;
    const canonical = [_][32]u8{ hash_b, hash_b, hash_b, hash_a }; // 108,107,106,105
    const fork = ring.findForkPoint(109, &canonical);
    try testing.expectEqual(@as(u64, 106), fork);
}

test "findForkPoint returns from when all match" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    const hash_a = [_]u8{0xAA} ** 32;
    for (100..105) |i| {
        try ring.insert(i, hash_a, &dummy_topic, &dummy_addr, &dummy_entry);
    }

    // First canonical hash matches immediately (no reorg)
    const canonical = [_][32]u8{hash_a};
    const fork = ring.findForkPoint(105, &canonical);
    try testing.expectEqual(@as(u64, 105), fork);
}

test "findForkPoint on empty ring returns from" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    const canonical = [_][32]u8{[_]u8{0} ** 32};
    try testing.expectEqual(@as(u64, 100), ring.findForkPoint(100, &canonical));
}

// Verifies the dense-ring invariant that head_follower's reorg recovery depends on:
// after truncate + canonical re-insert, getHash() returns the canonical hash for
// every block. The dense-array indexing in getHash() previously mis-reported
// presence when ring entries had gaps, which surfaced as silently-dropped blocks
// in WS-mode reorgs.
test "truncate + re-insert restores dense ring with canonical hashes" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ring = try PendingRing.open(tmp.dir, testing.allocator);
    defer ring.deinit();

    const hash_a = [_]u8{0xAA} ** 32;
    for (100..106) |i| {
        try ring.insert(i, hash_a, &dummy_topic, &dummy_addr, &dummy_entry);
    }
    try testing.expectEqual(@as(usize, 6), ring.count());

    // Reorg at block 103: fork point is 103 (blocks 103-105 diverge).
    _ = try ring.truncateFrom(103);
    try testing.expectEqual(@as(usize, 3), ring.count());
    try testing.expectEqual(@as(u64, 102), ring.latestBlock().?);

    // Recovery re-inserts canonical 103, 104, 105 with hash B.
    const hash_b = [_]u8{0xBB} ** 32;
    for (103..106) |i| {
        try ring.insert(i, hash_b, &dummy_topic, &dummy_addr, &dummy_entry);
    }
    try testing.expectEqual(@as(usize, 6), ring.count());
    try testing.expectEqual(@as(u64, 105), ring.latestBlock().?);

    // The dense-array index must resolve every block to the right hash.
    try testing.expectEqualSlices(u8, &hash_a, &ring.getHash(102).?);
    try testing.expectEqualSlices(u8, &hash_b, &ring.getHash(103).?);
    try testing.expectEqualSlices(u8, &hash_b, &ring.getHash(104).?);
    try testing.expectEqualSlices(u8, &hash_b, &ring.getHash(105).?);
}
