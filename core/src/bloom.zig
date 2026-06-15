//! Comptime-generic bloom filter. Topic blooms use 256 bytes (2048 bits).
//! Address blooms use 1024 bytes (8192 bits) for lower FP on blocks with
//! hundreds of unique addresses (~3.4% FP at 500 addresses vs 25% at 256 bytes).
const std = @import("std");

/// k=7 hash functions from non-overlapping 2-byte windows of the 32-byte key.
/// No extra hashing: topic0 values are already keccak256 (uniformly distributed).
/// Addresses are right-padded to 32 bytes.
const NUM_HASHES = 7;

pub fn BloomFilter(comptime SIZE: comptime_int) type {
    return struct {
        const Self = @This();
        pub const BITS: u16 = SIZE * 8;
        pub const BYTE_SIZE = SIZE;

        bits: [SIZE]u8,

        pub fn init() Self {
            return .{ .bits = std.mem.zeroes([SIZE]u8) };
        }

        pub fn insert(self: *Self, key: [32]u8) void {
            inline for (0..NUM_HASHES) |i| {
                const pos = bitPosition(key, i);
                self.bits[pos / 8] |= @as(u8, 1) << @intCast(pos % 8);
            }
        }

        pub fn mightContain(self: *const Self, key: [32]u8) bool {
            return bytesContain(&self.bits, key);
        }

        pub fn mightContainAny(self: *const Self, keys: []const [32]u8) bool {
            return bytesContainAny(&self.bits, keys);
        }

        /// Check bloom from a raw byte pointer (e.g., directly from mmap'd blooms.bin).
        pub fn bytesContain(bits: *const [SIZE]u8, key: [32]u8) bool {
            inline for (0..NUM_HASHES) |i| {
                const pos = bitPosition(key, i);
                if (bits[pos / 8] & (@as(u8, 1) << @intCast(pos % 8)) == 0)
                    return false;
            }
            return true;
        }

        pub fn bytesContainAny(bits: *const [SIZE]u8, keys: []const [32]u8) bool {
            for (keys) |k| {
                if (bytesContain(bits, k)) return true;
            }
            return false;
        }

        /// Extract bit position from the i-th 2-byte window of the key (big-endian).
        inline fn bitPosition(key: [32]u8, comptime i: usize) u16 {
            const raw = (@as(u16, key[i * 2]) << 8) | @as(u16, key[i * 2 + 1]);
            return raw % BITS;
        }

        /// Right-pad a 20-byte address to 32 bytes for bloom hashing.
        pub fn addrToBloomKey(addr: [20]u8) [32]u8 {
            var key: [32]u8 = std.mem.zeroes([32]u8);
            @memcpy(key[0..20], &addr);
            return key;
        }
    };
}

pub const BLOOM_SIZE = 256; // topic bloom: ~50 unique topic0s per block
pub const ADDR_BLOOM_SIZE = 1024; // address bloom: ~500 unique addresses per block

pub const Bloom = BloomFilter(BLOOM_SIZE);
pub const AddrBloom = BloomFilter(ADDR_BLOOM_SIZE);

// ── Tests ────────────────────────────────────────────────────────────────

test "insert and query" {
    const key_a = [_]u8{0xdd} ++ [_]u8{0xf2} ++ [_]u8{0x52} ++ [_]u8{0xad} ++ [_]u8{0} ** 28;
    const key_b = [_]u8{0x8c} ++ [_]u8{0x5b} ++ [_]u8{0xe1} ++ [_]u8{0xe5} ++ [_]u8{0} ** 28;

    var bloom = Bloom.init();
    bloom.insert(key_a);
    try std.testing.expect(bloom.mightContain(key_a));
    try std.testing.expect(!bloom.mightContain(key_b));
}

test "insert multiple" {
    const key_a = [_]u8{0xdd} ++ [_]u8{0xf2} ++ [_]u8{0x52} ++ [_]u8{0xad} ++ [_]u8{0} ** 28;
    const key_b = [_]u8{0x8c} ++ [_]u8{0x5b} ++ [_]u8{0xe1} ++ [_]u8{0xe5} ++ [_]u8{0} ** 28;

    var bloom = Bloom.init();
    bloom.insert(key_a);
    bloom.insert(key_b);
    try std.testing.expect(bloom.mightContain(key_a));
    try std.testing.expect(bloom.mightContain(key_b));
}

test "empty rejects" {
    const key = [_]u8{0xdd} ++ [_]u8{0xf2} ++ [_]u8{0} ** 30;
    const bloom = Bloom.init();
    try std.testing.expect(!bloom.mightContain(key));
}

test "mightContainAny" {
    const key_a = [_]u8{0xdd} ++ [_]u8{0xf2} ++ [_]u8{0} ** 30;
    const key_b = [_]u8{0x8c} ++ [_]u8{0x5b} ++ [_]u8{0} ** 30;

    var bloom = Bloom.init();
    bloom.insert(key_a);
    const topics = [_][32]u8{ key_a, key_b };
    try std.testing.expect(bloom.mightContainAny(&topics));
    const no_match = [_][32]u8{key_b};
    try std.testing.expect(!bloom.mightContainAny(&no_match));
}

test "addr bloom larger size" {
    var bloom = AddrBloom.init();
    const addr1 = AddrBloom.addrToBloomKey([_]u8{0xAA} ** 20);
    const addr2 = AddrBloom.addrToBloomKey([_]u8{0xBB} ** 20);
    bloom.insert(addr1);
    try std.testing.expect(bloom.mightContain(addr1));
    try std.testing.expect(!bloom.mightContain(addr2));
}
