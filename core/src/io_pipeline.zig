/// io_uring-based read pipeline for batch block reads from blocks.dat.
/// Comptime-generic on queue depth so each worker thread gets its own ring.
/// Linux-only. Non-Linux platforms use the pread fallback in filter workers.
const std = @import("std");

const builtin = @import("builtin");

const types = @import("types.zig");

const posix = std.posix;

pub const supported = builtin.target.os.tag == .linux;

pub const Completion = struct {
    block_number: u64,
    buf_slot: u16,
    result: i32 = 0,
};

pub fn ReadPipeline(comptime QUEUE_DEPTH: u32) type {
    if (!supported) return struct {};
    return struct {
        const Self = @This();
        const linux = std.os.linux;

        ring: linux.IoUring,
        fd: posix.fd_t,
        bufs: [QUEUE_DEPTH][types.BLOCK_BUF_SIZE]u8,
        completions: [QUEUE_DEPTH]Completion,
        free_stack: [QUEUE_DEPTH]u16,
        free_count: u32,
        in_flight: u32 = 0,
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator, fd: posix.fd_t) !*Self {
            const self = try allocator.create(Self);
            self.* = .{
                .ring = try linux.IoUring.init(QUEUE_DEPTH, 0),
                .fd = fd,
                .bufs = undefined,
                .completions = undefined,
                .free_stack = undefined,
                .free_count = QUEUE_DEPTH,
                .allocator = allocator,
            };
            for (0..QUEUE_DEPTH) |i| {
                self.free_stack[i] = @intCast(i);
            }
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.ring.deinit();
            self.allocator.destroy(self);
        }

        pub fn submit(self: *Self, slot: u16, block_number: u64, offset: u64, length: u32) !void {
            // Reject rather than truncate. Silent clipping defeats the bounds
            // check and surfaces as an opaque downstream LZ4 decompression failure.
            if (length > self.bufs[slot].len) return error.EntryExceedsBuffer;
            const sqe = try self.ring.get_sqe();
            sqe.prep_read(self.fd, self.bufs[slot][0..length], offset);
            self.completions[slot] = .{
                .block_number = block_number,
                .buf_slot = slot,
            };
            sqe.user_data = @intFromPtr(&self.completions[slot]);
            self.in_flight += 1;
        }

        pub fn claimSlot(self: *Self) ?u16 {
            if (self.free_count == 0) return null;
            self.free_count -= 1;
            return self.free_stack[self.free_count];
        }

        pub fn releaseSlot(self: *Self, slot: u16) void {
            self.free_stack[self.free_count] = slot;
            self.free_count += 1;
        }

        pub fn flush(self: *Self) !u32 {
            return @intCast(try self.ring.submit());
        }

        pub fn waitAtLeastOne(self: *Self, out: []*Completion) !usize {
            var cqes: [QUEUE_DEPTH]linux.io_uring_cqe = undefined;
            const wait_nr: u32 = if (self.in_flight > 0) 1 else 0;
            const n = try self.ring.copy_cqes(&cqes, wait_nr);
            const count = @min(n, out.len);
            for (0..count) |i| {
                const c: *Completion = @ptrFromInt(cqes[i].user_data);
                c.result = cqes[i].res;
                out[i] = c;
                self.in_flight -= 1;
            }
            return count;
        }

        pub fn getBuffer(self: *Self, c: *const Completion) []const u8 {
            if (c.result < 0) return &.{};
            const bytes: usize = @intCast(c.result);
            return self.bufs[c.buf_slot][0..bytes];
        }
    };
}
