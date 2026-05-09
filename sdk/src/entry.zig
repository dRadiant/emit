/// `sdk.run` and `sdk.init`: orchestrate the four-phase pipeline.
///
/// Phase 1: filter_builder.build         (static + factory addresses)
/// Phase 2: scanner.scanCreations        (only when factories declared)
/// Phase 3: filter_builder.appendChildren (only when Phase 2 found any)
/// Phase 4: scanner.replay               (with commit batching via Context)
///
/// `run` performs the full backfill and tears down the context. `init`
/// performs the same backfill but returns a live `Context` whose entity
/// stores stay open so the caller (e.g. an HTTP server) can read entities.
const std = @import("std");

const core = @import("core");
const lmdbx = @import("lmdbx");

const filter_builder = @import("filter_builder.zig");
const root = @import("root.zig");
const scanner = @import("scanner.zig");
const sdk_manifest = @import("manifest.zig");

pub const Options = struct {
    /// Directory containing the engine's flat store
    /// (blocks.dat / blocks.idx / blooms.bin / meta.bin). Read-only.
    engine_data_dir: []const u8,
    /// SDK-managed data root. The SDK creates `<data_dir>/entity/` for the
    /// entity MDBX env and `<data_dir>/filter/` for the filtered-index env
    /// on first run; both are mkdir'd if missing. Users only need to
    /// allocate this single directory.
    data_dir: []const u8,
    /// Flush + commit cadence during handler replay, in dispatched logs.
    commit_interval: u32 = 100_000,
};

/// Result of a backfill run; returned from `run` and embedded in `Context`.
/// `elapsed_ns - (filter_build_ns + scan_creations_ns + append_children_ns +
/// replay_ns)` is overhead (entity-store open, final commit, env init).
pub const RunStats = struct {
    filter_blocks_scanned: u64 = 0,
    filter_blocks_matched: u64 = 0,
    filter_total_logs: u64 = 0,
    children_blocks_matched: u64 = 0,
    children_total_logs: u64 = 0,
    discovered_children: u32 = 0,
    logs_dispatched: u64 = 0,
    blocks_dispatched: u64 = 0,
    commits_performed: u32 = 0,
    filter_build_ns: u64 = 0,
    scan_creations_ns: u64 = 0,
    append_children_ns: u64 = 0,
    replay_ns: u64 = 0,
    elapsed_ns: u64 = 0,
};

/// Comptime-generate the long-lived context type. `entities` is the user's
/// tuple of entity types; each entity declares
/// `pub const storage: sdk.StorageMode = .mutable | .immutable;`.
///
/// Heap-allocated by `init` so each store can hold a stable
/// `*const lmdbx.Transaction` pointer into `self._active_txn`. When
/// `commitCycle` reassigns `_active_txn`, every store's pointer-deref
/// transparently sees the new transaction without re-binding.
///
/// Underscore-prefixed fields are SDK internals — handlers should not read
/// or mutate them. Public surface for handlers is `block_number`,
/// `timestamp`, `stores`, `stats`, and the `ethCall` / `registerContract`
/// methods.
/// `entities` may be either a tuple of entity types — `.{ Account, … }` —
/// or the entities module itself, e.g. `@import("entities.zig")`. The
/// module form scans `pub` decls for any struct declaring
/// `pub const storage: sdk.StorageMode` and uses those, in source-
/// declaration order. Both forms produce identical Context types when
/// the entity sets match.
pub fn Context(comptime entities: anytype) type {
    const Stores = StoresStruct(entities);
    return struct {
        const Self = @This();

        block_number: u64 = 0,
        timestamp: u64 = 0,
        stores: Stores,
        stats: RunStats = .{},

        _allocator: std.mem.Allocator,
        _env: lmdbx.Environment,
        _active_txn: lmdbx.Transaction,

        /// Tear down: abort the open txn, deinit every entity store, close
        /// the entity env, free the heap-allocated Context itself.
        pub fn deinit(self: *Self) void {
            self._active_txn.abort() catch {};
            inline for (std.meta.fields(Stores)) |f| {
                var s = &@field(self.stores, f.name);
                s.deinit();
            }
            self._env.deinit() catch {};
            self._allocator.destroy(self);
        }

        /// Flush every entity store to MDBX, commit the active txn, open a
        /// new write txn. Called by `scanner.replay` every commit_interval
        /// events and once more from `init` after replay completes.
        pub fn commitCycle(self: *Self) !void {
            inline for (std.meta.fields(Stores)) |f| {
                var s = &@field(self.stores, f.name);
                try s.flush();
            }
            try self._active_txn.commit();
            self._active_txn = try self._env.transaction(.{});
            self.stats.commits_performed += 1;
        }

        /// Returns `error.NotYetImplemented` until the ethcall MDBX cache
        /// and Multicall3 batching land.
        pub fn ethCall(_: *Self, _: [20]u8, _: []const u8) ![]const u8 {
            return error.NotYetImplemented;
        }

        /// Factory pre-pass discovers child addresses directly from logs,
        /// so this is a stub for the full dynamic-registration API that
        /// will arrive with head following.
        pub fn registerContract(_: *Self, _: [20]u8) !void {
            return error.NotYetImplemented;
        }
    };
}

/// Backfill to completion and tear down. The entity stores are committed
/// and closed; the entity MDBX env is closed. To keep the stores readable
/// after backfill, use `init` instead.
pub fn run(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    comptime entities: anytype,
    options: Options,
    allocator: std.mem.Allocator,
) !RunStats {
    const ctx = try init(m, Handler, entities, options, allocator);
    const stats = ctx.stats;
    ctx.deinit();
    return stats;
}

/// Backfill to completion and return the live `Context`. Caller owns the
/// pointer and must call `deinit` when done reading. Heap-allocated so
/// that the entity stores' `*const lmdbx.Transaction` pointers (into the
/// Context's `active_txn` field) stay valid after `init` returns.
pub fn init(
    comptime m: sdk_manifest.Manifest,
    comptime Handler: type,
    comptime entities: anytype,
    options: Options,
    allocator: std.mem.Allocator,
) !*Context(entities) {
    var timer = try std.time.Timer.start();

    // Derive and mkdir the entity / filter subdirs under `data_dir`.
    const entity_dir = try std.fs.path.join(allocator, &.{ options.data_dir, "entity" });
    defer allocator.free(entity_dir);
    const filter_dir = try std.fs.path.join(allocator, &.{ options.data_dir, "filter" });
    defer allocator.free(filter_dir);
    try std.fs.cwd().makePath(entity_dir);
    try std.fs.cwd().makePath(filter_dir);
    const entity_dir_z = try allocator.dupeZ(u8, entity_dir);
    defer allocator.free(entity_dir_z);
    const filter_dir_z = try allocator.dupeZ(u8, filter_dir);
    defer allocator.free(filter_dir_z);

    var reader = try core.FlatStoreReader.open(options.engine_data_dir);
    defer reader.close();

    // Heap-allocate Context up front so its `_active_txn` field has a
    // stable address before stores take pointers to it. Stats are
    // populated in place across phases.
    const C = Context(entities);
    const ctx = try allocator.create(C);
    errdefer allocator.destroy(ctx);

    const max_dbs = comptime entitiesLen(entities);
    const env = try lmdbx.Environment.init(entity_dir_z, .{ .max_dbs = max_dbs });
    errdefer env.deinit() catch {};

    ctx.* = .{
        ._allocator = allocator,
        ._env = env,
        ._active_txn = try env.transaction(.{}),
        .stores = undefined,
    };
    errdefer ctx._active_txn.abort() catch {};

    // Phase 1.
    const primary_result = try filter_builder.build(&reader, m, filter_dir_z, allocator);
    ctx.stats.filter_blocks_scanned = primary_result.blocks_scanned;
    ctx.stats.filter_blocks_matched = primary_result.blocks_matched;
    ctx.stats.filter_total_logs = primary_result.total_logs;
    ctx.stats.filter_build_ns = primary_result.elapsed_ns;

    // Phases 2 + 3 (factory-only).
    if (comptime m.factories.len > 0) {
        const filter_env = try lmdbx.Environment.init(filter_dir_z, .{ .max_dbs = 2 });
        var phase23_timer = try std.time.Timer.start();
        var discovered = try scanner.scanCreations(filter_env, m, allocator);
        defer discovered.deinit();
        ctx.stats.scan_creations_ns = phase23_timer.read();
        filter_env.deinit() catch {};

        ctx.stats.discovered_children = discovered.count();

        if (discovered.count() > 0) {
            const child_addrs = try allocator.alloc([20]u8, discovered.count());
            defer allocator.free(child_addrs);
            var i: usize = 0;
            var it = discovered.keyIterator();
            while (it.next()) |addr| : (i += 1) child_addrs[i] = addr.*;

            const child_result = try filter_builder.appendChildren(
                &reader,
                m,
                child_addrs,
                filter_dir_z,
                allocator,
            );
            ctx.stats.children_blocks_matched = child_result.blocks_matched;
            ctx.stats.children_total_logs = child_result.total_logs;
            ctx.stats.append_children_ns = child_result.elapsed_ns;
        }
    }

    // Phase 4 store open.
    inline for (comptime resolveEntities(entities)) |T| {
        const StoreT = root.storeFor(T);
        const dbi_name = comptime entityFieldName(T);
        @field(ctx.stores, dbi_name) = try StoreT.open(allocator, &ctx._active_txn, dbi_name);
    }

    // Phase 4 run.
    const filter_env = try lmdbx.Environment.init(filter_dir_z, .{ .max_dbs = 2 });
    defer filter_env.deinit() catch {};

    const replay_result = try scanner.replay(
        filter_env,
        m,
        Handler,
        ctx,
        .{ .commit_interval = options.commit_interval },
    );
    ctx.stats.logs_dispatched = replay_result.logs_dispatched;
    ctx.stats.blocks_dispatched = replay_result.blocks_dispatched;
    ctx.stats.replay_ns = replay_result.elapsed_ns;

    // Final commit so any logs since the last commit boundary land.
    try ctx.commitCycle();
    ctx.stats.elapsed_ns = timer.read();
    return ctx;
}

fn entitiesLen(comptime entities: anytype) u32 {
    return @intCast(comptime resolveEntities(entities).len);
}

/// Comptime-build the inner struct that holds one entity store per tuple
/// element, used as the type of `Context.stores`. Field names derive from
/// the entity type's basename: lowercase the first byte and append `s`
/// (e.g. `Account` → `accounts`). An entity type may override this with
/// `pub const store_name = "balances";` — useful when the auto-derived
/// name is ugly (`LBTCBalance` → `lBTCBalances`) or collides with another
/// entity. Two entities whose effective store names collide raise
/// `@compileError`.
fn StoresStruct(comptime entities: anytype) type {
    const list = comptime resolveEntities(entities);

    comptime var derived: [list.len][:0]const u8 = undefined;
    inline for (list, 0..) |T, i| {
        // storeFor enforces `pub const storage: sdk.StorageMode`.
        _ = root.storeFor(T);
        derived[i] = entityFieldName(T);
    }

    inline for (derived, 0..) |a, i| {
        if (i + 1 >= derived.len) break;
        inline for (derived[i + 1 ..], i + 1..) |b, j| {
            if (std.mem.eql(u8, a, b)) @compileError(std.fmt.comptimePrint(
                "sdk.Context: entity types '{s}' and '{s}' both derive store field name '{s}'. Rename one of the entity types.",
                .{ @typeName(list[i]), @typeName(list[j]), a },
            ));
        }
    }

    var struct_fields: [list.len]std.builtin.Type.StructField = undefined;
    inline for (list, 0..) |T, i| {
        const StoreT = root.storeFor(T);
        struct_fields[i] = .{
            .name = derived[i],
            .type = StoreT,
            .default_value_ptr = null,
            .is_comptime = false,
            .alignment = @alignOf(StoreT),
        };
    }

    return @Type(.{ .@"struct" = .{
        .layout = .auto,
        .fields = &struct_fields,
        .decls = &.{},
        .is_tuple = false,
    } });
}

/// Normalize `entities` to a `[]const type`. Accepts either the tuple
/// form (`.{ A, B, C }`) or a module type (`@import("entities.zig")`).
fn resolveEntities(comptime entities: anytype) []const type {
    const T = @TypeOf(entities);

    // Module form: `entities` is a type whose pub decls include the
    // entity structs (those declaring `pub const storage`).
    if (T == type) {
        var out: []const type = &.{};
        inline for (@typeInfo(entities).@"struct".decls) |d| {
            const member = @field(entities, d.name);
            if (@TypeOf(member) == type and @hasDecl(member, "storage")) {
                out = out ++ &[_]type{member};
            }
        }
        if (out.len == 0) @compileError(
            "sdk.Context: module `" ++ @typeName(entities) ++ "` has no entities (no pub structs declaring `pub const storage: StorageMode`)",
        );
        return out;
    }

    // Tuple form: `entities` is a tuple value of entity types.
    const info = @typeInfo(T);
    if (info == .@"struct" and info.@"struct".is_tuple) {
        var out: []const type = &.{};
        inline for (info.@"struct".fields, 0..) |f, i| {
            const t = @field(entities, f.name);
            if (@TypeOf(t) != type) @compileError(std.fmt.comptimePrint(
                "sdk.Context: entities[{d}] is not a type. Pass entity types: `.{{ Account, Allowance, Transfer, Approval }}`.",
                .{i},
            ));
            out = out ++ &[_]type{t};
        }
        return out;
    }

    @compileError(
        "sdk.Context: expected tuple of entity types or entities module, got `" ++ @typeName(T) ++ "`",
    );
}

fn entityFieldName(comptime T: type) [:0]const u8 {
    return comptime blk: {
        if (@hasDecl(T, "store_name")) {
            const override: []const u8 = T.store_name;
            if (override.len == 0) @compileError(
                "sdk.Context: entity type '" ++ @typeName(T) ++ "' declared `pub const store_name` but it is empty.",
            );
            break :blk std.fmt.comptimePrint("{s}", .{override});
        }
        const full = @typeName(T);
        const start = if (std.mem.lastIndexOfScalar(u8, full, '.')) |idx| idx + 1 else 0;
        const basename = full[start..];
        if (basename.len == 0) @compileError(
            "sdk.Context: entity type '" ++ full ++ "' has empty basename. Cannot derive store field name. Add `pub const store_name = \"...\";` to override.",
        );
        break :blk std.fmt.comptimePrint("{c}{s}s", .{ std.ascii.toLower(basename[0]), basename[1..] });
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;
const types = core.types;
const log_serial = core.log_serial;
const flat_reader = core.flat_reader;
const bloom = core.bloom;

const Transfer = struct {
    pub const signature = "Transfer(address,address,uint256)";
};

const Account = struct {
    pub const storage: root.StorageMode = .mutable;
    id: [20]u8,
    balance: u64,
};

const LBTCBalance = struct {
    pub const storage: root.StorageMode = .mutable;
    pub const store_name = "balances";
    id: [20]u8,
    amount: u64,
};

const ADDR_TOKEN: [20]u8 = [_]u8{0xAE} ** 20;

test "store_name override beats the default basename derivation" {
    const Ctx = Context(.{LBTCBalance});
    const Stores = std.meta.fieldInfo(Ctx, .stores).type;
    try testing.expect(@hasField(Stores, "balances"));
    try testing.expect(!@hasField(Stores, "lBTCBalances"));
}

test "Context accepts a module type and produces the same Context as the tuple form" {
    // Inline module fixture: a struct with two pub entity decls plus a
    // non-entity helper that the resolver must skip.
    const FixtureModule = struct {
        pub const A = struct {
            pub const storage: root.StorageMode = .mutable;
            id: [20]u8,
            balance: u256,
        };
        pub const B = struct {
            pub const storage: root.StorageMode = .immutable;
            id: [16]u8,
            value: u64,
        };
        // Non-entity decl — must be ignored by the resolver.
        pub const helper_constant: u32 = 42;
    };

    const FromModule = Context(FixtureModule);
    const FromTuple = Context(.{ FixtureModule.A, FixtureModule.B });
    try testing.expectEqual(FromTuple, FromModule);

    const Stores = std.meta.fieldInfo(FromModule, .stores).type;
    try testing.expect(@hasField(Stores, "as"));
    try testing.expect(@hasField(Stores, "bs"));
}

const TransferHandler = struct {
    pub fn handleTransfer(log: @import("handler.zig").Log(Transfer), ctx: anytype) !void {
        // Transfer's signature is unnamed, so we still read positionally
        // here; named-arg access (`log.args.value`) is exercised by the
        // example indexers and the parser tests.
        const from = log.topics[1][12..32].*;
        const to = log.topics[2][12..32].*;
        const value: u64 = std.mem.readInt(u64, log.data[24..32], .big);

        if (!std.mem.eql(u8, &from, &([_]u8{0} ** 20))) {
            var sender = try ctx.stores.accounts.loadOrInit(from);
            sender.balance -%= value;
            try ctx.stores.accounts.save(sender);
        }
        var receiver = try ctx.stores.accounts.loadOrInit(to);
        receiver.balance +%= value;
        try ctx.stores.accounts.save(receiver);
    }
};

/// Caller owns `data_buf` and must keep it alive as long as the returned
/// RawLog is used; we used to stash data in a threadlocal static, which
/// silently aliased across calls and corrupted the test fixtures.
fn makeTransferLog(block: u64, log_index: u16, from: [20]u8, to: [20]u8, value: u64, data_buf: *[32]u8) core.RawLog {
    var from_topic: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(from_topic[12..32], &from);
    var to_topic: [32]u8 = std.mem.zeroes([32]u8);
    @memcpy(to_topic[12..32], &to);
    @memset(data_buf, 0);
    std.mem.writeInt(u64, data_buf[24..32], value, .big);
    return .{
        .block_number = block,
        .tx_index = 0,
        .log_index = log_index,
        .address = ADDR_TOKEN,
        .topic_count = 3,
        .topics = .{
            sdk_manifest.eventTopic0(Transfer),
            from_topic,
            to_topic,
            [_]u8{0} ** 32,
        },
        .data = data_buf,
        .tx_hash = [_]u8{0xFE} ** 32,
    };
}

fn writeFlatStoreFromLogs(dir: std.fs.Dir, blocks: []const []const core.RawLog, allocator: std.mem.Allocator) !void {
    var blocks_file = try dir.createFile("blocks.dat", .{});
    defer blocks_file.close();
    var idx_file = try dir.createFile("blocks.idx", .{});
    defer idx_file.close();
    var blooms_file = try dir.createFile("blooms.bin", .{});
    defer blooms_file.close();

    var idx_hdr: [flat_reader.INDEX_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, idx_hdr[0..8], blocks[0][0].block_number, .little);
    std.mem.writeInt(u64, idx_hdr[8..16], blocks.len, .little);
    try idx_file.writeAll(&idx_hdr);

    var blooms_hdr: [flat_reader.BLOOM_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, &blooms_hdr, blocks.len, .little);
    try blooms_file.writeAll(&blooms_hdr);

    const serialize_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(serialize_buf);
    const compress_buf = try allocator.alloc(u8, types.BLOCK_BUF_SIZE);
    defer allocator.free(compress_buf);

    var offset: u64 = 0;
    for (blocks) |logs| {
        const written = log_serial.serializeLogs(logs, serialize_buf);
        const entry_len = try log_serial.compressEntry(serialize_buf[0..written], compress_buf);
        try blocks_file.writeAll(compress_buf[0..entry_len]);

        var idx_entry: [flat_reader.INDEX_ENTRY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, idx_entry[0..8], offset, .little);
        std.mem.writeInt(u32, idx_entry[8..12], @intCast(entry_len), .little);
        try idx_file.writeAll(&idx_entry);

        const tb = log_serial.buildTopicBloom(logs);
        const ab = log_serial.buildAddrBloom(logs);
        var bloom_entry: [flat_reader.BLOOM_ENTRY_SIZE]u8 = std.mem.zeroes([flat_reader.BLOOM_ENTRY_SIZE]u8);
        std.mem.writeInt(u64, bloom_entry[0..8], logs[0].block_number, .big);
        @memcpy(bloom_entry[flat_reader.TOPIC_BLOOM_OFFSET..][0..bloom.BLOOM_SIZE], &tb.bits);
        @memcpy(bloom_entry[flat_reader.ADDR_BLOOM_OFFSET..][0..bloom.ADDR_BLOOM_SIZE], &ab.bits);
        try blooms_file.writeAll(&bloom_entry);

        offset += entry_len;
    }
}

test "init: backfills planted Transfers and final balances match" {
    const allocator = testing.allocator;

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    const BOB: [20]u8 = [_]u8{0xB2} ** 20;
    const CARL: [20]u8 = [_]u8{0xC3} ** 20;

    // Plant four Transfers: mint 100 to alice, mint 50 to bob,
    // alice → carl 30, bob → alice 20.
    var data_bufs: [4][32]u8 = undefined;
    const log_b100 = makeTransferLog(100, 0, [_]u8{0} ** 20, ALICE, 100, &data_bufs[0]);
    const log_b101 = makeTransferLog(101, 0, [_]u8{0} ** 20, BOB, 50, &data_bufs[1]);
    const log_b102 = makeTransferLog(102, 0, ALICE, CARL, 30, &data_bufs[2]);
    const log_b103 = makeTransferLog(103, 0, BOB, ALICE, 20, &data_bufs[3]);

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    const blocks = [_][]const core.RawLog{
        &.{log_b100},
        &.{log_b101},
        &.{log_b102},
        &.{log_b103},
    };
    try writeFlatStoreFromLogs(src_tmp.dir, &blocks, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);

    var data_tmp = testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data_tmp.dir.realpath(".", &data_path_buf);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "erc20",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    const ctx = try init(
        Manifest,
        TransferHandler,
        .{Account},
        .{
            .engine_data_dir = src_path,
            .data_dir = data_path,
            .commit_interval = 100_000,
        },
        allocator,
    );
    defer ctx.deinit();

    try testing.expectEqual(@as(u64, 4), ctx.stats.logs_dispatched);
    try testing.expectEqual(@as(u64, 4), ctx.stats.blocks_dispatched);
    try testing.expect(ctx.stats.commits_performed >= 1);

    // alice: +100 -30 +20 = 90
    // bob:   +50 -20      = 30
    // carl:  +30          = 30
    const alice = (try ctx.stores.accounts.load(ALICE)) orelse return error.MissingAlice;
    const bob = (try ctx.stores.accounts.load(BOB)) orelse return error.MissingBob;
    const carl = (try ctx.stores.accounts.load(CARL)) orelse return error.MissingCarl;
    try testing.expectEqual(@as(u64, 90), alice.balance);
    try testing.expectEqual(@as(u64, 30), bob.balance);
    try testing.expectEqual(@as(u64, 30), carl.balance);
}

test "run: returns stats and tears down without leaking" {
    const allocator = testing.allocator;

    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    const BOB: [20]u8 = [_]u8{0xB2} ** 20;
    var data_bufs: [2][32]u8 = undefined;
    const log = makeTransferLog(100, 0, [_]u8{0} ** 20, ALICE, 100, &data_bufs[0]);
    const log2 = makeTransferLog(101, 0, ALICE, BOB, 25, &data_bufs[1]);

    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();
    const blocks = [_][]const core.RawLog{ &.{log}, &.{log2} };
    try writeFlatStoreFromLogs(src_tmp.dir, &blocks, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);

    var data_tmp = testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data_tmp.dir.realpath(".", &data_path_buf);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "erc20",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    const stats = try run(
        Manifest,
        TransferHandler,
        .{Account},
        .{
            .engine_data_dir = src_path,
            .data_dir = data_path,
        },
        allocator,
    );
    try testing.expectEqual(@as(u64, 2), stats.logs_dispatched);
    try testing.expectEqual(@as(u64, 2), stats.blocks_dispatched);
}

test "init + replay commit batching: ctx.commitCycle fires per commit_interval" {
    const allocator = testing.allocator;

    // 12 Transfers split across 12 blocks. With commit_interval = 5,
    // commitCycle fires at events 5 and 10 (during replay) plus once
    // post-replay = 3 commits.
    var src_tmp = testing.tmpDir(.{});
    defer src_tmp.cleanup();

    var raw_logs: [12]core.RawLog = undefined;
    var data_bufs: [12][32]u8 = undefined;
    var blocks_storage: [12][1]core.RawLog = undefined;
    var blocks_slices: [12][]const core.RawLog = undefined;
    const ALICE: [20]u8 = [_]u8{0xA1} ** 20;
    const BOB: [20]u8 = [_]u8{0xB2} ** 20;
    for (0..12) |i| {
        raw_logs[i] = makeTransferLog(@as(u64, 100 + i), 0, ALICE, BOB, 1, &data_bufs[i]);
        blocks_storage[i] = .{raw_logs[i]};
        blocks_slices[i] = &blocks_storage[i];
    }
    try writeFlatStoreFromLogs(src_tmp.dir, &blocks_slices, allocator);
    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try src_tmp.dir.realpath(".", &src_path_buf);

    var data_tmp = testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = try data_tmp.dir.realpath(".", &data_path_buf);

    const Manifest: sdk_manifest.Manifest = .{
        .name = "erc20",
        .chain_id = 1,
        .start_block = 0,
        .contracts = &.{.{ .name = "T", .address = ADDR_TOKEN, .events = &.{Transfer} }},
    };

    const ctx = try init(
        Manifest,
        TransferHandler,
        .{Account},
        .{
            .engine_data_dir = src_path,
            .data_dir = data_path,
            .commit_interval = 5,
        },
        allocator,
    );
    defer ctx.deinit();

    // 12 events, interval 5 → commits at events 5 and 10 during replay, plus
    // a final commit from init. Total 3.
    try testing.expectEqual(@as(u32, 3), ctx.stats.commits_performed);
    try testing.expectEqual(@as(u64, 12), ctx.stats.logs_dispatched);
}
