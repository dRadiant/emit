const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lz4 = b.dependency("lz4", .{ .target = target, .optimize = optimize }).module("lz4");
    const eth = b.dependency("eth_zig", .{ .target = target, .optimize = optimize }).module("eth");

    // ── Modules ──────────────────────────────────────────────────────────
    const core = b.addModule("core", .{
        .root_source_file = b.path("core/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "lz4", .module = lz4 }},
    });

    const sdk = b.addModule("sdk", .{
        .root_source_file = b.path("sdk/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "core", .module = core },
            .{ .name = "lz4", .module = lz4 },
            .{ .name = "eth", .module = eth },
        },
    });
    if (b.lazyDependency("lmdbx", .{ .target = target, .optimize = optimize })) |dep| {
        sdk.addImport("lmdbx", dep.module("lmdbx"));
    }

    const engine_imports: []const std.Build.Module.Import = &.{
        .{ .name = "core", .module = core },
        .{ .name = "lz4", .module = lz4 },
        .{ .name = "eth", .module = eth },
    };

    // ── Engine binary ────────────────────────────────────────────────────
    const exe = b.addExecutable(.{
        .name = "emit-engine",
        .root_module = b.createModule(.{
            .root_source_file = b.path("engine/src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = engine_imports,
        }),
    });
    b.installArtifact(exe);

    const run_step = b.step("run", "Run emit-engine");
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    run_step.dependOn(&run_cmd.step);

    // ── Tests ────────────────────────────────────────────────────────────
    const test_step = b.step("test", "Run all tests");

    inline for (.{
        .{ "core-tests", "core/src/root.zig" },
        .{ "engine-tests", "engine/src/root.zig" },
    }) |entry| {
        const t = b.addTest(.{
            .name = entry[0],
            .root_module = b.createModule(.{
                .root_source_file = b.path(entry[1]),
                .target = target,
                .optimize = optimize,
                .imports = engine_imports,
            }),
        });
        test_step.dependOn(&b.addRunArtifact(t).step);
    }
    test_step.dependOn(&b.addRunArtifact(b.addTest(.{ .name = "sdk-tests", .root_module = sdk })).step);

    // Compile-fail harness: each sample MUST fail with an error line whose
    // suffix matches `expected`. Zig's expect_errors `.contains` matcher is
    // line-by-line `endsWith`, not free-form substring; write `expected` as
    // the tail of the actual @compileError text.
    const compile_fail = [_]struct { path: []const u8, expected: []const u8 }{
        .{ .path = "test/compile_fail/smoke.zig", .expected = "expected: smoke" },
        .{ .path = "test/compile_fail/append_store_load.zig", .expected = "ImmutableStore.load is not supported: cannot load immutable entities during backfill" },
        .{ .path = "test/compile_fail/entity_not_struct.zig", .expected = "is not a struct. Entities must be plain data structs whose first field is the primary key." },
        .{ .path = "test/compile_fail/entity_empty.zig", .expected = "has no fields. The first field must be the primary key." },
        .{ .path = "test/compile_fail/entity_missing_storage.zig", .expected = "must declare `pub const storage: sdk.StorageMode = .mutable;` or `.immutable;`. The choice is per-entity and intentional." },
        .{ .path = "test/compile_fail/event_missing_signature.zig", .expected = "must declare `pub const signature = \"Name(types,...)\";`. The SDK derives topic0 and name from it." },
        .{ .path = "test/compile_fail/handler_missing_method.zig", .expected = "is missing method `handleTransfer` for event `Transfer(address,address,uint256)`. Add `pub fn handleTransfer(log: sdk.DecodedLog, ctx: *Ctx) !void { ... }`." },
        .{ .path = "test/compile_fail/block_context_name_collision.zig", .expected = "both derive store field name 'items'. Rename one of the entity types." },
    };
    for (compile_fail) |s| {
        const obj = b.addObject(.{
            .name = std.fs.path.stem(s.path),
            .root_module = b.createModule(.{
                .root_source_file = b.path(s.path),
                .target = target,
                .optimize = optimize,
                .imports = &.{.{ .name = "sdk", .module = sdk }},
            }),
        });
        obj.expect_errors = .{ .contains = s.expected };
        test_step.dependOn(&obj.step);
    }

    // ── RocksDB import (lazy dep, only built on request) ─────────────────
    const import_step = b.step("import", "Build the RocksDB import tool");
    if (b.lazyDependency("rocksdb", .{ .target = target, .optimize = optimize, .enable_snappy = true })) |dep| {
        const import_exe = b.addExecutable(.{
            .name = "rocksdb-import",
            .root_module = b.createModule(.{
                .root_source_file = b.path("engine/src/rocksdb_import.zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "core", .module = core },
                    .{ .name = "lz4", .module = lz4 },
                    .{ .name = "rocksdb", .module = dep.module("rocksdb") },
                },
            }),
        });
        import_exe.linkLibC();
        import_step.dependOn(&b.addInstallArtifact(import_exe, .{}).step);
    }
}
