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

    inline for (.{ "core/src/root.zig", "engine/src/root.zig" }) |root| {
        const t = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path(root),
                .target = target,
                .optimize = optimize,
                .imports = engine_imports,
            }),
        });
        test_step.dependOn(&b.addRunArtifact(t).step);
    }
    test_step.dependOn(&b.addRunArtifact(b.addTest(.{ .root_module = sdk })).step);

    // Compile-fail harness: each sample MUST fail with stderr containing the
    // expected substring.
    const compile_fail = [_]struct { path: []const u8, expected: []const u8 }{
        .{ .path = "test/compile_fail/smoke.zig", .expected = "expected: smoke" },
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
