const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lz4_dep = b.dependency("lz4", .{ .target = target, .optimize = optimize });
    const lz4_mod = lz4_dep.module("lz4");
    const eth_dep = b.dependency("eth_zig", .{ .target = target, .optimize = optimize });
    const eth_mod = eth_dep.module("eth");

    // ── Modules ──────────────────────────────────────────────────────────
    const core_mod = b.addModule("core", .{
        .root_source_file = b.path("core/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "lz4", .module = lz4_mod }},
    });

    const engine_imports: []const std.Build.Module.Import = &.{
        .{ .name = "core", .module = core_mod },
        .{ .name = "lz4", .module = lz4_mod },
        .{ .name = "eth", .module = eth_mod },
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

    // ── RocksDB import (lazy dep, only built on request) ─────────────────
    const import_step = b.step("import", "Build the RocksDB import tool");
    if (b.lazyDependency("rocksdb", .{ .target = target, .optimize = optimize, .enable_snappy = true })) |rocksdb_dep| {
        const import_exe = b.addExecutable(.{
            .name = "rocksdb-import",
            .root_module = b.createModule(.{
                .root_source_file = b.path("engine/src/rocksdb_import.zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "core", .module = core_mod },
                    .{ .name = "lz4", .module = lz4_mod },
                    .{ .name = "rocksdb", .module = rocksdb_dep.module("rocksdb") },
                },
            }),
        });
        import_exe.linkLibC();
        import_step.dependOn(&b.addInstallArtifact(import_exe, .{}).step);
    }
}
