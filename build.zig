const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── External dependencies ────────────────────────────────────────────
    const lz4_dep = b.dependency("lz4", .{ .target = target, .optimize = optimize });
    const lz4_mod = lz4_dep.module("lz4");

    // ── core — shared package (types, bloom, flat reader, block filter, log serial, io_uring, parallel)
    const core_mod = b.addModule("core", .{
        .root_source_file = b.path("core/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "lz4", .module = lz4_mod },
        },
    });

    // ── core tests ───────────────────────────────────────────────────────
    const core_test_step = b.step("test", "Run core tests");
    const core_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("core/src/root.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "lz4", .module = lz4_mod },
            },
        }),
    });
    core_test_step.dependOn(&b.addRunArtifact(core_tests).step);

    // Suppress unused local warning — core_mod is consumed by engine/sdk (M1/M2).
    _ = core_mod;
}
