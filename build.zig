const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── External dependencies ────────────────────────────────────────────
    const lz4_dep = b.dependency("lz4", .{ .target = target, .optimize = optimize });
    const lz4_mod = lz4_dep.module("lz4");

    // ── core — shared package ────────────────────────────────────────────
    const core_mod = b.addModule("core", .{
        .root_source_file = b.path("core/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "lz4", .module = lz4_mod },
        },
    });

    // ── core tests ───────────────────────────────────────────────────────
    const test_step = b.step("test", "Run all tests");

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
    test_step.dependOn(&b.addRunArtifact(core_tests).step);

    // ── engine tests ─────────────────────────────────────────────────────
    const engine_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("engine/src/root.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "core", .module = core_mod },
                .{ .name = "lz4", .module = lz4_mod },
            },
        }),
    });
    test_step.dependOn(&b.addRunArtifact(engine_tests).step);
}
