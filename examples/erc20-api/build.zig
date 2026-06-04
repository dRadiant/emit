const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const emit = b.dependency("emit", .{ .target = target, .optimize = optimize });
    const sdk = emit.module("sdk");

    const httpz = b.dependency("httpz", .{ .target = target, .optimize = optimize }).module("httpz");

    // Shared CLI shell lives one level up so every example reuses it.
    const cli = b.createModule(.{
        .root_source_file = b.path("../utils/cli.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "sdk", .module = sdk }},
    });

    const imports = [_]std.Build.Module.Import{
        .{ .name = "sdk", .module = sdk },
        .{ .name = "httpz", .module = httpz },
        .{ .name = "cli", .module = cli },
    };

    const exe = b.addExecutable(.{
        .name = "erc20-api",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &imports,
        }),
    });
    b.installArtifact(exe);

    const run_step = b.step("run", "Run the erc20 API indexer");
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    run_step.dependOn(&run_cmd.step);

    const test_step = b.step("test", "Run example tests");
    const tests = b.addTest(.{
        .name = "erc20-api-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &imports,
        }),
    });
    test_step.dependOn(&b.addRunArtifact(tests).step);
}
