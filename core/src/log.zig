//! Process-global leveled output for the CLI binaries (engine + indexers).
//! Set the level once at startup from the parsed flags; every call gates on it.
//! Everything goes to stderr (via `std.debug.print`), so program results and
//! diagnostics share one stream the operator can silence as a unit.
//!
//! `err` always prints (failures, usage). `info` is the default progress +
//! result output, suppressed by `--silent`. `debug` is step-by-step detail,
//! shown only under `--verbose`. The level is a write-once-at-startup global,
//! read-only for the run, so there is no synchronization concern.
const std = @import("std");
const builtin = @import("builtin");

pub const Level = enum(u8) {
    /// Errors only. For scripted / CI runs.
    silent = 0,
    /// Errors + progress + the result/stats block. Default.
    normal = 1,
    /// + per-range / per-block / per-connection detail.
    verbose = 2,
};

/// Test binaries default to silent so suite output carries only failures.
/// Level-behavior tests set and restore the level explicitly.
var level: Level = if (builtin.is_test) .silent else .normal;

pub fn setLevel(l: Level) void {
    level = l;
}

pub fn getLevel() Level {
    return level;
}

/// Resolve a level from `--silent` / `--verbose` flags. `--silent` wins if both
/// are somehow passed.
pub fn levelFromFlags(silent: bool, verbose: bool) Level {
    if (silent) return .silent;
    if (verbose) return .verbose;
    return .normal;
}

/// Always printed, even under `--silent`: failures and usage. Suppressed in
/// test binaries: the suite asserts on returned errors, and tests exercising
/// fail-loud paths would otherwise leak expected messages into build output.
pub fn err(comptime fmt: []const u8, args: anytype) void {
    if (builtin.is_test) return;
    std.debug.print(fmt, args);
}

/// Default output: progress milestones and the result/stats block. Suppressed
/// by `--silent`.
pub fn info(comptime fmt: []const u8, args: anytype) void {
    if (@intFromEnum(level) >= @intFromEnum(Level.normal)) std.debug.print(fmt, args);
}

/// Step-by-step detail (per-range import, per-block follow, per-connection
/// serve). Shown only under `--verbose`.
pub fn debug(comptime fmt: []const u8, args: anytype) void {
    if (@intFromEnum(level) >= @intFromEnum(Level.verbose)) std.debug.print(fmt, args);
}

// ── Tests ──────────────────────────────────────────────────────────────────

const testing = std.testing;

test "levelFromFlags resolves silent over verbose, else verbose, else normal" {
    try testing.expectEqual(Level.silent, levelFromFlags(true, false));
    try testing.expectEqual(Level.silent, levelFromFlags(true, true));
    try testing.expectEqual(Level.verbose, levelFromFlags(false, true));
    try testing.expectEqual(Level.normal, levelFromFlags(false, false));
}

test "setLevel / getLevel round-trip" {
    const saved = getLevel();
    defer setLevel(saved);
    setLevel(.silent);
    try testing.expectEqual(Level.silent, getLevel());
    setLevel(.verbose);
    try testing.expectEqual(Level.verbose, getLevel());
}
