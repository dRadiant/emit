// expected: expected: smoke
//
// Verifies the compile_fail harness: this file MUST fail to compile, and the
// failure stderr MUST contain the expected substring above. If these
// invariants drift, the harness in build.zig stops protecting the @compileError
// paths in the SDK.

comptime {
    @compileError("expected: smoke");
}
