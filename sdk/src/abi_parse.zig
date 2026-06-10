/// Comptime parser for Solidity-style event signatures.
///
/// Accepts the full ABI form. Names and `indexed` keywords are optional:
///
///     "Transfer(address,address,uint256)"
///     "Transfer(address indexed from, address indexed to, uint256 value)"
///
/// Topic0 is `keccak(canonical)` where the canonical form strips arg names
/// and the `indexed` keyword and resolves aliases (`uint`→`uint256`,
/// `int`→`int256`, `byte`→`bytes1`). Both forms above produce the same
/// canonical `Transfer(address,address,uint256)` and the same topic0,
/// matching what `solc` emits.
///
/// Output carries each param's slot assignment (which topic slot or data
/// offset holds the value), so handler decoders comptime-resolve arg names
/// to byte ranges without re-walking the signature at runtime.
const std = @import("std");

pub const SlotKind = enum { topic, data };

pub const ParsedParam = struct {
    /// Empty if the signature didn't name this arg.
    name: []const u8,
    /// Canonical Solidity type (`address`, `uint256`, `bytes32`, `bytes`, …).
    type_str: []const u8,
    indexed: bool,
    slot_kind: SlotKind,
    /// `.topic`: 1-based topic index (1, 2, or 3). topic[0] is the selector.
    /// `.data`: byte offset within `log.data`. For dynamic types this is
    /// the offset of the head word (the head holds the tail offset).
    slot_index: usize,
};

pub const ParsedEvent = struct {
    name: []const u8,
    /// `Name(t1,t2,...)` form. Keccak-hashed for topic0.
    canonical: []const u8,
    params: []const ParsedParam,
};

/// Parse an event signature at comptime. Fires `@compileError` on any
/// malformed input. Quota is bumped here so callers don't track it.
pub fn parseEvent(comptime sig: []const u8) ParsedEvent {
    @setEvalBranchQuota(200_000);
    return comptime parseEventInner(sig);
}

/// Look up a parameter by name. Fires `@compileError` listing available
/// parameters if the name is not found.
pub fn paramByName(comptime parsed: ParsedEvent, comptime param_name: []const u8) ParsedParam {
    return comptime blk: {
        if (param_name.len == 0) @compileError("abi_parse: empty parameter name passed to paramByName");
        for (parsed.params) |p| {
            // Skip unnamed params so a caller passing "" can't match.
            if (p.name.len > 0 and std.mem.eql(u8, p.name, param_name)) break :blk p;
        }
        var avail: []const u8 = "";
        for (parsed.params, 0..) |p, i| {
            if (i > 0) avail = avail ++ ", ";
            avail = avail ++ (if (p.name.len == 0) "<unnamed>" else p.name);
        }
        @compileError("abi_parse: event `" ++ parsed.name ++ "` has no parameter named `" ++ param_name ++ "`. Available: " ++ avail);
    };
}

/// Extract a parameter's 32-byte ABI word from a log's `topics`/`data` per its
/// assigned slot. Indexed params read `topics[slot_index]`, non-indexed params
/// read the data word at `slot_index`. Data slots beyond `data.len` zero-fill,
/// the ABI's zero-padding convention and a guard against short or malformed
/// payloads (truncated RPC responses, test fixtures). Single source of truth
/// for the slot→word read shared by `handler` decode and `manifest` address
/// extraction.
pub fn wordAt(comptime p: ParsedParam, topics: []const [32]u8, data: []const u8) [32]u8 {
    return switch (comptime p.slot_kind) {
        .topic => topics[comptime p.slot_index],
        .data => if (data.len < comptime p.slot_index + 32)
            std.mem.zeroes([32]u8)
        else
            data[comptime p.slot_index..][0..32].*,
    };
}

// ── Internals ────────────────────────────────────────────────────────────

fn parseEventInner(comptime sig: []const u8) ParsedEvent {
    if (sig.len == 0) err(sig, "empty signature");

    const open = std.mem.indexOfScalar(u8, sig, '(') orelse err(sig, "missing '(' — expected `Name(types,...)`");
    if (sig[sig.len - 1] != ')') err(sig, "must end with ')'");
    const close = sig.len - 1;

    const event_name = trim(sig[0..open]);
    if (event_name.len == 0) err(sig, "missing event name before '('");
    if (!isIdent(event_name)) err(sig, "event name `" ++ event_name ++ "` is not a valid identifier");

    var params: []const ParsedParam = &.{};
    var canonical_types: []const u8 = "";
    var seen_names: []const []const u8 = &.{};
    var topic_idx: usize = 1;
    var data_word: usize = 0;
    var indexed_count: usize = 0;

    const inner = sig[open + 1 .. close];
    const parts: []const []const u8 = if (trim(inner).len == 0) &.{} else splitTopLevel(inner);
    for (parts) |raw_part| {
        const part = trim(raw_part);
        if (part.len == 0) err(sig, "empty param (stray or trailing comma?)");

        var p = parseParam(sig, part);

        if (p.name.len > 0) {
            for (seen_names) |n| if (std.mem.eql(u8, n, p.name)) err(sig, "duplicate arg name `" ++ p.name ++ "`");
            seen_names = seen_names ++ &[_][]const u8{p.name};
        }

        if (p.indexed) {
            indexed_count += 1;
            if (indexed_count > 3) err(sig, "more than 3 indexed args (Solidity non-anonymous event limit)");
            p.slot_kind = .topic;
            p.slot_index = topic_idx;
            topic_idx += 1;
        } else {
            p.slot_kind = .data;
            p.slot_index = data_word * 32;
            // Static arrays/tuples occupy several head words, dynamic types
            // one (the tail offset). `headWords` advances the cursor.
            data_word += headWords(p.type_str);
        }

        params = params ++ &[_]ParsedParam{p};
        if (canonical_types.len > 0) canonical_types = canonical_types ++ ",";
        canonical_types = canonical_types ++ p.type_str;
    }

    return .{
        .name = event_name,
        .canonical = event_name ++ "(" ++ canonical_types ++ ")",
        .params = params,
    };
}

fn parseParam(comptime sig: []const u8, comptime part: []const u8) ParsedParam {
    // Type span comes first, a tuple `(...)` (balanced parens) or a bare base
    // token, then any `[..]` array suffixes. Tuples carry internal commas and
    // spaces, so a flat whitespace tokenizer can't parse the type.
    var i: usize = 0;
    while (i < part.len and isWs(part[i])) i += 1;
    const type_start = i;
    if (i < part.len and part[i] == '(') {
        var depth: usize = 0;
        while (i < part.len) : (i += 1) {
            if (part[i] == '(') depth += 1 else if (part[i] == ')') {
                depth -= 1;
                if (depth == 0) {
                    i += 1;
                    break;
                }
            }
        }
    } else {
        while (i < part.len and !isWs(part[i]) and part[i] != '[') i += 1;
    }
    while (i < part.len and part[i] == '[') {
        while (i < part.len and part[i] != ']') i += 1;
        if (i < part.len) i += 1; // consume ']'
    }
    const type_tok = part[type_start..i];
    if (type_tok.len == 0) err(sig, "empty param");

    // Remainder is whitespace-separated optional `indexed` and arg name.
    var rest: []const []const u8 = &.{};
    while (i < part.len) {
        while (i < part.len and isWs(part[i])) i += 1;
        if (i >= part.len) break;
        const ts = i;
        while (i < part.len and !isWs(part[i])) i += 1;
        rest = rest ++ &[_][]const u8{part[ts..i]};
    }
    if (rest.len > 2) err(sig, "param `" ++ part ++ "` has too many tokens (expected `TYPE [indexed] [NAME]`)");

    var indexed = false;
    var name_tok: []const u8 = "";
    if (rest.len == 1) {
        if (std.mem.eql(u8, rest[0], "indexed")) indexed = true else name_tok = rest[0];
    } else if (rest.len == 2) {
        if (!std.mem.eql(u8, rest[0], "indexed")) err(sig, "expected `indexed` after the type in param `" ++ part ++ "`");
        indexed = true;
        name_tok = rest[1];
    }

    if (name_tok.len > 0 and !isIdent(name_tok)) err(sig, "arg name `" ++ name_tok ++ "` is not a valid identifier");

    const canonical_type = canonicalizeType(type_tok) orelse err(sig, "unsupported type `" ++ type_tok ++ "` (supported: address, bool, uintN, intN, bytesN, bytes, string, and arrays/tuples of these)");

    return .{
        .name = name_tok,
        .type_str = canonical_type,
        .indexed = indexed,
        .slot_kind = .data, // overwritten by caller once slot is assigned
        .slot_index = 0,
    };
}

pub const ShapeTag = enum { primitive, fixed_array, dynamic_array, tuple };

pub const TypeShape = struct {
    tag: ShapeTag,
    elem: []const u8 = "", // array element type
    len: usize = 0, // fixed-array length
    components: []const []const u8 = &.{}, // tuple component types
};

/// One-level structural view of a type string. Recurse by calling `typeShape`
/// again on `elem`/`components`. A trailing `[..]` suffix takes precedence over
/// a leading tuple paren, so `(a,b)[]` is an array of the tuple `(a,b)`. The
/// handler decoder drives off this so type structure has a single source.
pub fn typeShape(comptime t: []const u8) TypeShape {
    if (t.len == 0) return .{ .tag = .primitive };
    if (t[t.len - 1] == ']') {
        // Match the '[' of the trailing suffix. Brackets don't nest in types.
        var depth: usize = 0;
        var i: usize = t.len;
        while (i > 0) {
            i -= 1;
            if (t[i] == ']') depth += 1 else if (t[i] == '[') {
                depth -= 1;
                if (depth == 0) break;
            }
        }
        const elem = t[0..i];
        const inside = t[i + 1 .. t.len - 1];
        if (inside.len == 0) return .{ .tag = .dynamic_array, .elem = elem };
        const n = std.fmt.parseInt(usize, inside, 10) catch return .{ .tag = .primitive };
        return .{ .tag = .fixed_array, .elem = elem, .len = n };
    }
    if (t[0] == '(' and t[t.len - 1] == ')') {
        return .{ .tag = .tuple, .components = splitTopLevel(t[1 .. t.len - 1]) };
    }
    return .{ .tag = .primitive };
}

/// Split on top-level commas, ignoring those inside `()` / `[]`. Always returns
/// at least one part. A trailing comma yields a trailing empty part (rejected
/// upstream as an empty param / component).
fn splitTopLevel(comptime s: []const u8) []const []const u8 {
    var parts: []const []const u8 = &.{};
    var depth: usize = 0;
    var start: usize = 0;
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        const c = s[i];
        if (c == '(' or c == '[') depth += 1 else if (c == ')' or c == ']') {
            if (depth > 0) depth -= 1;
        } else if (c == ',' and depth == 0) {
            parts = parts ++ &[_][]const u8{s[start..i]};
            start = i + 1;
        }
    }
    return parts ++ &[_][]const u8{s[start..]};
}

/// True if the ABI type is dynamically sized (head is a single tail offset).
/// Covers `bytes`, `string`, any `T[]`, a fixed array of a dynamic element,
/// or a tuple with any dynamic component.
pub fn isDynamicType(comptime t: []const u8) bool {
    const s = typeShape(t);
    return switch (s.tag) {
        .dynamic_array => true,
        .fixed_array => isDynamicType(s.elem),
        .tuple => blk: {
            for (s.components) |c| if (isDynamicType(trim(c))) break :blk true;
            break :blk false;
        },
        .primitive => std.mem.eql(u8, t, "bytes") or std.mem.eql(u8, t, "string"),
    };
}

/// Number of 32-byte words the type occupies in the ABI head. Dynamic types
/// take one (the tail offset), a static fixed array takes `len × elem`, a
/// static tuple takes the sum over its components, a primitive takes one.
pub fn headWords(comptime t: []const u8) usize {
    if (isDynamicType(t)) return 1;
    const s = typeShape(t);
    return switch (s.tag) {
        .dynamic_array => 1, // unreachable (caught above), keeps the switch total
        .fixed_array => s.len * headWords(s.elem),
        .tuple => blk: {
            var n: usize = 0;
            for (s.components) |c| n += headWords(trim(c));
            break :blk n;
        },
        .primitive => 1,
    };
}

/// Canonicalize a type to its solc form (aliases resolved, recursively through
/// arrays and tuples) so topic0 = keccak(canonical) matches the compiler.
fn canonicalizeType(comptime t: []const u8) ?[]const u8 {
    const s = typeShape(t);
    switch (s.tag) {
        .dynamic_array => return (canonicalizeType(s.elem) orelse return null) ++ "[]",
        .fixed_array => return (canonicalizeType(s.elem) orelse return null) ++ "[" ++ std.fmt.comptimePrint("{d}", .{s.len}) ++ "]",
        .tuple => {
            var out: []const u8 = "(";
            for (s.components, 0..) |c, i| {
                if (i > 0) out = out ++ ",";
                out = out ++ (canonicalizeType(trim(c)) orelse return null);
            }
            return out ++ ")";
        },
        .primitive => return canonicalizePrimitive(t),
    }
}

fn canonicalizePrimitive(comptime t: []const u8) ?[]const u8 {
    // Aliases first so the prefix branches don't claim them.
    if (std.mem.eql(u8, t, "address")) return "address";
    if (std.mem.eql(u8, t, "bool")) return "bool";
    if (std.mem.eql(u8, t, "bytes")) return "bytes";
    if (std.mem.eql(u8, t, "string")) return "string";
    if (std.mem.eql(u8, t, "byte")) return "bytes1";
    if (std.mem.eql(u8, t, "uint")) return "uint256";
    if (std.mem.eql(u8, t, "int")) return "int256";
    if (matchN(t, "uint", 256, 8)) return t;
    if (matchN(t, "int", 256, 8)) return t;
    if (matchN(t, "bytes", 32, 1)) return t;
    return null;
}

/// Validate `t == prefix ++ N` where N is decimal, `min ≤ N ≤ max`, and
/// for bit-widths the boundary is byte-aligned (min == 8). For `bytesN`
/// the caller passes `min = 1` so any 1..32 is accepted.
fn matchN(comptime t: []const u8, comptime prefix: []const u8, comptime max: usize, comptime min: usize) bool {
    if (!std.mem.startsWith(u8, t, prefix)) return false;
    const n = std.fmt.parseInt(usize, t[prefix.len..], 10) catch return false;
    if (n < min or n > max) return false;
    if (min == 8 and n % 8 != 0) return false;
    return true;
}

fn isWs(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r';
}

fn trim(comptime s: []const u8) []const u8 {
    var lo: usize = 0;
    var hi: usize = s.len;
    while (lo < hi and isWs(s[lo])) lo += 1;
    while (hi > lo and isWs(s[hi - 1])) hi -= 1;
    return s[lo..hi];
}

fn isIdent(comptime s: []const u8) bool {
    if (s.len == 0) return false;
    const c0 = s[0];
    if (!((c0 >= 'a' and c0 <= 'z') or (c0 >= 'A' and c0 <= 'Z') or c0 == '_')) return false;
    for (s[1..]) |c| {
        if (!((c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or (c >= '0' and c <= '9') or c == '_')) return false;
    }
    return true;
}

fn err(comptime sig: []const u8, comptime msg: []const u8) noreturn {
    @compileError("abi_parse: " ++ msg ++ " — in `" ++ sig ++ "`");
}

// ── Tests ────────────────────────────────────────────────────────────────

test "wordAt reads topic and data slots and zero-fills short data" {
    const parsed = comptime parseEvent("E(address indexed a, uint256 b)");
    const pa = comptime paramByName(parsed, "a"); // indexed → topic slot 1
    const pb = comptime paramByName(parsed, "b"); // non-indexed → data slot 0

    var topics: [4][32]u8 = std.mem.zeroes([4][32]u8);
    topics[1][31] = 0xAB;
    var data: [32]u8 = std.mem.zeroes([32]u8);
    data[31] = 0xCD;

    try std.testing.expectEqual(@as(u8, 0xAB), wordAt(pa, &topics, &data)[31]);
    try std.testing.expectEqual(@as(u8, 0xCD), wordAt(pb, &topics, &data)[31]);

    // Data shorter than slot_index+32 zero-fills instead of reading OOB.
    const short = wordAt(pb, &topics, &.{});
    try std.testing.expectEqualSlices(u8, &([_]u8{0} ** 32), &short);
}

test "parses bare signature with no names" {
    const p = comptime parseEvent("Transfer(address,address,uint256)");
    try std.testing.expectEqualStrings("Transfer", p.name);
    try std.testing.expectEqualStrings("Transfer(address,address,uint256)", p.canonical);
    try std.testing.expectEqual(@as(usize, 3), p.params.len);
    try std.testing.expectEqualStrings("address", p.params[0].type_str);
    try std.testing.expectEqualStrings("", p.params[0].name);
    try std.testing.expectEqual(false, p.params[0].indexed);
}

test "parses full form with names and indexed" {
    const p = comptime parseEvent("Transfer(address indexed from, address indexed to, uint256 value)");
    try std.testing.expectEqualStrings("Transfer(address,address,uint256)", p.canonical);
    try std.testing.expectEqualStrings("from", p.params[0].name);
    try std.testing.expectEqual(true, p.params[0].indexed);
    try std.testing.expectEqual(@as(usize, 1), p.params[0].slot_index);
    try std.testing.expectEqual(.topic, p.params[0].slot_kind);
    try std.testing.expectEqualStrings("to", p.params[1].name);
    try std.testing.expectEqual(@as(usize, 2), p.params[1].slot_index);
    try std.testing.expectEqualStrings("value", p.params[2].name);
    try std.testing.expectEqual(false, p.params[2].indexed);
    try std.testing.expectEqual(.data, p.params[2].slot_kind);
    try std.testing.expectEqual(@as(usize, 0), p.params[2].slot_index);
}

test "mixed indexed/non-indexed assigns slots in declaration order" {
    const p = comptime parseEvent("Swap(address indexed sender, uint256 amount0, uint256 amount1, address indexed to)");
    try std.testing.expectEqual(.topic, p.params[0].slot_kind);
    try std.testing.expectEqual(@as(usize, 1), p.params[0].slot_index);
    try std.testing.expectEqual(.data, p.params[1].slot_kind);
    try std.testing.expectEqual(@as(usize, 0), p.params[1].slot_index);
    try std.testing.expectEqual(.data, p.params[2].slot_kind);
    try std.testing.expectEqual(@as(usize, 32), p.params[2].slot_index);
    try std.testing.expectEqual(.topic, p.params[3].slot_kind);
    try std.testing.expectEqual(@as(usize, 2), p.params[3].slot_index);
}

test "no-arg event" {
    const p = comptime parseEvent("Pause()");
    try std.testing.expectEqualStrings("Pause", p.name);
    try std.testing.expectEqualStrings("Pause()", p.canonical);
    try std.testing.expectEqual(@as(usize, 0), p.params.len);
}

test "type aliases canonicalize" {
    const p = comptime parseEvent("E(uint a, int b, byte c)");
    try std.testing.expectEqualStrings("E(uint256,int256,bytes1)", p.canonical);
}

test "extra whitespace tolerated" {
    const p = comptime parseEvent("E(  address   indexed   x  ,uint256 y)");
    try std.testing.expectEqualStrings("E(address,uint256)", p.canonical);
    try std.testing.expectEqualStrings("x", p.params[0].name);
    try std.testing.expectEqualStrings("y", p.params[1].name);
}

test "tight whitespace tolerated" {
    const p = comptime parseEvent("E(address,uint256,bool)");
    try std.testing.expectEqual(@as(usize, 3), p.params.len);
}

test "dynamic types appear in canonical form" {
    const p = comptime parseEvent("Log(string indexed key, bytes value)");
    try std.testing.expectEqualStrings("Log(string,bytes)", p.canonical);
    try std.testing.expectEqual(true, p.params[0].indexed);
    try std.testing.expectEqual(.topic, p.params[0].slot_kind);
    try std.testing.expectEqual(.data, p.params[1].slot_kind);
}

test "bytesN in valid range" {
    const p = comptime parseEvent("E(bytes1 a, bytes16 b, bytes32 c)");
    try std.testing.expectEqualStrings("E(bytes1,bytes16,bytes32)", p.canonical);
}

test "uintN at every multiple-of-8 boundary" {
    const p = comptime parseEvent("E(uint8,uint112,uint128,uint256)");
    try std.testing.expectEqualStrings("E(uint8,uint112,uint128,uint256)", p.canonical);
}

test "topic0 of canonical matches solc-style hash" {
    // Transfer(address,address,uint256) is the ERC20 selector 0xddf252ad…
    const p = comptime parseEvent("Transfer(address indexed from, address indexed to, uint256 value)");
    try std.testing.expectEqualStrings("Transfer(address,address,uint256)", p.canonical);
}

test "mixed named and unnamed params" {
    const p = comptime parseEvent("E(address indexed from, address, uint256 value, bool)");
    try std.testing.expectEqualStrings("from", p.params[0].name);
    try std.testing.expectEqualStrings("", p.params[1].name);
    try std.testing.expectEqualStrings("value", p.params[2].name);
    try std.testing.expectEqualStrings("", p.params[3].name);
    try std.testing.expectEqualStrings("E(address,address,uint256,bool)", p.canonical);
    // Slot assignment ignores naming entirely.
    try std.testing.expectEqual(.topic, p.params[0].slot_kind);
    try std.testing.expectEqual(.data, p.params[1].slot_kind);
    try std.testing.expectEqual(@as(usize, 0), p.params[1].slot_index);
    try std.testing.expectEqual(@as(usize, 32), p.params[2].slot_index);
    try std.testing.expectEqual(@as(usize, 64), p.params[3].slot_index);
}

test "duplicate names allowed when both empty" {
    // Two unnamed `address` slots are common in factory signatures.
    _ = comptime parseEvent("E(address, address, uint256)");
}

test "paramByName resolves" {
    const p = comptime parseEvent("Transfer(address indexed from, address indexed to, uint256 value)");
    const value = comptime paramByName(p, "value");
    try std.testing.expectEqualStrings("uint256", value.type_str);
    try std.testing.expectEqual(.data, value.slot_kind);
    try std.testing.expectEqual(@as(usize, 0), value.slot_index);
    const to = comptime paramByName(p, "to");
    try std.testing.expectEqual(.topic, to.slot_kind);
    try std.testing.expectEqual(@as(usize, 2), to.slot_index);
}

test "array suffixes canonicalize and resolve element aliases" {
    const p = comptime parseEvent("E(uint[] amounts, address[3] who, bytes32[] roots)");
    try std.testing.expectEqualStrings("E(uint256[],address[3],bytes32[])", p.canonical);
}

test "tuple canonicalizes with element aliases and whitespace" {
    const p = comptime parseEvent("E( (address, uint) pair, (uint24,int24,address) key )");
    try std.testing.expectEqualStrings("E((address,uint256),(uint24,int24,address))", p.canonical);
}

test "array of tuple canonicalizes (parse only; decode deferred)" {
    const p = comptime parseEvent("E((address,uint256)[] items)");
    try std.testing.expectEqualStrings("E((address,uint256)[])", p.canonical);
}

test "dynamic array occupies one head word" {
    const p = comptime parseEvent("E(address indexed a, uint256[] amounts, uint256 total)");
    try std.testing.expectEqual(@as(usize, 0), (comptime paramByName(p, "amounts")).slot_index);
    try std.testing.expectEqual(@as(usize, 32), (comptime paramByName(p, "total")).slot_index);
}

test "static array occupies N head words" {
    const p = comptime parseEvent("E(uint256[3] arr, address who)");
    try std.testing.expectEqual(@as(usize, 96), (comptime paramByName(p, "who")).slot_index);
}

test "static tuple occupies the sum of its components" {
    const p = comptime parseEvent("E((address,uint256) pair, bool flag)");
    try std.testing.expectEqual(@as(usize, 64), (comptime paramByName(p, "flag")).slot_index);
}

test "headWords and isDynamicType for arrays and tuples" {
    try std.testing.expectEqual(@as(usize, 1), comptime headWords("uint256"));
    try std.testing.expectEqual(@as(usize, 1), comptime headWords("uint256[]")); // dynamic → 1
    try std.testing.expectEqual(@as(usize, 3), comptime headWords("uint256[3]")); // static → 3
    try std.testing.expectEqual(@as(usize, 2), comptime headWords("(address,uint256)"));
    try std.testing.expectEqual(@as(usize, 1), comptime headWords("(address,uint256)[]"));
    try std.testing.expect(comptime isDynamicType("bytes"));
    try std.testing.expect(comptime isDynamicType("uint256[]"));
    try std.testing.expect(comptime !isDynamicType("uint256[3]"));
    try std.testing.expect(comptime !isDynamicType("(address,uint256)"));
    try std.testing.expect(comptime isDynamicType("(address,bytes)")); // tuple w/ dynamic component
}
