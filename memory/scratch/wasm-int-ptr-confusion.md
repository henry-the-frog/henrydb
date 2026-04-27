# WASM Integer/Pointer Confusion in Untagged Value Representation

**Created:** 2026-04-27
**Uses:** 1
**Project:** monkey-lang WASM compiler

## The Bug
Runtime dispatch functions (__add, __eq, __lt, __gt) need to distinguish integer values from heap pointers to strings/arrays. Without tagged values, they use heuristics: check if a value looks like a valid memory pointer by reading memory at that address.

## What Went Wrong
Data segment started at offset 16. Integer values >= 16 that happened to be 4-byte aligned would pass pointer validation because reading memory at those addresses returned bytes from actual string constants that looked like valid TAG_STRING + length headers.

Example: integer `20` → memory[20] happened to contain the length byte of a string constant, which looked like TAG_STRING=1. So `20 + 20` was interpreted as string concatenation instead of integer addition.

## The Fix
1. **Moved data segment start to 65536** — ensures typical integer values (0-65535) never collide with actual heap objects
2. **Strict isStrPtr validation** — check tag AND length AND bounds, not just tag
3. **Applied to all runtime dispatch functions** — __add, __eq, __lt, __gt, __type

## Lesson
Untagged value representations create a fundamental ambiguity between integers and pointers. Any heuristic to distinguish them is fragile. The high-offset workaround handles typical programs but could still fail for integer values > 65536 that happen to align with heap objects. 

**Proper fix:** Use NaN-boxing or low-bit tagging to make the distinction structural rather than heuristic. This is a known design tradeoff — V8, SpiderMonkey, and LuaJIT all use tagged values for exactly this reason.
