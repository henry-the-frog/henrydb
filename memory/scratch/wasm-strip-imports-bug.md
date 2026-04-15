# WASM stripUnusedImports Bug

**Created:** 2026-04-15 | **Uses:** 1

## Problem
`stripUnusedImports` in `wasm.js` was broken in two ways:
1. Accessed `func.body.bytes` but `FuncBodyBuilder` stores code in `func.body.code`
2. Raw byte scanning for `Op.call` (0x10) misidentified operand bytes as call instructions

The second issue is subtle: any instruction with a ULEB128 operand containing 0x10 would be treated as a call. For example, `br 16` emits `[0x0c, 0x10]` and the 0x10 gets picked up as a call opcode.

## Solution
Track call instruction positions in `FuncBodyBuilder.callSites[]` during emission, then use those known positions for import scanning and renumbering. Process sites in reverse offset order for safe in-place byte splicing.

## Key Insight
Never scan raw bytecode for opcodes by value matching — you'll hit false positives in operand bytes. Track instruction boundaries at emit time when you know the structure.

## Impact
- 33% binary size reduction (fib: 1124 → 747 bytes)
- 9 tests fixed
