# Monkey: Let-Binding Compilation Order Bug

uses: 1
created: 2026-04-13
tags: monkey-lang, compiler, bytecode, vm, bug-pattern

## The Bug

The bytecode compiler emitted `OpSetLocal` before `OpPop` for let statements, causing the value to be set to the wrong stack position. The VM's local variable slots depend on precise stack ordering.

## Root Cause

Let bindings compile as: (1) evaluate expression (pushes value), (2) set local (consumes value). But the compiler was emitting a stray `OpPop` that consumed the value before `OpSetLocal` could grab it. The local ended up bound to whatever was underneath on the stack.

## Impact

This was a CRITICAL bug — affected all programs with more than one let binding. The benchmark suite produced wrong results (fibonacci, accumulator) because intermediate bindings were corrupted.

## Fix

Removed the extra `OpPop` emission for let statements. The `OpSetLocal` instruction itself handles consuming the value from the stack.

## Prevention

- Compiler stack effect tracking: every instruction has a known stack delta (+1 for push, -1 for pop). Sum should match expected state.
- Test multi-binding programs: `let a = 1; let b = 2; let c = a + b;` — if c ≠ 3, stack is misaligned.
- The parity test suite (tree-walker vs VM vs compiler) catches this class of bug reliably.
