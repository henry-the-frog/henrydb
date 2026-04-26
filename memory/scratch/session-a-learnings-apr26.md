# Session A Learnings — 2026-04-26

## VM Callback Mechanism (callClosureSync)
- **Pattern:** Re-entrant VM execution via `_frameFloor`. The `run()` loop checks `this._frameFloor` instead of hardcoded `1` for return termination.
- **Key insight:** After `run()` returns, the frame is still on the frames stack. Must manually: read `stack[sp]`, set `framesIndex` back, restore `sp` to `basePointer - 1`.
- **Perf:** 4x map, 3.9x filter, 1.5x reduce speedup over recursive prelude HOFs.

## WASM Compiler
- **Simplicity:** WASM binary format is ~250 LOC to implement. The stack-based model maps directly from our bytecode VM.
- **LEB128:** Both signed and unsigned variants needed. Signed for `i32.const`, unsigned for everything else.
- **Two-pass:** First pass collects function signatures (needed for call resolution), second pass compiles bodies.
- **120x speedup:** fib(30) in 7ms (WASM) vs 892ms (bytecode VM). WASM JIT is dramatically faster.
- **if/else:** WASM uses `if_` opcode with block type annotation (`WASM_TYPE.I32` for i32 result). Must have matching `else_` branch.

## Class Compilation
- **Scope-level methods:** Methods compiled as let-bindings in enclosing scope. `rex.speak()` → `speak(rex)` via parseMethodCall. Simple and works.
- **Limitation:** No true polymorphism — method names are global. Two classes can't both have `speak` (last definition wins).
- **OpSetIndex:** New opcode (0x26) for property mutation. Had to handle ShapedHash with shape transitions (not just Map.set).
- **Opcode collision:** 0x1F was both OpMod and my first OpSetIndex. Always check the full opcode table!

## ShapedHash/Hidden Classes
- `OpHash 0` creates a ShapedHash with an empty shape, not a MonkeyHash.
- Adding properties requires `shape.transition(keyStr)` → new Shape, then expanding `slots` and `keys` arrays.
- `getByString(keyStr)` → `shape.getSlot(keyStr)` → slot index → `slots[slot]`.

## SQLite Type Affinity
- SQLite ordering: NULL < INTEGER/REAL < TEXT < BLOB.
- JS `<`/`>` operators don't respect this for mixed types (e.g., `42 < 'hello'` is `false` in JS, `true` in SQLite).
- `parseFloat('0') || '0'` returns `'0'` because `0` is falsy. Use `!isNaN(n) ? n : result`.

## SSA Builder Limitation
- `_renameExpr` returns toString() representations for complex expressions (if/while bodies).
- References inside these aren't renamed to SSA form (e.g., `a` not `a_0`).
- Workaround: extract base names from value strings and match against all SSA versions of that variable.

## DCE Integration
- AST-level DCE as compiler pre-pass conflicts with compiler's own inline constant folding.
- The compiler already handles `if(false)` → push null correctly. Running DCE before breaks ExpressionStatement expectations.
- DCE is better as a pipeline analysis tool, not a mandatory compiler step.
