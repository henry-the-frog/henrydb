# WASM Closure Bugs — Deep Analysis (Apr 27, 2026)

uses: 1
created: 2026-04-27

## Bug 1: Self-Referencing Closures with Multiple Captures

**Symptom:** When a closure captures both a regular variable AND references itself by name, the self-reference is broken.

```monkey
let make = fn() {
  let x = 100;
  let f = fn(n) { if (n <= 0) { x } else { f(n - 1) } };
  f(3)
};
make()  // Returns 0 instead of 100
```

**Root cause:** In `compileLetStatement`, the variable is defined in scope BEFORE its value is compiled:
```js
this.currentScope.define(name, localIdx, ...);  // f is now in scope
// ... later:
this.compileNode(stmt.value);  // compileFunctionLiteral runs, captures f from scope
```

`_findCaptures` finds `f` in scope and includes it as a capture. But `f`'s local is 0 (uninitialized). The env stores 0 for `f`'s slot.

When the closure is called and tries to call `f(n-1)`, it reads `f` from env → 0 → null closure pointer → reads garbage table index from memory[4] → undefined behavior.

**Works alone:** Self-referencing closure with NO other captures works because... (investigation needed — possibly the compiler detects it's the only capture and uses a different path, or the specific memory layout happens to work by accident with the first table slot).

**Fix options:**
1. **Patch env after definition:** After `localSet(localIdx)` in `compileLetStatement`, if the value is a FunctionLiteral that captures itself, patch the env: `env[f_offset] = f_closure_ptr`
2. **Self-reference detection in _findCaptures:** Exclude the variable being defined from captures, and instead compile self-calls as direct calls to the WASM function index
3. **Box/cell pattern:** All captured mutable variables go through heap-allocated boxes

## Bug 2: Sibling Closures Don't Share Mutable State

**Symptom:** Two closures that capture the same variable see independent copies.

```monkey
let make = fn() {
  let x = 0;
  let inc = fn() { x = x + 1; x };
  let get = fn() { x };
  [inc, get]
};
let pair = make();
let inc = pair[0];
let get = pair[1];
inc();
get()  // Returns 0 instead of 1
```

**Root cause:** Each closure gets its own environment with a COPY of `x`. When `inc` mutates `x`, it writes back to ITS env only. `get` has a separate env with its own copy of `x=0`.

**Fix:** The proper solution is the **box/cell pattern**:
1. During analysis, identify variables that are captured by multiple closures (or captured and mutated)
2. Heap-allocate these variables as "boxes" (4-byte cells)
3. All closures and the outer scope reference the box pointer, not the value directly
4. Reads: dereference box pointer → value
5. Writes: store new value through box pointer

This is how most real closure implementations work (Python cells, Lua upvalues, V8 Context objects).

## Bug 3: Recursive Closure + Mutable State = Crash

**Symptom:** Recursive closure with a captured mutable variable crashes at runtime.

```monkey
let make = fn() {
  let result = 0;
  let loop = fn(i) { if (i > 0) { result = result + i; loop(i - 1) } else { result } };
  loop(5)
};
make()  // RuntimeError: table index is out of bounds
```

**Root cause:** Combination of Bug 1 (self-reference = 0 in env) and Bug 2 (mutable state isolation). When the compiler hangs, it's likely an infinite loop in analysis rather than a runtime crash.

Actually: compilation itself hangs (SIGKILL after ~10s). The compiler may be entering an infinite loop during type inference or some other analysis pass when a closure captures a mutable variable AND references itself.

## Priority

Bug 2 (shared mutable state) is the most architecturally significant — it requires the box/cell pattern.
Bug 1 (self-reference) could be fixed independently with env patching.
Both are related and the box/cell pattern would fix both.
