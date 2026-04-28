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

## Bug 2: Closures Don't Share Mutable State With Enclosing Scope

**Symptom 2a:** Two closures that capture the same variable see independent copies.

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

**Symptom 2b:** Outer scope can't read mutations made by inner closure.

```monkey
let f = fn() {
  let result = 0;
  let inner = fn(i) { result = result + i; result };
  inner(1); inner(2); inner(3);
  result    // Returns 0 instead of 6! Outer local not updated.
};
```

**Root cause:** Each closure gets its own env with a COPY of captured vars (at creation time). Mutations write back to the closure's OWN env, not the outer scope's local. The outer scope and sibling closures all reference different storage locations.

**Note:** Single closures that both read and mutate DO work (e.g., counter pattern). The bug only manifests when two entities (two closures, or a closure + its enclosing scope) both need to see the same variable.

**Fix:** The proper solution is the **box/cell pattern**:
1. During analysis, identify variables that are captured and mutated (or captured by multiple closures)
2. Heap-allocate these variables as "boxes" (4-byte cells)
3. All closures and the outer scope reference the box pointer, not the value directly
4. Reads: dereference box pointer → value
5. Writes: store new value through box pointer

This is how most real closure implementations work (Python cells, Lua upvalues, V8 Context objects).

## Bug 3: Self-Referencing Closures with Multiple Captures = Crash

**Symptom:** Recursive closure with other captured variables crashes at runtime.

```monkey
let make = fn() {
  let result = 0;
  let loop = fn(i) { if (i > 0) { result = result + i; loop(i - 1) } else { result } };
  loop(5)
};
make()  // RuntimeError: table index is out of bounds
```

**Root cause:** Combination of Bug 1 (self-reference = 0 in env) and Bug 2 (mutable state isolation). `loop` captures `[result, loop]` but at closure creation time, `loop`'s local is 0 (uninitialized — the let is being defined). The closure reads `loop` from env → 0 → invalid table index → crash.

**Note:** Compilation does NOT hang/OOM (earlier reports were misleading — the process stayed alive due to open timers from module imports, not infinite loops). Compilation takes ~9ms.

## Debunked: Compiler OOM Bug

The "compiler OOM" reported in Session B was a misdiagnosis. The WasmCompiler does NOT hang or OOM. The issue was `process.exit()` vs open timers from ESM module imports keeping the event loop alive. Adding `process.exit(0)` after compilation confirms it takes 9ms and produces correct output. This bug can be removed from TASKS.md.

## Priority and Fix Strategy

**Box/cell pattern** fixes ALL THREE bugs:
- Bug 1 (self-ref): box for `f` gets patched after closure creation
- Bug 2 (shared state): all references go through the same box pointer
- Bug 3 (recursive + mutable): combination of fixes 1 and 2

**Implementation sketch:**
1. Analysis pass: for each scope, identify "boxed" variables — those that are (captured AND mutated) OR (captured by multiple closures) OR (self-referencing)
2. At variable definition: `alloc(4)` → box_ptr, store initial value via `i32.store(box_ptr, value)`, store box_ptr in local
3. At variable read: `i32.load(local)` → box_ptr, `i32.load(box_ptr)` → value
4. At variable write: `i32.load(local)` → box_ptr, `i32.store(box_ptr, new_value)`
5. In closure capture: store box_ptr, not value. Inner scope reads/writes through same box.
6. After self-referencing let: patch `i32.store(box_ptr, closure_ptr)` 

**Alternative minimal fix for Bug 1 only:** After `let loop = fn(i){...}` where loop captures itself, emit `i32.store(env + loop_offset, loop_closure_ptr)` to patch the env. Quick win but doesn't fix shared state (Bug 2).

**Estimated effort:** Box/cell ~200 LOC changes, touches Scope, compileFunctionLiteral, compileLetStatement, compileAssignExpression, compileIdentifier.
