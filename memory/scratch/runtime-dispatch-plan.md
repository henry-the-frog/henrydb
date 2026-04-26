# Runtime Method Dispatch Implementation Notes

## Problem
`rex.speak()` → `speak(rex)` resolves to whatever `speak` is last defined in scope.
Two classes with same method name collide.

## Solution: OpMethodCall with 3-level fallback

### Step 1: Pass global name→index map to VM
In `Compiler.bytecode()`, export a `Map<string, number>` of global names → global indices.
Store in the Bytecode object alongside `instructions` and `constants`.

```javascript
bytecode() {
  return {
    instructions: this.currentInstructions(),
    constants: this.constants,
    globalNames: this._buildGlobalNameMap(), // NEW
  };
}
```

The VM constructor reads this and stores it as `this._globalNames`.

### Step 2: Store methods on instance hash in _compileClass
Inside the constructor factory function body, after init:
```
set self["speak"] = Dog__speak;  // bind mangled function reference
```
This requires the mangled function to be in scope inside the factory.
Since factory is compiled after methods, the mangled names are already in the symbol table.

### Step 3: OpMethodCall handler in VM
```
case Opcodes.OpMethodCall: {
  const nameIdx = readUint16(instructions, ip + 1);
  const numArgs = instructions[ip + 3];
  ip += 3;
  
  const methodName = constants[nameIdx].value;
  const obj = stack[sp - numArgs]; // first arg is obj
  
  // Level 1: Hash lookup
  let method = null;
  if (obj is ShapedHash) {
    const slot = obj.shape.getSlot(methodName);
    if (slot >= 0) method = obj.slots[slot];
  }
  
  // Level 2: Builtin lookup
  if (!method) {
    const idx = builtinNameMap.get(methodName);
    if (idx !== undefined) method = builtins[idx];
  }
  
  // Level 3: Global lookup
  if (!method) {
    const globalIdx = this._globalNames.get(methodName);
    if (globalIdx !== undefined) method = this.globals[globalIdx];
  }
  
  // Dispatch
  if (method is Closure) callClosure(method, numArgs);
  else if (method is MonkeyBuiltin) callBuiltin(method, numArgs);
  else throw new Error(`undefined method: ${methodName}`);
}
```

### Step 4: Compiler emits OpMethodCall for _isMethodCall
Already partially done. Just need to emit:
```
OpNull               // placeholder for function slot
<push obj arg>
<push other args>
OpMethodCall nameConstIdx numArgs
```

### Estimated Work: ~100 LOC
- Bytecode global name map: ~10 LOC
- VM constructor: ~5 LOC  
- OpMethodCall handler: ~40 LOC
- Store methods on hash: ~20 LOC
- Tests: ~30 LOC

### Edge Cases
- Builtin `len` stored on array → array["len"] doesn't exist → falls to builtin
- Class method `speak` stored on hash → found at level 1
- User function `helper` in globals → found at level 3
- Unknown method → error
