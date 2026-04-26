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

### Additional Refinements (from T71 THINK)

#### Method Storage Optimization
Instead of storing closures on the hash (memory overhead per instance), consider:
- **Prototype chain**: Store methods on a shared "class prototype" hash
- Instance hash has `__proto__` slot pointing to class prototype
- Lookup: instance slots → prototype slots → builtin → global
- This is how V8/SpiderMonkey do it (hidden class + prototype chain)

#### Compile-Time Optimization
The compiler already knows the class hierarchy. For monomorphic call sites:
- If static analysis can determine the receiver type → emit direct call
- Only fall back to OpMethodCall for polymorphic or unknown receivers
- This is basically inline caching (already have IC infrastructure in shape.js)

#### Priority Order for Implementation
1. **Phase 1**: Store methods on instance hash + OpMethodCall (simple, works, ~100 LOC)
2. **Phase 2**: Shared prototype (reduces memory, ~50 LOC additional)
3. **Phase 3**: Inline caching for method dispatch (performance, ~100 LOC)

Phase 1 is sufficient for correctness. Phase 2 matters if many instances exist. Phase 3 matters for hot loops calling methods.

### Risk Assessment
- **Low risk**: OpMethodCall is a new opcode, doesn't touch existing dispatch
- **Medium risk**: Storing methods on hash during class compilation — must not break existing instance creation
- **Testing**: Need tests for same-method-name different classes, inheritance override, builtin fallback
