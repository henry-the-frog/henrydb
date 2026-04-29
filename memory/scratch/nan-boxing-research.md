# NaN-Boxing Research for Monkey-Lang
uses: 1
created: 2026-04-28

## What is NaN-Boxing?

IEEE 754 doubles use 64 bits: 1 sign, 11 exponent, 52 mantissa.
A NaN is any value with all 11 exponent bits set and non-zero mantissa.
That gives ~2^53 unused bit patterns — enough to encode pointers + type tags.

## Two Approaches

### Pointer-favoring (JSC/WebKit)
- Raw pointers stored directly (no masking needed)
- Doubles need adjustment (add/subtract offset)
- Better when pointer ops dominate

### Double-favoring / "nun-boxing" (SpiderMonkey)
- Raw doubles stored directly (no masking needed)
- Pointers stored in NaN space, need unmasking
- Better when float ops dominate

## LuaJIT's Approach
- Upper 13 bits = `0xFFF8` for non-number types
- Lower 47 bits = pointer to GC object
- Primitive types (nil, false, true) have special tags
- Optional `LJ_DUALNUM`: lower 32 bits = integer, upper 32 = itype tag

## For Monkey-Lang (JavaScript host)

### Key Insight: JS Already NaN-Boxes!
V8 uses tagged pointers internally but our Monkey objects are JS objects on the V8 heap.
We can't do true NaN-boxing at the metal level in JS — we'd be NaN-boxing inside V8's own representation.

### What We CAN Do: Tagged Union / Flat Value Encoding
Instead of `new MonkeyInteger(5)` (heap allocation), use a flat encoding:

**Option A: TypedArray + Tag Array**
```js
const values = new Float64Array(STACK_SIZE);  // all values as doubles
const tags = new Uint8Array(STACK_SIZE);       // type tag per slot
// Integer: values[i] = 5.0, tags[i] = TAG_INT
// Float:   values[i] = 3.14, tags[i] = TAG_FLOAT  
// Boolean: values[i] = 1.0, tags[i] = TAG_BOOL
// Pointer: values[i] = heapIndex, tags[i] = TAG_OBJ
```

**Option B: SMI-like Approach**
- Small integers as raw JS numbers (no wrapping)
- Only heap-allocate for strings, arrays, hashes, closures
- Type check: `typeof val === 'number'` for the fast path

**Option C: Actual NaN-Boxing in Float64**
```js
// Encode: set high bits of a Float64 to carry type tag + payload
const QNAN    = 0x7FF8000000000000n;
const TAG_INT = 0x0001000000000000n;  // int payload in lower 32 bits
const TAG_STR = 0x0002000000000000n;  // string index in lower 32 bits
// Pack: view.setFloat64(0, Number(QNAN | TAG_INT | BigInt(intValue)))
// Unpack: check high bits, extract low 32
```
This is real NaN-boxing! But BigInt conversions in JS are slow.

### Recommended: Option B (SMI-like) for Interpreter
Minimal changes, biggest bang for buck:
1. Integers < 2^31 stored as raw JS numbers on the stack
2. Booleans as `true`/`false` JS primitives  
3. Null as JS `null`
4. Only strings/arrays/hashes/closures use object wrappers
5. Type dispatch: `typeof` + `instanceof` for the rare heap cases

**Impact on hot path:**
- `executeBinaryIntegerOperation`: no `left.value`/`right.value` — just `left + right`
- `executeComparison`: no `.value` access
- `cachedInteger`: eliminated entirely
- `instanceof MonkeyInteger`: becomes `typeof x === 'number'`

**Risk:** Every consumer of stack values needs to handle both raw numbers and objects.
This is a significant refactor touching vm.js, evaluator.js, builtins, and tests.

### For WASM Compiler: Already Unboxed!
The WASM compiler already operates on raw i32/f64 values — it's effectively doing the
optimal thing. NaN-boxing only matters for the tree-walking evaluator and bytecode VM.

## Implementation Estimate
- ~300-500 lines changed in vm.js
- ~100 lines in evaluator.js  
- ~50 lines in object.js (keep classes for heap types, add type predicates)
- Most tests should pass unchanged if inspect() works on raw values
- Biggest risk: hash keys and equality checks that assume `.value` access
