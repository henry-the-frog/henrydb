# Superinstruction Analysis for Monkey-Lang VM
uses: 1
created: 2026-04-28

## Profile Summary (fib(30))
- Total: ~2.7M function calls, 915ms
- Loop overhead: 360ns/iteration (instruction decode + switch dispatch)
- Call overhead: 169ns/call (frame push/pop)
- Bottleneck: interpreter dispatch loop, not allocation (after unboxing)

## Hot Opcode Sequences

### fibonacci(n)
```
0: OpConstant <2>
1: OpGetLocal <n>     ← compare n < 2
2: OpGreaterThan
3: OpJumpNotTruthy
...
6: OpCurrentClosure   ← recursive call fib(n-1)
7: OpConstant <1>
8: OpSub
9: OpCall <1>
10: OpCurrentClosure  ← recursive call fib(n-2)
11: OpConstant <2>
12: OpSub
13: OpCall <1>
14: OpAdd             ← combine results
15: OpReturnValue
```

Dynamic frequency per fib(25) call:
- `CurrentClosure → Constant → Sub → Call`: 2x per call = ~5.4M dispatches
- `Constant → GetLocal → GT → JumpNT`: 1x per call = ~2.7M dispatches
- `Call → Add → ReturnValue`: 1x per call = ~2.7M dispatches

### Tight loop (sum += i)
```
16: OpConstant <limit> 
19: OpGetGlobal <i>    ← loop condition
22: OpGreaterThan
23: OpJumpNotTruthy
26: OpGetGlobal <sum>  ← body: sum = sum + i
29: OpGetGlobal <i>
32: OpAdd
33: OpSetGlobal <sum>
36: OpGetGlobal <i>    ← update: i = i + 1
39: OpConstant <1>
42: OpAdd
43: OpSetGlobal <i>
46: OpJump <16>        ← back to loop start
```

## Superinstruction Candidates (ranked by expected impact)

### 1. OpAddSetLocal / OpAddSetGlobal
Combines: OpAdd + OpSetLocal/OpSetGlobal
Pattern: `a + b` → store result immediately
Saves: 1 dispatch per iteration in tight loops
Expected: Very common (every `set x = x + op`)

### 2. OpGetLocalConst / OpGetGlobalConst 
Combines: OpGetLocal/Global + OpConstant
Pattern: Load variable and constant for comparison or arithmetic
Saves: 1 dispatch
Expected: Very common (loop conditions, increment by 1)

### 3. OpIncrementLocal / OpIncrementGlobal
Combines: OpGetLocal + OpConstant(1) + OpAdd + OpSetLocal
Pattern: `set i = i + 1` in one instruction
Saves: 3 dispatches per loop iteration!
Expected: Every for-loop uses this pattern

### 4. OpSelfCall
Combines: OpCurrentClosure + OpCall
Pattern: Direct recursive call
Saves: 1 dispatch per recursive call
Expected: Common in recursive algorithms

## Implementation Notes
- Superinstructions are emitted by the compiler as a peephole optimization
- Need new opcode values in code.js
- VM switch/case handles them as combined operations
- Backward compatible: compiler can emit either form
- Risk: code bloat in the switch statement, V8 might not optimize a huge switch as well
