# Monkey try/catch/throw/finally Implementation

## Architecture

### Opcodes
- `OpTry (0x3B)` — 2+2 byte operands: catchAddr, finallyAddr. Pushes handler onto handler stack.
- `OpThrow (0x3C)` — Pops value from stack, unwinds to nearest handler.
- `OpPopHandler (0x3D)` — Pops handler when try block completes normally.

### VM Handler Stack
Handler entries store: `{ frameIndex, catchAddr, finallyAddr, sp }`. On throw, the VM unwinds frames back to the handler's frame, restores SP, pushes the thrown value, and jumps to catchAddr.

**Critical:** The frame unwinding must update the local `frame` variable in the `run()` loop. Can't use `_handleThrow()` method because `frame` is a local var. The throw logic must be inline in the switch case.

### Evaluator
Uses `MonkeyThrown` wrapper (distinct from `MonkeyError`) to signal user-thrown values.

**Bug found:** Creating a new `Environment` for the catch block causes assignments to outer variables to create local shadows instead. Fixed by binding catch param directly in the existing env.

### Compiler Emit Pattern
```
OpTry [catchAddr] [finallyAddr]   ← placeholder 9999
  <try body>
OpPopHandler
OpJump [endAddr]                  ← placeholder 9999
[catchAddr]:
  OpSetLocal/OpPop (catch param or discard)
  <catch body>
[finallyAddr]:
  <finally body>
[endAddr]:
OpNull                            ← try/catch as expression produces null
```

## Key Insight
`isError()` expanded to catch `MonkeyThrown` enables automatic propagation through all existing evaluator code (function calls, block statements, etc.) without modifying every call site. MonkeyThrown propagates exactly like MonkeyError until caught by evalTryExpression.

## Bug: Finally-Only Propagation (Found Apr 15)
When `try { throw } finally { ... }` has no catch, the exception must propagate through the finally. Original design had a single catch/finally address in OpTry. Fix: compiler emits a separate "exception path" for finally-only blocks: `<finally code> + OpThrow` (re-throw after finally runs). The normal path (no exception) jumps directly to a second copy of finally code without the rethrow. This means finally code is duplicated in bytecode — acceptable tradeoff for correctness.

Created: 2026-04-15, uses: 0
