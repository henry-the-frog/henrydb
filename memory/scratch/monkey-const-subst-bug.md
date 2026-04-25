# Monkey-lang Constant Substitution Bug — Lessons Learned (2026-04-25)

## The Bug
Constant substitution (`const-subst.js`) replaced variable references with their initial values,
even when the variable was later mutated via `set` statements. This caused **silent wrong answers**.

### Example
```monkey
let s = 0;
set s = s + 5;
s        // Expected: 5, Got: 0 (substituted with initial value)
```

### Impact
- **28 test failures** (out of 33 total) were caused by this single bug
- Every program using `set` in loops was affected
- Programs that only used `let` (no mutation) were fine

## Root Cause
`constantSubstitution()` scanned for `LetStatement` with literal values and added them to a `constants` map.
It then replaced all `Identifier` references with those constants. But it never checked for `SetStatement`
(mutation), so mutated variables were still treated as constants.

## The Fix
Added `removeMutated()` function that scans ALL control flow paths for `SetStatement` nodes:
- if/else blocks
- for loops (body + update)
- while loops
- do-while loops
- for-in loops

Any variable that appears as the target of a `set` statement is removed from the constants map.

## Systemic Pattern
**Any optimization that caches "known values" must be invalidated by writes.**

This is the fundamental soundness requirement for:
- Constant propagation (SSA)
- Common subexpression elimination
- Loop-invariant code motion
- Register allocation (liveness analysis)
- Copy propagation

The const-subst pass violated this by not tracking stores. The SSA-based constant propagation
(`const-prop.js`) handles this correctly because SSA form makes mutations explicit (φ nodes).

## Additional Bug Found
The bytecode optimizer's `rebuildBytecode()` had a jump target remapping bug: when DCE removed
instructions, jump targets pointing to removed code used stale offsets instead of finding the
nearest surviving instruction. This caused nested-if miscompilation.

## Additional Bug Found  
Constant substitution and folding ran unconditionally (regardless of `optimize` flag).
This broke debugger tests that needed exact opcode sequences. Gated const-subst behind flag.

## Key Lesson
> "An optimization pass that reads but doesn't check writes is a bug factory."
> Every pass must prove it respects the aliasing/mutation model.
