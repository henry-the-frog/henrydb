# CPython JIT Trace Fitness Model

uses: 1
created: 2026-04-07
tags: jit, tracing, cpython, optimization
source: https://github.com/python/cpython/issues/146073

## Concept: Trace Quality via Fitness + Exit Quality

Mark Shannon's proposal for CPython's JIT compiler (April 2026):

### Fitness (decreasing stamina)
- Starts at `MAX_TARGET_LENGTH * OPTIMIZER_EFFECTIVENESS` (~400 * ~2 = ~800)
- Decreased by:
  - Each bytecode instruction (small, e.g., 2)
  - Conditional branches (proportional to unpredictability)
  - Backward edges (large — prevents loop unrolling)
  - Function call depth (prevents deep inlining)
- Side trace fitness: `(8-chain_depth) * BASE / 8`

### Exit Quality (how good is this stop point?)
- **Best**: ENTER_EXECUTOR (another compiled trace available)
- **Normal**: Ordinary bytecode location
- **Poor**: Specializable instruction (would need re-specialization)

### Decision: `fitness < exit_quality` → stop tracing

### Key Insight: Last Known Good Exit
Track best exit seen. When forced to stop, revert to last good exit.
This improves unsupported/too-long trace handling.

## Invariant Constraints (Mark Shannon's approach)
Instead of tuning parameters empirically, define invariants:
1. Fitness at max frame depth < min(exit_quality) — prevents traces spanning too many frames
2. Fitness after backward edge + N instructions = exit_quality of start — allows loop entry but not unrolling
3. Fitness after ~3 balanced branches < exit_quality of "good" exit — limits trace branching

## Application to Monkey JIT
Our tracing JIT uses fixed `MAX_TRACE_LENGTH = 200`. Could replace with:
- Fitness model: more nuanced trace termination
- Exit quality: prefer stopping at loop headers
- Branch bias: our profiling data gives branch frequencies
- Could lead to better traces (stop at good points, not arbitrary length)
