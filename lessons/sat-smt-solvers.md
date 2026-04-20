# SAT/SMT Solvers — Lessons

_Promoted from scratch/cdcl-sat-solver.md (uses: 3, Apr 6→16)_

## CDCL Critical Insight: Literal Negation in Learned Clauses
When adding lower-level literals to a learned clause during 1UIP conflict analysis:
- **WRONG:** negate the literal from the reason clause
- **CORRECT:** add literal as-is — it's already FALSE at the conflict point

This bug is insidious: small problems still solve correctly. Manifests on harder problems (729-var Sudoku) where many unsound learned clauses interact. The result is incorrect UNSAT on satisfiable problems.

## 2-Watched Literals
- `qhead` pattern: propagation processes trail from qhead forward
- When processing falsified literal, build `newWatchList` to avoid iterator invalidation
- Make falsified literal position 1, replacement search starts at position 2
- If position 0 literal is TRUE, clause is satisfied — keep watching

## VSIDS Variable Ordering
- Bump ALL variables in learned clause (not just UIP)
- `activityInc /= decay (0.95)` after each conflict → exponential growth
- Rescale when any activity exceeds 1e100
- Phase saving: prefer last polarity on next decision

## SMT Architecture (DPLL(T))
- Boolean abstraction: each theory atom → fresh boolean variable
- SAT solver works on boolean skeleton
- After assignment, theory solver checks consistency
- Theory conflict → learned clause from explanation

### EUF: Backtrackable Union-Find (no path compression — breaks undo), congruence closure
### Bounds Solver: Track lower/upper per variable, conflict when lower > upper
### Simplex: Tableau form with slack variables, Bland's rule for anti-cycling

## Bug Found (Apr 15)
SMT solver returns SAT for conflicting bounds (x > 10 AND x < 5) — bounds not propagated to Simplex check.

## Performance Benchmarks
- 8-Queens: 15ms, 22 conflicts
- Pigeonhole(6→5): ~100ms (exponential in size)
- Easy Sudoku: 15-20ms, ~50 conflicts
- Hard Sudoku: 15-20s, ~90K conflicts
