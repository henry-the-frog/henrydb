# CDCL SAT Solver — Implementation Notes
uses: 1
created: 2026-04-06
last-used: 2026-04-06
topics: sat, cdcl, watched-literals, 1uip, vsids, smt, euf, conflict-analysis

## The Literal Negation Bug (Critical Insight)

In 1UIP conflict analysis, when adding lower-level literals to the learned clause:
- **WRONG:** `learnt.push(-lit)` — negate the literal from the reason clause
- **CORRECT:** `learnt.push(lit)` — add the literal as-is

Why: these literals are already FALSE at the conflict point. The learned clause needs
all non-asserting literals to be FALSE after backtracking. They're already false, so
adding them as-is preserves this property.

This bug is subtle because:
1. Small problems still solve correctly (few conflicts, lucky decisions)
2. The bug causes *unsound* learned clauses (not entailed by original formula)
3. Unsound clauses lead to incorrect UNSAT on satisfiable problems
4. Manifests on harder problems (729-var Sudoku) where many learned clauses interact

## 2-Watched Literal Implementation Details

- `qhead` pattern: propagation processes trail from `qhead` index forward
- When falsified literal's watch list is processed, build `newWatchList` to avoid iterator invalidation
- Make falsified literal position 1 (not 0) before searching for replacement
- If first watched literal (position 0) is already TRUE, clause is satisfied — keep watching
- Replacement search starts at position 2 (skip both watched positions)

## VSIDS Tuning

- Bump ALL variables in learned clause (not just the UIP)
- activityInc /= decay (0.95) after each conflict → exponential growth
- Rescale when any activity exceeds 1e100 to prevent overflow
- Phase saving: remember polarity of last assignment, prefer it on next decision

## SMT Architecture (DPLL(T))

- Boolean abstraction: each theory atom gets a fresh boolean variable
- SAT solver works on boolean skeleton
- After SAT assignment, theory solver checks consistency
- Theory conflict → learned clause from theory explanation

### EUF (Equality + Uninterpreted Functions)
- Backtrackable Union-Find (no path compression — breaks undo)
- Congruence closure: f(a)=x, f(b)=y, a=b → x=y
- History stack for undo: push [node, oldParent, oldRank] before each union

### Bounds Solver (LIA subset)
- Track lower/upper bounds per variable
- Assert: x >= 5 tightens lower bound, x <= 10 tightens upper
- Conflict: lower > upper
- Negation: ~(x <= 5) → x >= 6 (integer domain)

## Performance Observations

- 8-Queens: 15ms, 22 conflicts, 38 decisions
- Pigeonhole(6→5): ~100ms, many conflicts (exponential in pigeonhole size)
- Easy Sudoku: 15-20ms, ~50 conflicts
- Hard Sudoku: 15-20s, ~90K conflicts (needs better preprocessing)

## Future Improvements

- Variable ordering: random initial perturbation reduces pathological cases
- Preprocessing: subsumption elimination, self-subsuming resolution
- Watched literal with blocking literal (further reduces cache misses)
- Luby restart sequence instead of geometric
- LBD (Literal Block Distance) for clause quality scoring
