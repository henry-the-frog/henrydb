# SAT/SMT Solver — From Scratch in JavaScript

A complete SAT/SMT solver built from scratch. CDCL SAT core with DPLL(T) SMT architecture, EUF theory solver (congruence closure), and Simplex for linear integer arithmetic. Zero dependencies.

## Architecture

```
┌──────────────────────────────────────────────────┐
│                DPLL(T) SMT Framework              │
│  ┌────────────────────────────────────────────┐  │
│  │              CDCL SAT Core                  │  │
│  │  Trail · 2-WL · 1UIP · VSIDS · Luby        │  │
│  │  LBD scoring · Subsumption · Probing        │  │
│  └────────────────────────────────────────────┘  │
│  ┌──────────────┐  ┌──────────────────────────┐  │
│  │  EUF Solver   │  │  Simplex (LIA)           │  │
│  │  Union-Find   │  │  Tableau · Bland's rule  │  │
│  │  Congruence   │  │  Slack vars · Backtrack   │  │
│  └──────────────┘  └──────────────────────────┘  │
├──────────────────────────────────────────────────┤
│              Problem Encoders                     │
│  Pigeonhole · N-Queens · Graph Coloring           │
│  Sudoku · Random 3-SAT · DIMACS Parser            │
└──────────────────────────────────────────────────┘
```

## Features

### CDCL SAT Solver
- **2-Watched Literals** — O(1) per propagation step
- **1UIP Conflict Analysis** — MiniSat-style single backward walk
- **Non-Chronological Backjumping** — skips irrelevant decision levels
- **VSIDS Decision Heuristic** — geometric activity decay
- **Phase Saving** — remembers last assignment polarity
- **Clause Minimization** — self-subsuming resolution on learned clauses
- **Luby Restarts** — optimal universal restart sequence
- **LBD Scoring** — Literal Block Distance for clause quality (glue clauses protected)
- **Subsumption Preprocessing** — removes subsumed clauses before search
- **Failed Literal Probing** — detect forced assignments via trial propagation

### SMT (DPLL(T))
- **EUF** — Equality + Uninterpreted Functions via backtrackable union-find + congruence closure
- **LIA** — Linear Integer Arithmetic via full Simplex (tableau, Bland's anti-cycling rule, slack variables)
- **S-expression parser** — `(and (= (f a) (f b)) (not (= a b)))` syntax
- **Checkpoint/Restore** — full backtracking support for theory solvers

## Quick Start

```javascript
const { Solver, createSolver, encodeNQueens, encodeSudoku } = require('./src/solver.cjs');

// Direct SAT API
const s = new Solver(3);
s.addClause([1, 2, 3]);
s.addClause([-1, 2]);
s.addClause([-2, 3]);
console.log(s.solve());      // 'SAT'
console.log(s.getModel());   // { 1: true, 2: true, 3: true }

// N-Queens
const queens = encodeNQueens(8);
const qs = createSolver(queens);
qs.solve();  // 'SAT' — finds a valid placement

// Sudoku
const grid = [
  [5,3,0,0,7,0,0,0,0],
  [6,0,0,1,9,5,0,0,0],
  // ... (0 = empty)
];
const sudoku = encodeSudoku(grid);
const ss = createSolver(sudoku);
ss.solve();  // 'SAT' — solves the puzzle
```

```javascript
const { SMTSolver } = require('./src/smt.cjs');

// SMT: EUF (congruence closure)
const smt = new SMTSolver();
smt.solve('(and (= a b) (= (f a) c) (not (= (f b) c)))');
// → 'UNSAT' (f(a)=c and a=b implies f(b)=c)

// SMT: LIA (linear arithmetic)
const { Simplex } = require('./src/simplex.cjs');
const simplex = new Simplex();
simplex.addVar('x'); simplex.addVar('y');
simplex.addConstraint([{var:'x',coeff:2},{var:'y',coeff:3}], '<=', 10);
simplex.addConstraint([{var:'x',coeff:1},{var:'y',coeff:1}], '>=', 5);
simplex.check();  // { feasible: true }
```

## Problem Encoders

| Encoder | Description | Example |
|---------|-------------|---------|
| `encodePigeonhole(n)` | n+1 pigeons → n holes (UNSAT) | Proof by conflict |
| `encodeNQueens(n)` | n non-attacking queens | 8-Queens in 15ms |
| `encodeGraphColoring(V, E, k)` | k-colorability | Petersen 3-coloring |
| `encodeSudoku(grid)` | 9×9 Sudoku (0=empty) | 729 vars, 3240 clauses |
| `randomSAT(v, c, k)` | Random k-SAT | Phase transition at 4.27 |
| `parseDIMACS(text)` | Standard DIMACS format | Competition files |

## Interactive CLI

```bash
node cli.cjs
```

Modes: `sudoku`, `queens <n>`, `pigeonhole <n>`, `random <vars> <clauses>`, `smt`

## Tests

```bash
node --test src/*.test.cjs
```

102 tests covering:
- SAT: unit propagation, backjumping, pigeonhole UNSAT proofs, N-Queens (2-12), graph coloring (K3, K4, Petersen), Sudoku, random 3-SAT at phase transition, CDCL learning behavior, edge cases
- SMT/EUF: congruence closure, transitivity, function equality, mixed satisfiability
- Simplex/LIA: feasible/infeasible systems, multi-variable, tight constraints, negative coefficients, mixed ≤/≥

## Key Insight: The 1UIP Literal Negation Bug

During development, the 1UIP conflict analysis had a subtle bug: lower-level literals were being *negated* before adding to the learned clause. This produced unsound learned clauses — not logically implied by the original formula. The solver returned UNSAT on satisfiable Sudoku puzzles. Small instances were unaffected because VSIDS found solutions before bad clauses accumulated.

## Stats

```
~1720 lines across 3 source files + 3 test files
102 tests | 0 failures
5 problem encoders | Interactive CLI | DIMACS parser
```
