# SAT Solver — CDCL from Scratch

A conflict-driven clause learning (CDCL) SAT solver built from scratch in JavaScript. Zero dependencies.

## Architecture

```
┌─────────────────────────────────────────────┐
│                  Solver                      │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  │
│  │  Trail    │  │  Watches │  │  VSIDS    │  │
│  │  (stack)  │  │  (2-WL)  │  │  (decide) │  │
│  └──────────┘  └──────────┘  └───────────┘  │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  │
│  │  Analyze │  │  Backjump│  │  Restarts  │  │
│  │  (1UIP)  │  │  (NCB)   │  │  (Luby)   │  │
│  └──────────┘  └──────────┘  └───────────┘  │
├─────────────────────────────────────────────┤
│              Problem Encoders                │
│  Pigeonhole · N-Queens · Graph Coloring      │
│  Sudoku · Random 3-SAT · DIMACS Parser       │
└─────────────────────────────────────────────┘
```

## Features

- **2-Watched Literals** — O(1) per propagation step via `qhead` pattern
- **1UIP Conflict Analysis** — MiniSat-style single backward walk
- **Non-Chronological Backjumping** — skips irrelevant decision levels
- **VSIDS Decision Heuristic** — geometric activity decay, recent conflicts prioritized
- **Phase Saving** — remembers polarity of last assignment
- **Clause Minimization** — self-subsuming resolution on learned clauses
- **Restarts** — geometric schedule with increasing intervals
- **Clause Database Reduction** — activity-based deletion of low-value learned clauses

## Quick Start

```javascript
const { Solver, createSolver, encodeNQueens, encodeSudoku } = require('./src/solver.cjs');

// Direct API
const s = new Solver(3);
s.addClause([1, 2, 3]);     // x1 ∨ x2 ∨ x3
s.addClause([-1, 2]);       // ¬x1 ∨ x2
s.addClause([-2, 3]);       // ¬x2 ∨ x3
console.log(s.solve());      // 'SAT'
console.log(s.getModel());   // { 1: true, 2: true, 3: true }

// N-Queens
const queens = encodeNQueens(8);
const qs = createSolver(queens);
qs.solve();  // 'SAT'
console.log(queens.decode(qs.getModel()));  // [[0,4],[1,6],...]

// Sudoku
const grid = [
  [5,3,0,0,7,0,0,0,0],
  [6,0,0,1,9,5,0,0,0],
  // ... (0 = empty)
];
const sudoku = encodeSudoku(grid);
const ss = createSolver(sudoku);
ss.solve();
console.log(sudoku.decode(ss.getModel()));  // solved 9x9 grid
```

## Problem Encoders

| Encoder | Description | Variables | Example |
|---------|-------------|-----------|---------|
| `encodePigeonhole(n)` | n+1 pigeons, n holes (always UNSAT) | n(n+1) | `encodePigeonhole(5)` |
| `encodeNQueens(n)` | Place n queens on n×n board | n² | `encodeNQueens(8)` |
| `encodeGraphColoring(nodes, edges, colors)` | K-colorability | nodes×colors | K3 with 3 colors |
| `encodeSudoku(grid)` | 9×9 Sudoku (0 = empty) | 729 | Any valid puzzle |
| `randomSAT(vars, clauses, len)` | Random k-SAT instance | vars | Phase transition at 4.27 |
| `parseDIMACS(text)` | DIMACS CNF format | from file | Standard SAT format |

## Tests

```
52 tests covering:
- Basic SAT/UNSAT detection
- Unit propagation cascades
- DIMACS parsing
- Pigeonhole principle (2-6 holes, all UNSAT)
- N-Queens (2-12, correct SAT/UNSAT)
- Graph coloring (K3, K4, K3,3, Petersen)
- Sudoku (easy + hard)
- Random 3-SAT (phase transition, overconstrained)
- CDCL behavior (learning, VSIDS, phase saving)
- Edge cases (tautologies, long clauses, components)
- Performance benchmarks (50-var, 100-var)
```

## Key Insight: The Literal Negation Bug

During development, the 1UIP conflict analysis had a subtle bug: lower-level literals from reason clauses were being *negated* before adding to the learned clause. This is wrong — these literals are already false at the conflict point and should be added as-is. The learned clause must have all non-asserting literals be false after backtracking, so they need to match their current (false) assignment.

This bug caused the solver to report UNSAT on satisfiable Sudoku puzzles — the learned clauses were not logically implied by the original clauses.

## Stats

```
52 tests | 1 source file | ~500 lines of core solver
```
