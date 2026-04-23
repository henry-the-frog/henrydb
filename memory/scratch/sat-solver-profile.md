# SAT Solver Performance Profile (2026-04-22)

## Architecture
- CDCL with VSIDS decision heuristic
- 2-Watched Literals (MiniSat-style)
- 1UIP conflict analysis with geometric decay
- Luby restart schedule
- JS implementation: ~666K propagations/sec

## Benchmarks

| Problem | Variables | Clauses | Time | Notes |
|---------|-----------|---------|------|-------|
| 4-Queens | 16 | 80 | <1ms | |
| 8-Queens | 64 | 736 | <1ms | |
| 12-Queens | 144 | 2608 | 1ms | |
| 16-Queens | 256 | 6336 | 23ms | |
| 20-Queens | 400 | ~12K | 12ms | |
| 30-Queens | 900 | ~50K | 82ms | |
| Random 3-SAT n=50 | 50 | 214 | 3ms | Below phase transition |
| Random 3-SAT n=100 | 100 | 427 | 3ms | |
| Random 3-SAT n=200 | 200 | 854 | 3.3s | **Phase transition!** |
| Pigeonhole(5,4) | 20 | 30 | <1ms | UNSAT |
| Pigeonhole(8,7) | 56 | 168 | 119ms | UNSAT, exponential |
| World's Hardest Sudoku | 729 | 3261 | 4.8s | 70K conflicts, 3.17M props |

## Performance vs MiniSat
Expected ~50x gap (JS vs C). MiniSat would solve Sudoku in ~100ms.
Micro-optimizations (TypedArrays, less allocation) could help but won't close the gap.

## Key Insight: Phase Transition
Random 3-SAT at ratio 4.27 (clauses/variables) is the known phase transition point.
n=100 is trivially easy but n=200 takes 1000x longer. This is a fundamental combinatorial explosion.
