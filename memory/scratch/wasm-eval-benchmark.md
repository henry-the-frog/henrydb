# WASM vs Evaluator Performance (Apr 28, 2026)

## Results Summary

| Benchmark | Evaluator | WASM (total) | WASM (exec) | Total Speedup | Exec Speedup |
|-----------|-----------|--------------|-------------|---------------|--------------|
| fib(25)   | 3105ms    | 94ms         | 1.5ms       | 33x           | 2071x        |
| sum 1-1000| 61ms      | 72ms         | 0.06ms      | 1x            | 978x         |
| map+filter| 3ms       | 8ms          | 0.92ms      | 0.4x          | 3x           |
| 100 pushes| 3ms       | 22ms         | 0.15ms      | 0.1x          | 22x          |

## Analysis
- **CPU-bound recursion (fib)**: WASM shines — 2000x exec speedup, 33x total including compilation
- **Simple loops (sum)**: 978x exec speedup but compilation overhead makes total ~1x
- **HOFs (map/filter)**: Only 3x exec speedup — host import crossing dominates
- **Array push**: 22x exec speedup — moderate improvement

## Key Insight
WASM compilation takes ~70-90ms. This means:
- Programs running >100ms in evaluator benefit from WASM compilation
- Programs running <10ms should use tree-walking evaluator
- A hybrid approach: evaluate short programs directly, compile long-running ones

## Bottleneck: Host Import Crossing
HOFs like map/filter/reduce call JavaScript functions from WASM. Each call crosses the WASM-JS boundary, which is ~0.001ms per crossing. For 100 map iterations, that's 0.1ms of overhead — significant compared to the 0.92ms total execution.

The solution: compile HOF bodies inline as WASM loops instead of using host imports.
This would turn map/filter from ~3x to ~100x+ speedup.

## Recommendation
1. Add a `compileIfBeneficial(code)` function that estimates whether WASM compilation would help
2. Consider caching compiled modules for repeated execution
3. Long-term: inline HOF compilation to avoid host import overhead
