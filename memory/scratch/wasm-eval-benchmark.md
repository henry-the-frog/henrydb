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

## Compilation Time Breakdown (fib program)
| Phase | Time | Notes |
|-------|------|-------|
| Parse | 3.2ms | Lexer + Parser |
| AST → WASM IR | 39.1ms | Runtime function setup dominates |
| Encode bytes | 2.0ms | WasmModuleBuilder.build() |
| V8 compile | 41.5ms | WebAssembly.Module() |
| Instantiate | 3.0ms | WebAssembly.Instance() |
| Execute | 1.5ms | fib(25) computation |
| **Total** | **86.4ms** | |

## Optimization Opportunities
1. **Module caching**: Hash source code, cache compiled Module. 2nd run: 4.5ms.
2. **Lazy runtime functions**: Only emit runtime functions actually used. Would cut AST→IR time by ~50%.
3. **Streaming compilation**: Use WebAssembly.compileStreaming() if available.
4. **JIT threshold**: Only compile to WASM if program is likely CPU-bound.

## Three-Way Benchmark (Eval vs JIT vs WASM)

| Backend | fib(25) time | Speedup vs Eval |
|---------|-------------|-----------------|
| Evaluator | 3103ms | 1x |
| JIT (tracing) | 102ms | 30x |
| WASM | 87ms | 36x |
| WASM (exec only) | 1.4ms | 2216x |

Key insight: JIT and WASM are nearly equivalent for total time (~100ms).
The JIT compiles to JavaScript → V8 JIT → native, while WASM goes direct to native.
For one-shot execution, they're within ~15% of each other.

The right approach: auto-select backend based on program complexity:
- < 1ms eval time → use evaluator (zero compile overhead)
- 1ms-100ms eval time → use JIT (fast compile, decent execution)
- > 100ms eval time → use WASM (slower compile, fastest execution)

## Architecture Notes (monkey-lang execution backends)
Three execution backends with different tradeoffs:
1. **Evaluator**: Tree-walking interpreter. Zero compile time, slowest execution (~3000x slower than WASM exec).
2. **JIT (tracing)**: Records hot loops, compiles to JavaScript → V8 JIT → native. ~100ms startup. Best for simple loops, HOFs, string ops (leverages V8 optimizations).
3. **WASM compiler**: Compiles to WebAssembly → native. ~85ms startup. Best for deep recursion. Has type-guided optimization (knownInt) with 23 optimization points.

Other subsystems: bytecode compiler + VM, type inference (Hindley-Milner), LSP, formatter, linter, optimizer, GC, profiler, debugger, REPL, package manager.

Type inference integration: WASM compiler uses `knownInt` flag to skip tagging/untagging for integer-only operations. This optimization is pervasive (23 code points).
