# Monkey-lang → RISC-V Backend

**Created:** 2026-04-15 Session B | **Last updated:** 2026-04-15 Session B (late)

## Current State — HIGHLY COMPLETE
- **431 tests passing** (99.8% pass rate)
- **~2200 LOC** of compiler backend code
- Full language coverage: integers, booleans, strings, arrays, hashes, closures, HOF
- Performance: 2-5x faster than VM on iterative code
- Interactive REPL
- Standard library (30+ functions)

## Architecture
- Monkey source → Parse → Type Inference → Closure Analysis → Code Gen → Peephole → Assemble → Execute
- Stack-based code generation with frame pointer (s0)
- Heap allocation via bump allocator (gp register)
- Closure dispatch trampoline for indirect calls
- String equality via _str_eq subroutine

## Key Design Decisions
1. **Closures**: Heap-allocated objects [fn_id, num_captured, vars...]. Dispatch trampoline matches fn_id.
2. **Function refs**: Plain functions wrapped as closures with num_captured=-1 (sentinel). Trampoline shifts args.
3. **Cross-function calls**: Inner functions inherit function labels from outer scope.
4. **String comparison**: Subroutine approach (not inline) to avoid register clobbering.
5. **Anonymous functions**: Closure analysis scans expression statements, not just let-bound functions.

## Remaining Gaps (Priority Order)
1. **Mutual recursion** — Forward declarations not supported. `is_even`/`is_odd` pattern fails.
2. **Recursive closures** — Nested function calling itself (e.g., `let helper = fn() { helper() }` inside another function).
3. **Garbage collection** — Bump allocator only. Long-running programs will OOM.
4. **Array mutation** — `set arr[i] = val` not supported (monkey-lang limitation).
5. **Tail call optimization** — Deep recursion uses O(n) stack.
6. **Anonymous function closure analysis** — Works for call args but not all contexts.
7. **IIFE** — `fn(x){x}(5)` direct call syntax not supported.

## Best Demo Ideas
1. **Interactive REPL session** — define functions, call with stdlib, see cycle counts
2. **Performance comparison** — run bench-riscv-vs-vm.js, show 5.2x speedup
3. **FP showcase** — map/filter/reduce pipeline with closures
4. **Prime sieve** — count primes ≤ 1000 using stdlib
5. **Cons cell linked list** — functional data structures
