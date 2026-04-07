# Query Compilation & Push-Based Execution

uses: 1
created: 2026-04-07
tags: database, query-compilation, hyper, umbra, parallelism

## HyPer's Push-Based Model (Neumann, 2011)

### Volcano (Pull) vs Push
- **Pull (Volcano)**: Each operator has `next()`, callee pulls from children → virtual function call per tuple, poor cache
- **Push**: Data pushed from leaves to root → compiled code keeps data in CPU registers, eliminates virtual dispatch

### produce()/consume() Pattern
```
Operator::produce() — generate data
Operator::consume(tuple, source) — process incoming data
```

Code generation: walk plan tree, emit code per pipeline. Pipeline = sequence of ops with no materialization
(pipeline breaker = hash join build, sort, group by hash table build).

### Key Insight: Data-Centric Code Generation
Instead of operator-at-a-time or tuple-at-a-time, generate code that processes one tuple through the ENTIRE pipeline before moving to the next. Data stays in registers.

## Morsel-Driven Parallelism (Leis et al., 2014)

### Concept
- Input split into small fixed-size "morsels" (~10K tuples)
- Worker threads pull morsels from dispatcher
- Thread processes entire pipeline for one morsel
- Work-stealing for load balancing

### NUMA Awareness
- Dispatcher assigns morsels to threads on the same NUMA node as the data
- Hash tables partitioned by NUMA node
- >95% local memory access in benchmarks

### Elasticity
- Degree of parallelism changes during query execution
- New queries can steal threads from long-running ones
- Near-perfect speedup up to 32+ cores

## Application to HenryDB

HenryDB has a Volcano-style iterator (volcano.js) and a compiler (compiler.js).
Could evolve toward push-based:
1. **Pipeline identification**: Identify pipeline breakers in query plan
2. **Code generation per pipeline**: Instead of iterator calls, generate a tight loop
3. **Register allocation**: Keep tuple columns in local variables
4. **In JS**: Use Function() constructor or template-based code generation

The compiler.js already does some of this with `compileToFunction()` — compiles predicates
to JS functions. Could extend to full pipeline compilation.

## Key Papers
- Neumann 2011: "Efficiently Compiling Efficient Query Plans for Modern Hardware"
- Leis et al. 2014: "Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework"
- Kersten et al. 2018: "Everything You Always Wanted to Know About Compiled and Vectorized Query Engines"
