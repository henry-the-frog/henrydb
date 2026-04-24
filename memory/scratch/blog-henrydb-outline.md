# Blog Post Outline: Building a SQL Database in JavaScript

## Title Options
- "HenryDB: A Complete SQL Database in 30K Lines of JavaScript"
- "98% SQLite Compatible — How We Built a JS Database From Scratch"
- "4,288 Tests Later: What I Learned Building a Database Engine"

## Hook
"What if you could implement every major database feature — from B+Trees to MVCC, from window functions to vectorized execution — in pure JavaScript, with zero dependencies?"

## Key Stats (attention-grabbers)
- 4,288 tests, all passing
- 98% SQLite SQL compatibility (47/47 common features)
- 5 execution engines (Volcano, Pipeline JIT, Vectorized, Vec Codegen, Query VM)
- 5 concurrency control schemes (2PL, MVCC, SSI, OCC, Timestamp Ordering)
- 12+ join algorithms, 10+ index types
- Performance: 14K inserts/s, 9.7K lookups/s, 500K scan rows/s

## Sections

### 1. Why Build a Database in JavaScript?
- Learning exercise → became a comprehensive implementation
- Pure JS means no FFI, no native dependencies
- Surprisingly capable for in-memory workloads

### 2. Architecture Overview
- Parser → Planner → Optimizer → Executor → Storage
- MVCC with SSI for true serializable isolation
- B+Tree indexes with composite key support

### 3. The Hardest Bugs
- **Equi-join key swap**: When ON clause column order didn't match table order, all qualified-name joins produced cross joins
- **evalExpr default: true**: Any unrecognized expression type returned true, making CASE WHEN NULL always match
- **Float division**: JS's `parseFloat("10.0") === 10` loses float literal type info

### 4. Feature Combinations Are Where Bugs Hide
- 5/7 critical bugs were "path not handled" — individual features worked fine, combinations failed
- View-table JOINs: view handler returned early, never processed JOINs
- CTE INSERT: parser only allowed WITH...SELECT, not WITH...INSERT
- The lesson: test combinations, not just features

### 5. Vectorized Execution in JavaScript
- DuckDB-style batch processing: VectorBatch → VSeqScan → VFilter → VHashAggregate
- 1.3-1.7x speedup on aggregations (modest in JS, huge in C/C++)
- Why it works: amortizing per-row overhead, reducing function call count

### 6. What I'd Do Differently
- Start with case-insensitive identifiers from day 1
- Build a fuzzer early (differential testing against SQLite)
- Column naming consistency matters more than you'd think

### 7. Conclusion
- Database internals aren't magic — they're careful engineering
- JS is surprisingly good for this (fast enough, great tooling)
- The real learning is in the edge cases

## Potential Code Examples
- The B+Tree implementation
- MVCC snapshot isolation
- The join key swap bug fix
- Window function evaluation

## Target: ~2000 words, 5-7 code snippets
