# MEMORY.md — Long-Term Memory

## Projects

### HenryDB
A comprehensive database system implemented in JavaScript. Not just a toy DB — it's a CS textbook in code.
- **369 source files, 873 test files, ~8172 tests, ALL PASSING**
- **207K+ lines of code**
- **5 execution engines**: Volcano, Pipeline JIT, Vectorized, Vec Codegen, Query VM
- **5 database paradigms**: Relational, Document, FTS, Vector, Time-series
- **5 concurrency control schemes**: 2PL, MVCC, SSI, OCC, Timestamp Ordering
- **3 storage layouts**: Heap, Columnar, Clustered B+tree
- **12+ join algorithms**, 10+ index types, 8 hash table variants, 8 cache policies
- Window functions, CTEs, triggers, NATURAL JOIN, UNIQUE constraints
- DATE/TIME/DATETIME modifiers (SQLite-compatible: +N days, start of month, etc.)
- REGEXP/RLIKE operator support
- TRUE/FALSE as proper boolean literals in SELECT
- LIMIT with subquery evaluation
- GENERATED columns (GENERATED ALWAYS AS expr STORED)
- Unified selectivity estimator (shared between classic and Volcano planners)
- Performance: 14K inserts/s, 9.7K lookups/s, 500K scan rows/s on 10K rows
- Blog post written: `projects/henrydb/blog/building-henrydb.md`

### Monkey-lang
A programming language implementation with lexer, parser, tree-walking evaluator, and bytecode VM.
- **38 test files, 894 tests**, all passing
- **CI added** (GitHub Actions, Node 20+22) — needs workflow scope on token for push
- Hindley-Milner type inference (Algorithm W)
- SSA, constant propagation, dead code elimination, escape analysis
- Escape analysis exists but results are unused by compiler (documented plan for stack-allocated closures)
- README corrected: removed false WASM/RISC-V backend claims
- Evaluator/VM parity verified (9/9 test cases match)

### Neural-net
A comprehensive deep learning library spanning 42 years of research (Hopfield 1982 → KAN 2024).
- **168 source modules, 192 test files, 2323+ tests, ~27K LOC**
- **CI GREEN** on GitHub Actions (Node 22)
- **166/166 library modules import successfully** (fixed llama.js re-exports Apr 24)
- **0 dependencies** — everything from scratch
- 40+ architectures: Transformer, MoE, GAN, VAE, DQN, PPO, MAML, GNN, Diffusion, KAN, Mamba, LoRA, DARTS...
- 10+ optimizers: SGD, Adam, AdamW, RMSProp, KFAC + 10+ LR schedulers
- MoE: proper softmax Jacobian gate gradient, batch gradient accumulation (fixed Apr 24)
- Full LLM pipeline: tokenizer → embedding → transformer → KV cache → sampling → training
- RLHF stack: PPO, DPO, reward model, Constitutional AI
- ~1 flaky test per run (~33% chance) due to random initialization

## Key Learnings (Apr 25, 2026)

### Silent Data Loss: The Scariest Bug
- HenryDB disk-manager.js had PAGE_SIZE=4096, while page.js had PAGE_SIZE=32768
- Rows >4076 bytes silently vanished — INSERT succeeded but SELECT returned no rows
- Root cause: `insertTuple` returned -1, code continued with negative slotIdx
- Fix: increased disk PAGE_SIZE to 32KB, added error check on insertTuple failure
- **Lesson**: Define constants in ONE place. Check return values. -1 is not a valid slot.
- **Pattern**: Same as Apr 24 "path not handled" — but this time it's DATA CORRUPTION

### Two-Source Constant Problem
- The DiskManager constructor had BOTH a default parameter value (4096) AND a constant (PAGE_SIZE)
- Even after changing PAGE_SIZE, the default parameter shadowed it
- **Lesson**: Don't double-define defaults. Use `param = CONSTANT`, not hardcoded values.

### Feature Combination Testing Gap
- VIEW + JOIN, CTE + INSERT, LIMIT + subquery, EXPLAIN classic vs Volcano — all individual features worked
- The combinations failed silently
- **Lesson**: Test the cross-product of composable features. Individual feature tests guarantee nothing about combinations.

## Key Learnings (Apr 24, 2026)

### MoE Gradient Computation
- Shared experts across batch samples require external gradient accumulators (Dense.backward replaces, not accumulates)
- Gate gradient needs proper softmax Jacobian: `dL/ds_e = Σ_j dL/dy_j * Σ_k expertOut_k_j * w_k * (δ_{ke} - w_e)`
- See `memory/scratch/moe-gradient-learning.md`

### HenryDB Bug Pattern: "Path Not Handled"
- 5/7 bugs were missing feature combinations: view path skipped joins, WITH didn't handle INSERT, NATURAL not in keywords, triggers didn't resolve NEW/OLD, UNIQUE not creating indexes
- The pattern: individual features work fine, combinations fail silently
- See `memory/scratch/henrydb-bug-fixes-apr24.md`

### JS Float Type Loss
- `parseFloat("10.0") === 10` and `Number.isInteger(10) === true`
- Must explicitly preserve `isFloat` flag through tokenizer → parser → evaluator

### Expression Evaluator Safety
- `default: return true` in switch statements is dangerous — makes all unrecognized types truthy
- Better: `default: evaluate as value and check truthiness`

## Key Decisions (Apr 24, 2026)
- Neural-net CI: Node 22 only (18/20 have flaky random-init tests)
- Vectorized engine: opt-in only (auto-enable causes 21 failures from column naming compat)
- HenryDB extraction: DONE (db.js is core dispatch/txn/eval)

## Reference Documents
- `memory/scratch/henrydb-architecture.md` — comprehensive architecture reference
- `memory/scratch/neural-net-architecture.md` — comprehensive ML library reference
- `memory/scratch/vectorized-execution.md` — vectorized batch engine design
- `memory/scratch/moe-gradient-learning.md` — MoE gradient computation
- `memory/scratch/henrydb-bug-fixes-apr24.md` — 7 bugs found+fixed
- `memory/scratch/henrydb-string-truncation-fix.md` — PAGE_SIZE data corruption fix (Apr 25)
- `memory/scratch/henrydb-optimizer-gaps.md` — 4 optimizer gaps identified (Apr 25)
- `memory/scratch/monkey-escape-analysis-research.md` — escape analysis → stack closures plan
- `memory/scratch/monkey-vm-stress-test.md` — VM stress test results
- `memory/scratch/INDEX.md` — full scratch note index
