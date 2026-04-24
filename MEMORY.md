# MEMORY.md — Long-Term Memory

## Projects

### HenryDB
A comprehensive database system implemented in JavaScript. Not just a toy DB — it's a CS textbook in code.
- **4288+ tests, ALL PASSING** (was 28 failures, fixed Apr 24)
- **98%+ SQLite SQL coverage** (47/47 common features tested)
- **5 execution engines**: Volcano, Pipeline JIT, Vectorized (new Apr 24), Vec Codegen, Query VM
- **5 database paradigms**: Relational, Document, FTS, Vector, Time-series
- **5 concurrency control schemes**: 2PL, MVCC, SSI, OCC, Timestamp Ordering
- **3 storage layouts**: Heap, Columnar, Clustered B+tree
- **12+ join algorithms**, 10+ index types, 8 hash table variants, 8 cache policies
- Window functions, CTEs, triggers with NEW/OLD, NATURAL JOIN, UNIQUE constraints
- DATE/TIME/DATETIME/JULIANDAY/UNIXEPOCH functions
- Performance: 14K inserts/s, 9.7K lookups/s, 500K scan rows/s on 10K rows
- Vectorized engine prototype: VectorBatch, VSeqScan, VFilter, VProject, VHashAggregate, VHashJoin (opt-in via {vectorized: true})

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
- `memory/scratch/INDEX.md` — full scratch note index
