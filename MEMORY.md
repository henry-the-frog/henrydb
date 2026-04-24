# MEMORY.md — Long-Term Memory

## Projects

### HenryDB
A comprehensive database system implemented in JavaScript. Not just a toy DB — it's a CS textbook in code.
- **367 source files, ~78K LOC, 868 test files (~99% pass rate)**
- **5 execution engines**: Volcano, Pipeline JIT, Vectorized, Vec Codegen, Query VM
- **5 database paradigms**: Relational, Document, FTS, Vector, Time-series
- **5 concurrency control schemes**: 2PL, MVCC, SSI, OCC, Timestamp Ordering
- **3 storage layouts**: Heap, Columnar, Clustered B+tree
- **12+ join algorithms**, 10+ index types, 8 hash table variants, 8 cache policies
- **12 distributed systems primitives** (Raft, Paxos, SWIM, consistent hashing, 2PC, vector clocks, HLC, phi detector, merkle tree, CRDTs, replication, CDC)
- Full PostgreSQL compatibility (wire protocol, RBAC, RLS, SCRAM-SHA-256, prepared statements)
- Known bugs: AFTER DELETE trigger drops OLD row values (6th arg in _fireTriggers)

### Neural-net
A comprehensive deep learning library spanning 42 years of research (Hopfield 1982 → KAN 2024).
- **168 source files, ~26K LOC, 150 test files (960 pass, 8 fail)**
- **10 backward passes verified at machine precision** via numerical gradient checks
- Full LLM pipeline: tokenizer → embedding → transformer → KV cache → sampling → training
- RLHF stack: PPO, DPO, reward model, Constitutional AI
- Deployment: continuous batching, prefix caching, quantization, speculative decoding
- Generative: GAN, VAE, DDPM, normalizing flows
- Neuroscience: SNN, Hopfield, predictive coding, NTM
- Known bugs: 8 broken test files (rope import, flashAttention API mismatch, MoE serialization), AdamW step counter, reward model bias gradients

### Combined
- **536 source files, 1018 test files, 104K LOC**
- **110+ academic papers/algorithms implemented**

## Key Decisions (Apr 23, 2026)
- HenryDB extraction declared DONE — db.js at 1633 lines is core dispatch/txn/eval
- Further extraction = more indirection, not less complexity
- Tomorrow: fix 7 known bugs, update README, run full test suites

## Reference Documents
- `memory/scratch/henrydb-architecture.md` — comprehensive architecture reference
- `memory/scratch/neural-net-architecture.md` — comprehensive ML library reference
- `memory/scratch/session-b-depth-findings.md` — Session B bug findings + analysis
