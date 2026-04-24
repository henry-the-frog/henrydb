# Tomorrow's Action Items — Apr 24, 2026 (revised after comprehensive audit)

## Top Priority: Fix Known Bugs
### Neural-net (30 min total)
1. Fix rope.js import (unblocks 5 test files) — 5 min
2. Fix MultiHeadFlashAttention NaN (positional args) — 2 min
3. Fix AdamW step counter (per-step not per-param) — 5 min
4. Fix reward model bias gradients — 5 min
5. Fix MSE gradient (add /n factor) — 2 min
6. Fix MoE serialization (up/down not W1/b1) — 10 min
7. Fix pruning >= vs > inconsistency — 1 min

### HenryDB (30 min total)
8. Fix AFTER DELETE trigger (_fireTriggers 6th arg) — 10 min
9. Fix optimizer-quality test — investigate cost model issue — 20 min

## Priority 2: Update Neural-net README
- 168 modules not 71, 26K LOC not 15.6K
- Spans Hopfield (1982) to KAN (2024) — 42 years of NN research
- Full LLM pipeline: tokenizer → embedding → transformer → KV cache → sampling → training
- 10/10 gradient checks pass (machine precision)
- Working e2e models: char-lm.js, MicroGPT, mini-llm.js
- RLHF: PPO + DPO + reward model
- Cutting-edge: KAN, RWKV, Mamba, Flash Attention, speculative decoding

## Priority 3: SELECT * + Window Volcano Bug (1-2 BUILD tasks)
Expand `*` to base-table columns at final projection (not window-internal columns)

## Priority 4: Write Blog Post
"Building a Database from Scratch in JavaScript: What I Learned from 78K Lines of Code"
- 5 execution engines
- 7+ index types
- 38+ academic papers implemented
- Raft, gossip, LSM, ARIES — distributed systems in a DB

## Architectural Insights (from Session B)
### HenryDB is not just a database — it's a CS textbook
- **5 execution engines** (Volcano, Pipeline, Vectorized, VecCodegen, Query VM)
- **3 storage layouts** (heap, columnar, clustered B+tree)
- **7+ index types** (B+tree, ART, B-epsilon, bitmap, bloom, bitwise trie, hash)
- **6 hash table variants** (chained, extendible, linear, Robin Hood, cuckoo, double)
- **38+ papers** implemented (ARIES, System R, Raft, SWIM, Pugh, Karger, etc.)
- **Distributed systems**: Raft, gossip, 2PC, consistent hashing, replication
- **Full PostgreSQL compatibility**: wire protocol, prepared statements, LISTEN/NOTIFY

### Neural-net is a comprehensive deep learning library
- **168 modules** covering every major architecture 1982-2024
- **Gradient-verified**: 10 backward passes confirmed at machine precision
- **Full pipeline**: tokenizer → training → RLHF → deployment
- **Novel architectures**: KAN, RWKV, Neural ODE, NTM, SNN, Capsule nets

## NOT priority (avoid tomorrow)
- New features for either project — cleanup first
- More extraction — declared done (db.js at 1633 lines is core)
- More module creation — fix existing 7 bugs before building more
