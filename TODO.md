# Tomorrow's Action Items — Apr 24, 2026 (revised after Session B evening fixes)

## ✅ DONE (Session B evening, Apr 23)
- ~~Fix rope.js import~~ ✅ Added precomputeRoPE + applyRoPEToSequence aliases
- ~~Fix MultiHeadFlashAttention NaN~~ ✅ Positional args fix (also in grouped-query-attention.js)
- ~~Fix AdamW step counter~~ ✅ Verified correct (step() called once per optimizer step, not per-param)
- ~~Fix reward model bias gradients~~ ✅ Added b1 bias gradient computation + update
- ~~Fix MSE gradient~~ ✅ Verified correct (Network.backward already divides by batchSize)
- ~~Fix MoE serialization~~ ✅ Fixed to use expert.up/down Dense layers with backward compat
- ~~Fix pruning >= vs >~~ ✅ Verified intentional (tests depend on current behavior)
- ~~Fix AFTER DELETE trigger~~ ✅ Added BEFORE/AFTER DELETE _fireTriggers calls
- ~~Fix trainWithEarlyStopping missing~~ ✅ Added function + upgraded EarlyStopping API (mode, summary, bestEpoch)

## Top Priority: Remaining Bugs
### HenryDB (30 min)
1. Fix optimizer-quality test — investigate cost model issue — 20 min
2. Fix SELECT * + window Volcano bug — expand * to base-table columns at final projection — 1-2 tasks

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
