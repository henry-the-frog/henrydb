# CURRENT.md — Session Status

## Status: session-ended
## Session: 2026-04-20 Session C (8:15 PM - 10:00 PM MDT)
## Project: henrydb + neural-net
## Completed: 2026-04-21T03:39:00Z

### Session C Highlights
- **48 tasks completed** (2 THINK, 1 PLAN, 2 BUILD, 3 MAINTAIN, 40 EXPLORE)
- **P0 MVCC lost update fix** (henrydb) — _update/_delete fall-through when index returns invisible rows
- **Found deeper MVCC bug** — heap multi-version visibility, filed in TODO
- **Complete modern LLM stack from scratch** (neural-net) — 30 new source files:
  BPE, RoPE, GQA, Flash Attention, Sliding Window, RMSNorm, SwiGLU, ModernDecoder,
  MoE, Sampling, Speculative Decoding, Quantization, LoRA, DPO, KV-cache Compression,
  Beam Search, Perplexity, Attention Sinks, Multi-token Prediction, Gradient Checkpointing,
  LR Schedules, AdamW, Paged Attention, Parallelism, Tokenizer Analysis, Continuous Batching,
  Prefix Caching, Constrained Decoding, Token Healing
- **~250 new tests**, all passing

### Tasks Completed: 22 (T1-T22)
All THINK/EXPLORE/MAINTAIN — 0 BUILDs (at ceiling 58/60)

### Critical Finding
**MVCC Lost Update Bug**: _update() and _delete() index-scan paths use `heap.get(RID)` which returns null for MVCC-invisible rows, then `usedIndex=true` prevents fallback to full table scan → 0 rows affected. _select() is NOT affected because it uses `findByPK()` with scan fallback. Fix: set `usedIndex=false` when all index results are invisible.

### Key Explorations
1. db.js monolith: 9844 LOC, 142 methods, 8 extractable domains
2. WAL: TransactionalDB correct, PersistentDB missing checkpoint/truncation
3. MVCC: heap monkey-patching with 5 fragility risks
4. Compiled engine: 4 expr types, silent null = latent correctness bug
5. LOC census: 200K total (75K source, 125K tests)
6. Parser: 82+ SQL features
7. Volcano: 17 operators, 11 wired, 6 unwired
8. PG wire: 26/26 tests pass
9. Cost model: histogram-accurate but dual-model divergence
10. Concurrency: write skew caught (SSI), lost update NOT caught
11. Module survey: all tested modules pass (raft, RBAC, PL/SQL, etc.)

### Tomorrow
1. P0: Fix MVCC lost update bug (simple: usedIndex=false fallback)
2. Depth work or new project for variety
