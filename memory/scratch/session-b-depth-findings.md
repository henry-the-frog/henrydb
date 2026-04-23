# Session B Depth Exploration Findings — 2026-04-23

## Context
Session A completed 60+ BUILD tasks (daily ceiling exceeded). Session B was EXPLORE/THINK/MAINTAIN only — going deep on quality assessment.

## Neural-net Findings

### 8 Broken Test Files — 3 Root Causes
1. **rope.js missing export** (5 files): gqa-attention.js imports `applyRoPEToSequence` and `precomputeRoPE` which don't exist. Cascades to llm-demo, model-serialization, modern-decoder, simple-train. **Fix: ~10 LOC wrapper in rope.js.**
2. **MultiHeadFlashAttention NaN** (2 files): `flashAttention()` called with options object `{blockSize, causal}` instead of positional args `(blockSize, causal)`. Object becomes NaN in sqrt. **Fix: 1 line.**
3. **MoE serialization TypeError** (1 file): network.js serialization expects `expert.W1/b1/W2/b2` but MoE experts have `{up, down}` properties. **Fix: ~30 LOC.**

### Gradient Checks — All Correct
| Module | Max Error | Notes |
|--------|-----------|-------|
| Autograd (20 ops) | ~1e-10 | add, mul, sub, div, pow, neg, relu, sigmoid, tanh, exp, log, sin, cos, sum, mean, chain rule |
| BatchNorm | 4.85e-10 | Ioffe & Szegedy 2015 formula |
| GroupNorm | 1.9e-5 | Wu & He 2018 formula |
| Conv2D (col2im) | 1.29e-12 | Proper overlapping-patch accumulation |
| LayerNorm | 2.49e-10 | Ba et al. 2016 |
| MHA backward | structural audit | Correct chain rule through Wo, softmax, Q/K/V |

### Architecture Assessment
- **168 source modules, 150 test files, ~26K LOC** (README says 71/100/15.6K — outdated)
- Full LLM pipeline exists: tokenizer → embedding → transformer → KV cache → sampling → training
- mini-llm.js works (7/7 tests)
- **No backward passes** for Flash Attention, GQA, Mamba (forward-only)

## HenryDB Findings

### True Test Pass Rate: ~99%
- Morning sweep reported 292/868 failures
- Actual individual-file testing: 2/200 failures (~1%)
- Most "failures" were runner crashes/hangs/timeouts counted as failures
- Real bugs: (1) AFTER DELETE trigger not logging (constraint-crash-depth), (2) optimizer-quality test

### Extraction Complete
- db.js: 1633 lines (was 3293 at session start)
- 22 modules extracted, 1566 LOC moved
- Remaining code is core dispatch + txn + expression eval
- Further extraction = more indirection, not less complexity
- **Decision: extraction is DONE**

### All Decorrelation Tests Pass (15/15)
- Morning's 3 failures already fixed by T40 (batch decorrelation operator preservation)

### Volcano Gaps (4 remaining)
1. NOT NOT NOT parser — low priority
2-4. SELECT * + window: star expansion sees window-internal columns. Fix: expand * to base-table cols only at final projection. Est: 1-2 BUILD tasks.

### Core Systems Verified
- **WAL Recovery**: 31 tests pass (12 crash, 7 wal-recovery, 12 aries). Two-pass ARIES approach correct.
- **MVCC**: 32 tests pass (21 stress, 11 adversarial). Snapshot isolation works correctly.
- **FK Cascade**: Double-delete edge case for multi-level shared refs (pre-existing, not regression)

### Performance Profile (5000 rows)
- Full scan: 21ms (4.2μs/row) — good
- GROUP BY: 14.5ms — good
- Window fn: 23.9ms — good
- JOIN: 14ms — good
- **INSERT: 2ms/insert — BOTTLENECK** (constraint validation + btree + heap per statement)

## Tomorrow's Plan
**Morning: Neural-net cleanup** — fix 8 broken tests (~1 hour), update README
**Afternoon: HenryDB polish** — trigger bug, optimizer-quality, SELECT *+window, NOT NOT NOT
