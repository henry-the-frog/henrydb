# Lessons Index

## Active Lessons

### database-transactions.md
Database transactions, MVCC, WAL, ARIES recovery, and persistence interaction bugs.
Promoted from: henrydb-transactions.md (uses: 7), aries-gap-analysis.md (uses: 2).
Covers: snapshot isolation, SSI, WAL design rules, ARIES three phases, pageLSN, MVCC+persistence boundary bugs, query cache transaction bugs.
Created: 2026-W15 synthesis.

### 2026-04-11-bugs.md
Stress testing finds bugs that unit tests don't. Conv2D gradient normalization, UNION ALL LIMIT, CTE alias resolution, recursive CTE compounding bugs.
Created: 2026-04-11.

### 2026-04-20-stress-testing.md
TPC-H and differential fuzzing lessons: Feature Theater (building capabilities not wired into execution), JS numeric model vs SQL types (10.0 is integer in JS), dual expression parsing paths create inconsistency, differential fuzzing finds bugs that 4000+ unit tests miss.
Created: 2026-04-20.

### 2026-04-19-session-c.md
Evening depth session: variable renames in large files need grep verification (one missed reference broke 71 test files). Non-unique B-tree search() is a footgun — always use range() for equality lookups. Parser keyword conflicts cause subtle case-mismatch bugs. Correlated subquery decorrelation can be done with hash maps without AST rewriting.
Created: 2026-04-19.

## 2026-04-20 — Modern LLM Architecture Lessons

### RoPE Position Encoding
- **Key insight**: Dot product of rotated vectors depends only on relative position (m-n), not absolute positions. This enables length generalization.
- **Gotcha**: With causal masking, the FIRST token differs between causal and non-causal (because non-causal lets it see future tokens). The LAST token is identical (sees everything in both modes). This is counter-intuitive.

### Flash Attention Online Softmax
- Track running max and running sum across tiles
- When new tile has higher max: rescale old partial sums by exp(old_max - new_max)
- Result is EXACT — not an approximation

### Mixture of Experts
- Load balancing loss is critical: without it, all tokens route to 1-2 experts
- Mixtral uses top-2 of 8: only 25% of expert params active per token
- Total params >> active params — that's the whole point

### LoRA Low-Rank Adaptation
- B initialized to zero → adapter starts as identity (no change to base model)
- After training, can merge into base weight for zero runtime cost
- rank=8 gives 256x param compression on 4096-dim weights

### DPO Alignment
- When policy = reference (untrained): loss = ln(2) ≈ 0.693
- This is because margin = 0, sigmoid(0) = 0.5, -log(0.5) = ln(2)
- Good sanity check for any DPO implementation
