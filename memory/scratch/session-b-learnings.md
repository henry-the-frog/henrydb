# Session B Learnings (Apr 25, 2026)

## Key Insights

### Type Systems
- SQLite type affinity is subtle: TEXT columns coerce integers to strings on INSERT
- `Number('')` === 0 in JavaScript — this caused `42 > ''` to return true
- `sqliteCompare` must be used for ordering (LT/GT/LE/GE), not JS defaults
- EQ/NE can coerce (numeric string to number), but ordering should not coerce

### Escape Analysis → Compiler
- The escape analysis existed but wasn't connected to the compiler
- Two bugs: (1) anonymous FunctionLiteral in escaping position not handled, (2) implicit return (last ExpressionStatement) not marked as escaping
- The optimization (skip GC tracking) is modest in a JS VM, but the annotation system enables future optimizations

### SSA Infrastructure
- `toSSA()` only accepted strings, not CFG objects — caused OOM when passed CFG
- SSABuilder works perfectly with function body CFGs
- Per-function SSA is now unblocked and ready to wire into the compiler

### Prelude Pattern
- HOFs (map/filter/reduce) can't be native VM builtins (need callback mechanism)
- Writing them as monkey-lang source compiled at startup is clean and maintainable
- 3x slower than native evaluator builtins, but adequate (2.1ms per map of 100)
- If perf becomes critical: add VM callback mechanism (frame push from builtins)

### Language Features
- F-strings (f"hello {x}") already existed — backtick syntax was a 14-line alias
- Comments (// and /* */) are implemented by extending skipWhitespace in the lexer
- Division near comments works because skipWhitespace checks peek character

## Bugs Found & Fixed
1. **WHERE comparison coercion**: Number('') = 0 caused 42 > '' to be true
2. **Non-aggregated column in aggregate query**: returned undefined, now picks first row value
3. **Escape analysis**: anonymous closures and implicit returns not detected
4. **SSA OOM**: toSSA() only accepted strings
5. **EXECUTE param count**: silently returned empty instead of error

## Architecture Decisions
- Shared `type-affinity.js` module for INSERT coercion and ORDER BY comparison
- Shared `percentile.js` for MEDIAN/PERCENTILE_CONT across 4 aggregate paths
- `prelude.js` pattern for VM HOFs — monkey-lang source compiled to bytecode

## MAJOR Discovery: neural-net (38K LOC)

This is not just "a neural network" — it's a **comprehensive deep learning framework** implementing state-of-the-art ML research in JavaScript:

### Paper Implementations
- Mamba SSM (Gu & Dao, 2023)
- Flash Attention (Dao et al., 2022)
- DPO (Rafailov et al., 2023)
- LoRA (Hu et al., 2021)
- CLIP (Radford et al., 2021)
- Constitutional AI (Bai et al., 2022)
- Chinchilla Scaling Laws (Hoffmann et al., 2022)
- Speculative Decoding (Leviathan et al., 2023)
- SimCLR (Chen et al., 2020)
- AdaLN/DiT (Peebles & Xie, 2023)
- Attention Sink/StreamingLLM
- Think Tokens (DeepSeek R1)

### Infrastructure
- BPE Tokenizer (GPT-style)
- GQA + KV-Cache (LLaMA-style)
- Continuous Batching (vLLM-style)
- Quantization (PTQ)
- MicroGPT (char-level transformer)
- AdamW optimizer
- Data loaders + augmentation
- Training callbacks (EarlyStopping)
- Autograd (reverse-mode AD)
- AutoML (hyperparameter search)

This is the most impressive project in the collection, surpassing even monkey-lang in scope.
