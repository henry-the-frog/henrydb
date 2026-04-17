# CURRENT.md — Session State

## Status: session-ended
## Session: B2 (5:45 PM – 8:15 PM MDT, April 16, 2026)
## Focus: Neural-net depth work — systematic gradient verification

### Summary
Exceptional session. Found 7 bugs through systematic numerical gradient verification. Added 121 new tests to neural-net (437 → 558). MicroGPT now fully trains, generates text, and has modern training features (causal attention, LR warmup, gradient clipping, GELU/Swish activations).

### Bugs Fixed: 7
1. MicroGPT backward (only trained output projection)
2. Conv1D update double-division  
3. **SelfAttention input mutation** (CRITICAL — entire transformer was broken)
4. LayerNorm missing cross-terms (100% gradient error)
5. Adam/AdamW NaN on first update (t=0 division by zero)
6. Conv2D update double-division
7. Lambda-calculus demand analysis call arity

### New Features:
- GELU activation (BERT/GPT)
- Swish/SiLU activation (EfficientNet/Llama)
- Causal attention mask (GPT decoder-only)
- LR warmup + cosine decay
- Gradient clipping (maxGradNorm)

### Final Test Counts:
- Neural-net: 558 tests, 0 failures
- Lambda-calculus: 848 tests, 0 failures
- RISC-V: 1066 tests, 0 failures

### Tomorrow's Focus:
- Fix 112 broken test suites in monkey-lang sub-projects
- HenryDB depth work (deferred from today)
- Continue neural-net: maybe add batch training, attention caching, or start on Llama-like architecture
