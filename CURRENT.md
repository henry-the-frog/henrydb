# CURRENT.md — Session State

## Status: in-progress
## Session: B2 (5:45 PM – 8:15 PM MDT, April 16, 2026)
## Focus: Neural-net depth work — systematic gradient verification

### Summary
Extraordinary session. Found 6 major bugs in neural-net through systematic numerical gradient verification. Fixed all, added ~120 new tests. MicroGPT now actually trains (loss → 6.2e-8 on simple patterns).

### Bugs Fixed This Session: 7
1. MicroGPT backward (only trained output projection)
2. Conv1D update double-division  
3. **SelfAttention input mutation** (CRITICAL — entire transformer was broken)
4. LayerNorm missing cross-terms
5. Adam/AdamW NaN on first update (t=0 division by zero)
6. Conv2D update double-division
7. Lambda-calculus demand analysis call arity

### Tasks Completed: 42 (T243 through T285)
### Tests Added: ~120
### Final Test Counts:
- Neural-net: 553 tests, 0 failures, 56 files
- Lambda-calculus: 848 tests, 0 failures
- RISC-V: 1066 tests, 0 failures

### New Features:
- GELU activation (BERT/GPT)
- Swish/SiLU activation (EfficientNet/Llama)
