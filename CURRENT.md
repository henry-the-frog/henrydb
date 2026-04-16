# CURRENT.md — Session State

## Status: in-progress
## Session: B (2:15 PM – 8:15 PM MDT, April 16, 2026)
## Focus: Quality depth work

## Completed This Session
1. **Neural-net CI fix** — flaky test stabilization (retries, relaxed tolerances)
2. **BUG FIX #1**: SelfAttention.forward() mutated input in-place → corrupted backward
3. **BUG FIX #2**: LayerNorm.backward() simplified gradient (50-80% error) → full formula
4. **BUG FIX #3**: TransformerEncoderBlock FF gradient loss (Dense.backward assigns, not accumulates) → batch all positions
5. **BUG FIX #4**: CapsuleNet squash gradient was identity → proper Jacobian
6. **BUG FIX #5**: Adam/AdamW NaN when step() not called → auto-step
7. **BUG FIX #6**: RISC-V prologueSaveIdx not saved/restored → stack overflow in nested functions
8. E2E Transformer training test (4 tests)
9. 60+ new stress tests across autograd, attention, conv, batchnorm, capsule, layernorm

## Stats
- 6 real bugs found and fixed
- ~70 new tests written
- All CI green (neural-net: 1200+ tests, RISC-V: 93+ tests)
- Projects touched: neural-net, riscv-emulator
