# CURRENT.md — Session State

## Status: in-progress
## Session: B (2:15 PM – 8:15 PM MDT, April 16, 2026)
## Focus: Quality depth work — 8 bugs, 200+ tests

## Completed This Session (23 tasks)
### Bug Fixes (8)
1. SelfAttention.forward() in-place input mutation
2. LayerNorm.backward() simplified gradient (50-80% error)
3. TransformerEncoderBlock FF gradient loss (Dense assigns not accumulates)
4. CapsuleNet squash gradient = identity
5. Adam/AdamW NaN when step() not called
6. RISC-V prologueSaveIdx not saved during function compilation
7. TransformerEncoderBlock residual gradient not split
8. RISC-V array element type hardcoded to int

### New Tests (~200)
- Systematic gradient checker (16 modules)
- Autograd stress (31), Attention stress (11), Conv stress (10)
- BatchNorm stress (3), LayerNorm stress (4), Capsule stress (4)
- Transformer E2E (4), Training benchmarks (3)
- GAN stress (4), VAE stress (5), Diffusion stress (8)
- Peephole optimizer (31), JIT equivalence (31), VM stress (19)
- HM type inference (27), CoC type theory (25)

### Other
- Blog post: "Seven Bugs in Seven Backward Passes"
- Character-level language model example
- Neural-net CI fixed (all green)
- Total test counts: neural-net 1240, monkey-lang 1752+50, RISC-V 1066
