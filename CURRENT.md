# CURRENT.md — Session State

## Status: session-ended
## Session: B (2:15 PM – 8:15 PM MDT, April 16, 2026)
## Focus: Quality depth work — 8 bugs, 280+ tests across 4 projects

## Final Accomplishments
### Bug Fixes (8)
1. **SelfAttention.forward()** in-place input mutation → corrupted backward
2. **LayerNorm.backward()** simplified gradient → 50-80% error, fixed with full formula
3. **TransformerEncoderBlock FF** gradient loss → Dense assigns not accumulates, batched all positions
4. **CapsuleNet squash** gradient = identity → proper Jacobian-vector product
5. **Adam/AdamW NaN** when step() not called → auto-step
6. **RISC-V prologueSaveIdx** not saved during function compilation → stack overflow
7. **TransformerEncoderBlock residual** gradient not split between skip+transform → fixed
8. **RISC-V array element type** hardcoded to int → changed to unknown for runtime dispatch

### New Tests (~280)
- **Neural-net**: 116 new tests (1185 → 1301)
  - Systematic gradient checker (16 modules), stress tests for autograd, attention, conv, batchnorm, layernorm, capsule, transformer E2E, training benchmarks, GAN, VAE, diffusion, DQN, optimizers, embeddings, LR schedulers, data augmentation, regularization, pruning/quantization, knowledge distillation, matrix ops
- **Monkey-lang**: 50 new tests (JIT equivalence 31, VM stress 19)
- **Lambda-calculus**: 112 new tests (HM 27, CoC 25, normalization 12, logical relations 9, supercompiler 15, game semantics 10, demand analysis 14)
- **RISC-V**: 31 new tests (peephole optimizer)

### Other Deliverables
- Blog post: "Seven Bugs in Seven Backward Passes" (deployed to GitHub Pages)
- Character-level language model example
- Performance benchmark script
- JIT speedup measurement (6.94x for fib(25))
- All CI green across all projects
- Total test counts: neural-net 1301, monkey-lang ~1800+, RISC-V ~1066, lambda-calculus ~2500+
