## CURRENT

status: in-progress
session: Work Session A (8:15 AM - 2:15 PM MDT)
date: 2026-04-16
tasks_completed_this_session: 71
active_projects: monkey-lang, lambda-calculus

## Lambda Calculus: 34 PL Theory Modules, 781 Tests
Foundations (1-10), Advanced Types (11-20), Research (21-34):
- 21-25: delimited continuations, intersection/union, HKT, type-level, small-step
- 26-30: effects rosetta, CEK machine, abstract interpretation, properties, Hindley-Milner
- 31-34: constraint inference, defunctionalization, closure conversion, ANF

## Monkey-lang Compiler Pipeline
Source → Parse → TypeCheck → DCE → CFG → SSA → ConstProp → Liveness → RegAlloc → Escape
Plus: Type Info (hover), Type Tracer, Typed Optimizer, RISC-V/WASM/Bytecode backends

## Bugs Found (5 this session)
1. Multi-shot continuation: applyCPS discarded outer k
2. ConstStatement undefined: class doesn't exist in ast.js
3. Intra-block liveness: dead assignments missed within-block uses
4. CEK eval double-dispatch: inline handlers conflicted with _step
5. Test expectation: shift(k=>k) returns Cont, not value
