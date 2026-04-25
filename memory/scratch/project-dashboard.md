# Project Dashboard (2026-04-25)

## Cross-Project Stats (Verified Apr 25)
| Project | LOC | Tests | Status |
|---------|-----|-------|--------|
| henrydb | 208K | 8,218 | ✅ Fuzzer 97%+ |
| monkey-lang | 199K | 8,735 | ✅ Const-subst wired |
| neural-net | 46K | 2,323 | ✅ Tutorial written |
| lambda-calculus | 43K | 469 (203 suites) | ✅ All pass |
| riscv-emulator | 9K | 166 | ✅ All pass |
| git | 10K | 100 | ✅ All pass |
| regex-engine | 1K | 113 | ✅ All pass |
| sat-solver | 3K | 12 | ✅ Profiled |
| dns-server | 2K | 26 | ✅ All pass |
| huffman | 3K | 4 | ✅ All pass |
| raft-consensus | 2K | ~20 | ✅ (in henrydb) |
| fft | 0 | 0 | ⬜ Empty |
| forth | 0 | 0 | ⬜ Empty |
| **TOTAL** | **~559K** | **~20,438** | **11/13 active** |

## Today's Session A (8:15 AM - ongoing)
- **71+ tasks** in ~2 hours
- **9 bugs found and fixed** (all in HenryDB)
- **30 BUILDs**, 15 EXPLOREs, 8 THINKs, 6 PLANs, 12 MAINTAINs
- **Differential fuzzer created** — 97%+ pass rate across 2000+ queries
- **Constant propagation wired** into monkey-lang compiler
- **2 blog posts** written (HenryDB: ~2500 words, monkey-lang: ~2500 words)
- **1 tutorial** written (neural-net: 8 sections)
- **10 new scratch notes** documenting research

## Key Discoveries
1. HenryDB has **6 execution engines** + adaptive layer (not 5 as documented)
2. monkey-lang is a **mini-V8** with 12 compiler modules, GC, debugger, shapes
3. Many TODO items were **already implemented** (GENERATED columns, CROSS APPLY, INLJ)
4. **559K lines of JavaScript** across 13 projects with **20K tests**
5. Differential fuzzing found bugs in 5 min that 8200 tests missed
