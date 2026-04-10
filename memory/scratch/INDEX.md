# Scratch Notes Index

## Active Notes
- **henrydb-persistence-bugs.md** — BufferPool bugs (#1-#3), layer mismatches, WAL recovery, autocommit txId=0 issue, architecture diagram (uses: 1, created: 2026-04-09)
- **copy-and-patch-compilation.md** — Copy-and-patch technique, comparison with HenryDB closure approach, sea-of-nodes verdict (overkill), action items (uses: 1, created: 2026-04-08)
- **henrydb-transactions.md** — MVCC/WAL bugs (#1-#9), SSI, 2PC, ARIES checkpointing, PITR, auto-checkpoint, WAL compaction, compiled query engine (uses: 6, created: 2026-04-07)
- **cdcl-sat-solver.md** — CDCL SAT solver design, clause learning, VSIDS (uses: 2, created: 2026-04-06)
- **thompsons-nfa.md** — Thompson's NFA construction, linear-time regex matching (uses: 1, created: 2026-04-07)
- **algorithm-w.md** — Hindley-Milner type inference, unification, generalization (uses: 1, created: 2026-04-07)
- **forth-compilation.md** — Forth compile vs interpret mode, IMMEDIATE words, threaded code (uses: 1, created: 2026-04-07)
- **riscv-architecture.md** — RISC-V instruction encoding, pipeline hazards, branch prediction, cache behavior, Sv32 page tables, Tomasulo OoO (uses: 1, created: 2026-04-07)


## Git Implementation (tiny-git) — 2026-04-10
- **Location:** projects/git/
- **Architecture:** Content-addressable store → Index → Refs → Commits (DAG)
- **Key files:** objects.js, index.js, refs.js, commands.js, diff.js, checkout.js, merge.js, pack.js, clone.js
- **Test count:** 132 (objects 24, index 18, commands 13, diff 16, checkout 10, merge 7, stress 14, pack 9, clone 5, compat-forward 9, compat-reverse 7)
- **Real git compatible:** Bidirectional — our repos readable by git, git repos readable by us
- **Key learnings:** Tree mode 40000 (not 040000 with leading zero), commit messages have trailing newline, Myers diff O(ND), three-way merge needs merge base

