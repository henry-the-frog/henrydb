# Scratch Notes Index

## Active Notes
- **depth-testing-lessons.md** — Stress-testing 8 modules: 3 bugs found (KAN boundary, Izhikevich voltage, BETWEEN SYMMETRIC). Methodology for verifying ML implementations. (uses: 0, created: 2026-04-12)
- **henrydb-persistence-bugs.md** — BufferPool bugs (#1-#3), layer mismatches, WAL recovery, autocommit txId=0 issue, architecture diagram (uses: 1, created: 2026-04-09)
- **aries-gap-analysis.md** — ARIES recovery vs HenryDB: what we have, what's missing, pageLSN, DPT, CLRs (uses: 2, created: 2026-04-11)
- **pagelsn-implementation.md** — Concrete implementation path for pageLSN in HenryDB, ~50-80 lines, eliminates lastAppliedLSN hack (uses: 1, created: 2026-04-11, IMPLEMENTED)
- **mvcc-persistence-lessons.md** — MVCC + persistence interaction bugs and solutions (uses: 0, created: 2026-04-11)
- **copy-and-patch-compilation.md** — Copy-and-patch technique, comparison with HenryDB closure approach, sea-of-nodes verdict (overkill), action items (uses: 1, created: 2026-04-08)
- **henrydb-transactions.md** — MVCC/WAL bugs (#1-#9), SSI, 2PC, ARIES checkpointing, PITR, auto-checkpoint, WAL compaction, compiled query engine (uses: 6, created: 2026-04-07)
- **cdcl-sat-solver.md** — CDCL SAT solver design, clause learning, VSIDS (uses: 2, created: 2026-04-06)
- **thompsons-nfa.md** — Thompson's NFA construction, linear-time regex matching (uses: 1, created: 2026-04-07)
- **algorithm-w.md** — Hindley-Milner type inference, unification, generalization (uses: 1, created: 2026-04-07)
- **forth-compilation.md** — Forth compile vs interpret mode, IMMEDIATE words, threaded code (uses: 1, created: 2026-04-07)
- **riscv-architecture.md** — RISC-V instruction encoding, pipeline hazards, branch prediction, cache behavior, Sv32 page tables, Tomasulo OoO (uses: 1, created: 2026-04-07)


## HenryDB Query Optimizer Pipeline — 2026-04-10 Session C
- **Tree-structured EXPLAIN:** 13 PlanNode types (SeqScan, IndexScan, HashJoin, NestedLoop, Sort, Aggregate, etc.)
- **Predicate pushdown:** Wired into execution (not just plan display). Outer join safety: NEVER push right-side preds below LEFT JOIN.
- **Hypothetical indexes:** PlanBuilder accepts proposed indexes for what-if analysis. IndexAdvisor.compareWithIndex() shows cost reduction.
- **Index advisor → RECOMMEND INDEXES → APPLY RECOMMENDED INDEXES:** Full pipeline from workload analysis to auto-creation.
- **Query statistics:** pg_stat_statements equivalent. SHOW QUERY STATS, SHOW SLOW QUERIES.
- **EXPLAIN formats:** TEXT (default), TREE (new), HTML (SVG tree viz), JSON.
- **HTTP endpoints:** /explain (interactive web UI), /dashboard (perf dashboard with slow queries + index recs).
- **Key bug:** Outer join pushdown trap. `WHERE o.id IS NULL` on LEFT JOIN right side means "no match", NOT null in column. Pushing it below the join breaks semantics.
- **AST gotcha:** Parser uses COMPARE/EQ not binary/= for some expressions. Both must be handled.
- **Row count:** Use `table.heap._rowCount`, not `count()` or `rows.length`.


- **SQL Fuzzer:** Differential testing against SQLite, 10,800 random queries, 17 SQL patterns
- **Key bugs found by fuzzer:**
  - SUM() over empty set returned 0 instead of NULL (SQL standard)
  - BETWEEN with NULL: JS coerces null to 0 (`null >= -10` → true), bypassing SQL NULL semantics
  - NULL ordering: SQLite treats NULL as smallest value (first in ASC, last in DESC)
  - Adaptive engine SELECT * failed because `_applyProjectAndLimit` didn't check `c.type === 'star'`
  - Query cache `get()` returned wrapper `{result, timestamp}` instead of just result
  - Extended query protocol (Bind) didn't invalidate query cache after mutations
  - UNION/INTERSECT/EXCEPT didn't remap right SELECT's column names to left's
- **JS null comparison trap:** `null >= -10` is `true` because JS coerces null to 0. Any comparison with null needs explicit null checks first.
- **Compiled engine:** Fixed `_extractAggregation` to recognize AST nodes (`col.type === 'aggregate'`, `col.func`). Fixed LIMIT handling (`ast.limit` is a number, not `{value: n}`).

## Git Implementation (tiny-git) — 2026-04-10
- **Location:** projects/git/
- **Architecture:** Content-addressable store → Index → Refs → Commits (DAG)
- **Key files:** objects.js, index.js, refs.js, commands.js, diff.js, checkout.js, merge.js, pack.js, clone.js
- **Test count:** 132 (objects 24, index 18, commands 13, diff 16, checkout 10, merge 7, stress 14, pack 9, clone 5, compat-forward 9, compat-reverse 7)
- **Real git compatible:** Bidirectional — our repos readable by git, git repos readable by us
- **Key learnings:** Tree mode 40000 (not 040000 with leading zero), commit messages have trailing newline, Myers diff O(ND), three-way merge needs merge base

