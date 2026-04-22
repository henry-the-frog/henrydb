# HenryDB Architecture — Apr 21 Update

## db.js Extraction History
| Date | LOC | Delta | Modules Extracted |
|------|-----|-------|-------------------|
| Apr 20 start | 8247 | — | monolith |
| Apr 20 end | ~6635 | -1612 | sql-functions.js |
| Apr 21 B-2 | 4939 | -1696 | pg-catalog.js (541), set-operations.js (130), expression-evaluator.js (803), dead code removal (114) |
| **Total** | **4939** | **-3308 (40%)** | 4 extracted modules |

## Volcano Planner Status
| Operator | Status | Tests |
|----------|--------|-------|
| SeqScan | ✅ | integrated |
| Filter | ✅ | integrated |
| Project | ✅ | integrated |
| Sort | ✅ | integrated |
| HashAggregate | ✅ | integrated |
| Limit | ✅ | integrated |
| Distinct | ✅ | integrated |
| HashJoin | ✅ | integrated |
| NestedLoopJoin | ✅ | integrated |
| IndexNestedLoopJoin | ✅ | integrated |
| MergeJoin | ✅ (fixed dup bug) | 7 tests |
| CTE (non-recursive) | ✅ | 5 tests |
| CTE (recursive) | ✅ | 4 tests |
| Union/Intersect/Except | ✅ | 13 tests |
| Cost Model | ✅ | 19 tests |
| Window | ❌ not wired | — |

**Total volcano tests: 146**

## MVCC Status
| Feature | Status |
|---------|--------|
| Snapshot isolation | ✅ |
| SSI (write skew) | ✅ |
| EvalPlanQual | ✅ (fixed Apr 21) |
| Phantom protection | ✅ |
| PK conflict detection | ✅ |
| Dead tuple vacuum | ❌ |
| findByPK optimization | ❌ (falls back to full scan) |

**Total MVCC tests: 63** (42 base + 13 transactional + 8 stress)

## P0 Remaining: Volcano → db.js Wiring
The Volcano engine has hash/merge/NL join, cost model, CTE, set ops — but db.js
executor STILL uses its own nested loop join. Wiring the Volcano output into the
execution path is the biggest remaining perf win (186x on multi-table TPC-H queries).

Approach: intercept at db._select() when joins present, delegate to buildPlan+execute.
Challenge: expression evaluation, GROUP BY, WHERE are deeply interleaved with join execution.

## Volcano Integration Benchmark (Apr 21)

| Scenario | Volcano (HashJoin) | Nested Loop | Speedup |
|----------|-------------------|-------------|---------|
| 2-table (100×1000) | 7.9ms | 154ms | **19.5×** |
| 3-table (5×200×2000) | 17.8ms | 664ms | **37.3×** |

P0 confirmed: hash join integration provides order-of-magnitude improvement.
Speedup scales with table count (O(n+m) vs O(n×m)).
