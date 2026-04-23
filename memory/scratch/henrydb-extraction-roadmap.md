# HenryDB Extraction Roadmap (Updated 2026-04-23)

## Status: db.js at 2760 lines (was 3293 at start of session, ~5000 originally)

## Completed Extractions (Session A, 2026-04-23)
| Module | LOC | Methods |
|--------|-----|---------|
| index-advisor-impl.js | 85 | _recommendIndexes, _applyRecommendedIndexes |
| merge-executor.js | 75 | _merge |
| prepared-stmts.js | 72 | _handlePrepare, _handleExecute, _handleDeallocate |
| savepoint-handler.js | 120 | _handleSavepoint, _handleRollbackToSavepoint, _handleReleaseSavepoint |
| fk-cascade.js | 115 | _handleForeignKeyDelete, _handleForeignKeyUpdate |
| constraint-validator.js | 120 | _validateConstraints, _validateConstraintsForUpdate |
| analyze-profile.js | 100 | _handleAnalyze, profile |
| checkpoint-handler.js | 30 | _checkpoint |
| prepared-stmts-ast.js | 110 | _prepareSql, _executePrepared, _bindParams, _deallocate, prepare |

**Total extracted today: ~830 LOC across 9 new modules**

## Previously Extracted (before this session)
- sql.js (parser)
- select-inner.js
- set-operations.js
- having-impl.js
- cte-executor.js
- ddl-executor.js
- insert-executor.js
- trigger-executor.js
- create-view.js
- volcano-planner.js
- expression-evaluator.js
- decorrelate.js
- btree-table.js
- heap-page.js

## Remaining Extraction Candidates (ordered by size)
| Method | LOC | Risk | Notes |
|--------|-----|------|-------|
| _tryIndexScan | 251 | Medium | Index scan logic, complex but self-contained |
| _insertRow | 126 | High | Hot path, uses many internal methods |
| _tryVolcanoSelect | 121 | Medium | Volcano routing, references multiple planners |
| executePaginated | 94 | Low | Simple pagination wrapper |
| _vacuum | 89 | Low | VACUUM command, isolated |
| _analyzeTable | 77 | Low | Table analysis (separate from _handleAnalyze) |
| _select | 76 | High | Core select routing, many dependencies |
| _acquireRowLocks | 61 | Low | Row locking, self-contained |
| serialize | 59 | Low | Serialization, self-contained |
| _applySelectColumns | 58 | Medium | Column projection logic |

**Next priority: Low-risk first (executePaginated, _vacuum, serialize, _acquireRowLocks)**
**Then medium: (_tryIndexScan, _tryVolcanoSelect, _applySelectColumns, _analyzeTable)**
**Defer: High-risk (_insertRow, _select) — these are core and have many callees**

## Extraction Pattern
1. Create new file with extracted function
2. First param is always `db` (the database instance)
3. Add import to db.js
4. Replace method body with one-liner delegation
5. Run related tests
6. Commit
