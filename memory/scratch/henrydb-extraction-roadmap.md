# HenryDB db.js Extraction Roadmap (2026-04-22, updated end of session)

## Already Extracted (8 modules, 3412 LOC)
- join-executor.js: 1074 LOC (15 join methods)
- group-by-executor.js: 525 LOC (_selectWithGroupBy)
- explain-executor.js: 496 LOC (_explain, _explainAnalyze, _explainCompiled, _fillScanActuals)
- plan-format.js: 215 LOC (_formatPlan, _planToYaml, _planToDot)
- select-inner.js: 681 LOC (_selectInner)
- cte-executor.js: 159 LOC (_withCTEs, _executeRecursiveCTE)
- catalog-queries.js: 150 LOC (_selectInfoSchema, _selectPgCatalog, _filterPgCatalogRows)
- set-operations.js: 112 LOC (_union, _unionInner, _intersect, _except)

## db.js Status: 3,293 lines (67% reduction from ~10K peak)

## Remaining Extraction Candidates
1. `_recommendIndexes` + `_applyRecommendedIndexes`: ~70 LOC (need rename: index-advisor.js exists)
2. `_validateConstraints` area: ~100 LOC
3. `_handlePrepare/Execute/Deallocate`: ~100 LOC — prepared statements
4. `_handleSavepoint/RollbackTo/Release`: ~100 LOC — savepoints
5. `_handleForeignKeyDelete/Update`: ~100 LOC — FK cascades
6. `_merge`: ~60 LOC

## Volcano Path Status
- **27/27 SQL patterns** build+execute through Volcano
- IN_SUBQUERY not handled (correctness bug — returns all rows)
- CASE WHEN works in both WHERE and SELECT
- Legacy is ~100x faster for small data (overhead from iterator objects)
