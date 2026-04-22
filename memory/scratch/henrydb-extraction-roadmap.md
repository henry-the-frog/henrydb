# HenryDB db.js Extraction Roadmap (2026-04-22)

## Already Extracted
- join-executor.js: 1074 LOC (15 join methods)
- group-by-executor.js: 525 LOC (_selectWithGroupBy)
- explain-executor.js: 496 LOC (_explain, _explainAnalyze, _explainCompiled, _fillScanActuals)
- plan-format.js: 215 LOC (_formatPlan, _planToYaml, _planToDot)
- select-inner.js: 681 LOC (_selectInner)
- **Total extracted: 2991 LOC**

## db.js Status: 3703 lines (63% reduction from ~10K)

## Remaining Extraction Candidates (by size)
1. `_withCTEs` + `_executeRecursiveCTE`: ~350 LOC — CTE execution
2. `_recommendIndexes` + `_applyRecommendedIndexes`: ~270 LOC — index advisor
3. `_selectInfoSchema` + `_selectPgCatalog` + `_filterPgCatalogRows`: ~160 LOC — metadata queries
4. `_union` + `_intersect` + `_except` + helpers: ~130 LOC — set operations
5. `_merge`: ~60 LOC
6. `_validateConstraints` area: ~100 LOC
7. `_handlePrepare/Execute/Deallocate`: ~100 LOC — prepared statements
8. `_handleSavepoint/RollbackTo/Release`: ~100 LOC — savepoints
9. `_handleForeignKeyDelete/Update`: ~100 LOC — FK cascades

## Notes
- Each extraction follows the `db` first-param delegation pattern
- Tests must be run after each extraction — missing imports are common
- Parser → consumer AST format mismatches found 3x during extraction
