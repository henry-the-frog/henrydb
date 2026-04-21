# HenryDB db.js Monolith Analysis
- created: 2026-04-20
- uses: 1
- tags: henrydb, architecture, refactoring

## Overview
`src/db.js` is 9,844 lines — a single `Database` class with ~142 methods. This is the main query engine monolith. Everything else (storage, indexes, data structures) is already split into separate files.

## Module Boundaries (by line count)

| Domain | ~Lines | Key Methods | Extraction Difficulty |
|--------|--------|-------------|----------------------|
| Joins + Optimizer | 1390 | _executeJoin, _tryIndexScan, _estimateFilteredRows, _optimizeJoinOrder, _hashJoin | MEDIUM — reads `this.tables`, `this.statistics` |
| SELECT | 1341 | _selectInner (636), _selectWithGroupBy (526) | HARD — tangled with joins, aggregates, window fns |
| Expression Eval | 1154 | _evalFunction (644!), _evalExpr, _evalValue | EASY — pure evaluation, minimal state deps |
| EXPLAIN + Formatting | 667 | _explainAnalyze, _explain, _formatPlan | EASY — mostly formatting, clear interface |
| INSERT | 656 | _insert, _insertRow, _validateConstraints | MEDIUM — triggers, WAL, MVCC interception |
| DDL | 615 | _createIndex, _alterTable, _createTable | MEDIUM — catalog + WAL + index interactions |
| Aggregates | 454 | _computeAggregates (271), _evalAggregateExpr | EASY — pure computation on row arrays |
| Window Functions | 425 | _computeWindowFunctions (376) | EASY — standalone, takes rows+specs |

## Biggest Wins (effort/impact)

1. **Expression Evaluator** (~1154 lines) → `expression-evaluator.js`
   - `_evalExpr`, `_evalValue`, `_evalFunction` (644 lines of switch/case!), `_dateArith`, `_resolveColumn`, `_likeToRegex`
   - Minimal coupling: needs `this.tables` for subquery eval, but `_evalSubquery` could delegate back
   - Pure function-like: row in → value out

2. **Aggregates** (~454 lines) → `aggregate-evaluator.js`  
   - `_computeAggregates`, `_computeSingleAggregate`, `_evalAggregateExpr`, `_evalGroupExpr`
   - Nearly stateless, operates on row arrays

3. **Window Functions** (~425 lines) → `window-evaluator.js`
   - `_computeWindowFunctions` is 376 lines — nearly self-contained
   - Takes rows + window definitions, returns augmented rows

4. **EXPLAIN** (~667 lines) → `explain-engine.js`
   - Formatting + analysis, delegates to actual execution
   - Clean interface: takes AST, returns formatted output

5. **Join Engine + Optimizer** (~1390 lines) → `join-engine.js`
   - Most complex extraction — optimizer reads statistics, join methods need table access
   - But clear API: (leftRows, rightTable, joinSpec) → joinedRows

## Duplicate Code Found
- `_exprToString` — grep shows references but may have multiple implementations
- `_analyzeTable` appears at BOTH line 1807 and line 5654 (!!)

## Architecture Issues Beyond Size
- **MVCC interception**: scattered across _insert, _update, _delete, _select (not centralizable without redesign)
- **WAL logging**: mixed into DDL/DML methods (would need a logging facade)
- **Statistics/cost model**: accessed throughout join + select code

## Recommended Extraction Order
1. Expression Evaluator (easiest, biggest win)
2. Window Functions (nearly standalone)
3. Aggregates (clean separation)
4. EXPLAIN (minimal deps)
5. Join Engine (complex but high value)
6. DDL (medium complexity)
7. SELECT (hardest — defer until others done)

## Duplicate _analyzeTable Discovery
Line 1807 and 5654 both define `_analyzeTable` — the second one likely shadows the first! The first is the full ANALYZE implementation; need to verify which one is actually called.
