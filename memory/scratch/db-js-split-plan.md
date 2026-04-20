# db.js Split Plan

## Current State
- 6484 lines, 125 methods, TWO duplicate _exprToString methods
- Single Database class doing everything

## Proposed Split

### 1. `db-core.js` (~500 lines) — Database class skeleton
- constructor, _getTable, _normalizeQuery, execute, execute_ast
- serialize/fromSerialized, save, toJSON/fromJSON
- checkpoint, _checkpoint

### 2. `db-ddl.js` (~800 lines) — DDL operations
- _createTable, _createTableAs, _alterTable, _dropTable
- _createIndex, _dropIndex
- _createView, _createMatView, _refreshMatView, _dropView
- _createSequence, _dropSequence, _nextval, _currval, _setval
- _createFunction, _callUserFunction
- _executeComment

### 3. `db-dml.js` (~1200 lines) — DML operations
- _insert, _insertRow, _insertSelect, bulkInsert
- _update, _tryIndexScanForUpdate
- _delete, _handleForeignKeyDelete
- _truncate
- _executeMerge
- _fireTriggers, _validateConstraints
- _orderValues, _applySelectColumns
- _computeGeneratedColumns, _validateNoGeneratedColumnWrites

### 4. `db-select.js` (~1500 lines) — Query execution
- _select, _selectInner, _selectInnerCore
- _executeJoin, _executeJoinWithRows, _executeLateralJoin
- _selectWithGroupBy, _expandGroupingSets
- _union, _intersect, _except
- _executeRecursiveCTE
- _renameCTEColumns, _splitPredicates

### 5. `db-eval.js` (~1500 lines) — Expression evaluation
- _evalExpr, _evalValue, _evalFunction, _evalSubquery
- _evalDefault, _resolveColumn
- _computeAggregates, _collectAggregateExprs
- _computeWindowFunctions
- _exprToString (single version!)
- _collectColumnRefs, _exprMatchesIndex

### 6. `db-util.js` (~500 lines) — Utilities and introspection
- _explain, _explainCompiled, _explainAnalyze, _buildExplainPlan
- _showTables, _describe, _vacuum, _analyzeTable
- _tryIndexScan, _findIndexedColumn, _computeIndexKey
- _resolveOrderByValue, _valuesToRow, _rebuildIndexes
- _getPgCatalog, _getInformationSchema

### 7. `db-session.js` (~400 lines) — Session features
- _prepare, _executePrepared, _substituteParams, _deallocate
- _declareCursor, _fetch, _closeCursor
- _savepoint, _releaseSavepoint, _rollbackTo
- _copy, _copyTo, _copyFrom, _executeCopy, _parseCsvLine
- _listen, _notify, _unlisten, onNotify, getNotifications

## Migration Strategy
Use mixins: each file exports a function that adds methods to Database.prototype.
The Database class stays in db-core.js, others are imported and applied.

```javascript
// db.js (thin entry point)
import { Database } from './db-core.js';
import { addDDL } from './db-ddl.js';
import { addDML } from './db-dml.js';
import { addSelect } from './db-select.js';
import { addEval } from './db-eval.js';
import { addUtil } from './db-util.js';
import { addSession } from './db-session.js';

addDDL(Database); addDML(Database); addSelect(Database);
addEval(Database); addUtil(Database); addSession(Database);

export { Database };
```

This preserves the external API while making the internals navigable.

## Risk
- Method interdependencies (e.g., _insert calls _evalExpr which calls _resolveColumn)
- Test files import from './db.js' — need thin wrapper
- Circular dependencies possible between eval ↔ select ↔ dml

## Priority: NORMAL — do it when there's a quality-focused session, not during a feature sprint.
