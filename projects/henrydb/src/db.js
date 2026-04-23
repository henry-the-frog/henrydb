// db.js — HenryDB query executor
// Ties together: HeapFile, BPlusTree, SQL parser

import { HeapFile, encodeTuple, decodeTuple } from './page.js';
import { BPlusTree } from './btree.js';
import { exprContains, exprCollect } from './expr-walker.js';
import { BTreeTable } from './btree-table.js';
import { ExtendibleHashTable } from './extendible-hash.js';
import { optimizeSelect } from './decorrelate.js';
import { QueryPlanner } from './planner.js';
import { makeCompositeKey } from './composite-key.js';
import { parse } from './sql.js';
import { WriteAheadLog } from './wal.js';
import { CompiledQueryEngine } from './compiled-query.js';
import { InvertedIndex, tokenize } from './fulltext.js';
import { PlanCache } from './plan-cache.js';
import { PlanBuilder, PlanFormatter } from './query-plan.js';
import { pushdownPredicates } from './pushdown.js';
import { planToHTML } from './plan-html.js';
import { IndexAdvisor } from './index-advisor.js';
import { evalFunction as _evalFunctionImpl, jsonExtract as _jsonExtractImpl, dateArith as _dateArithImpl, likeToRegex as _likeToRegexImpl } from './sql-functions.js';
import { computeWindowFunctions as _computeWindowFunctionsImpl, exprContainsWindow as _exprContainsWindowImpl, extractWindowNodes as _extractWindowNodesImpl, columnsHaveWindow as _columnsHaveWindowImpl, validateNoNestedAggregates as _validateNoNestedAggregatesImpl, validateNoWindowInWhere as _validateNoWindowInWhereImpl, windowOrderEqual as _windowOrderEqualImpl } from './window-functions.js';
import { createTable as _createTableImpl, createTableAs as _createTableAsImpl, dropTable as _dropTableImpl } from './ddl-tables.js';
import { createIndex as _createIndexImpl, dropIndex as _dropIndexImpl } from './ddl-indexes.js';
import { createView as _createViewImpl, dropView as _dropViewImpl, alterTable as _alterTableImpl, createFunction as _createFunctionImpl, dropFunction as _dropFunctionImpl } from './ddl-misc.js';
import { insert as _insertImpl, insertSelect as _insertSelectImpl, fireTriggers as _fireTriggersImpl } from './dml-insert.js';
import { update as _updateImpl, executeDelete as _executeDeleteImpl } from './dml-mutate.js';
import { executeJoinWithRows as _executeJoinWithRowsImpl, executeJoin as _executeJoinImpl, extractEquiJoinColumns as _extractEquiJoinColumnsImpl, extractEquiJoinKey as _extractEquiJoinKeyImpl, hashJoin as _hashJoinImpl, mergeJoin as _mergeJoinImpl, estimateRowCount as _estimateRowCountImpl, compareScanCosts as _compareScanCostsImpl, compareJoinCosts as _compareJoinCostsImpl, estimateFilteredRows as _estimateFilteredRowsImpl, estimateJoinSize as _estimateJoinSizeImpl, extractJoinColumns as _extractJoinColumnsImpl, optimizeJoinOrder as _optimizeJoinOrderImpl, popcount as _popcountImpl, getTableNdv as _getTableNdvImpl } from './join-executor.js';
import { selectWithGroupBy as _selectWithGroupByImpl } from './group-by-executor.js';
import { explain as _explainImpl, explainCompiled as _explainCompiledImpl, explainAnalyze as _explainAnalyzeImpl, fillScanActuals as _fillScanActualsImpl } from './explain-executor.js';
import { formatPlan as _formatPlanImpl, planToYaml as _planToYamlImpl, planToDot as _planToDotImpl } from './plan-format.js';
import { selectInner as _selectInnerImpl } from './select-inner.js';
import { withCTEs as _withCTEsImpl, executeRecursiveCTE as _executeRecursiveCTEImpl } from './cte-executor.js';
import { selectInfoSchema as _selectInfoSchemaImpl, selectPgCatalog as _selectPgCatalogImpl, filterPgCatalogRows as _filterPgCatalogRowsImpl } from './catalog-queries.js';
import { union_ as _unionImpl, unionInner as _unionInnerImpl, intersect as _intersectImpl, except_ as _exceptImpl } from './set-operations.js';
import { recommendIndexes as _recommendIndexesImpl, applyRecommendedIndexes as _applyRecommendedIndexesImpl } from './index-advisor-impl.js';
import { merge as _mergeImpl } from './merge-executor.js';
import { handlePrepare as _handlePrepareImpl, handleExecute as _handleExecuteImpl, handleDeallocate as _handleDeallocateImpl } from './prepared-stmts.js';
import { handleSavepoint as _handleSavepointImpl, handleRollbackToSavepoint as _handleRollbackToSavepointImpl, handleReleaseSavepoint as _handleReleaseSavepointImpl } from './savepoint-handler.js';
import { handleForeignKeyDelete as _handleForeignKeyDeleteImpl, handleForeignKeyUpdate as _handleForeignKeyUpdateImpl } from './fk-cascade.js';
import { validateConstraints as _validateConstraintsImpl, validateConstraintsForUpdate as _validateConstraintsForUpdateImpl } from './constraint-validator.js';
import { handleAnalyze as _handleAnalyzeImpl, profile as _profileImpl } from './analyze-profile.js';
import { handleCheckpoint as _handleCheckpointImpl } from './checkpoint-handler.js';
import { handleVacuum as _handleVacuumImpl } from './vacuum-handler.js';
import { serialize as _serializeImpl, save as _saveImpl, bulkInsert as _bulkInsertImpl } from './serialize-handler.js';
import { acquireRowLocks as _acquireRowLocksImpl, releaseRowLocks as _releaseRowLocksImpl } from './row-lock.js';
import { executePaginated as _executePaginatedImpl } from './paginated-exec.js';
import { analyzeTable as _analyzeTableImpl } from './analyze-table.js';
import { prepareSql as _prepareSqlImpl, executePrepared as _executePreparedImpl, deallocate as _deallocateImpl, bindParams as _bindParamsImpl, prepare as _prepareImpl } from './prepared-stmts-ast.js';
import { QueryStatsCollector } from './query-stats.js';
import { installExpressionEvaluator } from './expression-evaluator.js';
import { explainPlan as volcanoExplainPlan, buildPlan as volcanoBuildPlan } from './volcano-planner.js';
import { instrumentPlan } from './volcano.js';

export class Database {
  // PostgreSQL-compatible cost model parameters
  // Tuned for in-memory database (lower random_page_cost since no disk seeks)
  static COST_MODEL = {
    seq_page_cost: 1.0,       // Sequential I/O per page
    random_page_cost: 1.1,    // Random I/O per page (nearly same as seq for in-memory)
    cpu_tuple_cost: 0.01,     // Process one row
    cpu_index_tuple_cost: 0.005, // Process one index entry
    cpu_operator_cost: 0.0025,   // Evaluate one WHERE clause
    effective_cache_size: 1000,  // Pages in cache
  };

  constructor(options = {}) {
    this.tables = new Map();  // name -> { heap, schema, indexes }
    this._functions = new Map(); // name -> { params, returnType, language, body, volatility, isProcedure }
    this._rowLocks = new Map(); // "table:pageId:slotIdx" -> { txId, mode: 'UPDATE'|'SHARE', lockCount }
    this._prepared = new Map(); // name -> { ast, sql }
    this.catalog = [];
    this.indexCatalog = new Map();  // indexName -> { table, columns, unique }
    this.views = new Map();  // viewName -> { query (AST) }
    this.sequences = new Map(); // seqName -> { current, increment, min, max }
    this.triggers = [];      // { name, timing, event, table, bodySql }
    
    // Storage factory: can be overridden for file-backed storage
    this._heapFactory = options.heapFactory || ((name) => new HeapFile(name));
    
    // WAL: if dataDir is provided, create a persistent WAL for crash recovery
    this._dataDir = options.dataDir || null;
    if (this._dataDir) {
      const walDir = this._dataDir + '/wal';
      this.wal = new WriteAheadLog(walDir, { syncMode: options.walSync || 'batch' });
      this.wal.open();
    } else {
      this.wal = new WriteAheadLog(); // No-op WAL (in-memory only)
    }
    
    this.fulltextIndexes = new Map(); // indexName → InvertedIndex
    this._nextTxId = 1;
    this._currentTxId = 0;  // 0 = auto-commit mode
    this._planCache = new PlanCache(256);
    this._indexAdvisor = new IndexAdvisor(this);
    this._queryStats = new QueryStatsCollector();
    
    // Result cache: SQL → { result, tables }
    this._resultCache = new Map();
    this._resultCacheMaxSize = 128;
    this._resultCacheHits = 0;
    this._resultCacheMisses = 0;
    
    // Prepared statements: name → { sql, ast }
    this._preparedStatements = new Map();
    
    // Table statistics: tableName → { rowCount, columns: { colName: { distinct, nulls, min, max } } }
    this._tableStats = new Map();
    
    // Savepoint stack
    this._savepoints = [];
  }

  execute(sql) {
    // Handle special commands
    const trimmed = sql.trim().toUpperCase();
    if (trimmed === 'RECOMMEND INDEXES' || trimmed === 'RECOMMEND INDEXES;') {
      return this._recommendIndexes();
    }
    if (trimmed === 'SHOW QUERY STATS' || trimmed === 'SHOW QUERY STATS;') {
      return { type: 'ROWS', rows: this._queryStats.getAll({ limit: 50 }) };
    }
    if (trimmed === 'SHOW SLOW QUERIES' || trimmed === 'SHOW SLOW QUERIES;') {
      return { type: 'ROWS', rows: this._queryStats.getSlowest(20) };
    }
    if (trimmed === 'SHOW CACHE STATS' || trimmed === 'SHOW CACHE STATS;') {
      const total = this._resultCacheHits + this._resultCacheMisses;
      return {
        type: 'ROWS',
        rows: [{
          cache_size: this._resultCache.size,
          max_size: this._resultCacheMaxSize,
          hits: this._resultCacheHits,
          misses: this._resultCacheMisses,
          hit_rate: total > 0 ? (this._resultCacheHits / total * 100).toFixed(1) + '%' : 'N/A',
        }],
      };
    }
    if (trimmed === 'CLEAR CACHE' || trimmed === 'CLEAR CACHE;') {
      this._resultCache.clear();
      this._resultCacheHits = 0;
      this._resultCacheMisses = 0;
      return { type: 'OK', message: 'Result cache cleared' };
    }
    if (trimmed === 'RESET QUERY STATS' || trimmed === 'RESET QUERY STATS;') {
      this._queryStats.reset();
      return { type: 'OK', message: 'Query statistics reset' };
    }
    if (trimmed === 'APPLY RECOMMENDED INDEXES' || trimmed === 'APPLY RECOMMENDED INDEXES;') {
      return this._applyRecommendedIndexes();
    }

    // Prepared statements
    if (trimmed.startsWith('PREPARE ')) {
      return this._handlePrepare(sql);
    }
    if (trimmed.startsWith('EXECUTE ')) {
      return this._handleExecute(sql);
    }
    if (trimmed.startsWith('DEALLOCATE ')) {
      return this._handleDeallocate(sql);
    }
    if (trimmed.startsWith('ANALYZE')) {
      // Collect internal stats first (for _estimateFilteredRows)
      const match = sql.match(/ANALYZE\s+(?:TABLE\s+)?(\w+)/i);
      const tableNames = match ? [match[1].replace(/;$/, '')] : [...this.tables.keys()];
      for (const tn of tableNames) {
        const table = this.tables.get(tn) || this.tables.get(tn.toLowerCase());
        if (!table) continue;
        const allRows = this.execute(`SELECT * FROM ${tn}`).rows;
        const rowCount = allRows.length;
        const columns = {};
        for (const col of table.schema) {
          const values = allRows.map(r => r[col.name]);
          const distinct = new Set(values.filter(v => v !== null && v !== undefined)).size;
          const nulls = values.filter(v => v === null || v === undefined).length;
          const numericVals = values.filter(v => typeof v === 'number' && !isNaN(v));
          const min = numericVals.length > 0 ? Math.min(...numericVals) : null;
          const max = numericVals.length > 0 ? Math.max(...numericVals) : null;
          // Build equi-height histogram for numeric columns
          let histogram = null;
          if (numericVals.length >= 10) {
            const sorted = [...numericVals].sort((a, b) => a - b);
            const numBuckets = Math.min(50, Math.max(10, Math.ceil(Math.sqrt(sorted.length))));
            const bucketSize = sorted.length / numBuckets;
            histogram = [];
            for (let b = 0; b < numBuckets; b++) {
              const startIdx = Math.floor(b * bucketSize);
              const endIdx = Math.floor((b + 1) * bucketSize) - 1;
              histogram.push({
                lo: sorted[startIdx],
                hi: sorted[endIdx],
                count: endIdx - startIdx + 1,
                ndv: new Set(sorted.slice(startIdx, endIdx + 1)).size,
              });
            }
          }
          columns[col.name] = { distinct, nulls, min, max, selectivity: distinct > 0 ? 1 / distinct : 1, histogram };
        }
        this._tableStats.set(tn, { rowCount, columns, analyzedAt: Date.now() });
      }
      // Return in the standard format expected by tests
      const results = tableNames.map(tn => {
        const table = this.tables.get(tn) || this.tables.get(tn.toLowerCase());
        if (!table) return null;
        const stats = this._tableStats.get(tn);
        return {
          table: tn,
          rows: stats.rowCount,
          pages: Math.ceil(stats.rowCount / 100),
          columns: Object.entries(stats.columns).map(([name, cs]) => ({
            name,
            ndv: cs.distinct,
            nulls: cs.nulls,
            min: cs.min,
            max: cs.max,
            avg_width: 8,
          })),
        };
      }).filter(Boolean);
      return {
        type: 'ANALYZE',
        tables: results,
        message: `Analyzed ${results.length} table(s): ${results.map(r => `${r.table}(${r.rows} rows)`).join(', ')}`,
      };
    }
    if (trimmed.startsWith('SAVEPOINT ')) {
      return this._handleSavepoint(sql);
    }
    if (trimmed.startsWith('ROLLBACK TO ') || trimmed.startsWith('ROLLBACK TO SAVEPOINT ')) {
      return this._handleRollbackToSavepoint(sql);
    }
    if (trimmed.startsWith('RELEASE ') || trimmed.startsWith('RELEASE SAVEPOINT ')) {
      return this._handleReleaseSavepoint(sql);
    }
    if (trimmed === 'SHOW TABLE STATS' || trimmed === 'SHOW TABLE STATS;') {
      return { type: 'ROWS', rows: Array.from(this._tableStats.entries()).map(([name, s]) => ({
        table: name, row_count: s.rowCount,
        columns: Object.keys(s.columns).length,
      })) };
    }
    if (trimmed === 'SHOW STATUS' || trimmed === 'SHOW STATUS;') {
      const tableCount = this.tables.size;
      let totalRows = 0, indexCount = 0;
      for (const [, table] of this.tables) {
        totalRows += this._estimateRowCount(table);
        indexCount += table.indexes?.size || 0;
      }
      return {
        type: 'ROWS',
        rows: [{
          tables: tableCount,
          total_rows: totalRows,
          indexes: indexCount,
          cache_size: this._resultCache.size,
          cache_hit_rate: (this._resultCacheHits + this._resultCacheMisses) > 0
            ? ((this._resultCacheHits / (this._resultCacheHits + this._resultCacheMisses)) * 100).toFixed(1) + '%'
            : 'N/A',
          prepared_statements: this._preparedStatements.size,
          savepoints: this._savepoints.length,
          analyzed_tables: this._tableStats.size,
        }],
      };
    }

    // Check plan cache first (only for SELECT)
    let ast = this._planCache.get(sql);
    if (!ast) {
      ast = parse(sql);
      // Only cache read-only queries (SELECT)
      if (ast.type === 'SELECT') {
        this._planCache.put(sql, ast);
      }
    }
    
    // Check result cache for SELECT queries (skip for non-deterministic functions)
    const hasNonDeterministic = /NEXTVAL|CURRVAL|RANDOM|NOW|CURRENT_/i.test(sql);
    if (ast.type === 'SELECT' && !sql.trim().toUpperCase().startsWith('EXPLAIN') && !hasNonDeterministic && this._currentTxId === 0) {
      const cached = this._resultCache.get(sql);
      if (cached) {
        this._resultCacheHits++;
        return cached.result;
      }
      this._resultCacheMisses++;
    }
    
    // Feed SELECTs to the index advisor
    if (ast.type === 'SELECT') {
      try { this._indexAdvisor.analyze(sql); } catch (e) { /* advisory, don't fail */ }
    }
    
    // Execute with timing for statistics
    const startTime = performance.now();
    let result;
    try {
      result = this.execute_ast(ast);
    } catch (e) {
      this._queryStats.recordError(sql);
      throw e;
    }
    const elapsed = performance.now() - startTime;
    const rowCount = result?.rows?.length || 0;
    this._queryStats.record(sql, elapsed, rowCount);
    
    // Store SELECT results in cache (only outside transactions — snapshot-isolated
    // reads must NOT populate the shared cache, as their results are tx-specific)
    if (ast.type === 'SELECT' && !sql.trim().toUpperCase().startsWith('EXPLAIN') && this._currentTxId === 0) {
      // Extract table names from the AST for invalidation
      const tables = this._extractTables(ast);
      if (this._resultCache.size >= this._resultCacheMaxSize) {
        // Evict oldest entry (first key in Map)
        const firstKey = this._resultCache.keys().next().value;
        this._resultCache.delete(firstKey);
      }
      this._resultCache.set(sql, { result, tables });
    }
    
    // Invalidate cache on write operations
    if (['INSERT', 'UPDATE', 'DELETE', 'DROP', 'DROP_TABLE', 'DROP_INDEX', 'DROP_VIEW',
         'ALTER', 'ALTER_TABLE', 'CREATE', 'CREATE_TABLE', 'CREATE_TABLE_AS', 
         'TRUNCATE', 'TRUNCATE_TABLE', 'RENAME_TABLE'].includes(ast.type)) {
      const affectedTable = ast.table || ast.name || '';
      this._invalidateCache(affectedTable);
    }
    
    return result;
  }

  // Extract table names from AST for cache invalidation
  _extractTables(ast) {
    const tables = new Set();
    if (ast.from) {
      for (const f of Array.isArray(ast.from) ? ast.from : [ast.from]) {
        if (f.table) {
          tables.add(f.table);
          // If this is a view, also track the base tables it references
          const viewDef = this.views.get(f.table);
          if (viewDef && viewDef.query) {
            const baseTables = this._extractTables(viewDef.query);
            for (const bt of baseTables) tables.add(bt);
          }
        }
        else if (typeof f === 'string') {
          tables.add(f);
          const viewDef = this.views.get(f);
          if (viewDef && viewDef.query) {
            const baseTables = this._extractTables(viewDef.query);
            for (const bt of baseTables) tables.add(bt);
          }
        }
      }
    }
    if (ast.table) tables.add(ast.table);
    return tables;
  }

  // Invalidate all cached queries that reference a given table
  _invalidateCache(tableName) {
    const lower = tableName.toLowerCase();
    for (const [sql, entry] of this._resultCache) {
      if (entry.tables.has(tableName) || entry.tables.has(lower) || sql.toLowerCase().includes(lower)) {
        this._resultCache.delete(sql);
      }
    }
  }

  // PREPARE name AS sql_with_placeholders
  _handlePrepare(sql) { return _handlePrepareImpl(this, sql); }

  // EXECUTE name (val1, val2, ...)
  _handleExecute(sql) { return _handleExecuteImpl(this, sql); }

  // DEALLOCATE name
  _handleDeallocate(sql) { return _handleDeallocateImpl(this, sql); }

  // SAVEPOINT name — save current state
  _handleSavepoint(sql) { return _handleSavepointImpl(this, sql); }

  // ROLLBACK TO [SAVEPOINT] name — restore to savepoint
  _handleRollbackToSavepoint(sql) { return _handleRollbackToSavepointImpl(this, sql); }

  // RELEASE [SAVEPOINT] name — remove savepoint
  _handleReleaseSavepoint(sql) { return _handleReleaseSavepointImpl(this, sql); }

  _handleAnalyze(sql) { return _handleAnalyzeImpl(this, sql); }

  /**
   * Execute a query with detailed timing profile.
   */
  profile(sql) { return _profileImpl(this, sql); }

  checkpoint() {
    if (this._dataDir) {
      // Build checkpoint data from current state
      const tableNames = [...this.tables.keys()];
      return this.wal.checkpoint({ tables: tableNames, txId: this._nextTxId });
    }
    return this.wal.checkpoint();
  }

  /**
   * Close the database and flush the WAL.
   */
  close() {
    if (this.wal && this.wal.close) {
      this.wal.close();
    }
  }

  /**
   * Recover database state from WAL after a crash.
   * Creates a fresh Database, replays committed transactions from the WAL.
   * @param {string} dataDir — Path to the database data directory
   * @param {object} options — Additional options
   * @returns {Database} A recovered Database instance
   */
  /**
   * Simulate crash and recover — convenience method for testing.
   */
  crashAndRecover() {
    if (!this.dataDir) throw new Error('Cannot crash and recover without dataDir');
    // Flush WAL before "crash"
    if (this.wal) this.wal.flush();
    // Create a new database from the same directory, replaying WAL
    return Database.recover(this.dataDir);
  }

  static recover(dataDir, options = {}) {
    // Import here to avoid circular dependency at module level
    // WALReplayEngine is loaded dynamically
    const db = new Database({ dataDir, ...options });
    
    // Read and replay WAL records
    // We need ALL records for DDL (CREATE TABLE), but only post-checkpoint for DML
    const allRecords = [...db.wal.reader.readRecords()];
    if (allRecords.length > 0) {
      // Use inline replay (avoid importing wal-replay to prevent circular deps)
      // Two-pass: find committed txs, then apply
      const committed = new Set();
      for (const r of allRecords) {
        if (r.type === 'COMMIT') committed.add(r.payload.txId);
      }

      // Temporarily disable WAL writing during replay to avoid re-logging
      const origWal = db.wal;
      db.wal = new WriteAheadLog(); // No-op during replay

      for (const record of allRecords) {
        const txId = record.payload?.txId;
        if (txId !== undefined && txId !== null && !committed.has(txId)) continue;

        try {
          switch (record.type) {
            case 'CREATE_TABLE': {
              const { table, columns } = record.payload;
              const colDefs = columns.map(c => typeof c === 'string' ? `${c} TEXT` : `${c.name} ${c.type || 'TEXT'}`).join(', ');
              db.execute(`CREATE TABLE ${table} (${colDefs})`);
              break;
            }
            case 'INSERT': {
              const { table, row } = record.payload;
              
              // Handle two formats:
              // 1. Named columns: { id: 1, name: 'Alice' } (from WALReplayEngine)
              // 2. Array values: { _pageId, _slotIdx, values: [1, 'Alice'] } (from appendInsert)
              if (row.values && Array.isArray(row.values)) {
                // Get column names from table schema
                const tableObj = db.tables.get(table);
                if (tableObj) {
                  const colNames = tableObj.schema.map(c => c.name);
                  const vals = row.values.map(v => v === null ? 'NULL' : typeof v === 'number' ? String(v) : `'${String(v).replace(/'/g, "''")}'`);
                  db.execute(`INSERT INTO ${table} (${colNames.join(', ')}) VALUES (${vals.join(', ')})`);
                }
              } else {
                // Named columns format
                const cols = Object.keys(row).filter(k => !k.startsWith('_'));
                const vals = cols.map(c => {
                  const v = row[c];
                  return v === null ? 'NULL' : typeof v === 'number' ? String(v) : `'${String(v).replace(/'/g, "''")}'`;
                });
                db.execute(`INSERT INTO ${table} (${cols.join(', ')}) VALUES (${vals.join(', ')})`);
              }
              break;
            }
            case 'UPDATE': {
              const { table, old: oldRow, new: newRow } = record.payload;
              const tableObj = db.tables.get(table);
              if (!tableObj) break;
              const colNames = tableObj.schema.map(c => c.name);

              // Convert array format to named format if needed
              const oldVals = oldRow.values && Array.isArray(oldRow.values) ? oldRow.values : null;
              const newVals = newRow.values && Array.isArray(newRow.values) ? newRow.values : null;

              if (newVals && oldVals) {
                const setClauses = colNames.map((c, i) => {
                  const v = newVals[i];
                  return v === null ? `${c} = NULL` : typeof v === 'number' ? `${c} = ${v}` : `${c} = '${String(v).replace(/'/g, "''")}'`;
                });
                const where = colNames.map((c, i) => {
                  const v = oldVals[i];
                  return v === null ? `${c} IS NULL` : typeof v === 'number' ? `${c} = ${v}` : `${c} = '${String(v).replace(/'/g, "''")}'`;
                });
                db.execute(`UPDATE ${table} SET ${setClauses.join(', ')} WHERE ${where.join(' AND ')}`);
              } else {
                const setClauses = Object.entries(newRow).filter(([k]) => !k.startsWith('_')).map(([c, v]) => v === null ? `${c} = NULL` : typeof v === 'number' ? `${c} = ${v}` : `${c} = '${String(v).replace(/'/g, "''")}'`);
                const where = Object.entries(oldRow).filter(([k]) => !k.startsWith('_')).map(([c, v]) => v === null ? `${c} IS NULL` : typeof v === 'number' ? `${c} = ${v}` : `${c} = '${String(v).replace(/'/g, "''")}'`);
                db.execute(`UPDATE ${table} SET ${setClauses.join(', ')} WHERE ${where.join(' AND ')}`);
              }
              break;
            }
            case 'DELETE': {
              const { table, row } = record.payload;
              const tableObj = db.tables.get(table);
              if (!tableObj) break;

              if (row.values && Array.isArray(row.values)) {
                const colNames = tableObj.schema.map(c => c.name);
                const where = colNames.map((c, i) => {
                  const v = row.values[i];
                  return v === null ? `${c} IS NULL` : typeof v === 'number' ? `${c} = ${v}` : `${c} = '${String(v).replace(/'/g, "''")}'`;
                });
                db.execute(`DELETE FROM ${table} WHERE ${where.join(' AND ')}`);
              } else {
                const where = Object.entries(row).filter(([k]) => !k.startsWith('_')).map(([c, v]) => v === null ? `${c} IS NULL` : typeof v === 'number' ? `${c} = ${v}` : `${c} = '${String(v).replace(/'/g, "''")}'`);
                db.execute(`DELETE FROM ${table} WHERE ${where.join(' AND ')}`);
              }
              break;
            }
            case 'TRUNCATE': {
              const table = record.payload?.table;
              if (table && db.tables.has(table)) {
                db.execute(`DELETE FROM ${table} WHERE 1=1`);
              }
              break;
            }
            case 'DROP_TABLE': {
              const table = record.payload?.table;
              if (table && db.tables.has(table)) {
                db.execute(`DROP TABLE ${table}`);
              }
              break;
            }
            case 'DDL': {
              const sql = record.payload?.sql;
              if (sql) {
                try { db.execute(sql); } catch {}
              }
              break;
            }
          }
        } catch (e) {
          // Skip errors during replay — best effort
        }
      }

      // Restore real WAL
      db.wal = origWal;
    }

    return db;
  }

  getWALRecords() {
    return this.wal.getRecords();
  }

  planCacheStats() {
    return this._planCache.stats();
  }

  _recommendIndexes() { return _recommendIndexesImpl(this); }

  _applyRecommendedIndexes(minLevel = 'medium') { return _applyRecommendedIndexesImpl(this, minLevel); }

  /**
   * Serialize the entire database to a JSON-compatible object.
   * Includes table schemas, data, views, triggers.
   */
  serialize() { return _serializeImpl(this); }

  save(path) { return _saveImpl(this, path); }

  bulkInsert(tableName, rows) { return _bulkInsertImpl(this, tableName, rows); }

  /**
   * Execute a query and return paginated results.
   * 
   * @param {string} sql - SQL query
   * @param {number} page - Page number (1-indexed)
   * @param {number} pageSize - Rows per page
   * @returns {Object} Paginated result
   */
  executePaginated(sql, page = 1, pageSize = 100) { return _executePaginatedImpl(this, sql, page, pageSize); }

  static fromSerialized(data) {
    const obj = typeof data === 'string' ? JSON.parse(data) : data;
    const db = new Database();
    
    // Restore tables
    for (const [name, tableData] of Object.entries(obj.tables)) {
      // Create table from schema
      const schema = tableData.schema;
      const heap = db._heapFactory(name);
      const indexes = new Map();
      const tableObj = { schema, heap, indexes };
      db.tables.set(name, tableObj);
      
      // Insert rows
      for (const values of tableData.rows) {
        heap.insert(values);
      }
      
      // Rebuild indexes
      for (const colName of tableData.indexes || []) {
        const colIdx = schema.findIndex(c => c.name === colName);
        if (colIdx >= 0) {
          const index = new BPlusTree(32);
          for (const { pageId, slotIdx, values } of heap.scan()) {
            index.insert(values[colIdx], { pageId, slotIdx });
          }
          indexes.set(colName, index);
        }
      }
      
      // Restore index metadata
      if (tableData.indexMeta) {
        if (!tableObj.indexMeta) tableObj.indexMeta = new Map();
        for (const [key, meta] of Object.entries(tableData.indexMeta)) {
          tableObj.indexMeta.set(key, meta);
        }
      }
    }
    
    // Restore views
    for (const [name, view] of Object.entries(obj.views || {})) {
      db.views.set(name, view);
    }
    
    // Restore triggers
    db.triggers = obj.triggers || [];
    
    // Restore sequences
    for (const [name, seq] of Object.entries(obj.sequences || {})) {
      db.sequences.set(name, { ...seq });
    }
    
    // Restore materialized views
    for (const [name, mv] of Object.entries(obj.materializedViews || {})) {
      if (!db.materializedViews) db.materializedViews = new Map();
      db.materializedViews.set(name, mv);
    }
    
    // Restore comments
    for (const [key, val] of Object.entries(obj.comments || {})) {
      if (!db._comments) db._comments = new Map();
      db._comments.set(key, val);
    }
    
    // Restore indexCatalog (for composite unique/PK constraints)
    for (const [name, meta] of Object.entries(obj.indexCatalog || {})) {
      db.indexCatalog.set(name, meta);
    }
    
    return db;
  }

  prepare(sql) {
    const ast = parse(sql);
    const db = this;
    return {
      execute(params = []) {
        // Clone AST and substitute $1, $2, etc with actual values
        const cloned = JSON.parse(JSON.stringify(ast));
        const substitute = (node) => {
          if (!node || typeof node !== 'object') return node;
          if (node.type === 'PARAM') {
            const idx = node.index - 1;
            return { type: 'literal', value: params[idx] };
          }
          for (const key of Object.keys(node)) {
            if (Array.isArray(node[key])) {
              node[key] = node[key].map(substitute);
            } else if (typeof node[key] === 'object' && node[key] !== null) {
              node[key] = substitute(node[key]);
            }
          }
          return node;
        };
        substitute(cloned);
        return db.execute_ast(cloned);
      }
    };
  }

  // Execute a function with CTEs registered as temporary views
  _withCTEs(ast, fn) { return _withCTEsImpl(this, ast, fn); }

  execute_ast(ast) {
    switch (ast.type) {
      case 'CREATE_TABLE': this._planCache.invalidateAll(); return this._createTable(ast);
      case 'CREATE_TABLE_AS': this._planCache.invalidateAll(); return this._createTableAs(ast);
      case 'ALTER_TABLE': this._planCache.invalidateAll(); return this._alterTable(ast);
      case 'DROP_TABLE': this._planCache.invalidateAll(); return this._dropTable(ast);
      case 'TRUNCATE_TABLE': {
        const table = this.tables.get(ast.table);
        if (!table) throw new Error(`Table ${ast.table} not found`);
        // WAL: log the truncate for crash recovery
        const truncTxId = this._nextTxId++;
        this.wal.appendTruncate(truncTxId, ast.table);
        this.wal.appendCommit(truncTxId);
        table.heap = this._heapFactory(ast.table);
        // Rebuild indexes (empty)
        for (const [colName] of table.indexes) {
          table.indexes.set(colName, new BPlusTree(32));
        }
        this._planCache.invalidateAll();
        return { type: 'OK', message: `Table ${ast.table} truncated` };
      }
      case 'RENAME_TABLE': {
        const table = this.tables.get(ast.from);
        if (!table) throw new Error(`Table ${ast.from} not found`);
        if (this.tables.has(ast.to)) throw new Error(`Table ${ast.to} already exists`);
        // WAL: log rename for crash recovery
        if (this.wal && this.wal.logDDL) {
          this.wal.logDDL(`ALTER TABLE ${ast.from} RENAME TO ${ast.to}`);
        }
        this.tables.set(ast.to, table);
        this.tables.delete(ast.from);
        if (table.heap) table.heap.name = ast.to;
        this._planCache.invalidateAll();
        return { type: 'OK', message: `Table ${ast.from} renamed to ${ast.to}` };
      }
      case 'SHOW_TABLES': {
        const rows = [];
        for (const [name, table] of this.tables) {
          let count = 0;
          for (const _ of table.heap.scan()) count++;
          rows.push({ table_name: name, columns: table.schema.length, rows: count, indexes: table.indexes.size });
        }
        return { type: 'ROWS', rows };
      }
      case 'SHOW_CREATE_TABLE': {
        const table = this.tables.get(ast.table);
        if (!table) throw new Error(`Table ${ast.table} not found`);
        const cols = table.schema.map(c => {
          let def = `${c.name} ${c.type}`;
          if (c.primaryKey) def += ' PRIMARY KEY';
          if (c.notNull) def += ' NOT NULL';
          return def;
        });
        const sql = `CREATE TABLE ${ast.table} (${cols.join(', ')})`;
        return { type: 'ROWS', rows: [{ sql }] };
      }
      case 'SHOW_COLUMNS': {
        const table = this.tables.get(ast.table);
        if (!table) throw new Error(`Table ${ast.table} not found`);
        const rows = table.schema.map(c => ({
          column_name: c.name,
          type: c.type,
          primary_key: c.primaryKey || false,
          not_null: c.notNull || false,
          default_value: c.defaultValue || null,
        }));
        return { type: 'ROWS', rows };
      }
      case 'SHOW_INDEXES': {
        const tableName = ast.table;
        const rows = [];
        for (const [name, idx] of this.indexCatalog.entries()) {
          if (idx.table === tableName) {
            rows.push({
              index_name: name,
              table_name: idx.table,
              columns: idx.columns.join(', '),
              unique: idx.unique || false,
              type: 'btree',
            });
          }
        }
        return { type: 'ROWS', rows };
      }
      case 'CREATE_INDEX': return this._createIndex(ast);
      case 'DROP_INDEX': return this._dropIndex(ast);
      case 'ALTER_TABLE': return this._alterTable(ast);
      case 'CREATE_VIEW': return this._createView(ast);
      case 'CREATE_MATVIEW': return this._createMatView(ast);
      case 'CREATE_FUNCTION': return this._createFunction(ast);
      case 'CALL': return this._callProcedure(ast);
      case 'CREATE_EXTENSION': return { type: 'OK', message: `CREATE EXTENSION ${ast.name}` };
      case 'DROP_EXTENSION': return { type: 'OK', message: `DROP EXTENSION ${ast.name}` };
      case 'CREATE_SCHEMA': return { type: 'OK', message: `CREATE SCHEMA ${ast.name}` };
      case 'DROP_SCHEMA': return { type: 'OK', message: `DROP SCHEMA ${ast.name}` };
      case 'GRANT': return { type: 'OK', message: 'GRANT' };
      case 'REVOKE': return { type: 'OK', message: 'REVOKE' };
      case 'DROP_FUNCTION': return this._dropFunction(ast);
      case 'ANALYZE': return this._analyzeTable(ast);
      case 'CREATE_TRIGGER': {
        this.triggers.push({
          name: ast.name,
          timing: ast.timing,
          event: ast.event,
          table: ast.table,
          bodySql: ast.bodySql,
        });
        return { type: 'OK', message: `Trigger ${ast.name} created` };
      }
      case 'CREATE_FULLTEXT_INDEX': {
        const table = this.tables.get(ast.table);
        if (!table) throw new Error(`Table ${ast.table} not found`);
        const colIdx = table.schema.findIndex(c => c.name === ast.column);
        if (colIdx === -1) throw new Error(`Column ${ast.column} not found`);
        
        const ftIdx = new InvertedIndex(ast.name, ast.table, ast.column);
        
        // Index existing rows
        let docId = 0;
        for (const { values } of table.heap.scan()) {
          ftIdx.addDocument(docId++, String(values[colIdx] || ''));
        }
        
        this.fulltextIndexes.set(ast.name, ftIdx);
        return { type: 'OK', message: `Fulltext index ${ast.name} created with ${docId} documents` };
      }
      case 'REFRESH_MATVIEW': return this._refreshMatView(ast);
      case 'DROP_VIEW': return this._dropView(ast);
      case 'INSERT': return this._withCTEs(ast, () => this._insert(ast));
      case 'INSERT_SELECT': return this._withCTEs(ast, () => this._insertSelect(ast));
      case 'SELECT': {
        const result = this._select(ast);
        // FOR UPDATE/SHARE: acquire row-level locks after selecting
        if (ast.forUpdate && result.rows && result.rows.length > 0) {
          this._acquireRowLocks(ast, result.rows);
        }
        return result;
      }
      case 'VALUES': return this._values(ast);
      case 'UNION': return this._union(ast);
      case 'INTERSECT': return this._intersect(ast);
      case 'EXCEPT': return this._except(ast);
      case 'UPDATE': return this._withCTEs(ast, () => this._update(ast));
      case 'DELETE': return this._withCTEs(ast, () => this._delete(ast));
      case 'TRUNCATE': return this._truncate(ast);
      case 'MERGE': return this._merge(ast);
      case 'CREATE_SEQUENCE': return this._createSequence(ast);
      case 'SHOW_TABLES': return this._showTables();
      case 'COMMENT_ON': return this._commentOn(ast);
      case 'DESCRIBE': return this._describe(ast);
      case 'EXPLAIN': return this._explain(ast);
      case 'BEGIN': {
        this._inTransaction = true;
        // Assign a transaction ID so WAL records are grouped under this tx
        this._currentTxId = this._nextTxId++;
        // Auto-create internal savepoint for transaction rollback
        this._handleSavepoint('SAVEPOINT __txn_begin__');
        return { type: 'OK', message: 'BEGIN' };
      }
      case 'COMMIT': {
        // Write COMMIT record to WAL so recovery knows this tx is committed
        const commitTxId = this._currentTxId;
        if (this._currentTxId && this.wal) {
          this.wal.appendCommit(this._currentTxId);
        }
        // Release row locks held by this transaction
        if (commitTxId) this._releaseRowLocks(commitTxId);
        this._currentTxId = 0;
        this._inTransaction = false;
        // Remove internal savepoint on commit
        const idx = this._savepoints.findLastIndex(sp => sp.name === '__txn_begin__');
        if (idx >= 0) this._savepoints.splice(idx, 1);
        return { type: 'OK', message: 'COMMIT' };
      }
      case 'ROLLBACK': {
        // Write ABORT record to WAL so recovery knows to skip this tx
        const rollbackTxId = this._currentTxId;
        if (this._currentTxId && this.wal) {
          this.wal.appendAbort(this._currentTxId);
        }
        // Restore from internal savepoint
        const idx = this._savepoints.findLastIndex(sp => sp.name === '__txn_begin__');
        if (idx >= 0) {
          this._handleRollbackToSavepoint('ROLLBACK TO __txn_begin__');
          this._savepoints.splice(idx, 1);
        }
        // Release row locks held by this transaction
        if (rollbackTxId) this._releaseRowLocks(rollbackTxId);
        this._currentTxId = 0;
        this._inTransaction = false;
        return { type: 'OK', message: 'ROLLBACK' };
      }
      case 'VACUUM': return this._vacuum(ast);
      case 'PREPARE': return this._prepareSql(ast);
      case 'EXECUTE_PREPARED': return this._executePrepared(ast);
      case 'DEALLOCATE': return this._deallocate(ast);
      case 'CHECKPOINT': return this._checkpoint(ast);
      case 'ANALYZE_TABLE': return this._analyzeTable(ast);
      default: throw new Error(`Unknown statement: ${ast.type}`);
    }
  }

  _createTable(ast) { return _createTableImpl(this, ast); }

  _values(ast) {
    const rows = [];
    const numCols = ast.tuples[0]?.length || 0;
    const colNames = Array.from({ length: numCols }, (_, i) => `column${i + 1}`);
    for (const tuple of ast.tuples) {
      const row = {};
      for (let i = 0; i < colNames.length; i++) {
        row[colNames[i]] = tuple[i]?.value ?? this._evalValue(tuple[i], {});
      }
      rows.push(row);
    }
    return { type: 'ROWS', rows };
  }

  _createTableAs(ast) { return _createTableAsImpl(this, ast); }

  _logAlterTableDDL(ast) {
    if (!this.wal || !this.wal.logDDL) return;
    switch (ast.action) {
      case 'ADD_COLUMN': {
        const colName = typeof ast.column === 'string' ? ast.column : (ast.column?.name || 'unknown');
        const type = ast.dataType || (typeof ast.column === 'object' ? ast.column?.type : null) || 'TEXT';
        let ddl = `ALTER TABLE ${ast.table} ADD COLUMN ${colName} ${type}`;
        // Extract default value from AST (can be ast.defaultValue or ast.column.default)
        const defVal = ast.defaultValue ?? (typeof ast.column === 'object' ? ast.column?.default : null);
        if (defVal !== undefined && defVal !== null) {
          const dv = typeof defVal === 'object' && defVal.value !== undefined
            ? (typeof defVal.value === 'string' ? `'${defVal.value}'` : defVal.value)
            : (typeof defVal === 'string' ? `'${defVal}'` : defVal);
          ddl += ` DEFAULT ${dv}`;
        }
        this.wal.logDDL(ddl);
        break;
      }
      case 'DROP_COLUMN': {
        const colName = typeof ast.column === 'string' ? ast.column : (ast.column?.name || ast.columnName);
        this.wal.logDDL(`ALTER TABLE ${ast.table} DROP COLUMN ${colName}`);
        break;
      }
      case 'RENAME_COLUMN': {
        const oldName = ast.oldName || ast.column?.oldName;
        const newName = ast.newName || ast.column?.newName;
        this.wal.logDDL(`ALTER TABLE ${ast.table} RENAME COLUMN ${oldName} TO ${newName}`);
        break;
      }
      case 'RENAME_TABLE':
        this.wal.logDDL(`ALTER TABLE ${ast.table} RENAME TO ${ast.newName}`);
        break;
    }
  }

  // NOTE: _alterTable is defined later (line ~1729) with the full implementation
  // that handles FileBackedHeap correctly (using updateInPlace + delete/re-insert).

  _dropTable(ast) { return _dropTableImpl(this, ast); }

  _createIndex(ast) { return _createIndexImpl(this, ast); }

  _logCreateIndexDDL(ast) {
    if (!this.wal || !this.wal.logDDL) return;
    const unique = ast.unique ? 'UNIQUE ' : '';
    const ifNotExists = ast.ifNotExists ? 'IF NOT EXISTS ' : '';
    const cols = ast.columns.join(', ');
    this.wal.logDDL(`CREATE ${unique}INDEX ${ifNotExists}${ast.name} ON ${ast.table} (${cols})`);
  }

  _dropIndex(ast) { return _dropIndexImpl(this, ast); }

  /**
   * Acquire row-level locks for SELECT FOR UPDATE/SHARE.
   * Re-scans the heap to find RIDs for the selected rows.
   */
  _acquireRowLocks(ast, rows) { return _acquireRowLocksImpl(this, ast, rows); }

  _releaseRowLocks(txId) { return _releaseRowLocksImpl(this, txId); }

  /**
   * ANALYZE: gather table statistics for query optimization.
   * Computes per-column: ndistinct, nullFraction, min, max, mostCommonValues.
   */
  _createFunction(ast) { return _createFunctionImpl(this, ast); }

  _dropFunction(ast) { return _dropFunctionImpl(this, ast); }

  _callProcedure(ast) {
    const funcDef = this._functions.get(ast.name.toLowerCase());
    if (!funcDef) throw new Error(`Procedure ${ast.name} not found`);
    
    const args = ast.args.map(a => this._evalValue(a, {}));
    
    // Execute procedure body
    let body = funcDef.body;
    for (let i = 0; i < funcDef.params.length; i++) {
      const param = funcDef.params[i];
      const val = args[i];
      const regex = new RegExp('\\b' + param.name + '\\b', 'gi');
      if (val === null) {
        body = body.replace(regex, 'NULL');
      } else if (typeof val === 'number') {
        body = body.replace(regex, String(val));
      } else {
        body = body.replace(regex, `'${String(val).replace(/'/g, "''")}'`);
      }
    }
    
    return this.execute(body);
  }

  /**
   * Evaluate a user-defined SQL function call.
   * Substitutes parameter values into the body expression and evaluates it.
   */
  _callUserFunction(funcDef, args) {
    if (funcDef.language === 'sql') {
      let body = funcDef.body;
      
      // Handle RETURN expr → SELECT expr
      if (body.toUpperCase().startsWith('RETURN ')) {
        body = 'SELECT ' + body.substring(7);
      }
      
      if (body.toUpperCase().startsWith('SELECT')) {
        // Substitute params
        for (let i = 0; i < funcDef.params.length; i++) {
          const param = funcDef.params[i];
          const val = args[i];
          const regex = new RegExp('\\b' + param.name + '\\b', 'gi');
          if (val === null) {
            body = body.replace(regex, 'NULL');
          } else if (typeof val === 'number') {
            body = body.replace(regex, String(val));
          } else {
            body = body.replace(regex, `'${String(val).replace(/'/g, "''")}'`);
          }
        }
        const result = this.execute(body);
        const rows = result.rows || result;

        // Table-returning function: return the full result set
        if (funcDef.returnType === 'TABLE') {
          return { type: 'TABLE_RESULT', rows: rows || [] };
        }

        // Scalar function: return first column of first row
        if (!rows || rows.length === 0) return null;
        const firstRow = rows[0];
        const keys = Object.keys(firstRow);
        return firstRow[keys[0]];
      }
      throw new Error(`Function body must start with SELECT: ${body}`);
    } else if (funcDef.language === 'js') {
      const paramNames = funcDef.params.map(p => p.name);
      try {
        const fn = new Function(...paramNames, `return ${funcDef.body}`);
        return fn(...args);
      } catch (e) {
        throw new Error(`Error in JS function: ${e.message}`);
      }
    }
    throw new Error(`Unsupported function language: ${funcDef.language}`);
  }

  _createView(ast) { return _createViewImpl(this, ast); }

  _createMatView(ast) {
    // Execute the query and store results as a materialized table
    const result = this._select(ast.query);
    
    if (result.rows.length === 0) {
      this.views.set(ast.name, { query: ast.query, materializedRows: [], isMaterialized: true });
      return { type: 'OK', message: `Materialized view ${ast.name} created (empty)` };
    }

    const firstRow = result.rows[0];
    const schema = Object.keys(firstRow).filter(k => !k.includes('.')).map(name => ({
      name,
      type: typeof firstRow[name] === 'number' ? 'INT' : 'TEXT',
      primaryKey: false,
    }));

    // Store as a real table + view metadata
    const heap = this._heapFactory(ast.name);
    const indexes = new Map();
    const tableObj = { schema, heap, indexes };
    this.tables.set(ast.name, tableObj);

    for (const row of result.rows) {
      const values = schema.map(col => row[col.name]);
      this._insertRow(tableObj, null, values);
    }

    // Also store the query for REFRESH
    this.views.set(ast.name, { query: ast.query, isMaterialized: true });

    return { type: 'OK', message: `Materialized view ${ast.name} created with ${result.rows.length} rows` };
  }

  _refreshMatView(ast) {
    const viewDef = this.views.get(ast.name);
    if (!viewDef || !viewDef.isMaterialized) {
      throw new Error(`${ast.name} is not a materialized view`);
    }

    // Re-execute the query
    const result = this._select(viewDef.query);
    
    // Replace the table data
    const table = this.tables.get(ast.name);
    if (table) {
      // Clear old data — delete all existing rows
      if (table.heap.truncate) {
        table.heap.truncate(); // FileBackedHeap: clear all pages
      } else {
        // In-memory HeapFile: replace with new one
        table.heap = this._heapFactory(ast.name);
      }
      
      // Re-insert new data
      for (const row of result.rows) {
        const values = table.schema.map(col => row[col.name]);
        this._insertRow(table, null, values);
      }
    }

    // Invalidate result cache since materialized view data changed
    if (this._resultCache) this._resultCache.clear();

    return { type: 'OK', message: `Materialized view ${ast.name} refreshed with ${result.rows.length} rows` };
  }

  _dropView(ast) { return _dropViewImpl(this, ast); }

  _alterTable(ast) { return _alterTableImpl(this, ast); }

  _insert(ast) { return _insertImpl(this, ast); }

  _insertSelect(ast) { return _insertSelectImpl(this, ast); }

  // Validate column constraints (NOT NULL, CHECK) for a row
  _fireTriggers(event, table, row, newRow, schema) { return _fireTriggersImpl(this, event, table, row, newRow, schema); }

  _validateConstraints(table, values) { return _validateConstraintsImpl(this, table, values); }

  _validateConstraintsForUpdate(table, values, excludeRid, oldValues) { return _validateConstraintsForUpdateImpl(this, table, values, excludeRid, oldValues); }

  /**
   * Check if ORDER BY sort can be eliminated because the table's storage engine
   * already provides the required ordering (e.g., BTreeTable sorted by PK).
   * Returns true if sort can be skipped.
   */
  _canEliminateSort(ast) {
    if (!ast.orderBy || ast.orderBy.length === 0) return false;
    
    // Only works for single-table queries (no JOINs)
    if (ast.joins && ast.joins.length > 0) return false;
    
    const tableName = ast.from?.table;
    if (!tableName) return false;
    
    const tableInfo = this.tables.get(tableName);
    if (!tableInfo) return false;
    
    // Check if storage engine is BTreeTable
    if (!(tableInfo.heap instanceof BTreeTable)) return false;
    
    const btreeTable = tableInfo.heap;
    const pkIndices = btreeTable.pkIndices;
    
    // Get PK column name(s)
    const pkColNames = pkIndices.map(i => tableInfo.schema[i]?.name);
    
    // ORDER BY must match PK column(s) in order, and all ASC
    // (BTreeTable stores in ascending PK order)
    if (ast.orderBy.length !== pkColNames.length) return false;
    
    for (let i = 0; i < ast.orderBy.length; i++) {
      const orderCol = ast.orderBy[i].column;
      const dir = ast.orderBy[i].direction || 'ASC';
      if (orderCol !== pkColNames[i]) return false;
      if (dir !== 'ASC') return false;
    }
    
    return true;
  }

  _applySelectColumns(ast, rows) {
    // Pre-compute SELECT alias expressions for ORDER BY access
    if (ast.orderBy && ast.columns) {
      this._preComputeOrderByAliases(ast, rows);
    }
    
    // Apply ORDER BY (with sort elimination for BTree tables)
    if (ast.orderBy && !this._canEliminateSort(ast)) {
      rows.sort((a, b) => {
        for (const { column, direction } of ast.orderBy) {
          const av = this._orderByValue(column, a);
          const bv = this._orderByValue(column, b);
          // NULL handling: NULL is smaller than any value (SQLite behavior)
          const aNull = av === null || av === undefined;
          const bNull = bv === null || bv === undefined;
          if (aNull && bNull) continue;
          if (aNull) return direction === 'DESC' ? 1 : -1; // null is smallest
          if (bNull) return direction === 'DESC' ? -1 : 1;
          if (av < bv) return direction === 'DESC' ? 1 : -1;
          if (av > bv) return direction === 'DESC' ? -1 : 1;
        }
        return 0;
      });
    }
    // Apply LIMIT/OFFSET
    if (ast.offset) rows = rows.slice(Math.max(0, ast.offset));
    if (ast.limit != null) rows = rows.slice(0, ast.limit);
    
    // Apply SELECT columns
    const isStar = ast.columns.length === 1 && (ast.columns[0].name === '*' || ast.columns[0].type === 'star');
    const hasQualifiedStar = ast.columns.some(c => c.type === 'qualified_star');
    if (isStar) {
      // For SELECT *, handle column name collisions from joins
      rows = rows.map(row => this._projectStarRow(row));
    } else {
      // Non-star SELECT: project specific columns
      rows = rows.map(row => {
        const result = {};
        for (const col of ast.columns) {
          const alias = col.alias || col.name;
          if (col.type === 'column') {
            result[alias] = row[col.name];
          } else if (col.type === 'expression' || col.type === 'aggregate') {
            result[alias] = this._evalValue(col.expr || col, row);
          } else if (col.type === 'function_call') {
            result[alias] = this._evalValue(col, row);
          } else if (col.type === 'window') {
            // Window function values are pre-computed by _computeWindowFunctions
            result[alias] = row[`__window_${alias}`];
          } else {
            result[alias] = this._evalValue(col, row);
          }
        }
        return result;
      });
    }
    return { type: 'ROWS', rows };
  }

  _resolveDefault(defaultValue) {
    if (defaultValue == null) return null;
    if (typeof defaultValue === 'object' && defaultValue.type) {
      // It's an expression node — evaluate it
      try { return this._evalValue(defaultValue, {}); } catch { return null; }
    }
    return defaultValue;
  }

  _resolveReturning(returning, rows) {
    if (returning === '*') return rows;
    return rows.map(row => {
      const filtered = {};
      for (const col of returning) {
        if (typeof col === 'string') {
          filtered[col] = row[col];
        } else if (col.expr && col.alias) {
          filtered[col.alias] = this._evalValue(col.expr, row);
        } else if (col.type === 'column_ref') {
          const name = col.column || col.name;
          filtered[name] = row[name];
        } else if (col.type) {
          const val = this._evalValue(col, row);
          const name = col.column || col.name || `expr_${Object.keys(filtered).length}`;
          filtered[name] = val;
        } else {
          filtered[col] = row[col];
        }
      }
      return filtered;
    });
  }

  _orderValues(table, columns, values, resolveDefaults = false) {
    if (columns) {
      const ordered = new Array(table.schema.length).fill(null);
      if (resolveDefaults) {
        for (let i = 0; i < table.schema.length; i++) {
          if (table.schema[i].defaultValue != null) ordered[i] = this._resolveDefault(table.schema[i].defaultValue);
        }
      }
      for (let i = 0; i < columns.length; i++) {
        const colIdx = table.schema.findIndex(c => c.name.toLowerCase() === columns[i].toLowerCase());
        if (colIdx >= 0) ordered[colIdx] = values[i];
      }
      return ordered;
    }
    return values;
  }

  _insertRow(table, columns, values) {
    let orderedValues;
    if (columns) {
      orderedValues = new Array(table.schema.length).fill(null);
      // Apply default values first
      for (let i = 0; i < table.schema.length; i++) {
        if (table.schema[i].defaultValue !== undefined && table.schema[i].defaultValue !== null) {
          orderedValues[i] = this._resolveDefault(table.schema[i].defaultValue);
        }
      }
      for (let i = 0; i < columns.length; i++) {
        const colIdx = table.schema.findIndex(c => c.name.toLowerCase() === columns[i].toLowerCase());
        if (colIdx === -1) throw new Error(`Column ${columns[i]} not found`);
        orderedValues[colIdx] = values[i];
      }
    } else {
      orderedValues = values;
      // Pad short value arrays with defaults (e.g., after ALTER TABLE ADD COLUMN)
      if (orderedValues.length < table.schema.length) {
        orderedValues = [...orderedValues];
        for (let i = orderedValues.length; i < table.schema.length; i++) {
          if (table.schema[i].defaultValue !== undefined && table.schema[i].defaultValue !== null) {
            orderedValues[i] = this._resolveDefault(table.schema[i].defaultValue);
          } else {
            orderedValues[i] = null;
          }
        }
      }
    }

    // SERIAL auto-increment: assign next value for SERIAL columns with null value
    for (let i = 0; i < table.schema.length; i++) {
      if (table.schema[i].type === 'SERIAL' && (orderedValues[i] === null || orderedValues[i] === undefined)) {
        if (!table._serialCounters) table._serialCounters = {};
        if (!table._serialCounters[i]) {
          // Find max existing value
          let max = 0;
          for (const tuple of table.heap.scan()) {
            const v = tuple.values ? tuple.values[i] : tuple[i];
            if (typeof v === 'number' && v > max) max = v;
          }
          table._serialCounters[i] = max;
        }
        table._serialCounters[i]++;
        orderedValues[i] = table._serialCounters[i];
      } else if (table.schema[i].type === 'SERIAL' && typeof orderedValues[i] === 'number') {
        // Explicit value provided — update counter to at least this value
        if (!table._serialCounters) table._serialCounters = {};
        if (!table._serialCounters[i] || orderedValues[i] > table._serialCounters[i]) {
          table._serialCounters[i] = orderedValues[i];
        }
      }
    }

    // Validate constraints
    // Handle SERIAL columns: auto-increment if null
    for (let i = 0; i < table.schema.length; i++) {
      if (table.schema[i].serial && (orderedValues[i] === null || orderedValues[i] === undefined)) {
        const seqName = table.schema[i].serial.toLowerCase();
        const seq = this.sequences.get(seqName);
        if (seq) {
          seq.current += seq.increment;
          orderedValues[i] = seq.current;
        }
      } else if (table.schema[i].serial) {
        // Explicit value — advance sequence past it
        if (typeof orderedValues[i] === 'number') {
          const seqName = table.schema[i].serial.toLowerCase();
          const seq = this.sequences.get(seqName);
          if (seq && orderedValues[i] > seq.current) {
            seq.current = orderedValues[i];
          }
        }
      }
    }

    // Compute generated/computed columns
    for (let i = 0; i < table.schema.length; i++) {
      if (table.schema[i].generated) {
        // Build a row object for expression evaluation
        const row = {};
        for (let j = 0; j < table.schema.length; j++) {
          row[table.schema[j].name] = orderedValues[j];
        }
        orderedValues[i] = this._evalValue(table.schema[i].generated, row);
      }
    }

    this._validateConstraints(table, orderedValues);

    // BEFORE INSERT triggers
    const tableName = table.heap?.name || '';
    this._fireTriggers('BEFORE', 'INSERT', tableName, orderedValues, table.schema);

    const rid = table.heap.insert(orderedValues);

    // WAL: log the insert
    const txId = this._currentTxId || this._batchTxId || this._nextTxId++;
    this.wal.appendInsert(txId, tableName, rid.pageId, rid.slotIdx, orderedValues);
    if (!this._currentTxId && !this._batchTxId) {
      // Auto-commit mode (single row): immediately commit
      this.wal.appendCommit(txId);
    }

    // Update indexes
    for (const [colName, index] of table.indexes) {
      const colIdx = table.schema.findIndex(c => c.name === colName);
      if (index._isHash) {
        const existing = index.get(orderedValues[colIdx]);
        if (existing !== undefined) {
          const arr = Array.isArray(existing) ? existing : [existing];
          arr.push(rid);
          index.insert(orderedValues[colIdx], arr);
        } else {
          index.insert(orderedValues[colIdx], rid);
        }
      } else {
        index.insert(orderedValues[colIdx], rid);
      }
    }

    // AFTER INSERT triggers
    this._fireTriggers('AFTER', 'INSERT', tableName, orderedValues, table.schema);

    return rid;
  }

  _select(ast) {
    // Handle information_schema queries
    if (ast.from) {
      const tableName = (ast.from.table || '').toLowerCase();
      if (tableName.startsWith('information_schema.')) {
        return this._selectInfoSchema(ast);
      }
      if (tableName.startsWith('pg_catalog.') || tableName === 'pg_tables' || tableName === 'pg_indexes' || tableName === 'pg_stat_user_tables') {
        return this._selectPgCatalog(ast);
      }
    }
    // Handle CTEs — register as temporary views
    const tempViews = [];
    if (ast.ctes) {
      for (const cte of ast.ctes) {
        if (this.views.has(cte.name)) throw new Error(`CTE name ${cte.name} conflicts with existing view`);
        
        if (cte.recursive && (cte.unionQuery || cte.query.type === 'UNION')) {
          // Recursive CTE: iterate until fixed point
          const allRows = this._executeRecursiveCTE(cte);
          this.views.set(cte.name, { materializedRows: allRows, isCTE: true });
        } else if (cte.unionQuery || cte.query.type === 'UNION') {
          // Non-recursive CTE with UNION: materialize by executing both parts
          const leftResult = this._select(cte.query);
          const rightResult = this.execute_ast(cte.unionQuery);
          const allRows = [...leftResult.rows, ...rightResult.rows];
          this.views.set(cte.name, { materializedRows: allRows, isCTE: true });
        } else {
          this.views.set(cte.name, { query: cte.query, isCTE: true });
        }
        tempViews.push(cte.name);
      }
    }

    try {
      // Optimize: decorrelate subqueries
      const optimizedAst = optimizeSelect(ast, this);
      
      // Try Volcano engine first (faster for many patterns)
      // Skip Volcano when inside a transaction (MVCC not yet supported)
      let result = null;
      if (this._useVolcano !== false && this._currentTxId === 0) {
        try {
                    result = this._tryVolcanoSelect(optimizedAst);
        } catch (e) {
          // Volcano failed — fall through to legacy
          result = null;
        }
      }
      
      if (!result) {
        result = this._selectInner(optimizedAst);
      }
      
      // PIVOT: transform rows into crosstab
      if (ast.pivot) {
        result = this._applyPivot(result, ast.pivot);
      }
      // UNPIVOT: transform columns into rows
      if (ast.unpivot) {
        result = this._applyUnpivot(result, ast.unpivot);
      }
      
      return result;
    } finally {
      // Clean up temporary CTE views
      for (const name of tempViews) {
        this.views.delete(name);
      }
    }
  }

  // Join with pre-materialized rows (for CTE/view joins)
  _executeJoinWithRows(leftRows, rightRows, join, rightAlias) { return _executeJoinWithRowsImpl(this, leftRows, rightRows, join, rightAlias); }

  _selectInner(ast) { return _selectInnerImpl(this, ast); }
  
  _tryVolcanoSelect(ast) {
    // SELECT without FROM: now supported via Volcano (single empty row)
    if (this._outerRow) return null; // Correlated context (LATERAL JOIN) — use legacy path
    if (ast.joins?.some(j => j.lateral)) return null; // LATERAL JOINs — use legacy path
    // Derived tables in FROM — now supported in Volcano (materialized)
    if (ast.recursive) return null; // Recursive CTEs
    if (ast.ctes?.some(c => c.recursive)) return null; // Individual recursive CTEs
    // Skip CTEs containing window functions (UNION now supported)
    if (ast.ctes?.some(c => {
      const s = JSON.stringify(c);
      return s.includes('"type":"window"') || s.includes('"over":{');
    })) return null;
    if (ast.pivot) return null; // PIVOT queries
    if (ast.unpivot) return null; // UNPIVOT queries
    // Window functions — now supported in Volcano via Window iterator
    // (including nested in expressions like CASE WHEN ROW_NUMBER() OVER ... = 1)
    // Function-wrapped aggregates (COALESCE(SUM(x), 0)) — now supported in Volcano
    // Skip unsupported aggregate functions
    const unsupportedAggs = ['PERCENTILE_CONT', 'PERCENTILE_DISC', 
      'STDDEV', 'STDDEV_POP', 'STDDEV_SAMP', 'VARIANCE', 'VAR_POP', 'VAR_SAMP', 'MODE', 'MEDIAN',
      'REGR_SLOPE', 'REGR_INTERCEPT', 'CORR', 'COVAR_POP', 'COVAR_SAMP'];
    const hasUnsupportedAgg = ast.columns.some(c => 
      c.type === 'aggregate' && unsupportedAggs.includes(c.func?.toUpperCase())
    );
    if (hasUnsupportedAgg) return null;
    // Aggregate FILTER clause — now supported in Volcano
    // Also check HAVING and subqueries for unsupported aggregates
    const astStr = JSON.stringify(ast);
    if (unsupportedAggs.some(a => astStr.includes(`"func":"${a}"`) || astStr.includes(`"func":"${a.toLowerCase()}"`) )) return null;
    // Check for derived tables in nested subqueries
    // Derived tables (__subquery) — now supported in Volcano
    // Skip JSON operations
    if (astStr.includes('"->>"') || astStr.toLowerCase().includes('"json_') || astStr.includes('"JSON_')) {
      return null;
    }
    // Skip MATCH AGAINST (fulltext search) — not handled by Volcano predicate builder
    if (astStr.includes('MATCH_AGAINST') || astStr.includes('TS_MATCH')) return null;
    
    // Build tables map including materialized CTE views
    let volcanoTables = this.tables;
    if (this.views.size > 0) {
      volcanoTables = new Map(this.tables);
      for (const [name, view] of this.views) {
        if (view.materializedRows && !volcanoTables.has(name)) {
          const rows = view.materializedRows;
          const rawKeys = rows.length > 0 ? Object.keys(rows[0]) : [];
          const schema = rawKeys.map(k => ({ name: k }));
          volcanoTables.set(name, {
            heap: {
              scan: function*() { for (const r of rows) yield { values: schema.map(c => r[c.name]), pageId: 0, slotIdx: 0 }; },
              rowCount: rows.length,
              tupleCount: rows.length
            },
            schema
          });
        }
      }
    }
    
    const plan = volcanoBuildPlan(ast, volcanoTables, this._indexes, this._tableStats);
    if (!plan) return null;
    
    plan.open();
    const rows = [];
    let row;
    while ((row = plan.next()) !== null) {
      // Clean up row keys: use unqualified names, strip internal _keys
      // Keep qualified names when there would be column name collisions
      const clean = {};
      const seen = new Set();
      // First pass: collect unqualified names that appear multiple times
      const nameCounts = {};
      for (const k of Object.keys(row)) {
        if (k.startsWith('_')) continue;
        const unqual = k.includes('.') ? k.split('.').pop() : k;
        nameCounts[unqual] = (nameCounts[unqual] || 0) + 1;
      }
      for (const [k, v] of Object.entries(row)) {
        if (k.startsWith('_')) continue;
        if (k.includes('.')) {
          const unqual = k.split('.').pop();
          if (nameCounts[unqual] > 1) {
            // Column name collision — keep qualified name
            clean[k] = v;
          } else if (!(unqual in clean)) {
            clean[unqual] = v;
          }
        } else {
          clean[k] = v;
        }
      }
      rows.push(clean);
    }
    plan.close();
    return { rows, columns: rows.length > 0 ? Object.keys(rows[0]) : [] };
  }

  _executeJoin(leftRows, join, leftAlias) { return _executeJoinImpl(this, leftRows, join, leftAlias); }
  _extractEquiJoinColumns(onExpr) { return _extractEquiJoinColumnsImpl(this, onExpr); }
  _extractEquiJoinKey(onExpr, leftAlias, rightAlias) { return _extractEquiJoinKeyImpl(this, onExpr, leftAlias, rightAlias); }
  _hashJoin(leftRows, rightTable, keys, rightAlias, joinType, pushdownFilter) { return _hashJoinImpl(this, leftRows, rightTable, keys, rightAlias, joinType, pushdownFilter); }
  _mergeJoin(leftRows, rightTable, keys, rightAlias, joinType) { return _mergeJoinImpl(this, leftRows, rightTable, keys, rightAlias, joinType); }
  _estimateRowCount(table) { return _estimateRowCountImpl(this, table); }
  _compareScanCosts(totalRows, estimatedResultRows) { return _compareScanCostsImpl(this, totalRows, estimatedResultRows); }
  _compareJoinCosts(leftRows, rightRows, hasEquiJoin, hasRightIndex) { return _compareJoinCostsImpl(this, leftRows, rightRows, hasEquiJoin, hasRightIndex); }
  _estimateFilteredRows(tableName, where, totalRows) { return _estimateFilteredRowsImpl(this, tableName, where, totalRows); }
  _estimateJoinSize(leftTable, leftRows, rightTableName, joinOn) { return _estimateJoinSizeImpl(this, leftTable, leftRows, rightTableName, joinOn); }
  _extractJoinColumns(on) { return _extractJoinColumnsImpl(this, on); }
  _optimizeJoinOrder(fromTable, joins) { return _optimizeJoinOrderImpl(this, fromTable, joins); }
  _popcount(n) { return _popcountImpl(this, n); }
  _getTableNdv(table1, table2, joinConditions) { return _getTableNdvImpl(this, table1, table2, joinConditions); }

  _update(ast) { return _updateImpl(this, ast); }

  // Handle foreign key actions when a parent row is deleted
  _handleForeignKeyDelete(parentTableName, parentTable, parentValues) { return _handleForeignKeyDeleteImpl(this, parentTableName, parentTable, parentValues); }

  // Handle foreign key actions when a parent row's PK is updated
  _handleForeignKeyUpdate(parentTableName, parentTable, oldValues, newValues) { return _handleForeignKeyUpdateImpl(this, parentTableName, parentTable, oldValues, newValues); }

  _delete(ast) { return _executeDeleteImpl(this, ast); }

  _truncate(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    // WAL: log the truncate for crash recovery
    const txId = this._nextTxId++;
    this.wal.appendTruncate(txId, ast.table);
    this.wal.appendCommit(txId);

    // Clear heap file
    const count = table.heap.rowCount || 0;
    table.heap = this._heapFactory();

    // Rebuild all indexes (empty)
    for (const [colName, oldIndex] of table.indexes) {
      table.indexes.set(colName, new BPlusTree(32, { unique: oldIndex.unique }));
    }

    return { type: 'OK', message: `${ast.table} truncated`, count };
  }

  _selectInfoSchema(ast) { return _selectInfoSchemaImpl(this, ast); }
  _selectPgCatalog(ast) { return _selectPgCatalogImpl(this, ast); }
  _filterPgCatalogRows(rows, ast) { return _filterPgCatalogRowsImpl(this, rows, ast); }


  _createSequence(ast) {
    this.sequences.set(ast.name.toLowerCase(), {
      current: ast.start - ast.increment, // Will be incremented on first NEXTVAL
      increment: ast.increment,
      min: ast.minValue,
      max: ast.maxValue,
    });
    return { type: 'OK', message: `Sequence ${ast.name} created` };
  }

  _merge(ast) { return _mergeImpl(this, ast); }

  _showTables() {
    const rows = [];
    for (const [name] of this.tables) {
      rows.push({ table_name: name });
    }
    return { type: 'ROWS', rows };
  }

  _describe(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);
    const rows = table.schema.map(col => ({
      column_name: col.name,
      data_type: col.type,
      primary_key: col.primaryKey ? 'YES' : 'NO',
      indexed: table.indexes.has(col.name) ? 'YES' : 'NO',
    }));
    return { type: 'ROWS', rows };
  }

  /**
   * Auto-vacuum trigger: runs lightweight vacuum when dead tuple count exceeds threshold.
   * PostgreSQL-style: threshold = autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor * n_live_tup
   * We use: max(50, 0.2 * liveTupleCount)
   */
  _maybeAutoVacuum(tableName, table) {
    if (!table.deadTupleCount) return;
    const threshold = Math.max(50, Math.floor(0.2 * (table.liveTupleCount || 0)));
    if (table.deadTupleCount < threshold) return;
    
    // Run lightweight vacuum on just this table
    try {
      if (table.mvccHeap && this._mvccManager) {
        const result = table.mvccHeap.vacuum(this._mvccManager);
        table.deadTupleCount = Math.max(0, table.deadTupleCount - (result.deadTuplesRemoved || 0));
      } else if (table.heap && table.heap.compact) {
        table.heap.compact();
        table.deadTupleCount = 0;
      }
    } catch (e) {
      // Auto-vacuum failure is non-fatal
    }
  }

  _vacuum(ast) { return _handleVacuumImpl(this, ast); }

  _checkpoint() { return _handleCheckpointImpl(this); }

  // === Prepared Statements ===
  
  _prepareSql(ast) { return _prepareSqlImpl(this, ast); }

  _executePrepared(ast) { return _executePreparedImpl(this, ast); }

  _deallocate(ast) { return _deallocateImpl(this, ast); }

  _bindParams(node, params) { return _bindParamsImpl(node, params); }

  prepare(sql) { return _prepareImpl(this, sql); }

  _analyzeTable(ast) { return _analyzeTableImpl(this, ast); }

  _union(ast) { return _unionImpl(this, ast); }
  _unionInner(ast) { return _unionInnerImpl(this, ast); }
  _intersect(ast) { return _intersectImpl(this, ast); }
  _except(ast) { return _exceptImpl(this, ast); }

  _applySetOrderLimit(ast, rows) {
    if (ast.orderBy) {
      rows.sort((a, b) => {
        for (const { column, direction } of ast.orderBy) {
          const av = this._orderByValue(column, a);
          const bv = this._orderByValue(column, b);
          const cmp = av < bv ? -1 : av > bv ? 1 : 0;
          if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
        }
        return 0;
      });
    }
    if (ast.offset) rows = rows.slice(Math.max(0, ast.offset));
    if (ast.limit != null) rows = rows.slice(0, ast.limit);
    return rows;
  }

  /** Remap rows' column names to match target column names (for UNION/INTERSECT/EXCEPT) */
  _remapUnionColumns(rows, targetCols) {
    if (rows.length === 0 || targetCols.length === 0) return rows;
    const srcCols = Object.keys(rows[0]);
    if (srcCols.join() === targetCols.join()) return rows;
    return rows.map(row => {
      const mapped = {};
      const vals = Object.values(row);
      for (let i = 0; i < targetCols.length && i < vals.length; i++) {
        mapped[targetCols[i]] = vals[i];
      }
      return mapped;
    });
  }

  _explain(ast) { return _explainImpl(this, ast); }

  _formatPlan(plan, format, stmt) { return _formatPlanImpl(this, plan, format, stmt); }
  _planToYaml(plan, indent = 0) { return _planToYamlImpl(this, plan, indent); }
  _planToDot(plan) { return _planToDotImpl(this, plan); }

  _executeRecursiveCTE(cte) { return _executeRecursiveCTEImpl(this, cte); }

  _explainCompiled(stmt) { return _explainCompiledImpl(this, stmt); }
  _explainAnalyze(stmt) { return _explainAnalyzeImpl(this, stmt); }
  _fillScanActuals(node, stmt, totalActualRows) { return _fillScanActualsImpl(this, node, stmt, totalActualRows); }

  _findIndexedColumn(where) {
    if (!where) return null;
    if (where.type === 'COMPARE' && where.op === 'EQ') {
      const colRef = where.left.type === 'column_ref' ? where.left : (where.right.type === 'column_ref' ? where.right : null);
      if (colRef) return colRef.name.includes('.') ? colRef.name.split('.').pop() : colRef.name;
    }
    if (where.type === 'AND') {
      return this._findIndexedColumn(where.left) || this._findIndexedColumn(where.right);
    }
    return null;
  }

  // Helper: check if an expression tree contains a window function node
  _exprContainsWindow(node) { return _exprContainsWindowImpl(this, node); }

  // Helper: extract all window function nodes from an expression tree, assigning each a unique key
  _extractWindowNodes(node, results = [], prefix = "__wexpr") { return _extractWindowNodesImpl(this, node, results, prefix); }

  // Helper: check if columns list contains any window function (top-level or nested in expressions)
  _columnsHaveWindow(columns) { return _columnsHaveWindowImpl(this, columns); }

  // Validation: detect nested aggregate function calls (e.g., SUM(COUNT(*)))
  _validateNoNestedAggregates(columns) { return _validateNoNestedAggregatesImpl(this, columns); }

  // Validation: window functions cannot appear in WHERE or HAVING clauses
  _validateNoWindowInWhere(where, clause = "WHERE") { return _validateNoWindowInWhereImpl(this, where, clause); }

  _computeWindowFunctions(columns, rows, windowDefs) { return _computeWindowFunctionsImpl(this, columns, rows, windowDefs); }

  _windowOrderEqual(rowA, rowB, orderBy) { return _windowOrderEqualImpl(this, rowA, rowB, orderBy); }

  _commentOn(ast) {
    if (!this._comments) this._comments = new Map();
    if (ast.objectType === 'TABLE') {
      if (!this.tables.has(ast.table)) throw new Error(`Table ${ast.table} not found`);
      this._comments.set(`table:${ast.table}`, ast.comment);
      return { type: 'OK', message: `COMMENT on table ${ast.table}` };
    } else if (ast.objectType === 'COLUMN') {
      if (ast.table && !this.tables.has(ast.table)) throw new Error(`Table ${ast.table} not found`);
      const key = ast.table ? `column:${ast.table}.${ast.column}` : `column:*.${ast.column}`;
      this._comments.set(key, ast.comment);
      return { type: 'OK', message: `COMMENT on column ${ast.table ? ast.table + '.' : ''}${ast.column}` };
    }
    throw new Error('Unknown COMMENT ON object type');
  }

  _rebuildIndexes(table) {
    for (const [colName, oldIndex] of table.indexes) {
      const colIdx = table.schema.findIndex(c => c.name === colName);
      if (colIdx === -1) continue;
      const newIndex = new BPlusTree(32, { unique: oldIndex.unique });
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        newIndex.insert(values[colIdx], { pageId, slotIdx });
      }
      table.indexes.set(colName, newIndex);
    }
    // Clear HOT chains after rebuild — all index entries now point to current RIDs
    if (table.heap._hotChains) {
      table.heap._hotChains.clear();
    }
  }

  /**
   * Get a row from heap, following HOT chains if the original RID is stale.
   * When an UPDATE doesn't change indexed columns, the index still points to the old RID.
   * The HOT chain links old RID → new RID so we can find the current version.
   */
  _heapGetFollowHot(heap, pageId, slotIdx) {
    // Try direct lookup first
    let values = heap.get(pageId, slotIdx);
    if (values) return values;

    // If direct lookup failed, try following HOT chain
    if (heap.followHotChain) {
      const latest = heap.followHotChain(pageId, slotIdx);
      if (latest.pageId !== pageId || latest.slotIdx !== slotIdx) {
        values = heap.get(latest.pageId, latest.slotIdx);
        if (values) return values;
      }
    }
    return null;
  }

    _applyPivot(result, pivot) {
    const { aggFunc, aggCol, pivotCol, pivotValues } = pivot;
    const rows = result.rows || [];
    
    // Determine grouping columns (all columns except aggCol and pivotCol)
    const allCols = rows.length > 0 ? Object.keys(rows[0]) : [];
    const groupCols = allCols.filter(c => c !== aggCol && c !== pivotCol);
    
    // Group rows by the grouping columns
    const groups = new Map();
    for (const row of rows) {
      const key = groupCols.map(c => String(row[c] ?? 'NULL')).join('|||');
      if (!groups.has(key)) {
        const base = {};
        for (const c of groupCols) base[c] = row[c];
        // Initialize pivot columns to null
        for (const v of pivotValues) base[String(v)] = null;
        groups.set(key, { base, values: [] });
      }
      groups.get(key).values.push(row);
    }
    
    // Apply aggregate for each pivot value
    const pivotRows = [];
    for (const { base, values } of groups.values()) {
      const outRow = { ...base };
      for (const pv of pivotValues) {
        const pvStr = String(pv);
        const matching = values.filter(r => String(r[pivotCol]) === pvStr);
        outRow[pvStr] = this._pivotAggregate(aggFunc, aggCol, matching);
      }
      pivotRows.push(outRow);
    }
    
    return { type: 'ROWS', rows: pivotRows };
  }
  
  _pivotAggregate(func, col, rows) {
    const vals = rows.map(r => r[col]).filter(v => v != null);
    if (vals.length === 0) return null;
    switch (func) {
      case 'SUM': return vals.reduce((a, b) => Number(a) + Number(b), 0);
      case 'COUNT': return vals.length;
      case 'AVG': return vals.reduce((a, b) => Number(a) + Number(b), 0) / vals.length;
      case 'MAX': return vals.reduce((a, b) => (a > b ? a : b));
      case 'MIN': return vals.reduce((a, b) => (a < b ? a : b));
      default: return vals.length > 0 ? vals[0] : null;
    }
  }
  
  _applyUnpivot(result, unpivot) {
    const { valueCol, nameCol, sourceCols } = unpivot;
    const rows = result.rows || [];
    const unpivotRows = [];
    
    // For each row, create one output row per source column
    for (const row of rows) {
      // Keep columns that are NOT in the sourceCols list
      const baseCols = {};
      for (const [k, v] of Object.entries(row)) {
        if (!sourceCols.includes(k)) baseCols[k] = v;
      }
      for (const sc of sourceCols) {
        const val = row[sc];
        if (val != null) { // UNPIVOT excludes NULLs by default
          unpivotRows.push({ ...baseCols, [nameCol]: sc, [valueCol]: val });
        }
      }
    }
    
    return { type: 'ROWS', rows: unpivotRows };
  }
  
  _selectWithGroupBy(ast, rows) { return _selectWithGroupByImpl(this, ast, rows); }

  _extractEqualityConditions(where, tableAlias) {
    const conditions = [];
    const extract = (node) => {
      if (!node) return;
      if (node.type === 'AND') {
        extract(node.left);
        extract(node.right);
      } else if (node.type === 'COMPARE' && node.op === 'EQ') {
        const colRef = node.left?.type === 'column_ref' ? node.left : (node.right?.type === 'column_ref' ? node.right : null);
        const literal = node.left?.type === 'literal' ? node.left : (node.right?.type === 'literal' ? node.right : null);
        if (colRef && literal) {
          const colName = colRef.name.includes('.') ? colRef.name.split('.').pop() : colRef.name;
          conditions.push({ col: colName, value: literal.value });
        }
      }
    };
    extract(where);
    return conditions;
  }

  _tryCompositeIndexPrefix(table, tableAlias, eqConditions) {
    // Look for composite indexes where eqConditions match a prefix
    const eqCols = new Set(eqConditions.map(c => c.col));
    const eqMap = new Map(eqConditions.map(c => [c.col, c.value]));
    
    for (const [indexKey, index] of table.indexes) {
      if (index._isHash) continue; // Hash doesn't support prefix scans
      
      // Check if this is a composite index
      const indexCols = indexKey.split(',');
      if (indexCols.length < 2) continue;
      
      // Check for prefix match: the first N columns of the index must all be in eqConditions
      let prefixLen = 0;
      for (let i = 0; i < indexCols.length; i++) {
        if (eqCols.has(indexCols[i])) {
          prefixLen = i + 1;
        } else {
          break; // Must be a contiguous prefix
        }
      }
      
      if (prefixLen === 0) continue;
      
      // Build prefix key for range scan
      const prefixValues = indexCols.slice(0, prefixLen).map(c => eqMap.get(c));
      
      // Scan the composite index for matching prefix
      // Use table heap scan + index verification instead of iterating index directly
      // This avoids needing to iterate an unfamiliar BPlusTree implementation
      const rows = [];
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        // Check if this row matches the prefix conditions
        let matches = true;
        for (let i = 0; i < prefixLen; i++) {
          const colIdx = table.schema.findIndex(s => s.name === indexCols[i]);
          if (colIdx < 0 || values[colIdx] !== prefixValues[i]) {
            matches = false;
            break;
          }
        }
        if (matches) {
          rows.push(this._valuesToRow(values, table.schema, tableAlias));
        }
      }
      
      // Return with residual if not all conditions were in the index prefix
      const residualNeeded = eqConditions.some(c => !indexCols.slice(0, prefixLen).includes(c.col));
      return { rows, residual: residualNeeded ? 'partial' : null, compositePrefix: true };
    }
    
    return null;
  }

  _tryIndexScan(table, where, tableAlias) {
    if (!where) return null;

    // Fast path: BTreeTable PK equality lookup — O(log n) without secondary index
    if (where.type === 'COMPARE' && where.op === 'EQ' && table.heap instanceof BTreeTable) {
      const colRef = where.left.type === 'column_ref' ? where.left : (where.right.type === 'column_ref' ? where.right : null);
      const literal = where.left.type === 'literal' ? where.left : (where.right.type === 'literal' ? where.right : null);
      if (colRef && literal) {
        const colName = colRef.name.includes('.') ? colRef.name.split('.').pop() : colRef.name;
        const pkColNames = table.heap.pkIndices.map(i => table.schema[i]?.name);
        if (pkColNames.length === 1 && pkColNames[0] === colName) {
          // Direct B+tree lookup — no secondary index needed
          const values = table.heap.findByPK(literal.value);
          if (values) {
            const row = this._valuesToRow(values, table.schema, tableAlias);
            return { rows: [row], residual: null, btreeLookup: true };
          }
          return { rows: [], residual: null, btreeLookup: true };
        }
      }
    }

    // Simple equality: col = literal where col is indexed
    if (where.type === 'COMPARE' && where.op === 'EQ') {
      const colRef = where.left.type === 'column_ref' ? where.left : (where.right.type === 'column_ref' ? where.right : null);
      const literal = where.left.type === 'literal' ? where.left : (where.right.type === 'literal' ? where.right : null);
      if (colRef && literal) {
        const colName = colRef.name.includes('.') ? colRef.name.split('.').pop() : colRef.name;
        const index = table.indexes.get(colName);
        if (index) {
          // Hash index: use get() for equality lookup — may return array of rids
          // B+tree index: use range() for equality
          let entries;
          if (index._isHash) {
            const val = index.get(literal.value);
            if (val !== undefined) {
              // Hash index stores arrays of rids for non-unique indexes
              const rids = Array.isArray(val) ? val : [val];
              entries = rids.map(rid => ({ key: literal.value, value: rid }));
            } else {
              entries = [];
            }
          } else {
            entries = index.range(literal.value, literal.value);
          }
          const rows = [];
          for (const entry of entries) {
            const rid = entry.value;
            // Check for index-only scan possibility
            if (rid.includedValues && this._requestedColumns) {
              const neededCols = this._requestedColumns;
              const indexCols = new Set([colName, ...Object.keys(rid.includedValues)]);
              const allCovered = neededCols.every(c => indexCols.has(c));
              if (allCovered) {
                // Index-only scan: build row from index data
                const row = {};
                row[colName] = literal.value;
                if (tableAlias) row[`${tableAlias}.${colName}`] = literal.value;
                for (const [k, v] of Object.entries(rid.includedValues)) {
                  row[k] = v;
                  if (tableAlias) row[`${tableAlias}.${k}`] = v;
                }
                rows.push(row);
                continue;
              }
            }
            // Fall back to heap access
            const values = this._heapGetFollowHot(table.heap, rid.pageId, rid.slotIdx);
            if (values) {
              rows.push(this._valuesToRow(values, table.schema, tableAlias));
            }
          }
          // MVCC fallback: if index had entries but all heap lookups returned null
          // (invisible under current snapshot), fall back to full scan to find
          // the version visible to this transaction.
          if (rows.length === 0 && entries.length > 0) {
            const fallbackRows = [];
            for (const { pageId, slotIdx, values } of table.heap.scan()) {
              const row = this._valuesToRow(values, table.schema, tableAlias);
              fallbackRows.push(row);
            }
            return { rows: fallbackRows, residual: where };
          }
          return { rows, residual: null, indexOnly: rows.length > 0 && rows[0]?.includedValues !== undefined };
        }
      }
    }

    // Range comparison: col > literal, col >= literal, col < literal, col <= literal
    if (where.type === 'COMPARE' && ['GT', 'GTE', 'LT', 'LTE'].includes(where.op)) {
      const colRef = where.left.type === 'column_ref' ? where.left : (where.right.type === 'column_ref' ? where.right : null);
      const literal = where.left.type === 'literal' ? where.left : (where.right.type === 'literal' ? where.right : null);
      if (colRef && literal) {
        const colName = colRef.name.includes('.') ? colRef.name.split('.').pop() : colRef.name;
        const index = table.indexes.get(colName);
        if (index && !index._isHash && index.range) {
          // B+tree scan — iterate all index entries and filter by comparison
          const isColLeft = where.left.type === 'column_ref';
          const rows = [];
          for (const entry of index.scan()) {
            const val = entry.key;
            let passes;
            if (isColLeft) {
              switch (where.op) {
                case 'GT':  passes = val > literal.value; break;
                case 'GTE': passes = val >= literal.value; break;
                case 'LT':  passes = val < literal.value; break;
                case 'LTE': passes = val <= literal.value; break;
              }
            } else {
              switch (where.op) {
                case 'GT':  passes = val < literal.value; break;
                case 'GTE': passes = val <= literal.value; break;
                case 'LT':  passes = val > literal.value; break;
                case 'LTE': passes = val >= literal.value; break;
              }
            }
            if (!passes) continue;
            const rid = entry.value;
            const values = this._heapGetFollowHot(table.heap, rid.pageId, rid.slotIdx);
            if (values) {
              rows.push(this._valuesToRow(values, table.schema, tableAlias));
            }
          }
          return { rows, residual: null };
        }
        
        // Try composite index prefix matching: WHERE a = val using index (a, b, c)
        if (!index) {
          const compositeHit = this._tryCompositeIndexPrefix(table, tableAlias, [{ col: colName, value: literal.value }]);
          if (compositeHit) return compositeHit;
        }
      }
    }

    // AND: try to combine conditions for composite index prefix matching
    if (where.type === 'AND') {
      const eqConditions = this._extractEqualityConditions(where, tableAlias);
      if (eqConditions.length >= 2) {
        const compositeHit = this._tryCompositeIndexPrefix(table, tableAlias, eqConditions);
        if (compositeHit) return compositeHit;
      }
    }

    // BETWEEN: col BETWEEN lo AND hi
    if (where.type === 'BETWEEN') {
      const colRef = (where.left?.type === 'column_ref') ? where.left : (where.expr?.type === 'column_ref' ? where.expr : null);
      if (colRef) {
        const colName = colRef.name.includes('.') ? colRef.name.split('.').pop() : colRef.name;
        const index = table.indexes.get(colName);
        if (index && !index._isHash && where.low?.type === 'literal' && where.high?.type === 'literal') {
          let lo = where.low.value, hi = where.high.value;
          if (where.symmetric && lo > hi) { const tmp = lo; lo = hi; hi = tmp; }
          const entries = index.range(lo, hi);
          const rows = [];
          for (const entry of entries) {
            const rid = entry.value;
            const values = this._heapGetFollowHot(table.heap, rid.pageId, rid.slotIdx);
            if (values) {
              rows.push(this._valuesToRow(values, table.schema, tableAlias));
            }
          }
          return { rows, residual: null };
        }
      }
    }

    // IN list: col IN (val1, val2, ...)
    if (where.type === 'IN_LIST' && where.left?.type === 'column_ref') {
      const colName = where.left.name.includes('.') ? where.left.name.split('.').pop() : where.left.name;
      const index = table.indexes.get(colName);
      if (index && where.values.every(v => v.type === 'literal')) {
        const rows = [];
        const seen = new Set(); // dedup by pageId+slotIdx
        for (const val of where.values) {
          let entries;
          if (index._isHash) {
            const found = index.get(val.value);
            if (found !== undefined) {
              const rids = Array.isArray(found) ? found : [found];
              entries = rids.map(rid => ({ key: val.value, value: rid }));
            } else {
              entries = [];
            }
          } else {
            entries = index.range(val.value, val.value);
          }
          for (const entry of entries) {
            const rid = entry.value;
            const key = `${rid.pageId}:${rid.slotIdx}`;
            if (seen.has(key)) continue;
            seen.add(key);
            const values = this._heapGetFollowHot(table.heap, rid.pageId, rid.slotIdx);
            if (values) {
              rows.push(this._valuesToRow(values, table.schema, tableAlias));
            }
          }
        }
        return { rows, residual: null };
      }
    }

    // AND: try to use index on one side, residual on the other
    if (where.type === 'AND') {
      const leftScan = this._tryIndexScan(table, where.left, tableAlias);
      if (leftScan) {
        return { rows: leftScan.rows, residual: where.right };
      }
      const rightScan = this._tryIndexScan(table, where.right, tableAlias);
      if (rightScan) {
        return { rows: rightScan.rows, residual: where.left };
      }
    }

    // OR: if both sides can use indexes, union the results (bitmap OR)
    if (where.type === 'OR') {
      const leftScan = this._tryIndexScan(table, where.left, tableAlias);
      const rightScan = this._tryIndexScan(table, where.right, tableAlias);
      if (leftScan && rightScan) {
        // Union: deduplicate by row identity
        const seen = new Set();
        const rows = [];
        for (const row of leftScan.rows) {
          // Use all column values as key for dedup
          const key = JSON.stringify(Object.entries(row).filter(([k]) => !k.includes('.')).sort());
          if (!seen.has(key)) {
            seen.add(key);
            rows.push(row);
          }
        }
        for (const row of rightScan.rows) {
          const key = JSON.stringify(Object.entries(row).filter(([k]) => !k.includes('.')).sort());
          if (!seen.has(key)) {
            seen.add(key);
            rows.push(row);
          }
        }
        // Apply residuals from both sides (already handled within each scan)
        const leftResidual = leftScan.residual;
        const rightResidual = rightScan.residual;
        // If either side had a residual, we need to re-evaluate the original OR condition
        // on the unioned rows to ensure correctness
        if (leftResidual || rightResidual) {
          return { rows, residual: where };
        }
        return { rows, residual: null };
      }
    }

    return null;
  }

  _valuesToRow(values, schema, tableAlias) {
    const row = {};
    for (let i = 0; i < schema.length; i++) {
      // If values has fewer elements than schema (e.g., after ALTER TABLE ADD COLUMN),
      // pad with null (the column's default value for missing data)
      const val = i < values.length ? values[i] : null;
      row[schema[i].name] = val;
      row[`${tableAlias}.${schema[i].name}`] = val;
    }
    return row;
  }

  // Collect aggregate_expr nodes from an expression tree (for HAVING pre-computation)
  // Serialize an expression to a canonical string for key matching
  _serializeExpr(expr) {
    if (expr == null) return '*';
    if (typeof expr === 'string') return expr;
    if (typeof expr !== 'object') return String(expr);
    switch (expr.type) {
      case 'column_ref': return expr.table ? `${expr.table}.${expr.name}` : expr.name;
      case 'literal': return String(expr.value);
      case 'arith': return `${this._serializeExpr(expr.left)} ${expr.op} ${this._serializeExpr(expr.right)}`;
      case 'unary_minus': return `-(${this._serializeExpr(expr.operand)})`;
      default: return JSON.stringify(expr);
    }
  }


  /**
   * Import CSV data into a table.
   * @param {string} tableName - Target table
   * @param {string} csv - CSV string
   * @param {object} opts - { header: true, delimiter: ',' }
   */
  copyFrom(tableName, csv, opts = {}) {
    const table = this.tables.get(tableName);
    if (!table) throw new Error(`Table ${tableName} not found`);
    
    const delimiter = opts.delimiter || ',';
    const hasHeader = opts.header !== false;
    
    const lines = csv.trim().split('\n');
    if (lines.length === 0) return { count: 0 };
    
    let csvColumns = null; // column names from CSV header (for mapping)
    let startLine = 0;
    
    if (hasHeader) {
      csvColumns = this._parseCsvLine(lines[0], delimiter);
      startLine = 1;
    }
    
    // Use table schema column order, mapping from CSV header if present
    const schemaColumns = table.schema.map(c => c.name);
    
    let count = 0;
    for (let i = startLine; i < lines.length; i++) {
      if (!lines[i].trim()) continue;
      const vals = this._parseCsvLine(lines[i], delimiter);
      
      // Map CSV columns to schema order
      const newValues = new Array(schemaColumns.length).fill(null);
      for (let j = 0; j < vals.length; j++) {
        const csvCol = csvColumns ? csvColumns[j] : schemaColumns[j];
        const schemaIdx = schemaColumns.indexOf(csvCol);
        if (schemaIdx === -1) continue;
        const v = vals[j];
        if (v === '' || v === null) { newValues[schemaIdx] = null; continue; }
        const n = Number(v);
        if (!isNaN(n) && v.trim() !== '') { newValues[schemaIdx] = n; continue; }
        newValues[schemaIdx] = v;
      }
      
      // Direct insert via heap (bypass SQL parsing for speed and keyword safety)
      table.heap.insert(newValues);
      count++;
    }
    
    return { count };
  }

  /**
   * Export query results as CSV.
   * @param {string} query - SELECT query
   * @param {object} opts - { header: true, delimiter: ',' }
   */
  copyTo(query, opts = {}) {
    const result = this.execute(query);
    if (!result.rows || result.rows.length === 0) return '';
    
    const delimiter = opts.delimiter || ',';
    const includeHeader = opts.header !== false;
    
    const columns = Object.keys(result.rows[0]);
    const lines = [];
    
    if (includeHeader) {
      lines.push(columns.map(c => this._escapeCsvField(c, delimiter)).join(delimiter));
    }
    
    for (const row of result.rows) {
      const fields = columns.map(c => {
        const val = row[c];
        if (val === null || val === undefined) return '';
        return this._escapeCsvField(String(val), delimiter);
      });
      lines.push(fields.join(delimiter));
    }
    
    return lines.join('\n') + '\n';
  }

  _parseCsvLine(line, delimiter) {
    const fields = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (inQuotes) {
        if (ch === '"' && line[i + 1] === '"') {
          current += '"';
          i++;
        } else if (ch === '"') {
          inQuotes = false;
        } else {
          current += ch;
        }
      } else {
        if (ch === '"') {
          inQuotes = true;
        } else if (ch === delimiter) {
          fields.push(current);
          current = '';
        } else {
          current += ch;
        }
      }
    }
    fields.push(current);
    return fields;
  }

    // Convert SQL LIKE pattern to regex string, with optional ESCAPE character
  // LIKE: % → .*, _ → ., escape char makes next char literal
  _likeToRegex(pattern, escapeChar) {
    return _likeToRegexImpl(pattern, escapeChar);
  }

  _escapeCsvField(val, delimiter) {
    if (val.includes(delimiter) || val.includes('"') || val.includes('\n')) {
      return '"' + val.replace(/"/g, '""') + '"';
    }
    return val;
  }
}

// Install expression evaluator methods as a mixin
installExpressionEvaluator(Database);
