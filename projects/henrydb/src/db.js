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
  _handlePrepare(sql) {
    // PREPARE stmt_name AS SELECT ... WHERE id = $1
    const match = sql.match(/PREPARE\s+(\w+)\s+AS\s+(.*)/is);
    if (!match) throw new Error('Invalid PREPARE syntax. Use: PREPARE name AS sql');
    const name = match[1];
    const template = match[2].replace(/;$/, '').trim();
    const ast = parse(template);
    this._preparedStatements.set(name, { sql: template, ast });
    return { type: 'OK', message: `Prepared statement "${name}" created` };
  }

  // EXECUTE name (val1, val2, ...)
  _handleExecute(sql) {
    const match = sql.match(/EXECUTE\s+(\w+)\s*(?:\(([^)]*)\))?/is);
    if (!match) throw new Error('Invalid EXECUTE syntax. Use: EXECUTE name (val1, val2)');
    const name = match[1];
    const paramsStr = match[2] || '';
    
    const stmt = this._preparedStatements.get(name);
    if (!stmt) throw new Error(`Prepared statement "${name}" not found`);
    
    // Parse parameter values
    const params = paramsStr.split(',').map(p => p.trim()).filter(p => p);
    
    // Substitute $1, $2, etc. in the SQL
    let resolved = stmt.sql;
    for (let i = 0; i < params.length; i++) {
      resolved = resolved.replace(new RegExp('\\$' + (i + 1), 'g'), params[i]);
    }
    
    return this.execute(resolved);
  }

  // DEALLOCATE name
  _handleDeallocate(sql) {
    const match = sql.match(/DEALLOCATE\s+(\w+)/i);
    if (!match) throw new Error('Invalid DEALLOCATE syntax');
    const name = match[1].replace(/;$/, '');
    if (name.toUpperCase() === 'ALL') {
      this._preparedStatements.clear();
      return { type: 'OK', message: 'All prepared statements deallocated' };
    }
    if (!this._preparedStatements.delete(name)) {
      throw new Error(`Prepared statement "${name}" not found`);
    }
    return { type: 'OK', message: `Prepared statement "${name}" deallocated` };
  }

  // SAVEPOINT name — save current state
  _handleSavepoint(sql) {
    const match = sql.match(/SAVEPOINT\s+(\w+)/i);
    if (!match) throw new Error('Invalid SAVEPOINT syntax');
    const name = match[1].replace(/;$/, '');
    
    // Snapshot: deep-clone each table's row data directly from heap scan
    const snapshot = {};
    for (const [tableName, table] of this.tables) {
      const rows = [];
      for (const item of table.heap.scan()) {
        // Deep-clone row values to prevent mutation
        rows.push({ values: item.values.map(v => v) });
      }
      snapshot[tableName] = rows;
    }
    
    this._savepoints.push({ name, snapshot });
    return { type: 'OK', message: `Savepoint "${name}" created` };
  }

  // ROLLBACK TO [SAVEPOINT] name — restore to savepoint
  _handleRollbackToSavepoint(sql) {
    const match = sql.match(/ROLLBACK\s+TO\s+(?:SAVEPOINT\s+)?(\w+)/i);
    if (!match) throw new Error('Invalid ROLLBACK TO syntax');
    const name = match[1].replace(/;$/, '');
    
    // Find the savepoint
    const idx = this._savepoints.findLastIndex(sp => sp.name === name);
    if (idx === -1) throw new Error(`Savepoint "${name}" not found`);
    
    const { snapshot } = this._savepoints[idx];
    
    // Restore each table by replacing heap contents directly
    for (const [tableName, savedRows] of Object.entries(snapshot)) {
      const table = this.tables.get(tableName);
      if (!table) continue;
      
      // Create a fresh heap of the same type and re-insert saved rows
      const oldHeap = table.heap;
      if (oldHeap instanceof BTreeTable) {
        // BTreeTable: create fresh with same primary key column
        const pkCol = table.schema?.findIndex(c => c.primaryKey);
        table.heap = new BTreeTable(tableName, pkCol >= 0 ? pkCol : 0);
      } else {
        table.heap = this._heapFactory(tableName);
      }
      
      // Copy HOT chains config
      if (oldHeap._hotChains) {
        table.heap._hotChains = new Map();
      }
      
      for (const { values } of savedRows) {
        table.heap.insert(values);
      }
      
      // Rebuild indexes from the restored data
      if (table.indexes) {
        for (const [indexName, idx] of table.indexes) {
          if (idx.clear) idx.clear();
          else if (idx.root !== undefined) {
            // B+Tree: create fresh
            // Skip for now — indexes will be stale but queries fall back to heap scan
          }
        }
        // Re-index all rows
        for (const item of table.heap.scan()) {
          for (const [indexName, idx] of table.indexes) {
            if (idx.insert) {
              try {
                const keyCol = idx.column ?? idx.columns?.[0];
                if (keyCol !== undefined) {
                  const keyVal = typeof keyCol === 'number' ? item.values[keyCol] : item.values[table.schema.findIndex(c => c.name === keyCol)];
                  idx.insert(keyVal, { pageId: item.pageId, slotIdx: item.slotIdx });
                }
              } catch (e) {
                // Index rebuild failure is non-fatal for savepoint rollback
              }
            }
          }
        }
      }
    }
    
    // Remove savepoints after the rollback target
    this._savepoints.splice(idx + 1);
    
    // Clear result cache — stale cached query results from before rollback
    if (this._resultCache) this._resultCache.clear();
    
    return { type: 'OK', message: `Rolled back to savepoint "${name}"` };
  }

  // RELEASE [SAVEPOINT] name — remove savepoint
  _handleReleaseSavepoint(sql) {
    const match = sql.match(/RELEASE\s+(?:SAVEPOINT\s+)?(\w+)/i);
    if (!match) throw new Error('Invalid RELEASE syntax');
    const name = match[1].replace(/;$/, '');
    
    const idx = this._savepoints.findLastIndex(sp => sp.name === name);
    if (idx === -1) throw new Error(`Savepoint "${name}" not found`);
    
    // Remove this and all later savepoints
    this._savepoints.splice(idx);
    return { type: 'OK', message: `Savepoint "${name}" released` };
  }

  _handleAnalyze(sql) {
    const match = sql.match(/ANALYZE\s+(?:TABLE\s+)?(\w+)/i);
    if (!match) throw new Error('Invalid ANALYZE syntax. Use: ANALYZE TABLE name');
    const tableName = match[1].replace(/;$/, '');
    const table = this.tables.get(tableName) || this.tables.get(tableName.toLowerCase());
    if (!table) throw new Error(`Table "${tableName}" not found`);
    
    // Collect all rows
    const allRows = this.execute(`SELECT * FROM ${tableName}`).rows;
    const rowCount = allRows.length;
    
    // Compute per-column statistics
    const columns = {};
    for (const col of table.schema) {
      const values = allRows.map(r => r[col.name]);
      const distinct = new Set(values.filter(v => v !== null && v !== undefined)).size;
      const nulls = values.filter(v => v === null || v === undefined).length;
      
      const numericVals = values.filter(v => typeof v === 'number' && !isNaN(v));
      const min = numericVals.length > 0 ? Math.min(...numericVals) : null;
      const max = numericVals.length > 0 ? Math.max(...numericVals) : null;
      
      columns[col.name] = { distinct, nulls, min, max, selectivity: distinct > 0 ? 1 / distinct : 1 };
    }
    
    this._tableStats.set(tableName, { rowCount, columns, analyzedAt: Date.now() });
    
    return {
      type: 'ROWS',
      rows: Object.entries(columns).map(([col, stats]) => ({
        column: col,
        distinct_values: stats.distinct,
        null_count: stats.nulls,
        min: stats.min,
        max: stats.max,
        selectivity: stats.selectivity.toFixed(4),
      })),
    };
  }

  /**
   * Execute a query with detailed timing profile.
   * Returns { result, profile } where profile has phase-level timing.
   */
  profile(sql) {
    const phases = [];
    const t0 = performance.now();
    
    // PARSE phase
    const parseStart = performance.now();
    let ast = this._planCache.get(sql);
    const cached = !!ast;
    if (!ast) {
      ast = parse(sql);
      if (ast.type === 'SELECT') this._planCache.put(sql, ast);
    }
    const parseEnd = performance.now();
    phases.push({ name: 'PARSE', durationMs: parseEnd - parseStart, cached });
    
    // EXECUTE phase (includes scan, filter, sort, aggregate)
    const execStart = performance.now();
    const result = this.execute_ast(ast);
    const execEnd = performance.now();
    phases.push({ name: 'EXECUTE', durationMs: execEnd - execStart, rows: result?.rows?.length || 0 });
    
    const totalMs = performance.now() - t0;
    
    // Format report
    const lines = [`Query: ${sql.slice(0, 80)}${sql.length > 80 ? '...' : ''}`];
    lines.push('─'.repeat(60));
    lines.push(`${'Phase'.padEnd(15)} ${'Duration'.padStart(12)} ${'Pct'.padStart(6)} Details`);
    lines.push('─'.repeat(60));
    for (const p of phases) {
      const pct = totalMs > 0 ? (p.durationMs / totalMs * 100).toFixed(1) : '0.0';
      const details = p.cached ? '(cached)' : p.rows !== undefined ? `${p.rows} rows` : '';
      lines.push(`${p.name.padEnd(15)} ${(p.durationMs.toFixed(3) + 'ms').padStart(12)} ${(pct + '%').padStart(6)} ${details}`);
    }
    lines.push('─'.repeat(60));
    lines.push(`${'TOTAL'.padEnd(15)} ${(totalMs.toFixed(3) + 'ms').padStart(12)} ${'100%'.padStart(6)}`);
    
    return {
      result,
      profile: {
        totalMs: parseFloat(totalMs.toFixed(3)),
        phases,
        formatted: lines.join('\n'),
      },
    };
  }

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

  _recommendIndexes() {
    // Refresh index list to catch any manually created indexes
    this._indexAdvisor._existingIndexes = this._indexAdvisor._collectExistingIndexes();
    const recs = this._indexAdvisor.recommend();
    if (recs.length === 0) {
      return {
        type: 'ROWS',
        rows: [{ recommendation: 'No index recommendations. Run more queries to build workload profile.', impact: '', sql: '' }],
      };
    }
    return {
      type: 'ROWS',
      rows: recs.map(r => ({
        table: r.table,
        columns: r.columns.join(', '),
        impact: r.level,
        score: r.impact,
        costReduction: r.costReduction != null ? `${r.costReduction}%` : null,
        reason: r.reason,
        sql: r.sql,
      })),
    };
  }

  _applyRecommendedIndexes(minLevel = 'medium') {
    // Refresh index list to catch any manually created indexes
    this._indexAdvisor._existingIndexes = this._indexAdvisor._collectExistingIndexes();
    const recs = this._indexAdvisor.recommend();
    const levels = { high: 3, medium: 2, low: 1 };
    const minLevelVal = levels[minLevel] || 2;
    
    const toApply = recs.filter(r => (levels[r.level] || 0) >= minLevelVal);
    
    if (toApply.length === 0) {
      return {
        type: 'OK',
        message: 'No high/medium impact index recommendations to apply.',
        rows: [],
      };
    }
    
    const results = [];
    for (const rec of toApply) {
      try {
        this.execute_ast(parse(rec.sql));
        results.push({
          status: 'created',
          sql: rec.sql,
          impact: rec.level,
          costReduction: rec.costReduction != null ? `${rec.costReduction}%` : null,
        });
      } catch (e) {
        results.push({
          status: 'failed',
          sql: rec.sql,
          error: e.message,
        });
      }
    }
    
    // Refresh the advisor's index list
    this._indexAdvisor._existingIndexes = this._indexAdvisor._collectExistingIndexes();
    
    return {
      type: 'ROWS',
      rows: results,
      message: `Applied ${results.filter(r => r.status === 'created').length}/${toApply.length} recommended indexes`,
    };
  }

  /**
   * Serialize the entire database to a JSON-compatible object.
   * Includes table schemas, data, views, triggers.
   */
  serialize() {
    const tables = {};
    for (const [name, table] of this.tables) {
      const rows = [];
      for (const { values } of table.heap.scan()) {
        rows.push(values);
      }
      tables[name] = {
        schema: table.schema,
        rows,
        indexes: [...table.indexes.keys()],
        indexMeta: table.indexMeta ? Object.fromEntries(table.indexMeta) : {},
      };
    }
    
    const views = {};
    for (const [name, view] of this.views) {
      views[name] = view;
    }
    
    // Serialize sequences
    const sequences = {};
    for (const [name, seq] of this.sequences) {
      sequences[name] = {
        current: seq.current,
        increment: seq.increment,
        min: seq.min,
        max: seq.max,
        cycle: seq.cycle,
        ownedBy: seq.ownedBy,
      };
    }
    
    // Serialize materialized views
    const matViews = {};
    for (const [name, mv] of (this.materializedViews || new Map())) {
      matViews[name] = mv;
    }
    
    // Serialize comments
    const comments = {};
    for (const [key, val] of (this._comments || new Map())) {
      comments[key] = val;
    }
    
    return {
      version: 1,
      tables,
      views,
      triggers: this.triggers,
      sequences,
      materializedViews: matViews,
      comments,
      indexCatalog: Object.fromEntries(this.indexCatalog),
    };
  }

  /**
   * Save database to a file (Node.js environments).
   */
  save(path) {
    const fs = globalThis.__fs || null;
    if (!fs) {
      // Return serialized string for environments without fs
      return JSON.stringify(this.serialize());
    }
    fs.writeFileSync(path, JSON.stringify(this.serialize(), null, 2));
    return { type: 'OK', message: `Database saved to ${path}` };
  }

  /**
   * Load database from a serialized object.
   */
  /**
   * Bulk insert rows without parsing SQL for each row.
   * Much faster than individual INSERT statements.
   * 
   * @param {string} tableName - Target table
   * @param {Array<Array>} rows - Array of value arrays
   * @returns {Object} Result with count
   */
  bulkInsert(tableName, rows) {
    const table = this.tables.get(tableName);
    if (!table) throw new Error(`Table ${tableName} not found`);
    
    let inserted = 0;
    for (const values of rows) {
      this._insertRow(table, null, values);
      inserted++;
    }
    if (table.liveTupleCount !== undefined) table.liveTupleCount += inserted;
    return { type: 'OK', message: `${inserted} row(s) inserted`, count: inserted };
  }

  /**
   * Execute a query and return paginated results.
   * 
   * @param {string} sql - SQL query
   * @param {number} page - Page number (1-indexed)
   * @param {number} pageSize - Rows per page
   * @returns {Object} Paginated result
   */
  executePaginated(sql, page = 1, pageSize = 100) {
    const result = this.execute(sql);
    if (result.type !== 'ROWS') return result;
    
    const totalRows = result.rows.length;
    const totalPages = Math.ceil(totalRows / pageSize);
    const start = (page - 1) * pageSize;
    const end = start + pageSize;
    
    return {
      type: 'ROWS',
      rows: result.rows.slice(start, end),
      pagination: {
        page,
        pageSize,
        totalRows,
        totalPages,
        hasNext: page < totalPages,
        hasPrev: page > 1,
      },
    };
  }

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
  _acquireRowLocks(ast, rows) {
    const tableName = ast.from?.table || ast.from?.name;
    if (!tableName || tableName.startsWith('__')) return;
    
    const forMode = ast.forUpdate.includes('SHARE') ? 'SHARE' : 'UPDATE';
    const nowait = ast.forUpdate.includes('NOWAIT');
    const skipLocked = ast.forUpdate.includes('SKIP LOCKED');
    const txId = this._currentTxId || 0;
    
    const table = this.tables.get(tableName);
    if (!table) return;
    
    // Build a set of PKs from result rows to lock
    const pkIndices = table.schema
      .map((c, i) => c.primaryKey ? i : -1)
      .filter(i => i >= 0);
    const pkNames = pkIndices.map(i => table.schema[i].name);
    
    // For each result row, find its heap location and lock it
    for (let i = rows.length - 1; i >= 0; i--) {
      const row = rows[i];
      
      // Find the heap RID by scanning for matching PK
      let rid = null;
      for (const item of table.heap.scan()) {
        const vals = item.values || item;
        let match = true;
        for (const pi of pkIndices) {
          if (vals[pi] !== row[table.schema[pi].name]) { match = false; break; }
        }
        if (match) {
          rid = { pageId: item.pageId, slotIdx: item.slotIdx };
          break;
        }
      }
      
      if (!rid) continue;
      
      const lockKey = `${tableName}:${rid.pageId}:${rid.slotIdx}`;
      const existingLock = this._rowLocks.get(lockKey);
      
      if (existingLock && existingLock.txId !== txId) {
        if (existingLock.mode === 'UPDATE' || forMode === 'UPDATE') {
          if (skipLocked) {
            rows.splice(i, 1);
            continue;
          }
          if (nowait) {
            throw new Error(`Could not obtain lock on row in "${tableName}": locked by transaction ${existingLock.txId}`);
          }
          throw new Error(`Row locked by transaction ${existingLock.txId} in "${tableName}"`);
        }
        // SHARE + SHARE is compatible
      }
      
      this._rowLocks.set(lockKey, { txId, mode: forMode });
    }
  }

  /**
   * Release all row locks held by a transaction.
   */
  _releaseRowLocks(txId) {
    for (const [key, lock] of this._rowLocks) {
      if (lock.txId === txId) {
        this._rowLocks.delete(key);
      }
    }
  }

  /**
   * ANALYZE: gather table statistics for query optimization.
   * Computes per-column: ndistinct, nullFraction, min, max, mostCommonValues.
   */
  _analyzeTable(ast) {
    const tablesToAnalyze = ast.table ? [ast.table] : [...this.tables.keys()];
    const results = {};
    
    for (const tableName of tablesToAnalyze) {
      const table = this.tables.get(tableName);
      if (!table) throw new Error(`Table "${tableName}" does not exist`);
      
      let rowCount = 0;
      const columnValues = {};
      for (const col of table.schema) {
        columnValues[col.name] = [];
      }
      
      // Scan the heap and collect values (sample up to 10000 rows)
      const maxSample = 10000;
      for (const item of table.heap.scan()) {
        if (rowCount >= maxSample) { rowCount++; continue; }
        const values = item.values || item;
        for (let i = 0; i < table.schema.length; i++) {
          columnValues[table.schema[i].name].push(values[i]);
        }
        rowCount++;
      }
      
      const stats = { rowCount, columns: {} };
      
      for (const col of table.schema) {
        const vals = columnValues[col.name];
        const nonNull = vals.filter(v => v !== null && v !== undefined);
        const distinct = new Set(nonNull);
        
        // Most common values (top 10)
        const freq = new Map();
        for (const v of nonNull) {
          freq.set(v, (freq.get(v) || 0) + 1);
        }
        const mostCommon = [...freq.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, 10)
          .map(([value, count]) => ({ value, frequency: count / vals.length }));
        
        // Numeric stats
        let min = null, max = null, avg = null;
        if (nonNull.length > 0 && typeof nonNull[0] === 'number') {
          const nums = nonNull.filter(v => typeof v === 'number');
          min = Math.min(...nums);
          max = Math.max(...nums);
          avg = nums.reduce((a, b) => a + b, 0) / nums.length;
        }
        
        stats.columns[col.name] = {
          ndistinct: distinct.size,
          nullFraction: (vals.length - nonNull.length) / Math.max(vals.length, 1),
          mostCommonValues: mostCommon,
          min,
          max,
          avg,
        };
      }
      
      // Store stats on the table
      table._stats = stats;
      results[tableName] = stats;
    }
    
    const count = tablesToAnalyze.length;
    return {
      type: 'OK',
      message: `ANALYZE: ${count} table(s) analyzed`,
      stats: results,
    };
  }

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

  _validateConstraints(table, values) {
    return this._validateConstraintsForUpdate(table, values, null);
  }

  /**
   * Validate constraints, optionally excluding a specific row (for UPDATE/UPSERT
   * where the old row should not trigger UNIQUE false positives).
   */
  _validateConstraintsForUpdate(table, values, excludeRid, oldValues) {
    for (let i = 0; i < table.schema.length; i++) {
      const col = table.schema[i];
      const val = values[i];

      // Skip UNIQUE check if value hasn't changed (UPDATE with same PK/UNIQUE value)
      // This avoids false positives when HOT chains make index RIDs stale
      if (excludeRid && oldValues && (col.unique || col.primaryKey) && val === oldValues[i]) {
        continue;  // Value unchanged — it was already validated when first inserted
      }

      // NOT NULL constraint (PRIMARY KEY columns are implicitly NOT NULL)
      if ((col.notNull || col.primaryKey) && val == null) {
        throw new Error(`NOT NULL constraint violated for column ${col.name}`);
      }

      // CHECK constraint
      if (col.check) {
        const row = {};
        for (let j = 0; j < table.schema.length; j++) {
          row[table.schema[j].name] = values[j];
        }
        // Per SQL standard: CHECK passes when result is TRUE or NULL (unknown)
        // NULL values in the check expression should cause it to pass
        // Check if the column being checked is NULL — if so, skip the check
        const colIdx = table.schema.indexOf(col);
        if (colIdx >= 0 && values[colIdx] == null) continue;
        const result = this._evalExpr(col.check, row);
        if (!result) {
          throw new Error(`CHECK constraint violated for column ${col.name}`);
        }
      }

    // Table-level CHECK constraints
    if (table.tableChecks && table.tableChecks.length > 0) {
      const row = {};
      for (let j = 0; j < table.schema.length; j++) {
        row[table.schema[j].name] = values[j];
      }
      for (const checkExpr of table.tableChecks) {
        const result = this._evalExpr(checkExpr, row);
        // Per SQL standard: CHECK passes when result is TRUE or NULL (unknown)
        if (result === false) {
          throw new Error('CHECK constraint violated');
        }
      }
    }

      // UNIQUE and PRIMARY KEY uniqueness check
      if ((col.unique || col.primaryKey) && val != null) {
        // Try fast index-based lookup first (O(log N) instead of O(N))
        const index = table.indexes?.get(col.name);
        if (index && typeof index.search === 'function') {
          const found = index.search(val);
          if (found !== undefined && found !== null) {
            const rids = Array.isArray(found) ? found : [found];
            // Filter out stale index entries (deleted rows, HOT chain ghosts)
            const liveRids = rids.filter(r => {
              if (excludeRid && r.pageId === excludeRid.pageId && r.slotIdx === excludeRid.slotIdx) return false;
              // Verify the row actually exists in the heap
              try {
                const row = table.heap.get(r.pageId, r.slotIdx);
                return row !== null && row !== undefined;
              } catch { return false; }
            });
            if (liveRids.length > 0) {
              throw new Error(`UNIQUE constraint violated: duplicate value '${val}' for column ${col.name}`);
            }
          }
        } else {
          // Fallback: full heap scan (slow, O(N))
          for (const tuple of table.heap.scan()) {
            if (excludeRid && tuple.pageId === excludeRid.pageId && tuple.slotIdx === excludeRid.slotIdx) continue;
            const tupleValues = tuple.values || tuple;
            if (tupleValues[i] === val) {
              throw new Error(`UNIQUE constraint violated: duplicate value '${val}' for column ${col.name}`);
            }
          }
        }
      }

      // FOREIGN KEY constraint
      if (col.references && val != null) {
        const refTable = this.tables.get(col.references.table);
        if (!refTable) throw new Error(`Referenced table ${col.references.table} not found`);
        const refColIdx = refTable.schema.findIndex(c => c.name === col.references.column);
        let found = false;
        for (const { values: refValues } of refTable.heap.scan()) {
          if (refValues[refColIdx] === val) { found = true; break; }
        }
        if (!found) {
          throw new Error(`Foreign key constraint violated: ${val} not found in ${col.references.table}(${col.references.column})`);
        }
      }
    }
    // Composite PRIMARY KEY uniqueness check
    const pkIndices = [];
    for (let i = 0; i < table.schema.length; i++) {
      if (table.schema[i].primaryKey) pkIndices.push(i);
    }
    if (pkIndices.length > 1) {
      for (const { values: existing } of table.heap.scan()) {
        let match = true;
        for (const idx of pkIndices) {
          if (existing[idx] !== values[idx]) { match = false; break; }
        }
        if (match) {
          const keyDesc = pkIndices.map(i => `${table.schema[i].name}=${values[i]}`).join(', ');
          throw new Error(`Duplicate key value violates unique constraint: (${keyDesc})`);
        }
      }
    }
  }

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
    // Skip Volcano for unsupported patterns
    if (!ast.from) return null; // No FROM clause (e.g., SELECT 1)
    if (ast.from.subquery) return null; // Derived table in FROM (not JOIN)
    if (ast.recursive) return null; // Recursive CTEs
    if (ast.pivot) return null; // PIVOT queries
    if (ast.unpivot) return null; // UNPIVOT queries
    // Skip if WINDOW is used with GROUP BY and aggregate window functions
    const hasWindowFn = ast.columns.some(c => c.type === 'window' || (c.type === 'expression' && c.expr?.over));
    if (hasWindowFn) return null; // Window functions not fully supported in Volcano
    // Skip function-wrapped aggregates (COALESCE(SUM(x), 0)) — not yet supported
    const hasFuncWrappedAgg = ast.columns.some(c => 
      (c.type === 'function' || c.type === 'function_call') && 
      c.args?.some(a => a.type === 'aggregate' || a.type === 'aggregate_expr')
    );
    if (hasFuncWrappedAgg) return null;
    // Skip unsupported aggregate functions
    const unsupportedAggs = ['ARRAY_AGG', 'STRING_AGG', 'PERCENTILE_CONT', 'PERCENTILE_DISC', 
      'STDDEV', 'STDDEV_POP', 'STDDEV_SAMP', 'VARIANCE', 'VAR_POP', 'VAR_SAMP', 'MODE', 'MEDIAN',
      'REGR_SLOPE', 'REGR_INTERCEPT', 'CORR', 'COVAR_POP', 'COVAR_SAMP'];
    const hasUnsupportedAgg = ast.columns.some(c => 
      c.type === 'aggregate' && unsupportedAggs.includes(c.func?.toUpperCase())
    );
    if (hasUnsupportedAgg) return null;
    // Also check HAVING and subqueries for unsupported aggregates
    const astStr = JSON.stringify(ast);
    if (unsupportedAggs.some(a => astStr.includes(`"func":"${a}"`) || astStr.includes(`"func":"${a.toLowerCase()}"`) )) return null;
    // Skip JSON operations
    if (JSON.stringify(ast).includes('"->>"') || JSON.stringify(ast).includes('"json_')){
      return null;
    }
    
    const plan = volcanoBuildPlan(ast, this.tables, this._indexes, this._tableStats);
    if (!plan) return null;
    
    plan.open();
    const rows = [];
    let row;
    while ((row = plan.next()) !== null) {
      // Clean up row keys: use unqualified names, strip internal _keys
      const clean = {};
      for (const [k, v] of Object.entries(row)) {
        if (k.startsWith('_')) continue;
        // For qualified names (t.id), prefer unqualified form if not already present
        if (k.includes('.')) {
          const unqual = k.split('.').pop();
          if (!(unqual in clean)) clean[unqual] = v;
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
  _handleForeignKeyDelete(parentTableName, parentTable, parentValues) {
    // Find all child tables that reference this table
    for (const [childTableName, childTable] of this.tables) {
      for (const col of childTable.schema) {
        if (col.references && col.references.table === parentTableName) {
          const parentColIdx = parentTable.schema.findIndex(c => c.name === col.references.column);
          const parentValue = parentValues[parentColIdx];
          const childColIdx = childTable.schema.findIndex(c => c.name === col.name);

          if (col.references.onDelete === 'CASCADE') {
            // Delete child rows (recursively cascade)
            const toDelete = [];
            for (const { pageId, slotIdx, values: childValues } of childTable.heap.scan()) {
              if (childValues[childColIdx] === parentValue) {
                toDelete.push({ pageId, slotIdx, values: childValues });
              }
            }
            for (const { pageId, slotIdx, values: childValues } of toDelete) {
              // Recursively handle FK cascades from this child row
              this._handleForeignKeyDelete(childTableName, childTable, childValues);
              childTable.heap.delete(pageId, slotIdx);
            }
          } else if (col.references.onDelete === 'SET NULL') {
            // Set child column to NULL — collect rows first, then update
            const toUpdate = [];
            for (const { pageId, slotIdx, values } of childTable.heap.scan()) {
              if (values[childColIdx] === parentValue) {
                toUpdate.push({ pageId, slotIdx, values: [...values] });
              }
            }
            for (const { pageId, slotIdx, values } of toUpdate) {
              values[childColIdx] = null;
              childTable.heap.delete(pageId, slotIdx);
              childTable.heap.insert(values);
            }
          } else {
            // RESTRICT: check if any child rows exist
            for (const { values } of childTable.heap.scan()) {
              if (values[childColIdx] === parentValue) {
                throw new Error(`Cannot delete: row is referenced by ${childTableName}(${col.name})`);
              }
            }
          }
        }
      }
    }
  }

  // Handle foreign key actions when a parent row's PK is updated
  _handleForeignKeyUpdate(parentTableName, parentTable, oldValues, newValues) {
    for (const [childTableName, childTable] of this.tables) {
      for (const col of childTable.schema) {
        if (col.references && col.references.table === parentTableName) {
          const parentColIdx = parentTable.schema.findIndex(c => c.name === col.references.column);
          const oldValue = oldValues[parentColIdx];
          const newValue = newValues[parentColIdx];
          if (oldValue === newValue) continue;
          
          const childColIdx = childTable.schema.findIndex(c => c.name === col.name);
          
          if (col.references.onUpdate === 'CASCADE') {
            const toUpdate = [];
            for (const { pageId, slotIdx, values: childValues } of childTable.heap.scan()) {
              if (childValues[childColIdx] === oldValue) {
                toUpdate.push({ pageId, slotIdx, values: childValues });
              }
            }
            for (const { pageId, slotIdx, values: childValues } of toUpdate) {
              const updated = [...childValues];
              updated[childColIdx] = newValue;
              childTable.heap.delete(pageId, slotIdx);
              childTable.heap.insert(updated);
            }
          } else if (col.references.onUpdate === 'SET NULL') {
            // Check if the FK column has NOT NULL constraint
            const childCol = childTable.schema[childColIdx];
            if (childCol && childCol.notNull) {
              throw new Error(`Cannot SET NULL on column ${childCol.name}: NOT NULL constraint violated`);
            }
            const toUpdate = [];
            for (const { pageId, slotIdx, values: childValues } of childTable.heap.scan()) {
              if (childValues[childColIdx] === oldValue) {
                toUpdate.push({ pageId, slotIdx, values: childValues });
              }
            }
            for (const { pageId, slotIdx, values: childValues } of toUpdate) {
              const updated = [...childValues];
              updated[childColIdx] = null;
              childTable.heap.delete(pageId, slotIdx);
              childTable.heap.insert(updated);
            }
          }
        }
      }
    }
  }

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

  _merge(ast) {
    const targetTable = this.tables.get(ast.target);
    if (!targetTable) throw new Error(`Table ${ast.target} not found`);
    const sourceTable = this.tables.get(ast.source);
    if (!sourceTable) throw new Error(`Table ${ast.source} not found`);
    
    const targetAlias = ast.targetAlias || ast.target;
    const sourceAlias = ast.sourceAlias || ast.source;
    
    let updated = 0, inserted = 0;
    
    // For each source row, check if it matches any target row
    for (const sourceItem of sourceTable.heap.scan()) {
      const sourceRow = this._valuesToRow(sourceItem.values, sourceTable.schema, sourceAlias);
      
      let matched = false;
      
      // Check against all target rows
      for (const targetItem of targetTable.heap.scan()) {
        const targetRow = this._valuesToRow(targetItem.values, targetTable.schema, targetAlias);
        const mergedRow = { ...targetRow, ...sourceRow };
        
        if (this._evalExpr(ast.on, mergedRow)) {
          matched = true;
          
          // Find WHEN MATCHED clause
          const matchClause = ast.whenClauses.find(c => c.type === 'MATCHED');
          if (matchClause && matchClause.action === 'UPDATE') {
            const newValues = [...targetItem.values];
            for (const assignment of matchClause.assignments) {
              const colIdx = targetTable.schema.findIndex(c => c.name === assignment.column);
              if (colIdx >= 0) {
                newValues[colIdx] = this._evalValue(assignment.value, mergedRow);
              }
            }
            // Validate constraints before modifying
            this._validateConstraintsForUpdate(targetTable, newValues, { pageId: targetItem.pageId, slotIdx: targetItem.slotIdx }, targetItem.values);
            targetTable.heap.delete(targetItem.pageId, targetItem.slotIdx);
            targetTable.heap.insert(newValues);
            updated++;
          }
          break; // Only match once per source row
        }
      }
      
      if (!matched) {
        // Find WHEN NOT MATCHED clause
        const notMatchClause = ast.whenClauses.find(c => c.type === 'NOT_MATCHED');
        if (notMatchClause && notMatchClause.action === 'INSERT') {
          const values = notMatchClause.values.map(v => this._evalValue(v, sourceRow));
          this._validateConstraints(targetTable, values);
          targetTable.heap.insert(values);
          inserted++;
        }
      }
    }
    
    // Invalidate cache
    if (this._resultCache) this._resultCache.clear();
    
    return { type: 'OK', message: `MERGE: ${updated} updated, ${inserted} inserted`, updated, inserted };
  }

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

  _vacuum(ast) {
    const tables = ast.table ? [this.tables.get(ast.table.toUpperCase()) || this.tables.get(ast.table)] 
                             : [...this.tables.values()];
    let totalDead = 0, totalBytes = 0, totalPages = 0, totalTables = 0;

    // MVCC-level vacuum (clean old versions across all keys)
    if (this._mvccManager) {
      try {
        const gcResult = this._mvccManager.gc();
        totalDead += gcResult.cleaned;
      } catch (e) {
        // GC is best-effort
      }
    }

    for (const table of tables) {
      if (!table) continue;
      totalTables++;
      
      // Table-level MVCC vacuum
      if (table.mvccHeap && this._mvccManager) {
        try {
          const result = table.mvccHeap.vacuum(this._mvccManager);
          totalDead += result.deadTuplesRemoved || 0;
          totalBytes += result.bytesFreed || 0;
          totalPages += result.pagesCompacted || 0;
          
          // After removing dead tuples, rebuild indexes to clear stale HOT chain entries
          // and ensure all index entries point to live tuples
          if (result.deadTuplesRemoved > 0 && table.indexes && table.indexes.size > 0) {
            this._rebuildIndexes(table);
          }
        } catch (e) {
          // Table-level vacuum not supported, skip
        }
        continue;
      }
      
      // Non-MVCC vacuum: compact heap pages, update statistics
      const heap = table.heap;
      if (!heap) continue;
      
      // Count live tuples and update statistics
      let liveRows = 0;
      if (typeof heap.scan === 'function') {
        for (const _ of heap.scan()) liveRows++;
      } else if (heap.rowCount !== undefined) {
        liveRows = heap.rowCount;
      }
      
      // Update table-level stats
      if (table.stats) {
        table.stats.rowCount = liveRows;
        table.stats.lastVacuum = Date.now();
      }
      // Reset dead tuple counter after vacuum
      table.deadTupleCount = 0;
      
      // For file-backed heaps, flush dirty pages
      if (typeof heap.flush === 'function') {
        heap.flush();
      }
      
      // Clear HOT chains and rebuild indexes after vacuum
      if (heap._hotChains && heap._hotChains.size > 0 && table.indexes && table.indexes.size > 0) {
        this._rebuildIndexes(table);
      }
    }

    return {
      type: 'OK',
      message: `VACUUM: ${totalTables} table(s) processed, ${totalDead} dead tuples removed, ${totalBytes} bytes freed`,
      details: { tablesProcessed: totalTables, deadTuplesRemoved: totalDead, bytesFreed: totalBytes, pagesCompacted: totalPages },
    };
  }

  _checkpoint() {
    // CHECKPOINT command — creates a WAL checkpoint for durability
    // In the base Database class (no WAL), this is a no-op but still valid SQL.
    // TransactionalDatabase overrides this with real fuzzy checkpoint logic.
    const stats = {
      tables: this.tables.size,
      totalRows: 0,
    };
    for (const [, table] of this.tables) {
      if (table.heap && table.heap._pages) {
        for (const page of table.heap._pages) {
          stats.totalRows += page ? page.filter(Boolean).length : 0;
        }
      }
    }
    return {
      type: 'CHECKPOINT',
      message: `CHECKPOINT complete: ${stats.tables} tables, ${stats.totalRows} rows`,
      details: stats,
    };
  }

  // === Prepared Statements ===
  
  _prepareSql(ast) {
    const name = ast.name;
    if (this._prepared.has(name)) {
      throw new Error(`Prepared statement '${name}' already exists`);
    }
    this._prepared.set(name, { ast: ast.query, name });
    return { message: `PREPARE ${name}` };
  }

  _executePrepared(ast) {
    const name = ast.name;
    if (!this._prepared.has(name)) {
      throw new Error(`Prepared statement '${name}' not found`);
    }
    const stmt = this._prepared.get(name);
    
    // Bind parameters: replace PARAM nodes in the AST with literal values
    const paramValues = ast.params.map(p => {
      if (p.type === 'literal') return p.value;
      if (p.type === 'PARAM') throw new Error('Cannot use parameters in EXECUTE parameter list');
      return p.value;
    });
    
    const boundAst = this._bindParams(JSON.parse(JSON.stringify(stmt.ast)), paramValues);
    return this.execute_ast(boundAst);
  }

  _deallocate(ast) {
    if (ast.all) {
      const count = this._prepared.size;
      this._prepared.clear();
      return { message: `DEALLOCATE ALL (${count} statements)` };
    }
    if (!this._prepared.has(ast.name)) {
      throw new Error(`Prepared statement '${ast.name}' not found`);
    }
    this._prepared.delete(ast.name);
    return { message: `DEALLOCATE ${ast.name}` };
  }

  /**
   * Bind parameter values into an AST by replacing PARAM nodes with literals.
   */
  _bindParams(node, params) {
    if (!node || typeof node !== 'object') return node;
    
    if (node.type === 'PARAM') {
      const idx = node.index - 1; // $1 is index 0
      if (idx < 0 || idx >= params.length) {
        throw new Error(`Parameter $${node.index} not provided (got ${params.length} params)`);
      }
      return { type: 'literal', value: params[idx] };
    }
    
    // Recursively bind in all object properties
    for (const key of Object.keys(node)) {
      if (Array.isArray(node[key])) {
        node[key] = node[key].map(item => this._bindParams(item, params));
      } else if (typeof node[key] === 'object' && node[key] !== null) {
        node[key] = this._bindParams(node[key], params);
      }
    }
    
    return node;
  }

  /**
   * Programmatic API: prepare a statement for repeated execution.
   * Returns a PreparedStatement object.
   */
  prepare(sql) {
    const ast = parse(sql);
    const name = `__stmt_${this._prepared.size}`;
    this._prepared.set(name, { ast, name });
    
    const db = this;
    return {
      name,
      execute(...params) {
        // Accept either execute([...params]) or execute(p1, p2, ...)
        const flatParams = params.length === 1 && Array.isArray(params[0]) ? params[0] : params;
        const bound = db._bindParams(JSON.parse(JSON.stringify(ast)), flatParams);
        return db.execute_ast(bound);
      },
      close() {
        db._prepared.delete(name);
      },
    };
  }

  _analyzeTable(ast) {
    const planner = new QueryPlanner(this);
    const tables = ast.table ? [ast.table] : [...this.tables.keys()];
    const results = [];

    for (const tableName of tables) {
      if (!this.tables.has(tableName)) continue;
      const stats = planner.analyzeTable(tableName);
      results.push({
        table: tableName,
        rows: stats.rowCount,
        pages: stats.pageCount,
        columns: [...stats.columns.entries()].map(([name, cs]) => ({
          name,
          ndv: cs.ndv,
          nulls: cs.nullCount,
          min: cs.min,
          max: cs.max,
          avg_width: Math.round(cs.avgWidth),
        })),
      });
    }

    return {
      type: 'ANALYZE',
      tables: results,
      message: `Analyzed ${results.length} table(s): ${results.map(r => `${r.table}(${r.rows} rows)`).join(', ')}`,
    };
  }

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
