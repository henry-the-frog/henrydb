// db.js — HenryDB query executor
// Ties together: HeapFile, BPlusTree, SQL parser

import { HeapFile, encodeTuple, decodeTuple } from './page.js';
import { BPlusTree } from './btree.js';
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
import { QueryStatsCollector } from './query-stats.js';

export class Database {
  constructor(options = {}) {
    this.tables = new Map();  // name -> { heap, schema, indexes }
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
    
    // Store SELECT results in cache
    if (ast.type === 'SELECT' && !sql.trim().toUpperCase().startsWith('EXPLAIN')) {
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
        if (f.table) tables.add(f.table);
        else if (typeof f === 'string') tables.add(f);
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
    
    // Snapshot: serialize all tables to JSON
    const snapshot = {};
    for (const [tableName, table] of this.tables) {
      const rows = this.execute(`SELECT * FROM ${tableName}`).rows;
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
    
    // Restore each table
    for (const [tableName, rows] of Object.entries(snapshot)) {
      // Delete all current rows
      this.execute(`DELETE FROM ${tableName}`);
      // Re-insert saved rows
      if (rows.length > 0) {
        const cols = Object.keys(rows[0]);
        for (const row of rows) {
          const vals = cols.map(c => {
            const v = row[c];
            return typeof v === 'string' ? `'${v.replace(/'/g, "''")}'` : (v === null ? 'NULL' : v);
          });
          this.execute(`INSERT INTO ${tableName} (${cols.join(', ')}) VALUES (${vals.join(', ')})`);
        }
      }
    }
    
    // Remove savepoints after the rollback target
    this._savepoints.splice(idx + 1);
    
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
      case 'CREATE_INDEX': return this._createIndex(ast);
      case 'DROP_INDEX': return this._dropIndex(ast);
      case 'ALTER_TABLE': return this._alterTable(ast);
      case 'CREATE_VIEW': return this._createView(ast);
      case 'CREATE_MATVIEW': return this._createMatView(ast);
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
      case 'INSERT': return this._insert(ast);
      case 'INSERT_SELECT': return this._insertSelect(ast);
      case 'SELECT': return this._select(ast);
      case 'VALUES': return this._values(ast);
      case 'UNION': return this._union(ast);
      case 'INTERSECT': return this._intersect(ast);
      case 'EXCEPT': return this._except(ast);
      case 'UPDATE': return this._update(ast);
      case 'DELETE': return this._delete(ast);
      case 'TRUNCATE': return this._truncate(ast);
      case 'MERGE': return this._merge(ast);
      case 'CREATE_SEQUENCE': return this._createSequence(ast);
      case 'SHOW_TABLES': return this._showTables();
      case 'COMMENT_ON': return this._commentOn(ast);
      case 'DESCRIBE': return this._describe(ast);
      case 'EXPLAIN': return this._explain(ast);
      case 'BEGIN': {
        this._inTransaction = true;
        // Auto-create internal savepoint for transaction rollback
        this._handleSavepoint('SAVEPOINT __txn_begin__');
        return { type: 'OK', message: 'BEGIN' };
      }
      case 'COMMIT': {
        this._inTransaction = false;
        // Remove internal savepoint on commit
        const idx = this._savepoints.findLastIndex(sp => sp.name === '__txn_begin__');
        if (idx >= 0) this._savepoints.splice(idx, 1);
        return { type: 'OK', message: 'COMMIT' };
      }
      case 'ROLLBACK': {
        // Restore from internal savepoint
        const idx = this._savepoints.findLastIndex(sp => sp.name === '__txn_begin__');
        if (idx >= 0) {
          this._handleRollbackToSavepoint('ROLLBACK TO __txn_begin__');
          this._savepoints.splice(idx, 1);
        }
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

  _createTable(ast) {
    if (this.tables.has(ast.table)) {
      if (ast.ifNotExists) return { type: 'OK', message: `Table ${ast.table} already exists (IF NOT EXISTS)` };
      throw new Error(`Table ${ast.table} already exists`);
    }
    const schema = ast.columns.map(c => {
      // Handle SERIAL columns: create sequence and set default
      if (c.serial) {
        const seqName = `${ast.table}_${c.name}_seq`;
        this.sequences.set(seqName.toLowerCase(), {
          current: 0, increment: 1, min: 1, max: Infinity,
        });
        return {
          name: c.name,
          type: c.type,
          primaryKey: c.primaryKey || false,
          notNull: true,
          unique: c.unique || false,
          check: c.check || null,
          defaultValue: null,
          references: c.references || null,
          generated: null,
          serial: seqName, // Store sequence name for auto-increment
        };
      }
      return {
        name: c.name,
        type: c.type,
        primaryKey: c.primaryKey || false,
        notNull: c.notNull || false,
        unique: c.unique || false,
        check: c.check || null,
        defaultValue: c.defaultValue ?? null,
        references: c.references || null,
        generated: c.generated || null,
      };
    });
    // Choose storage engine: BTREE (clustered) or HEAP (default)
    let heap;
    const pkCols = schema.filter(c => c.primaryKey);
    const pkCol = pkCols.length === 1 ? pkCols[0] : null;
    if (ast.engine === 'BTREE' && pkCol) {
      const pkIdx = schema.findIndex(c => c.primaryKey);
      heap = new BTreeTable(ast.table, { pkIndices: [pkIdx] });
    } else {
      heap = this._heapFactory(ast.table);
    }
    const indexes = new Map();

    // Create index for single-column primary key only
    // Composite PKs don't get individual column indexes (they're not unique per-column)
    if (pkCol) {
      indexes.set(pkCol.name, new BPlusTree(32));
    }
    
    // Store composite PK metadata for uniqueness enforcement
    if (pkCols.length > 1) {
      // Will be used during INSERT to check composite uniqueness
    }
    
    // Create unique indexes
    for (const col of schema) {
      if (col.unique && !col.primaryKey) {
        indexes.set(`unique_${col.name}`, new BPlusTree(32));
      }
    }

    this.tables.set(ast.table, { heap, schema, indexes });
    this.catalog.push({ name: ast.table, columns: schema });
    
    // Log DDL to WAL for crash recovery
    if (this._dataDir && this.wal && this.wal.logCreateTable) {
      this.wal.logCreateTable(ast.table, schema.map(c => ({ name: c.name, type: c.type })));
    }
    
    return { type: 'OK', message: `Table ${ast.table} created` };
  }

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

  _createTableAs(ast) {
    const result = this._select(ast.query);
    const rows = result.rows || [];
    
    if (rows.length === 0) {
      const cols = ast.query.columns || [];
      const schema = cols.map(c => ({
        name: c.alias || c.name || c.value || 'column',
        type: 'TEXT'
      }));
      this._createTable({
        type: 'CREATE_TABLE',
        table: ast.table,
        ifNotExists: false,
        columns: schema.length > 0 ? schema : [{ name: 'empty', type: 'TEXT' }]
      });
      return { type: 'OK', count: 0 };
    }
    
    // Infer schema from first row
    const schema = Object.keys(rows[0]).map(key => {
      const val = rows[0][key];
      let type = 'TEXT';
      if (typeof val === 'number') type = Number.isInteger(val) ? 'INT' : 'FLOAT';
      return { name: key, type };
    });
    
    this._createTable({
      type: 'CREATE_TABLE',
      table: ast.table,
      ifNotExists: false,
      columns: schema
    });
    
    // Insert all rows
    const table = this.tables.get(ast.table);
    for (const row of rows) {
      const values = schema.map(col => row[col.name]);
      table.heap.insert(values);
    }
    
    return { type: 'OK', count: rows.length };
  }

  _logAlterTableDDL(ast) {
    if (!this.wal || !this.wal.logDDL) return;
    switch (ast.action) {
      case 'ADD_COLUMN': {
        const colName = typeof ast.column === 'string' ? ast.column : (ast.column?.name || 'unknown');
        const type = ast.dataType || (typeof ast.column === 'object' ? ast.column?.type : null) || 'TEXT';
        this.wal.logDDL(`ALTER TABLE ${ast.table} ADD COLUMN ${colName} ${type}`);
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

  _alterTable(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);
    this._logAlterTableDDL(ast);

    switch (ast.action) {
      case 'ADD_COLUMN': {
        // Check column doesn't already exist
        if (table.schema.find(c => c.name === ast.column)) {
          throw new Error(`Column ${ast.column} already exists`);
        }
        table.schema.push({ name: ast.column, type: ast.dataType, primaryKey: false });
        
        // Add default value to all existing rows
        const colIdx = table.schema.length - 1;
        for (const { pageId, slotIdx, values } of table.heap.scan()) {
          values.push(ast.defaultValue ?? null);
          // Re-encode and update the tuple in place
          const encoded = encodeTuple(values);
          table.heap.pages.find(p => p.id === pageId)?.updateTuple(slotIdx, encoded);
        }
        
        return { type: 'OK', message: `Column ${ast.column} added to ${ast.table}` };
      }

      case 'DROP_COLUMN': {
        const colIdx = table.schema.findIndex(c => c.name === ast.column);
        if (colIdx === -1) throw new Error(`Column ${ast.column} not found`);
        if (table.schema[colIdx].primaryKey) throw new Error(`Cannot drop primary key column`);
        
        // Remove from schema
        table.schema.splice(colIdx, 1);
        
        // Remove from all existing rows
        for (const { pageId, slotIdx, values } of table.heap.scan()) {
          values.splice(colIdx, 1);
          const encoded = encodeTuple(values);
          table.heap.pages.find(p => p.id === pageId)?.updateTuple(slotIdx, encoded);
        }
        
        // Remove index if exists
        table.indexes.delete(ast.column);
        
        return { type: 'OK', message: `Column ${ast.column} dropped from ${ast.table}` };
      }

      case 'RENAME_COLUMN': {
        const col = table.schema.find(c => c.name === ast.oldName);
        if (!col) throw new Error(`Column ${ast.oldName} not found`);
        col.name = ast.newName;
        
        // Update index if exists
        if (table.indexes.has(ast.oldName)) {
          const idx = table.indexes.get(ast.oldName);
          table.indexes.delete(ast.oldName);
          table.indexes.set(ast.newName, idx);
        }
        
        return { type: 'OK', message: `Column ${ast.oldName} renamed to ${ast.newName}` };
      }

      default:
        throw new Error(`Unknown ALTER TABLE action: ${ast.action}`);
    }
  }

  _dropTable(ast) {
    if (!this.tables.has(ast.table)) {
      if (ast.ifExists) return { type: 'OK', message: `Table ${ast.table} does not exist (IF EXISTS)` };
      throw new Error(`Table ${ast.table} not found`);
    }
    
    // Check for dependent views
    const dependentViews = [];
    for (const [viewName, view] of this.views) {
      if (view.isCTE) continue; // Skip CTEs
      // Check if the view references this table
      const viewDef = view.sql || view.definition || '';
      if (viewDef.toLowerCase().includes(ast.table.toLowerCase())) {
        dependentViews.push(viewName);
      }
    }
    
    // Check for foreign key references from other tables
    const dependentFKs = [];
    for (const [tableName, table] of this.tables) {
      if (tableName === ast.table) continue;
      for (const col of table.schema) {
        if (col.references && col.references.table === ast.table) {
          dependentFKs.push({ table: tableName, column: col.name });
        }
      }
    }
    
    if ((dependentViews.length > 0 || dependentFKs.length > 0) && !ast.cascade) {
      const deps = [];
      if (dependentViews.length > 0) deps.push(`views: ${dependentViews.join(', ')}`);
      if (dependentFKs.length > 0) deps.push(`foreign keys: ${dependentFKs.map(fk => `${fk.table}.${fk.column}`).join(', ')}`);
      throw new Error(`Cannot drop table ${ast.table}: dependent objects exist (${deps.join('; ')}). Use CASCADE to drop them too.`);
    }
    
    // CASCADE: drop dependent objects
    const dropped = [];
    if (ast.cascade) {
      for (const viewName of dependentViews) {
        this.views.delete(viewName);
        dropped.push(`view ${viewName}`);
      }
      // Remove FK references (set to null) in dependent tables
      for (const { table: depTable, column: depCol } of dependentFKs) {
        const table = this.tables.get(depTable);
        const col = table.schema.find(c => c.name === depCol);
        if (col) delete col.references;
        dropped.push(`FK ${depTable}.${depCol}`);
      }
    }
    
    // WAL: log the drop for crash recovery
    if (this.wal && this.wal.logDropTable) {
      this.wal.logDropTable(ast.table);
    }
    // Remove any indexes for this table
    for (const [idxName, meta] of this.indexCatalog) {
      if (meta.table === ast.table) this.indexCatalog.delete(idxName);
    }
    this.tables.delete(ast.table);
    this.catalog = this.catalog.filter(t => t.name !== ast.table);
    
    const cascadeMsg = dropped.length > 0 ? ` (also dropped: ${dropped.join(', ')})` : '';
    return { type: 'OK', message: `Table ${ast.table} dropped${cascadeMsg}` };
  }

  _createIndex(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    // Check IF NOT EXISTS
    const colName = ast.columns.join(',');
    if (ast.ifNotExists && table.indexes?.has(colName)) {
      return { type: 'OK', message: 'CREATE INDEX' };
    }

    // Validate columns exist
    for (const col of ast.columns) {
      if (!table.schema.find(c => c.name === col)) {
        throw new Error(`Column ${col} not found in table ${ast.table}`);
      }
    }

    // For simplicity, support single-column indexes (composite uses CompositeKey)
    const isComposite = ast.columns.length > 1;
    const colIndices = ast.columns.map(c => table.schema.findIndex(s => s.name === c));

    // Choose index type: HASH or BTREE (default)
    const indexType = (ast.indexType || 'BTREE').toUpperCase();
    let index;
    if (indexType === 'HASH') {
      index = new ExtendibleHashTable(16);
      index._isHash = true; // Tag for query optimizer
    } else {
      index = new BPlusTree(32, { unique: ast.unique || false });
    }
    const colIdx = colIndices[0];

    // CONCURRENTLY: two-phase build
    // Phase 1: Mark index as building (not used by query optimizer)
    const isConcurrent = ast.concurrently || false;
    let buildStats = null;
    if (isConcurrent) {
      buildStats = { phase: 1, rowsIndexed: 0, startTime: Date.now() };
    }

    // Populate from existing data
    const includeColIdxs = (ast.include || []).map(col => 
      table.schema.findIndex(c => c.name === col)
    ).filter(i => i >= 0);

    let rowsScanned = 0;
    for (const { pageId, slotIdx, values } of table.heap.scan()) {
      // Partial index: skip rows that don't match the WHERE clause
      if (ast.where) {
        const row = this._valuesToRow(values, table.schema, ast.table);
        if (!this._evalExpr(ast.where, row)) continue;
      }
      
      const key = isComposite
        ? makeCompositeKey(colIndices.map(i => values[i]))
        : values[colIdx];
      if (ast.unique && !index._isHash) {
        const existing = index.search(key);
        if (existing !== undefined) {
          throw new Error(`Duplicate key ${key} violates unique constraint on index ${ast.name}`);
        }
      } else if (ast.unique && index._isHash) {
        const existing = index.get(key);
        if (existing !== undefined) {
          throw new Error(`Duplicate key ${key} violates unique constraint on index ${ast.name}`);
        }
      }
      const rid = { pageId, slotIdx };
      // Store included column values for covering index
      if (includeColIdxs.length > 0) {
        rid.includedValues = {};
        for (const idx of includeColIdxs) {
          rid.includedValues[table.schema[idx].name] = values[idx];
        }
      }
      if (index._isHash) {
        // Hash indexes store arrays of rids for non-unique support
        const existing = index.get(key);
        if (existing !== undefined) {
          const arr = Array.isArray(existing) ? existing : [existing];
          arr.push(rid);
          index.insert(key, arr);
        } else {
          index.insert(key, rid);
        }
      } else {
        index.insert(key, rid);
      }
      rowsScanned++;
    }

    // CONCURRENTLY Phase 2: validation pass
    // In a real concurrent build, rows could have been modified during Phase 1.
    // We verify the index is consistent with current table state.
    if (isConcurrent) {
      buildStats.phase = 2;
      buildStats.rowsIndexed = rowsScanned;
      // Validate: count entries match scan count (minus filtered)
      let verifyCount = 0;
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        if (ast.where) {
          const row = this._valuesToRow(values, table.schema, ast.table);
          if (!this._evalExpr(ast.where, row)) continue;
        }
        verifyCount++;
      }
      if (verifyCount !== rowsScanned) {
        // Table was modified during build — rebuild needed
        // In single-threaded mode this shouldn't happen, but we handle it for completeness
        throw new Error(`CREATE INDEX CONCURRENTLY failed: table ${ast.table} was modified during build (expected ${rowsScanned} rows, found ${verifyCount})`);
      }
      buildStats.validatedRows = verifyCount;
      buildStats.endTime = Date.now();
    }

    table.indexes.set(colName, index);
    // Store index metadata for the planner
    if (!table.indexMeta) table.indexMeta = new Map();
    table.indexMeta.set(colName, {
      name: ast.name,
      columns: ast.columns,
      include: ast.include || [],
      unique: ast.unique || false,
      partial: ast.where || null,
      indexType,
      concurrently: isConcurrent,
    });
    this.indexCatalog.set(ast.name, {
      table: ast.table,
      columns: ast.columns,
      unique: ast.unique || false,
    });

    this._logCreateIndexDDL(ast);
    const msg = isConcurrent 
      ? `Index ${ast.name} created concurrently (${rowsScanned} rows indexed, validated in ${buildStats.endTime - buildStats.startTime}ms)`
      : `Index ${ast.name} created`;
    return { type: 'OK', message: msg, buildStats };
  }

  _logCreateIndexDDL(ast) {
    if (!this.wal || !this.wal.logDDL) return;
    const unique = ast.unique ? 'UNIQUE ' : '';
    const ifNotExists = ast.ifNotExists ? 'IF NOT EXISTS ' : '';
    const cols = ast.columns.join(', ');
    this.wal.logDDL(`CREATE ${unique}INDEX ${ifNotExists}${ast.name} ON ${ast.table} (${cols})`);
  }

  _dropIndex(ast) {
    const meta = this.indexCatalog.get(ast.name);
    if (!meta) {
      if (ast.ifExists) return { type: 'OK', message: 'DROP INDEX' };
      throw new Error(`Index ${ast.name} not found`);
    }

    // WAL: log the drop
    if (this.wal && this.wal.logDDL) {
      this.wal.logDDL(`DROP INDEX IF EXISTS ${ast.name}`);
    }

    const table = this.tables.get(meta.table);
    if (table) {
      const colName = meta.columns[0];
      // Don't drop primary key indexes
      const isPK = table.schema.find(c => c.name === colName && c.primaryKey);
      if (!isPK) {
        table.indexes.delete(colName);
      }
    }

    this.indexCatalog.delete(ast.name);
    return { type: 'OK', message: `Index ${ast.name} dropped` };
  }

  _createView(ast) {
    if (this.views.has(ast.name) && !ast.orReplace) throw new Error(`View ${ast.name} already exists`);
    this.views.set(ast.name, { query: ast.query });
    return { type: 'OK', message: `View ${ast.name} ${ast.orReplace ? 'replaced' : 'created'}` };
  }

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
      // Clear old data
      table.heap = this._heapFactory(ast.name);
      
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

  _dropView(ast) {
    if (!this.views.has(ast.name)) throw new Error(`View ${ast.name} not found`);
    this.views.delete(ast.name);
    return { type: 'OK', message: `View ${ast.name} dropped` };
  }

  _alterTable(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);
    this._logAlterTableDDL(ast);

    switch (ast.action) {
      case 'ADD_COLUMN': {
        const col = ast.column;
        if (table.schema.find(c => c.name === col.name)) {
          throw new Error(`Column ${col.name} already exists`);
        }
        table.schema.push({ name: col.name, type: col.type, primaryKey: false });

        // Add default value to all existing rows
        const defaultVal = col.default ? col.default.value : null;
        const allRows = [];
        for (const { pageId, slotIdx, values } of table.heap.scan()) {
          allRows.push({ pageId, slotIdx, values });
        }
        for (const row of allRows) {
          table.heap.delete(row.pageId, row.slotIdx);
          table.heap.insert([...row.values, defaultVal]);
        }

        // Rebuild all indexes (RIDs changed)
        this._rebuildIndexes(table);

        // Update catalog
        const catEntry = this.catalog.find(c => c.name === ast.table);
        if (catEntry) catEntry.columns = table.schema;

        return { type: 'OK', message: `Column ${col.name} added` };
      }

      case 'DROP_COLUMN': {
        const col = ast.column;
        const colIdx = table.schema.findIndex(c => c.name === col.name);
        if (colIdx === -1) throw new Error(`Column ${col.name} not found`);
        if (table.schema[colIdx].primaryKey) throw new Error(`Cannot drop primary key column`);

        // Drop any index on this column first
        if (table.indexes.has(col.name)) {
          table.indexes.delete(col.name);
          for (const [idxName, meta] of this.indexCatalog) {
            if (meta.table === ast.table && meta.columns.includes(col.name)) {
              this.indexCatalog.delete(idxName);
            }
          }
        }

        // Remove column from schema
        table.schema.splice(colIdx, 1);

        // Remove column value from all rows
        const allRows = [];
        for (const { pageId, slotIdx, values } of table.heap.scan()) {
          allRows.push({ pageId, slotIdx, values });
        }
        for (const row of allRows) {
          table.heap.delete(row.pageId, row.slotIdx);
          const newValues = [...row.values];
          newValues.splice(colIdx, 1);
          table.heap.insert(newValues);
        }

        // Rebuild remaining indexes (RIDs changed)
        this._rebuildIndexes(table);

        // Update catalog
        const catEntry = this.catalog.find(c => c.name === ast.table);
        if (catEntry) catEntry.columns = table.schema;

        return { type: 'OK', message: `Column ${col.name} dropped` };
      }

      case 'RENAME_COLUMN': {
        const { oldName, newName } = ast.column;
        const col = table.schema.find(c => c.name === oldName);
        if (!col) throw new Error(`Column ${oldName} not found`);
        if (table.schema.find(c => c.name === newName)) throw new Error(`Column ${newName} already exists`);
        col.name = newName;

        // Update index if exists
        if (table.indexes.has(oldName)) {
          const idx = table.indexes.get(oldName);
          table.indexes.delete(oldName);
          table.indexes.set(newName, idx);
        }

        // Update catalog
        const catEntry = this.catalog.find(c => c.name === ast.table);
        if (catEntry) catEntry.columns = table.schema;

        return { type: 'OK', message: `Column ${oldName} renamed to ${newName}` };
      }

      case 'RENAME_TABLE': {
        const tableData = this.tables.get(ast.table);
        this.tables.delete(ast.table);
        this.tables.set(ast.newName, tableData);

        // Update catalog
        const catEntry = this.catalog.find(c => c.name === ast.table);
        if (catEntry) catEntry.name = ast.newName;

        // Update index catalog
        for (const [, meta] of this.indexCatalog) {
          if (meta.table === ast.table) meta.table = ast.newName;
        }

        return { type: 'OK', message: `Table ${ast.table} renamed to ${ast.newName}` };
      }

      default:
        throw new Error(`Unknown ALTER TABLE action: ${ast.action}`);
    }
  }

  _insert(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    let inserted = 0;
    const returnedRows = [];
    
    for (const row of ast.rows) {
      const values = row.map(r => {
        if (r.type === 'literal') return r.value;
        // Evaluate expression (for INSERT VALUES with arithmetic, CASE, etc.)
        try { return this._evalValue(r, {}); } catch { return r.value; }
      });
      
      // UPSERT: ON CONFLICT handling
      if (ast.onConflict) {
        const pkIdx = table.schema.findIndex(c => c.primaryKey);
        const orderedValues = this._orderValues(table, ast.columns, values);
        
        if (pkIdx >= 0) {
          // Check if PK already exists
          let existing = null;
          let existingRid = null;
          for (const tuple of table.heap.scan()) {
            const tupleValues = tuple.values || tuple;
            if (tupleValues[pkIdx] === orderedValues[pkIdx]) { 
              existing = tupleValues; 
              existingRid = { pageId: tuple.pageId, slotIdx: tuple.slotIdx };
              break; 
            }
          }
          
          if (existing) {
            if (ast.onConflict.action === 'NOTHING') {
              continue; // Skip this row
            }
            if (ast.onConflict.action === 'UPDATE') {
              // Build row object for expression evaluation
              const existingRow = {};
              table.schema.forEach((c, i) => { existingRow[c.name] = existing[i]; });
              // Also expose excluded.* (the values that would have been inserted)
              table.schema.forEach((c, i) => { existingRow[`excluded.${c.name}`] = orderedValues[i]; });
              
              // Evaluate SET expressions
              const newValues = [...existing];
              for (const set of ast.onConflict.sets) {
                const colIdx = table.schema.findIndex(c => c.name === set.column);
                if (colIdx >= 0) {
                  newValues[colIdx] = this._evalValue(set.value, existingRow);
                }
              }
              
              // Write back to heap: delete old row, insert new one
              if (existingRid) {
                table.heap.delete(existingRid.pageId, existingRid.slotIdx);
                const newRid = table.heap.insert(newValues);
                
                // Update primary key index
                if (pkIdx >= 0 && table.indexes.has(table.schema[pkIdx].name)) {
                  const idx = table.indexes.get(table.schema[pkIdx].name);
                  idx.insert(newValues[pkIdx], newRid);
                }
              }
              
              if (ast.returning) {
                const retRow = {};
                table.schema.forEach((c, i) => { retRow[c.name] = newValues[i]; });
                returnedRows.push(retRow);
              }
              inserted++;
              continue;
            }
          }
        }
      }
      
      // Check UNIQUE constraints
      for (let ci = 0; ci < table.schema.length; ci++) {
        if (table.schema[ci].unique && values[ci] != null) {
          for (const tuple of table.heap.scan()) {
            const tv = tuple.values || tuple;
            if (tv[ci] === values[ci]) {
              throw new Error(`UNIQUE constraint violated on column ${table.schema[ci].name}: duplicate value '${values[ci]}'`);
            }
          }
        }
      }
      
      const rid = this._insertRow(table, ast.columns, values);
      inserted++;
      
      if (ast.returning) {
        // Read actual inserted values (including SERIAL-assigned IDs)
        const lastTuple = [...table.heap.scan()].pop();
        const actualValues = lastTuple?.values || lastTuple || [];
        const retRow = {};
        table.schema.forEach((c, i) => { retRow[c.name] = actualValues[i]; });
        returnedRows.push(retRow);
      }
    }

    if (ast.returning) {
      const filteredRows = this._resolveReturning(ast.returning, returnedRows);
      return { type: 'ROWS', rows: filteredRows, count: inserted };
    }
    return { type: 'OK', message: `${inserted} row(s) inserted`, count: inserted };
  }

  _insertSelect(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    const result = this.execute_ast(ast.query);
    let inserted = 0;
    
    // Determine how many SELECT columns we expect
    const selectCols = ast.query?.columns?.length || table.schema.length;
    
    for (const row of result.rows) {
      const values = [];
      if (ast.columns) {
        // Explicit column list: INSERT INTO t (col1, col2) SELECT ...
        for (const col of ast.columns) {
          values.push(row[col] !== undefined ? row[col] : null);
        }
      } else {
        // No explicit column list: map SELECT result to table schema by column name or position
        const rowKeys = Object.keys(row);
        // Try name-based mapping first
        let nameMatch = true;
        for (const col of table.schema) {
          if (row[col.name] === undefined && row[col.name] !== null) {
            nameMatch = false;
            break;
          }
        }
        if (nameMatch && table.schema.every(col => col.name in row)) {
          // Name-based mapping: match by column name
          for (const col of table.schema) {
            values.push(row[col.name] !== undefined ? row[col.name] : null);
          }
        } else {
          // Position-based mapping: use only unqualified keys
          const unqualifiedKeys = rowKeys.filter(k => !k.includes('.'));
          for (let i = 0; i < table.schema.length; i++) {
            if (i < unqualifiedKeys.length) {
              values.push(row[unqualifiedKeys[i]]);
            } else {
              values.push(null);
            }
          }
        }
      }
      this._insertRow(table, null, values);
      inserted++;
    }

    return { type: 'OK', message: `${inserted} row(s) inserted`, count: inserted };
  }

  // Validate column constraints (NOT NULL, CHECK) for a row
  _fireTriggers(timing, event, tableName, rowValues) {
    for (const trigger of this.triggers) {
      if (trigger.timing === timing && trigger.event === event && trigger.table === tableName) {
        try {
          this.execute(trigger.bodySql);
        } catch (e) {
          // Trigger errors propagate
          throw new Error(`Trigger ${trigger.name} failed: ${e.message}`);
        }
      }
    }
  }

  _validateConstraints(table, values) {
    for (let i = 0; i < table.schema.length; i++) {
      const col = table.schema[i];
      const val = values[i];

      // NOT NULL constraint
      if (col.notNull && val == null && !col.primaryKey) {
        throw new Error(`NOT NULL constraint violated for column ${col.name}`);
      }

      // CHECK constraint
      if (col.check) {
        const row = {};
        for (let j = 0; j < table.schema.length; j++) {
          row[table.schema[j].name] = values[j];
        }
        const result = this._evalExpr(col.check, row);
        if (!result) {
          throw new Error(`CHECK constraint violated for column ${col.name}`);
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
    if (ast.offset) rows = rows.slice(ast.offset);
    if (ast.limit != null) rows = rows.slice(0, ast.limit);
    
    // Apply SELECT columns
    const isStar = ast.columns.length === 1 && (ast.columns[0].name === '*' || ast.columns[0].type === 'star');
    const hasQualifiedStar = ast.columns.some(c => c.type === 'qualified_star');
    if (!isStar) {
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

  _orderValues(table, columns, values) {
    if (columns) {
      const ordered = new Array(table.schema.length).fill(null);
      for (let i = 0; i < table.schema.length; i++) {
        if (table.schema[i].defaultValue != null) ordered[i] = this._resolveDefault(table.schema[i].defaultValue);
      }
      for (let i = 0; i < columns.length; i++) {
        const colIdx = table.schema.findIndex(c => c.name === columns[i]);
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
        const colIdx = table.schema.findIndex(c => c.name === columns[i]);
        if (colIdx === -1) throw new Error(`Column ${columns[i]} not found`);
        orderedValues[colIdx] = values[i];
      }
    } else {
      orderedValues = values;
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
    this._fireTriggers('BEFORE', 'INSERT', tableName, orderedValues);

    const rid = table.heap.insert(orderedValues);

    // WAL: log the insert
    const txId = this._currentTxId || this._nextTxId++;
    this.wal.appendInsert(txId, tableName, rid.pageId, rid.slotIdx, orderedValues);
    if (!this._currentTxId) {
      // Auto-commit mode: immediately commit
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
    this._fireTriggers('AFTER', 'INSERT', tableName, orderedValues);

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
      let result = this._selectInner(optimizedAst);
      
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
  _executeJoinWithRows(leftRows, rightRows, join, rightAlias) {
    const result = [];
    
    if (join.joinType === 'CROSS') {
      for (const leftRow of leftRows) {
        for (const rightRow of rightRows) {
          result.push({ ...leftRow, ...rightRow });
        }
      }
      return result;
    }

    // Hash join optimization for equi-join in _executeJoinWithRows
    // Currently uses nested loop for correctness with complex column resolution.
    // TODO: implement hash join with proper column resolution for performance.

    // Fallback: nested loop join
    const rightMatched = new Set();
    for (const leftRow of leftRows) {
      let matched = false;
      for (let ri = 0; ri < rightRows.length; ri++) {
        const rightRow = rightRows[ri];
        // For NATURAL JOIN: preserve left values before merge overwrites them
        const combined = { ...leftRow, ...rightRow };
        if (join.natural) {
          for (const key of Object.keys(leftRow)) {
            combined[`__natural_left_${key}`] = leftRow[key];
          }
        }
        if (!join.on || this._evalExpr(join.on, combined)) {
          result.push(combined);
          matched = true;
          rightMatched.add(ri);
        }
      }
      if (!matched && (join.joinType === 'LEFT' || join.joinType === 'LEFT_OUTER' || join.joinType === 'FULL' || join.joinType === 'FULL_OUTER')) {
        const nullRow = {};
        for (const key of Object.keys(rightRows[0] || {})) {
          nullRow[key] = null;
        }
        result.push({ ...leftRow, ...nullRow });
      }
    }
    
    // RIGHT and FULL: add unmatched right rows
    if (join.joinType === 'RIGHT' || join.joinType === 'RIGHT_OUTER' || join.joinType === 'FULL' || join.joinType === 'FULL_OUTER') {
      for (let ri = 0; ri < rightRows.length; ri++) {
        if (!rightMatched.has(ri)) {
          const nullRow = {};
          for (const key of Object.keys(leftRows[0] || {})) {
            nullRow[key] = null;
          }
          result.push({ ...nullRow, ...rightRows[ri] });
        }
      }
    }
    
    return result;
  }

  _selectInner(ast) {
    // Handle SELECT without FROM (e.g., SELECT 1 AS n)
    if (!ast.from) {
      const row = {};
      let noFromExprIdx = 0;
      for (const col of ast.columns) {
        if (col.type === 'expression') {
          const name = col.alias || `expr_${noFromExprIdx++}`;
          row[name] = this._evalValue(col.expr, {});
        } else if (col.type === 'scalar_subquery') {
          const name = col.alias || 'subquery';
          const subResult = this._evalSubquery(col.subquery, {});
          row[name] = subResult.length > 0 ? Object.values(subResult[0])[0] : null;
        } else if (col.type === 'column') {
          const name = col.alias || String(col.name);
          // If the column name is a number literal, use it directly
          row[name] = typeof col.name === 'number' ? col.name : col.name;
        } else if (col.type === 'function') {
          const name = col.alias || `${col.func}(...)`;
          row[name] = this._evalFunction(col.func, col.args, {});
        }
      }
      return { type: 'ROWS', rows: [row] };
    }

    // Check if FROM is GENERATE_SERIES
    const tableName = ast.from.table;
    if (tableName === '__generate_series') {
      const start = this._evalValue(ast.from.start, {});
      const stop = this._evalValue(ast.from.stop, {});
      const step = ast.from.step ? this._evalValue(ast.from.step, {}) : 1;
      let rows = [];
      if (step > 0) {
        for (let i = start; i <= stop; i += step) {
          rows.push({ value: i });
        }
      } else if (step < 0) {
        for (let i = start; i >= stop; i += step) {
          rows.push({ value: i });
        }
      }
      // Apply WHERE
      if (ast.where) rows = rows.filter(row => this._evalExpr(ast.where, row));
      
      // Handle aggregates / GROUP BY on GENERATE_SERIES results
      const gsHasAggregates = ast.columns.some(c => c.type === 'aggregate');
      const gsHasWindow = ast.columns.some(c => c.type === 'window');
      if (ast.groupBy) {
        return this._selectWithGroupBy(ast, rows);
      }
      if (gsHasAggregates) {
        return { type: 'ROWS', rows: [this._computeAggregates(ast.columns, rows)] };
      }
      if (gsHasWindow) {
        rows = this._computeWindowFunctions(ast.columns, rows, ast.windowDefs);
      }
      
      // Apply columns
      return this._applySelectColumns(ast, rows);
    }

    // UNNEST: expand array to rows
    if (tableName === '__unnest') {
      const arrayVal = this._evalValue(ast.from.arrayExpr, {});
      let arr = Array.isArray(arrayVal) ? arrayVal : [];
      if (typeof arrayVal === 'string') {
        try { arr = JSON.parse(arrayVal); } catch { arr = []; }
      }
      const colName = ast.from.columnAlias || 'value';
      let rows = arr.map(v => ({ [colName]: v }));
      
      if (ast.where) rows = rows.filter(row => this._evalExpr(ast.where, row));
      if (ast.groupBy) return this._selectWithGroupBy(ast, rows);
      
      const hasAgg = ast.columns.some(c => c.type === 'aggregate');
      if (hasAgg) return { type: 'ROWS', rows: [this._computeAggregates(ast.columns, rows)] };
      
      if (ast.orderBy) rows = this._sortRows(rows, ast.orderBy);
      if (ast.limit != null) rows = rows.slice(ast.offset || 0, (ast.offset || 0) + ast.limit);
      return { type: 'ROWS', rows };
    }

    // Check if FROM is a subquery
    if (tableName === '__subquery') {
      const subAst = ast.from.subquery;
      // Handle UNION/INTERSECT/EXCEPT in derived tables
      const subResult = (subAst.type === 'UNION' || subAst.type === 'INTERSECT' || subAst.type === 'EXCEPT')
        ? this.execute_ast(subAst)
        : this._select(subAst);
      let rows = subResult.rows || [];
      
      // Add qualified column names (sub.col) so alias-prefixed references work
      const subAlias = ast.from.alias;
      if (subAlias) {
        rows = rows.map(row => {
          const newRow = { ...row };
          for (const key of Object.keys(row)) {
            if (!key.includes('.')) {
              newRow[`${subAlias}.${key}`] = row[key];
            }
          }
          return newRow;
        });
        // Strip table alias prefix from column references (e.g., sub.col → col)
        const prefix = subAlias + '.';
        for (const col of ast.columns) {
          if (col.name && col.name.startsWith(prefix)) {
            col.name = col.name.substring(prefix.length);
          }
        }
        if (ast.orderBy) {
          for (const o of ast.orderBy) {
            if (typeof o.column === 'string' && o.column.startsWith(prefix)) {
              o.column = o.column.substring(prefix.length);
            }
          }
        }
      }
      if (ast.where) rows = rows.filter(row => this._evalExpr(ast.where, row));
      for (const join of ast.joins || []) {
        rows = this._executeJoin(rows, join, ast.from.alias || '__subquery');
      }
      // Handle aggregates / GROUP BY on subquery results
      const sqHasAggregates = ast.columns.some(c => c.type === 'aggregate');
      const sqHasWindow = ast.columns.some(c => c.type === 'window');
      if (ast.groupBy) {
        return this._selectWithGroupBy(ast, rows);
      }
      if (sqHasAggregates) {
        return { type: 'ROWS', rows: [this._computeAggregates(ast.columns, rows)] };
      }
      if (sqHasWindow) {
        rows = this._computeWindowFunctions(ast.columns, rows, ast.windowDefs);
      }
      return this._applySelectColumns(ast, rows);
    }

    // Check if FROM references a view
    if (this.views.has(tableName)) {
      const viewDef = this.views.get(tableName);
      // Execute view query or use materialized rows (for recursive CTEs)
      let rows;
      if (viewDef.isMaterialized && this.tables.has(tableName)) {
        // Materialized view: read from stored table
        const mvTable = this.tables.get(tableName);
        rows = [];
        for (const { values } of mvTable.heap.scan()) {
          const row = this._valuesToRow(values, mvTable.schema, tableName);
          rows.push(row);
        }
      } else if (viewDef.materializedRows) {
        rows = [...viewDef.materializedRows];
      } else {
        // Execute the view query — handle UNION/INTERSECT/EXCEPT via execute_ast
        const viewResult = viewDef.query.type === 'UNION' || viewDef.query.type === 'INTERSECT' || viewDef.query.type === 'EXCEPT'
          ? this.execute_ast(viewDef.query)
          : this._select(viewDef.query);
        rows = viewResult.rows;
      }

      // Add qualified column names (alias.col) for alias-prefixed references
      const viewAlias = ast.from.alias || ast.from.table;
      if (viewAlias) {
        rows = rows.map(row => {
          const newRow = { ...row };
          for (const key of Object.keys(row)) {
            if (!key.includes('.')) {
              newRow[`${viewAlias}.${key}`] = row[key];
            }
          }
          return newRow;
        });
      }

      // Apply WHERE
      if (ast.where) {
        rows = rows.filter(row => this._evalExpr(ast.where, row));
      }

      // Handle JOINs on view results
      for (const join of ast.joins || []) {
        rows = this._executeJoin(rows, join, viewAlias || tableName);
      }

      // Handle aggregates / GROUP BY on view results
      const hasAggregates = ast.columns.some(c => c.type === 'aggregate');
      const hasWindow = ast.columns.some(c => c.type === 'window');
      if (ast.groupBy) {
        return this._selectWithGroupBy(ast, rows);
      }
      if (hasAggregates) {
        return { type: 'ROWS', rows: [this._computeAggregates(ast.columns, rows)] };
      }
      if (hasWindow) {
        rows = this._computeWindowFunctions(ast.columns, rows, ast.windowDefs);
      }

      // ORDER BY
      if (ast.orderBy) {
        rows.sort((a, b) => {
          for (const { column, direction } of ast.orderBy) {
            const av = this._orderByValue(column, a);
            const bv = this._orderByValue(column, b);
            const aNull = av === null || av === undefined;
            const bNull = bv === null || bv === undefined;
            if (aNull && bNull) continue;
            if (aNull) return direction === 'DESC' ? 1 : -1;
            if (bNull) return direction === 'DESC' ? -1 : 1;
            const cmp = av < bv ? -1 : av > bv ? 1 : 0;
            if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
          }
          return 0;
        });
      }

      if (ast.offset) rows = rows.slice(ast.offset);
      if (ast.limit != null) rows = rows.slice(0, ast.limit);

      // Project if not star
      if (ast.columns[0]?.type !== 'star') {
        rows = rows.map(row => {
          const result = {};
          let viewExprIdx = 0;
          for (const col of ast.columns) {
            if (col.type === 'function') {
              const name = col.alias || `${col.func}(...)`;
              result[name] = this._evalFunction(col.func, col.args, row);
            } else if (col.type === 'expression') {
              const name = col.alias || `expr_${viewExprIdx++}`;
              result[name] = this._evalValue(col.expr, row);
            } else if (col.type === 'window') {
              const name = col.alias || `${col.func}(${col.arg || ''})`;
              result[name] = row[`__window_${name}`];
            } else {
              const rawName = col.alias || col.name;
              // Strip table alias prefix for output key: ds.dept_name → dept_name
              const name = rawName.includes('.') ? rawName.split('.').pop() : rawName;
              result[name] = row[col.name] !== undefined ? row[col.name] : row[rawName] !== undefined ? row[rawName] : row[name];
            }
          }
          return result;
        });
      }

      // DISTINCT
      if (ast.distinct) {
        const seen = new Set();
        rows = rows.filter(row => {
          const key = JSON.stringify(row);
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });
      }

      return { type: 'ROWS', rows };
    }

    const table = this.tables.get(ast.from.table);
    if (!table) throw new Error(`Table ${ast.from.table} not found`);

    let rows = [];
    const hasJoins = ast.joins && ast.joins.length > 0;

    // Apply predicate pushdown for joins: push WHERE filters to individual table scans
    let workingAst = ast;
    if (hasJoins && ast.where) {
      const { ast: pushedAst, pushed } = pushdownPredicates(ast);
      if (pushed > 0) {
        workingAst = pushedAst;
      }
    }

    // Try index scan for simple equality WHERE clauses (only when no JOINs)
    if (!hasJoins) {
      // Set requested columns for index-only scan detection
      this._requestedColumns = ast.columns[0]?.type === 'star' ? null : 
        ast.columns.filter(c => c.type === 'column' && typeof c.name === 'string').map(c => {
          const name = c.name;
          return name.includes('.') ? name.split('.').pop() : name;
        });
      const indexScan = this._tryIndexScan(table, ast.where, ast.from.alias || ast.from.table);
      this._requestedColumns = null;
      if (indexScan) {
        rows = indexScan.rows;
        // Apply remaining where conditions (if any beyond what the index handled)
        if (indexScan.residual) {
          rows = rows.filter(row => this._evalExpr(indexScan.residual, row));
        }
      } else {
        // Full table scan with optional early LIMIT
        // Can push limit into scan when: no ORDER BY, no GROUP BY, no DISTINCT, no HAVING, no windows
        const canEarlyLimit = ast.limit != null && !ast.orderBy && !ast.groupBy && !ast.distinct &&
          !ast.having && !ast.columns.some(c => c.type === 'window');
        const earlyLimit = canEarlyLimit ? (ast.limit + (ast.offset || 0)) : Infinity;
        
        for (const { pageId, slotIdx, values } of table.heap.scan()) {
          const row = this._valuesToRow(values, table.schema, ast.from.alias || ast.from.table);
          if (ast.where && !this._evalExpr(ast.where, row)) continue;
          rows.push(row);
          if (rows.length >= earlyLimit) break;
        }
      }
    } else {
      // With JOINs: scan FROM table, apply pushed filter if available
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const row = this._valuesToRow(values, table.schema, workingAst.from.alias || workingAst.from.table);
        rows.push(row);
      }
      // Apply pushed-down filter for the FROM table
      if (workingAst.from.filter) {
        rows = rows.filter(row => this._evalExpr(workingAst.from.filter, row));
      }
    }

    // Apply TABLESAMPLE (random row sampling)
    if (ast.from.tablesample) {
      const pct = ast.from.tablesample.percentage / 100;
      rows = rows.filter(() => Math.random() < pct);
    }

    // Handle JOINs — optimize join order if possible
    let joinList = workingAst.joins || [];
    if (joinList.length >= 2) {
      const fromTableName = workingAst.from.table;
      if (fromTableName) {
        joinList = this._optimizeJoinOrder(fromTableName, joinList);
      }
    }
    for (const join of joinList) {
      rows = this._executeJoin(rows, join, workingAst.from.alias || workingAst.from.table);
    }

    // WHERE filter after JOINs (only remaining predicates)
    if (hasJoins && workingAst.where) {
      rows = rows.filter(row => this._evalExpr(workingAst.where, row));
    }

    // Aggregates / GROUP BY / Window functions
    const hasAggregates = ast.columns.some(c =>
      c.type === 'aggregate' || this._exprContainsAggregate(c.expr)
    );
    const hasWindow = ast.columns.some(c => c.type === 'window');

    if (ast.groupBy) {
      return this._selectWithGroupBy(ast, rows);
    }
    if (hasAggregates && !hasWindow) {
      const aggRow = this._computeAggregates(ast.columns, rows);
      // HAVING without GROUP BY: entire result is one group
      if (ast.having) {
        // Evaluate HAVING using the group computation path
        const computeAgg = (func, arg, distinct) => {
          return this._computeSingleAggregate(func, arg, rows, distinct);
        };
        const passes = this._evalGroupCond(ast.having, rows, aggRow, computeAgg);
        if (!passes) {
          return { type: 'ROWS', rows: [] };
        }
      }
      return { type: 'ROWS', rows: [aggRow] };
    }

    // Window functions: compute window values before projection
    if (hasWindow) {
      rows = this._computeWindowFunctions(ast.columns, rows, ast.windowDefs);
    }

    // Build alias→expression map for ORDER BY resolution
    const aliasExprs = new Map();
    for (const col of ast.columns) {
      if (col.alias) {
        if (col.type === 'expression') {
          aliasExprs.set(col.alias, col.expr);
        } else if (col.type === 'function') {
          aliasExprs.set(col.alias, col);
        } else if (col.type === 'column') {
          // Simple column alias: val as v → resolve as column_ref
          aliasExprs.set(col.alias, { type: 'column_ref', name: col.name });
        } else if (col.type === 'aggregate') {
          aliasExprs.set(col.alias, col);
        }
      }
    }

    // ORDER BY
    if (ast.orderBy) {
      rows.sort((a, b) => {
        for (const { column, direction, nulls } of ast.orderBy) {
          let av, bv;
          if (typeof column === 'number') {
            // Numeric column reference (ORDER BY 1, 2, etc.)
            // Resolve using SELECT column list
            const selCol = ast.columns[column - 1];
            if (selCol) {
              const colName = selCol.alias || selCol.name;
              av = a[colName] !== undefined ? a[colName] : this._resolveColumn(colName, a);
              bv = b[colName] !== undefined ? b[colName] : this._resolveColumn(colName, b);
            }
          } else if (typeof column === 'object' && column !== null) {
            // Expression node (ORDER BY -val, ORDER BY col + 1, etc.)
            av = this._evalValue(column, a);
            bv = this._evalValue(column, b);
          } else if (column in a) {
            // Direct key match (works for aliased columns in the result)
            av = a[column];
            bv = b[column];
          } else if (aliasExprs.has(column)) {
            const expr = aliasExprs.get(column);
            if (expr.type === 'function') {
              av = this._evalFunction(expr.func, expr.args, a);
              bv = this._evalFunction(expr.func, expr.args, b);
            } else {
              av = this._evalValue(expr, a);
              bv = this._evalValue(expr, b);
            }
          } else if (a[`__window_${column}`] !== undefined) {
            // Window function alias — resolve from computed window columns
            av = a[`__window_${column}`];
            bv = b[`__window_${column}`];
          } else {
            av = this._resolveColumn(column, a);
            bv = this._resolveColumn(column, b);
          }
          // NULLS FIRST: nulls sort before non-nulls; NULLS LAST: nulls sort after
          // Default: NULLS FIRST for ASC, NULLS LAST for DESC (PostgreSQL convention)
          const nullsFirst = nulls === 'FIRST' || (nulls == null && direction !== 'DESC');
          if (av == null && bv == null) continue;
          if (av == null) return nullsFirst ? -1 : 1;
          if (bv == null) return nullsFirst ? 1 : -1;
          const cmp = av < bv ? -1 : av > bv ? 1 : 0;
          if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
        }
        return 0;
      });
    }

    // OFFSET (before LIMIT, but LIMIT deferred until after DISTINCT)
    if (ast.offset && !ast.distinct) rows = rows.slice(ast.offset);

    // LIMIT (only apply before projection if no DISTINCT)
    if (ast.limit != null && !ast.distinct) rows = rows.slice(0, ast.limit);

    // Project columns
    const projected = rows.map(row => {
      if (ast.columns[0]?.type === 'star') {
        // Strip qualified column names (table.col) for clean output
        const clean = {};
        for (const [key, val] of Object.entries(row)) {
          if (!key.includes('.') && !key.startsWith('__')) clean[key] = val;
        }
        return clean;
      }
      const result = {};
      let exprIdx = 0;
      for (const col of ast.columns) {
        if (col.type === 'function') {
          const name = col.alias || `${col.func}(...)`;
          result[name] = this._evalFunction(col.func, col.args, row);
        } else if (col.type === 'expression') {
          const name = col.alias || `expr_${exprIdx++}`;
          result[name] = this._evalValue(col.expr, row);
        } else if (col.type === 'scalar_subquery') {
          const name = col.alias || 'subquery';
          const subResult = this._evalSubquery(col.subquery, row);
          result[name] = subResult.length > 0 ? Object.values(subResult[0])[0] : null;
        } else if (col.type === 'window') {
          const name = col.alias || `${col.func}(${col.arg || ''})`;
          result[name] = row[`__window_${name}`];
        } else if (col.name) {
          // Strip table prefix from output name (c1.email → email)
          const colName = String(col.name);
          const baseName = colName.includes('.') ? colName.split('.').pop() : colName;
          const name = col.alias || baseName;
          result[name] = this._resolveColumn(colName, row);
        } else if (col.type === 'qualified_star') {
          // Expand table.* — add all columns from that table
          const prefix = col.table + '.';
          for (const [key, val] of Object.entries(row)) {
            if (key.startsWith(prefix)) {
              result[key.slice(prefix.length)] = val;
            } else if (!key.includes('.')) {
              // In single-table or self-join context, include unqualified columns
              // only if no other table has claimed them
              // (we'll skip this for now — prefer qualified matches)
            }
          }
          // If no qualified matches found, try matching via table schema
          if (Object.keys(result).length === 0 || !Object.keys(result).some(k => k !== undefined)) {
            const table = this.tables.get(col.table);
            if (table) {
              for (const s of table.schema) {
                const val = row[prefix + s.name] ?? row[s.name];
                if (val !== undefined) result[s.name] = val;
              }
            }
          }
        }
      }
      return result;
    });

    // DISTINCT / DISTINCT ON
    let finalRows = projected;
    if (ast.distinctOn) {
      // DISTINCT ON: keep first row per unique combination of ON expressions
      // Uses pre-ORDER BY rows for key evaluation, then filters projected
      const seen = new Set();
      finalRows = [];
      for (let i = 0; i < projected.length; i++) {
        const row = rows[i]; // use pre-projection row for expression evaluation
        const key = ast.distinctOn.map(expr => JSON.stringify(this._evalValue(expr, row))).join('|');
        if (!seen.has(key)) {
          seen.add(key);
          finalRows.push(projected[i]);
        }
      }
      if (ast.offset) finalRows = finalRows.slice(ast.offset);
      if (ast.limit != null) finalRows = finalRows.slice(0, ast.limit);
    } else if (ast.distinct) {
      const seen = new Set();
      finalRows = projected.filter(row => {
        const key = JSON.stringify(row);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
      // Apply OFFSET and LIMIT after DISTINCT
      if (ast.offset) finalRows = finalRows.slice(ast.offset);
      if (ast.limit != null) finalRows = finalRows.slice(0, ast.limit);
    }

    return { type: 'ROWS', rows: finalRows };
  }

  _executeJoin(leftRows, join, leftAlias) {
    // LATERAL JOIN: for each left row, evaluate the subquery with left row in scope
    if (join.lateral && join.subquery) {
      const rightAlias = join.alias || '__lateral';
      const result = [];
      
      for (const leftRow of leftRows) {
        // Set outer row for correlated subquery resolution
        const prevOuter = this._outerRow;
        this._outerRow = leftRow;
        
        let rightRows;
        try {
          const subResult = this._select(join.subquery);
          rightRows = subResult.rows.map(r => {
            const row = {};
            for (const [k, v] of Object.entries(r)) {
              row[k] = v;
              row[`${rightAlias}.${k}`] = v;
            }
            return row;
          });
        } finally {
          this._outerRow = prevOuter;
        }
        
        if (rightRows.length === 0) {
          if (join.joinType === 'LEFT') {
            result.push({ ...leftRow });
          }
          // INNER/CROSS: skip
        } else {
          for (const rightRow of rightRows) {
            const merged = { ...leftRow, ...rightRow };
            if (join.on) {
              if (this._evalExpr(join.on, merged)) {
                result.push(merged);
              } else if (join.joinType === 'LEFT') {
                result.push({ ...leftRow });
              }
            } else {
              result.push(merged);
            }
          }
        }
      }
      
      return result;
    }

    const rightTable = this.tables.get(join.table);
    const rightView = this.views.get(join.table);

    if (!rightTable && !rightView) throw new Error(`Table ${join.table} not found`);

    const rightAlias = join.alias || join.table;

    // SELF-JOIN OPTIMIZATION: detect when right table is explicitly the same table with different alias
    // Only detect based on table name match — column overlap is unreliable
    const leftTableName = leftAlias; // This is the alias used when scanning the left table
    const rightTableName = join.table;
    // We no longer try to route self-joins differently — the existing code handles them correctly
    // The key optimization is in EXPLAIN showing [Self-Join] detection

    // NATURAL JOIN or USING clause: auto-generate ON condition
    if ((join.natural || join.usingColumns) && rightTable && !join.on) {
      let sharedCols;
      if (join.usingColumns) {
        sharedCols = join.usingColumns;
      } else {
        const leftColNames = new Set();
        if (leftRows.length > 0) {
          for (const k of Object.keys(leftRows[0])) {
            const parts = k.split('.');
            leftColNames.add(parts[parts.length - 1]);
          }
        }
        const rightCols = rightTable.schema.map(c => c.name);
        sharedCols = rightCols.filter(c => leftColNames.has(c));
      }
      if (sharedCols.length > 0) {
        // Add qualified names to left rows so the join condition can resolve them
        for (const leftRow of leftRows) {
          for (const col of sharedCols) {
            if (leftRow[col] !== undefined && leftRow[`${leftAlias}.${col}`] === undefined) {
              leftRow[`${leftAlias}.${col}`] = leftRow[col];
            }
          }
        }
        // Build standard COMPARE conditions
        let onCondition = null;
        for (const col of sharedCols) {
          const cond = {
            type: 'COMPARE', op: 'EQ',
            left: { type: 'column_ref', name: `${leftAlias}.${col}` },
            right: { type: 'column_ref', name: `${rightAlias}.${col}` },
          };
          if (!onCondition) onCondition = cond;
          else onCondition = { type: 'AND', left: onCondition, right: cond };
        }
        join = { ...join, on: onCondition };
      }
    }

    // If right side is a view/CTE, get its rows
    if (rightView) {
      let rightRows;
      if (rightView.materializedRows) {
        rightRows = rightView.materializedRows.map(r => {
          const row = {};
          for (const [k, v] of Object.entries(r)) {
            row[k] = v;
            row[`${rightAlias}.${k}`] = v;
          }
          return row;
        });
      } else {
        const viewResult = this._select(rightView.query);
        rightRows = viewResult.rows.map(r => {
          const row = {};
          for (const [k, v] of Object.entries(r)) {
            row[k] = v;
            row[`${rightAlias}.${k}`] = v;
          }
          return row;
        });
      }
      return this._executeJoinWithRows(leftRows, rightRows, join, rightAlias);
    }

    const result = [];

    // CROSS JOIN
    if (join.joinType === 'CROSS') {
      for (const leftRow of leftRows) {
        for (const { values } of rightTable.heap.scan()) {
          const rightRow = this._valuesToRow(values, rightTable.schema, rightAlias);
          if (join.filter && !this._evalExpr(join.filter, rightRow)) continue;
          result.push({ ...leftRow, ...rightRow });
        }
      }
      return result;
    }

    // RIGHT or FULL JOIN: track matched right rows
    if (join.joinType === 'RIGHT' || join.joinType === 'FULL') {
      const rightMatchedSet = new Set();
      const rightRows = [];
      for (const { values } of rightTable.heap.scan()) {
        const row = this._valuesToRow(values, rightTable.schema, rightAlias);
        if (join.filter && !this._evalExpr(join.filter, row)) continue;
        rightRows.push(row);
      }

      for (const leftRow of leftRows) {
        let matched = false;
        for (let i = 0; i < rightRows.length; i++) {
          const combined = { ...leftRow, ...rightRows[i] };
          if (this._evalExpr(join.on, combined)) {
            result.push(combined);
            rightMatchedSet.add(i);
            matched = true;
          }
        }
        // FULL JOIN: add unmatched left rows with null right
        if (!matched && join.joinType === 'FULL') {
          const nullRow = {};
          for (const col of rightTable.schema) {
            nullRow[col.name] = null;
            nullRow[`${rightAlias}.${col.name}`] = null;
          }
          result.push({ ...leftRow, ...nullRow });
        }
      }

      // Add unmatched right rows with null left
      for (let i = 0; i < rightRows.length; i++) {
        if (!rightMatchedSet.has(i)) {
          const nullRow = {};
          for (const leftKey of Object.keys(leftRows[0] || {})) nullRow[leftKey] = null;
          result.push({ ...nullRow, ...rightRows[i] });
        }
      }

      return result;
    }

    // INNER or LEFT JOIN
    // Try hash join for equi-join conditions
    const equiJoinKey = this._extractEquiJoinKey(join.on, leftAlias, rightAlias);
    if (equiJoinKey) {
      const hashResult = this._hashJoin(leftRows, rightTable, equiJoinKey, rightAlias, join.joinType, join.filter);
      if (hashResult) return hashResult;
    }

    // Try index nested-loop join for equi-join conditions
    if (equiJoinKey) {
      const rightColName = equiJoinKey.rightKey;
      const rightIndex = rightTable.indexes?.get(rightColName);
      if (rightIndex) {
        // Index nested-loop join: for each left row, look up matching right rows via index
        for (const leftRow of leftRows) {
          const lookupVal = leftRow[equiJoinKey.leftKey] !== undefined
            ? leftRow[equiJoinKey.leftKey]
            : this._resolveColumn(equiJoinKey.leftKey, leftRow);
          let matched = false;
          if (lookupVal != null) {
            // Use range(val, val) for non-unique indexes (returns all matching entries)
            const entries = rightIndex.range ? rightIndex.range(lookupVal, lookupVal) : [];
            for (const entry of entries) {
              const rid = entry.value || entry;
              const values = rightTable.heap.get(rid.pageId, rid.slotIdx);
              if (!values) continue;
              const rightRow = this._valuesToRow(values, rightTable.schema, rightAlias);
              if (join.filter && !this._evalExpr(join.filter, rightRow)) continue;
              const combined = { ...leftRow, ...rightRow };
              // Verify full join condition (handles compound conditions)
              if (this._evalExpr(join.on, combined)) {
                result.push(combined);
                matched = true;
              }
            }
          }
          if (!matched && join.joinType === 'LEFT') {
            const nullRow = {};
            for (const col of rightTable.schema) {
              nullRow[col.name] = null;
              nullRow[`${rightAlias}.${col.name}`] = null;
            }
            result.push({ ...leftRow, ...nullRow });
          }
        }
        return result;
      }
    }

    // Fallback: nested loop join (full table scan)
    for (const leftRow of leftRows) {
      let matched = false;
      for (const { values } of rightTable.heap.scan()) {
        const rightRow = this._valuesToRow(values, rightTable.schema, rightAlias);
        // Apply pushed-down filter on right side
        if (join.filter && !this._evalExpr(join.filter, rightRow)) continue;
        const combined = { ...leftRow, ...rightRow };
        if (this._evalExpr(join.on, combined)) {
          result.push(combined);
          matched = true;
        }
      }
      if (!matched && join.joinType === 'LEFT') {
        const nullRow = {};
        for (const col of rightTable.schema) nullRow[`${rightAlias}.${col.name}`] = null;
        result.push({ ...leftRow, ...nullRow });
      }
    }

    return result;
  }

  /**
   * Extract equi-join key columns from a join condition AST.
   * Returns { leftKey, rightKey } if it's a simple equality, null otherwise.
   */
  _extractEquiJoinColumns(onExpr) {
    if (!onExpr || onExpr.type !== 'COMPARE' || onExpr.op !== 'EQ') return null;
    if (onExpr.left.type !== 'column_ref' || onExpr.right.type !== 'column_ref') return null;
    return { leftCol: onExpr.left.name, rightCol: onExpr.right.name };
  }

  _extractEquiJoinKey(onExpr, leftAlias, rightAlias) {
    if (!onExpr || onExpr.type !== 'COMPARE' || onExpr.op !== 'EQ') return null;
    if (onExpr.left.type !== 'column_ref' || onExpr.right.type !== 'column_ref') return null;

    const leftCol = onExpr.left.name;
    const rightCol = onExpr.right.name;

    // Determine which column belongs to which table
    // Column refs can be "alias.col" or just "col"
    const isLeftSide = (col) => {
      if (col.startsWith(leftAlias + '.')) return true;
      // If no prefix, check if it exists in left rows
      return false;
    };
    const isRightSide = (col) => {
      if (col.startsWith(rightAlias + '.')) return true;
      return false;
    };

    let leftKey, rightKey;
    if (isLeftSide(leftCol) && isRightSide(rightCol)) {
      leftKey = leftCol;
      rightKey = rightCol.includes('.') ? rightCol.split('.').pop() : rightCol;
    } else if (isRightSide(leftCol) && isLeftSide(rightCol)) {
      leftKey = rightCol;
      rightKey = leftCol.includes('.') ? leftCol.split('.').pop() : leftCol;
    } else {
      // Can't determine sides — try both orientations
      // If left col has right alias prefix, swap
      if (leftCol.startsWith(rightAlias + '.')) {
        leftKey = rightCol;
        rightKey = leftCol.split('.').pop();
      } else {
        leftKey = leftCol;
        rightKey = rightCol.includes('.') ? rightCol.split('.').pop() : rightCol;
      }
    }

    return { leftKey, rightKey };
  }

  /**
   * Hash join: build hash table on right table, probe with left rows.
   * O(n + m) instead of O(n * m).
   */
  _hashJoin(leftRows, rightTable, keys, rightAlias, joinType, pushdownFilter) {
    const { leftKey, rightKey } = keys;

    // Build phase: hash the right table by join key
    const hashMap = new Map();
    const rightKeyIdx = rightTable.schema.findIndex(c => c.name === rightKey);
    if (rightKeyIdx < 0) {
      // Key not found in schema — fall back to nested loop
      return null;
    }

    for (const { values } of rightTable.heap.scan()) {
      // Apply pushed-down filter during build phase
      if (pushdownFilter) {
        const rightRow = this._valuesToRow(values, rightTable.schema, rightAlias);
        if (!this._evalExpr(pushdownFilter, rightRow)) continue;
      }
      const keyVal = values[rightKeyIdx];
      if (keyVal == null) continue; // NULL keys never match in SQL
      const keyStr = String(keyVal);
      if (!hashMap.has(keyStr)) hashMap.set(keyStr, []);
      hashMap.get(keyStr).push(values);
    }

    // Probe phase: look up each left row in the hash map
    const result = [];
    for (const leftRow of leftRows) {
      // Get the left key value — try with and without alias prefix
      let leftVal = leftRow[leftKey];
      if (leftVal === undefined) {
        // Try without alias prefix
        const bare = leftKey.includes('.') ? leftKey.split('.').pop() : leftKey;
        leftVal = leftRow[bare];
      }
      if (leftVal === undefined) {
        // Try all keys that end with the column name
        const bare = leftKey.includes('.') ? leftKey.split('.').pop() : leftKey;
        for (const k of Object.keys(leftRow)) {
          if (k === bare || k.endsWith('.' + bare)) {
            leftVal = leftRow[k];
            break;
          }
        }
      }

      const keyStr = leftVal == null ? null : String(leftVal);
      const matches = keyStr != null ? hashMap.get(keyStr) : undefined;
      let matched = false;

      if (matches) {
        for (const values of matches) {
          const rightRow = this._valuesToRow(values, rightTable.schema, rightAlias);
          result.push({ ...leftRow, ...rightRow });
          matched = true;
        }
      }

      if (!matched && joinType === 'LEFT') {
        const nullRow = {};
        for (const col of rightTable.schema) {
          nullRow[col.name] = null;
          nullRow[`${rightAlias}.${col.name}`] = null;
        }
        result.push({ ...leftRow, ...nullRow });
      }
    }

    return result;
  }

  _estimateRowCount(table) {
    // Use tracked row count if available
    if (table.heap?.rowCount !== undefined) return table.heap.rowCount;
    // Fallback: quick scan
    let count = 0;
    for (const _ of table.heap.scan()) count++;
    return count;
  }

  /**
   * Estimate rows for a WHERE condition using ANALYZE stats.
   * Returns { estimated: number, method: string }
   */
  _estimateFilteredRows(tableName, where, totalRows) {
    if (!where) return { estimated: totalRows, method: 'no filter' };
    
    const stats = this._tableStats.get(tableName);
    if (!stats) return { estimated: Math.ceil(totalRows * 0.33), method: 'default 33%' };
    
    // Equality: col = value → use histogram if available, else uniform selectivity
    if (where.type === 'COMPARE' && where.op === 'EQ') {
      const col = where.left?.type === 'column_ref' ? where.left.name : 
                  (where.right?.type === 'column_ref' ? where.right.name : null);
      const val = where.left?.type === 'literal' ? where.left.value :
                  (where.right?.type === 'literal' ? where.right.value : null);
      if (col) {
        const colName = col.includes('.') ? col.split('.').pop() : col;
        const colStats = stats.columns[colName];
        if (colStats) {
          // Try histogram for better estimate on skewed data
          if (colStats.histogram && val != null && typeof val === 'number') {
            // Sum all buckets containing the target value
            // For exact-match buckets (lo===hi===val), use full count
            // For mixed buckets, estimate as count/ndv
            let est = 0;
            let found = false;
            for (const bucket of colStats.histogram) {
              if (val >= bucket.lo && val <= bucket.hi) {
                found = true;
                if (bucket.lo === bucket.hi) {
                  // Bucket contains only this value
                  est += bucket.count;
                } else {
                  // Mixed bucket: estimate frequency as count/ndv
                  est += (bucket.ndv > 0 ? bucket.count / bucket.ndv : bucket.count);
                }
              }
            }
            if (found) {
              return {
                estimated: Math.max(1, Math.ceil(est)),
                method: `histogram_eq(${colName})=${est.toFixed(1)}`,
              };
            }
            // Value outside all buckets → likely 0 rows
            return { estimated: 1, method: `histogram_eq(${colName})=out_of_range` };
          }
          return {
            estimated: Math.max(1, Math.ceil(totalRows * colStats.selectivity)),
            method: `selectivity(${colName})=${colStats.selectivity.toFixed(3)}`,
          };
        }
      }
    }
    
    // Range: col > val, col < val → use histogram if available, else linear interpolation
    if (where.type === 'COMPARE' && ['GT', 'GE', 'GTE', 'LT', 'LE', 'LTE'].includes(where.op)) {
      const col = where.left?.type === 'column_ref' ? where.left.name : null;
      const val = where.right?.type === 'literal' ? where.right.value : null;
      if (col && val != null) {
        const colName = col.includes('.') ? col.split('.').pop() : col;
        const colStats = stats.columns[colName];
        if (colStats) {
          // Try histogram for better range estimate
          if (colStats.histogram && typeof val === 'number') {
            let matchingRows = 0;
            const isGreater = where.op === 'GT' || where.op === 'GE' || where.op === 'GTE';
            const isInclusive = where.op === 'GE' || where.op === 'GTE' || where.op === 'LE' || where.op === 'LTE';
            for (const bucket of colStats.histogram) {
              if (isGreater) {
                // For > or >=: count rows in buckets above val
                if (bucket.lo > val || (isInclusive && bucket.lo >= val)) {
                  matchingRows += bucket.count; // entire bucket qualifies
                } else if (val >= bucket.lo && val <= bucket.hi) {
                  // Partial bucket: linear interpolation within bucket
                  const bucketRange = bucket.hi - bucket.lo;
                  const fraction = bucketRange > 0 ? (bucket.hi - val) / bucketRange : 0.5;
                  matchingRows += Math.ceil(bucket.count * fraction);
                }
              } else {
                // For < or <=: count rows in buckets below val
                if (bucket.hi < val || (isInclusive && bucket.hi <= val)) {
                  matchingRows += bucket.count; // entire bucket qualifies
                } else if (val >= bucket.lo && val <= bucket.hi) {
                  // Partial bucket
                  const bucketRange = bucket.hi - bucket.lo;
                  const fraction = bucketRange > 0 ? (val - bucket.lo) / bucketRange : 0.5;
                  matchingRows += Math.ceil(bucket.count * fraction);
                }
              }
            }
            return {
              estimated: Math.max(1, matchingRows),
              method: `histogram_range(${colName} ${where.op} ${val})=${matchingRows}`,
            };
          }
          // Fallback: linear interpolation with min/max
          if (colStats.min != null && colStats.max != null && colStats.max > colStats.min) {
            const range = colStats.max - colStats.min;
            let fraction;
            if (where.op === 'GT' || where.op === 'GE' || where.op === 'GTE') {
              fraction = Math.max(0, Math.min(1, (colStats.max - val) / range));
            } else {
              fraction = Math.max(0, Math.min(1, (val - colStats.min) / range));
            }
            return {
              estimated: Math.max(1, Math.ceil(totalRows * fraction)),
              method: `range(${colName}: ${fraction.toFixed(3)})`,
            };
          }
        }
      }
      return { estimated: Math.ceil(totalRows * 0.33), method: 'range ~33%' };
    }
    
    // AND: multiply selectivities
    if (where.type === 'AND') {
      const left = this._estimateFilteredRows(tableName, where.left, totalRows);
      const right = this._estimateFilteredRows(tableName, where.right, totalRows);
      return {
        estimated: Math.max(1, Math.ceil(left.estimated * right.estimated / totalRows)),
        method: `AND(${left.method}, ${right.method})`,
      };
    }
    
    // OR: add selectivities (capped at totalRows)
    if (where.type === 'OR') {
      const left = this._estimateFilteredRows(tableName, where.left, totalRows);
      const right = this._estimateFilteredRows(tableName, where.right, totalRows);
      return {
        estimated: Math.min(totalRows, left.estimated + right.estimated),
        method: `OR(${left.method}, ${right.method})`,
      };
    }
    
    // IS NULL / IS NOT NULL
    if (where.type === 'IS_NULL' || where.type === 'IS_NOT_NULL' || 
        (where.type === 'COMPARE' && (where.op === 'IS' || where.op === 'IS_NOT'))) {
      const col = where.left?.name || where.column?.name || where.operand?.name;
      if (col) {
        const colName = col.includes('.') ? col.split('.').pop() : col;
        const colStats = stats.columns[colName];
        if (colStats) {
          const nullFraction = totalRows > 0 ? colStats.nulls / totalRows : 0;
          if (where.type === 'IS_NULL' || where.op === 'IS') {
            return { estimated: Math.max(1, Math.ceil(totalRows * nullFraction)), method: `null_frac(${colName})=${nullFraction.toFixed(3)}` };
          } else {
            return { estimated: Math.max(1, Math.ceil(totalRows * (1 - nullFraction))), method: `not_null_frac(${colName})=${(1-nullFraction).toFixed(3)}` };
          }
        }
      }
    }

    // BETWEEN: use min/max interpolation
    if (where.type === 'BETWEEN') {
      const col = where.left?.name || where.column?.name || where.expr?.name;
      if (col) {
        const colName = col.includes('.') ? col.split('.').pop() : col;
        const colStats = stats.columns[colName];
        const lo = where.low?.value;
        const hi = where.high?.value;
        if (colStats && colStats.min != null && colStats.max != null && lo != null && hi != null) {
          const range = colStats.max - colStats.min;
          if (range > 0) {
            const fraction = Math.max(0, Math.min(1, (Math.min(hi, colStats.max) - Math.max(lo, colStats.min)) / range));
            return { estimated: Math.max(1, Math.ceil(totalRows * fraction)), method: `between(${colName}: ${fraction.toFixed(3)})` };
          }
        }
      }
    }

    // IN list: sum of per-value selectivities
    if (where.type === 'IN' || where.type === 'IN_LIST') {
      const col = where.left?.name || where.column?.name;
      if (col) {
        const colName = col.includes('.') ? col.split('.').pop() : col;
        const colStats = stats.columns[colName];
        const listLen = where.values?.length || where.list?.length || 3;
        if (colStats) {
          return { estimated: Math.max(1, Math.ceil(totalRows * colStats.selectivity * listLen)), method: `in(${colName}, ${listLen} values)` };
        }
      }
    }

    return { estimated: Math.ceil(totalRows * 0.33), method: 'default 33%' };
  }

  /**
   * Estimate the result size of joining two relations.
   * Uses the principle: |R ⋈ S| = |R| * |S| / max(ndv(R.key), ndv(S.key))
   */
  _estimateJoinSize(leftTable, leftRows, rightTableName, joinOn) {
    if (!joinOn) return leftRows * 10; // No join condition — cross join estimate
    
    // Extract join columns from ON condition (e.g., a.id = b.foreign_id)
    const joinCols = this._extractJoinColumns(joinOn);
    if (!joinCols) return leftRows * 10; // Can't parse — conservative estimate
    
    const rightTable = this.tables.get(rightTableName);
    if (!rightTable) return leftRows * 10;
    const rightRows = this._estimateRowCount(rightTable);
    
    // Get ndv from stats
    const leftStats = this._tableStats.get(joinCols.leftTable);
    const rightStats = this._tableStats.get(rightTableName);
    
    let leftNdv = leftRows; // default: assume unique
    let rightNdv = rightRows;
    
    if (leftStats?.columns[joinCols.leftCol]) {
      leftNdv = leftStats.columns[joinCols.leftCol].distinct || leftRows;
    }
    if (rightStats?.columns[joinCols.rightCol]) {
      rightNdv = rightStats.columns[joinCols.rightCol].distinct || rightRows;
    }
    
    // Standard formula: |R ⋈ S| = |R| * |S| / max(ndv_R, ndv_S)
    const maxNdv = Math.max(leftNdv, rightNdv, 1);
    return Math.max(1, Math.ceil(leftRows * rightRows / maxNdv));
  }

  /**
   * Extract left/right table and column from a join ON condition.
   * Handles: a.col = b.col or col = col patterns.
   */
  _extractJoinColumns(on) {
    if (!on || on.type !== 'COMPARE' || on.op !== 'EQ') return null;
    
    const left = on.left;
    const right = on.right;
    
    if (left?.type === 'column_ref' && right?.type === 'column_ref') {
      const leftParts = (left.table ? [left.table, left.name] : left.name.split('.'));
      const rightParts = (right.table ? [right.table, right.name] : right.name.split('.'));
      
      return {
        leftTable: leftParts.length > 1 ? leftParts[0] : null,
        leftCol: leftParts.length > 1 ? leftParts[1] : leftParts[0],
        rightTable: rightParts.length > 1 ? rightParts[0] : null,
        rightCol: rightParts.length > 1 ? rightParts[1] : rightParts[0],
      };
    }
    return null;
  }

  /**
   * Cost-based join ordering using dynamic programming (System R style).
   * For N tables, considers all orderings and picks the cheapest.
   * Only reorders INNER joins — LEFT/RIGHT/FULL preserve user order.
   */
  _optimizeJoinOrder(fromTable, joins) {
    // Only optimize if we have 2+ INNER joins and stats available
    const innerJoins = joins.filter(j => !j.joinType || j.joinType === 'INNER');
    if (innerJoins.length < 2) return joins;
    
    // Check if all joined tables have stats
    const tables = [fromTable, ...innerJoins.map(j => j.table)];
    const allHaveStats = tables.every(t => this._tableStats.has(t));
    if (!allHaveStats) return joins; // Can't optimize without stats
    
    // For small join counts (≤6 tables), do full DP enumeration
    if (innerJoins.length > 5) return joins; // Too many — don't try
    
    // Build adjacency: which tables can join which?
    const joinConditions = new Map(); // "tableA:tableB" -> join ON condition
    for (const j of innerJoins) {
      const cols = this._extractJoinColumns(j.on);
      if (cols) {
        const key1 = `${cols.leftTable || fromTable}:${j.table}`;
        const key2 = `${j.table}:${cols.leftTable || fromTable}`;
        joinConditions.set(key1, j);
        joinConditions.set(key2, j);
      }
    }
    
    // DP over subsets: dp[bitmask] = { cost, order, resultRows }
    const n = innerJoins.length;
    const tableNames = innerJoins.map(j => j.table);
    const allTables = [fromTable, ...tableNames];
    
    // Initialize single tables
    const dp = new Map();
    for (let i = 0; i < allTables.length; i++) {
      const mask = 1 << i;
      const table = this.tables.get(allTables[i]);
      const rows = table ? this._estimateRowCount(table) : 100;
      dp.set(mask, { cost: rows, rows, order: [i], lastTable: i });
    }
    
    // Build up larger subsets
    const fullMask = (1 << allTables.length) - 1;
    for (let size = 2; size <= allTables.length; size++) {
      // Enumerate all subsets of this size
      for (let mask = 1; mask <= fullMask; mask++) {
        if (this._popcount(mask) !== size) continue;
        
        let bestCost = Infinity;
        let bestPlan = null;
        
        // Try all ways to split this subset into (subset-1) + 1
        for (let i = 0; i < allTables.length; i++) {
          if (!(mask & (1 << i))) continue; // i not in mask
          
          const subMask = mask ^ (1 << i); // mask without table i
          const subPlan = dp.get(subMask);
          if (!subPlan) continue;
          
          // Check if table i can join with any table in subPlan
          const leftTable = allTables[subPlan.lastTable];
          const key = `${leftTable}:${allTables[i]}`;
          const altKey = `${allTables[i]}:${leftTable}`;
          
          // Also check any table in the subset
          let canJoin = joinConditions.has(key) || joinConditions.has(altKey);
          if (!canJoin) {
            for (const idx of subPlan.order) {
              const k1 = `${allTables[idx]}:${allTables[i]}`;
              const k2 = `${allTables[i]}:${allTables[idx]}`;
              if (joinConditions.has(k1) || joinConditions.has(k2)) {
                canJoin = true;
                break;
              }
            }
          }
          if (!canJoin) continue;
          
          // Estimate cost: subPlan.cost + join cost
          const rightTable = this.tables.get(allTables[i]);
          const rightRows = rightTable ? this._estimateRowCount(rightTable) : 100;
          
          // Join result estimate
          const maxNdv = Math.max(
            this._getTableNdv(allTables[subPlan.lastTable], allTables[i], joinConditions),
            1
          );
          const joinRows = Math.max(1, Math.ceil(subPlan.rows * rightRows / maxNdv));
          const cost = subPlan.cost + joinRows; // Total tuples processed
          
          if (cost < bestCost) {
            bestCost = cost;
            bestPlan = { cost, rows: joinRows, order: [...subPlan.order, i], lastTable: i };
          }
        }
        
        if (bestPlan) {
          const existing = dp.get(mask);
          if (!existing || bestPlan.cost < existing.cost) {
            dp.set(mask, bestPlan);
          }
        }
      }
    }
    
    // Get optimal order for all tables
    const optimal = dp.get(fullMask);
    if (!optimal) return joins; // DP failed, use original order
    
    // Reconstruct join list in optimal order
    // optimal.order gives indices into allTables; index 0 is fromTable (already the base)
    const reordered = [];
    const availableTables = new Set([fromTable]); // Tables whose columns are available
    const remainingJoins = [];
    
    for (const idx of optimal.order) {
      if (idx === 0) continue; // Skip the base table
      const tableName = allTables[idx];
      const join = innerJoins.find(j => j.table === tableName);
      if (join) remainingJoins.push(join);
    }
    
    // Greedy: emit joins in order where all referenced tables are available
    while (remainingJoins.length > 0) {
      let found = false;
      for (let i = 0; i < remainingJoins.length; i++) {
        const join = remainingJoins[i];
        const cols = this._extractJoinColumns(join.on);
        // Check if both sides of the ON condition reference available tables
        let canExecute = true;
        if (cols) {
          if (cols.leftTable && cols.leftTable !== join.table && !availableTables.has(cols.leftTable)) {
            canExecute = false;
          }
          if (cols.rightTable && cols.rightTable !== join.table && !availableTables.has(cols.rightTable)) {
            canExecute = false;
          }
        }
        if (canExecute) {
          reordered.push(join);
          availableTables.add(join.table);
          remainingJoins.splice(i, 1);
          found = true;
          break;
        }
      }
      if (!found) {
        // Can't find a valid next join — fallback: emit remaining in original order
        reordered.push(...remainingJoins);
        break;
      }
    }
    
    // Append any non-inner joins at the end (preserved in original order)
    const nonInner = joins.filter(j => j.joinType && j.joinType !== 'INNER');
    return [...reordered, ...nonInner];
  }

  _popcount(n) {
    let count = 0;
    while (n) { count += n & 1; n >>= 1; }
    return count;
  }

  _getTableNdv(table1, table2, joinConditions) {
    const key = `${table1}:${table2}`;
    const join = joinConditions.get(key);
    if (!join) return 1;
    
    const cols = this._extractJoinColumns(join.on);
    if (!cols) return 1;
    
    const stats1 = this._tableStats.get(table1);
    const stats2 = this._tableStats.get(table2);
    
    const ndv1 = stats1?.columns[cols.leftCol]?.distinct || 
                 stats1?.columns[cols.rightCol]?.distinct || 1;
    const ndv2 = stats2?.columns[cols.leftCol]?.distinct ||
                 stats2?.columns[cols.rightCol]?.distinct || 1;
    
    return Math.max(ndv1, ndv2);
  }

  _update(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    let updated = 0;
    const toUpdate = [];

    if (ast.from) {
      // UPDATE ... FROM: join with another table
      const fromTable = this.tables.get(ast.from);
      if (!fromTable) throw new Error(`Table ${ast.from} not found`);
      const fromAlias = ast.fromAlias || ast.from;
      
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const row = this._valuesToRow(values, table.schema, ast.table);
        
        // For each from-table row, check WHERE
        for (const fromItem of fromTable.heap.scan()) {
          const fromRow = this._valuesToRow(fromItem.values, fromTable.schema, fromAlias);
          const merged = { ...row, ...fromRow };
          
          if (!ast.where || this._evalExpr(ast.where, merged)) {
            toUpdate.push({ pageId, slotIdx, values: [...values], mergedRow: merged });
            break; // Only update target row once per match
          }
        }
      }
    } else {
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const row = this._valuesToRow(values, table.schema, ast.table);
        if (!ast.where || this._evalExpr(ast.where, row)) {
          toUpdate.push({ pageId, slotIdx, values: [...values], mergedRow: row });
        }
      }
    }

    const returnedRows = [];
    
    // Batch WAL: use a single transaction for all updates
    const batchTxId = this._currentTxId || this._nextTxId++;
    const isAutoCommit = !this._currentTxId;

    for (const item of toUpdate) {
      const newValues = [...item.values];
      const row = item.mergedRow || this._valuesToRow(item.values, table.schema, ast.table);
      for (const { column, value } of ast.assignments) {
        const colIdx = table.schema.findIndex(c => c.name === column);
        if (colIdx === -1) throw new Error(`Column ${column} not found`);
        newValues[colIdx] = this._evalValue(value, row);
      }

      // Recompute generated columns
      for (let gi = 0; gi < table.schema.length; gi++) {
        if (table.schema[gi].generated) {
          const genRow = {};
          for (let gj = 0; gj < table.schema.length; gj++) {
            genRow[table.schema[gj].name] = newValues[gj];
          }
          newValues[gi] = this._evalValue(table.schema[gi].generated, genRow);
        }
      }

      // Remove old index entries
      for (const [colName, index] of table.indexes) {
        const colIdx = table.schema.findIndex(c => c.name === colName);
        // B+ tree doesn't have delete, so we rebuild affected indexes after
      }

      // Delete old, insert new
      table.heap.delete(item.pageId, item.slotIdx);
      const newRid = table.heap.insert(newValues);

      // WAL: log the update
      this.wal.appendUpdate(batchTxId, ast.table, newRid.pageId, newRid.slotIdx, item.values, newValues);

      // Update indexes with new entries
      for (const [colName, index] of table.indexes) {
        const colIdx = table.schema.findIndex(c => c.name === colName);
        index.insert(newValues[colIdx], newRid);
      }

      // Handle ON UPDATE CASCADE for foreign keys
      this._handleForeignKeyUpdate(ast.table, table, item.values, newValues);

      if (ast.returning) {
        const cleanRow = {};
        for (let i = 0; i < table.schema.length; i++) {
          cleanRow[table.schema[i].name] = newValues[i];
        }
        returnedRows.push(cleanRow);
      }

      updated++;
    }

    // Single WAL commit for all updates
    if (isAutoCommit && updated > 0) this.wal.appendCommit(batchTxId);

    if (ast.returning) {
      const filteredRows = this._resolveReturning(ast.returning, returnedRows);
      return { type: 'ROWS', rows: filteredRows, count: updated };
    }

    return { type: 'OK', message: `${updated} row(s) updated`, count: updated };
  }

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
            // Set child column to NULL
            for (const { pageId, slotIdx, values } of childTable.heap.scan()) {
              if (values[childColIdx] === parentValue) {
                values[childColIdx] = null;
                const encoded = encodeTuple(values);
                childTable.heap.pages.find(p => p.id === pageId)?.updateTuple(slotIdx, encoded);
              }
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

  _delete(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    let deleted = 0;
    const toDelete = [];

    if (ast.using) {
      // DELETE ... USING: join with another table
      const usingTable = this.tables.get(ast.using);
      if (!usingTable) throw new Error(`Table ${ast.using} not found`);
      const usingAlias = ast.usingAlias || ast.using;
      
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const row = this._valuesToRow(values, table.schema, ast.table);
        for (const usingItem of usingTable.heap.scan()) {
          const usingRow = this._valuesToRow(usingItem.values, usingTable.schema, usingAlias);
          const merged = { ...row, ...usingRow };
          if (!ast.where || this._evalExpr(ast.where, merged)) {
            toDelete.push({ pageId, slotIdx });
            break; // Only delete target row once per match
          }
        }
      }
    } else {
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const row = this._valuesToRow(values, table.schema, ast.table);
        if (!ast.where || this._evalExpr(ast.where, row)) {
          toDelete.push({ pageId, slotIdx });
        }
      }
    }

    const deletedRows = [];
    
    // Batch WAL: use a single transaction for all deletes
    const batchTxId = this._currentTxId || this._nextTxId++;
    const isAutoCommit = !this._currentTxId;

    for (const { pageId, slotIdx } of toDelete) {
      const values = table.heap.get(pageId, slotIdx);
      
      if (values && ast.returning) {
        const cleanRow = {};
        for (let i = 0; i < table.schema.length; i++) {
          cleanRow[table.schema[i].name] = values[i];
        }
        deletedRows.push(cleanRow);
      }

      // Check foreign key constraints from child tables
      if (values) {
        this._handleForeignKeyDelete(ast.table, table, values);
      }
      
      table.heap.delete(pageId, slotIdx);
      
      // WAL: log the delete
      if (values) {
        this.wal.appendDelete(batchTxId, ast.table, pageId, slotIdx, values);
      }
      
      deleted++;
    }

    // Single WAL commit for all deletes
    if (isAutoCommit && deleted > 0) this.wal.appendCommit(batchTxId);

    if (ast.returning) {
      const filteredRows = this._resolveReturning(ast.returning, deletedRows);
      return { type: 'ROWS', rows: filteredRows, count: deleted };
    }

    return { type: 'OK', message: `${deleted} row(s) deleted`, count: deleted };
  }

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

  _selectInfoSchema(ast) {
    const tableName = (ast.from.table || '').toLowerCase().replace('information_schema.', '');
    
    if (tableName === 'tables' || tableName === 'information_schema.tables') {
      const rows = [];
      for (const [name, table] of this.tables) {
        rows.push({
          table_catalog: 'henrydb',
          table_schema: 'public',
          table_name: name,
          table_type: 'BASE TABLE',
          column_count: table.schema.length,
        });
      }
      for (const [name] of this.views) {
        rows.push({
          table_catalog: 'henrydb',
          table_schema: 'public',
          table_name: name,
          table_type: 'VIEW',
          column_count: 0,
        });
      }
      let filtered = rows;
      if (ast.where) {
        filtered = rows.filter(r => this._evalExpr(ast.where, r));
      }
      if (ast.orderBy) {
        filtered.sort((a, b) => {
          for (const o of ast.orderBy) {
            const col = typeof o.column === 'string' ? o.column : o.column.name;
            if (a[col] < b[col]) return o.direction === 'DESC' ? 1 : -1;
            if (a[col] > b[col]) return o.direction === 'DESC' ? -1 : 1;
          }
          return 0;
        });
      }
      return { rows: filtered, columns: Object.keys(rows[0] || {}) };
    }

    if (tableName === 'columns' || tableName === 'information_schema.columns') {
      const rows = [];
      for (const [tname, table] of this.tables) {
        for (let i = 0; i < table.schema.length; i++) {
          const col = table.schema[i];
          rows.push({
            table_catalog: 'henrydb',
            table_schema: 'public',
            table_name: tname,
            column_name: col.name,
            ordinal_position: i + 1,
            data_type: col.type || 'TEXT',
            is_nullable: col.notNull ? 'NO' : 'YES',
            column_default: col.defaultValue,
          });
        }
      }
      let filtered = rows;
      if (ast.where) {
        filtered = rows.filter(r => this._evalExpr(ast.where, r));
      }
      return { rows: filtered, columns: Object.keys(rows[0] || {}) };
    }

    throw new Error(`Unknown information_schema table: ${tableName}`);
  }

  _selectPgCatalog(ast) {
    const rawName = (ast.from.table || '').toLowerCase().replace('pg_catalog.', '');
    
    if (rawName === 'pg_tables') {
      const rows = [];
      for (const [name, table] of this.tables) {
        rows.push({
          schemaname: 'public',
          tablename: name,
          tableowner: 'henrydb',
          tablespace: null,
          hasindexes: table.indexes && table.indexes.size > 0,
          hasrules: false,
          hastriggers: false,
          rowsecurity: false,
        });
      }
      return this._filterPgCatalogRows(rows, ast);
    }
    
    if (rawName === 'pg_indexes') {
      const rows = [];
      for (const [tableName, table] of this.tables) {
        if (!table.indexMeta) continue;
        for (const [colKey, meta] of table.indexMeta) {
          const unique = meta.unique ? 'UNIQUE ' : '';
          const using = meta.indexType === 'HASH' ? 'USING hash ' : '';
          rows.push({
            schemaname: 'public',
            tablename: tableName,
            indexname: meta.name,
            tablespace: null,
            indexdef: `CREATE ${unique}INDEX ${meta.name} ON public.${tableName} ${using}(${meta.columns.join(', ')})`,
          });
        }
      }
      return this._filterPgCatalogRows(rows, ast);
    }
    
    if (rawName === 'pg_stat_user_tables') {
      const rows = [];
      for (const [name, table] of this.tables) {
        const stats = this._tableStats?.get(name);
        rows.push({
          schemaname: 'public',
          relname: name,
          seq_scan: 0, // Would need tracking
          seq_tup_read: 0,
          idx_scan: 0,
          idx_tup_fetch: 0,
          n_tup_ins: 0,
          n_tup_upd: 0,
          n_tup_del: 0,
          n_live_tup: table.heap?.tupleCount || 0,
          n_dead_tup: 0,
          last_analyze: stats?.analyzedAt ? new Date(stats.analyzedAt).toISOString() : null,
        });
      }
      return this._filterPgCatalogRows(rows, ast);
    }
    
    throw new Error(`Unknown pg_catalog table: ${rawName}`);
  }
  
  _filterPgCatalogRows(rows, ast) {
    if (ast.where) {
      rows = rows.filter(row => this._evalExpr(ast.where, row));
    }
    if (ast.orderBy) {
      rows.sort((a, b) => {
        for (const ob of ast.orderBy) {
          const col = ob.column || ob.expr?.name;
          if (!col) continue;
          const va = a[col], vb = b[col];
          const cmp = va < vb ? -1 : va > vb ? 1 : 0;
          if (cmp !== 0) return ob.desc ? -cmp : cmp;
        }
        return 0;
      });
    }
    if (ast.limit != null) rows = rows.slice(0, ast.limit);
    return { type: 'ROWS', rows };
  }

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
      
      // For file-backed heaps, flush dirty pages
      if (typeof heap.flush === 'function') {
        heap.flush();
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

  _union(ast) {
    const leftResult = this.execute_ast(ast.left);
    const rightResult = this.execute_ast(ast.right);
    
    // Remap right result's columns to match left result's column names
    const leftCols = leftResult.rows.length > 0 ? Object.keys(leftResult.rows[0]) : [];
    const rightRows = this._remapUnionColumns(rightResult.rows, leftCols);
    
    let rows = [...leftResult.rows, ...rightRows];

    if (!ast.all) {
      // UNION (not ALL) — remove duplicates
      const seen = new Set();
      rows = rows.filter(row => {
        const key = JSON.stringify(row);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    return { type: 'ROWS', rows };
  }

  _intersect(ast) {
    const leftResult = this.execute_ast(ast.left);
    const rightResult = this.execute_ast(ast.right);
    const leftCols = leftResult.rows.length > 0 ? Object.keys(leftResult.rows[0]) : [];
    const rightRemapped = this._remapUnionColumns(rightResult.rows, leftCols);
    
    if (ast.all) {
      // Bag semantics: count occurrences, take min
      const rightCounts = new Map();
      for (const row of rightRemapped) {
        const key = JSON.stringify(row);
        rightCounts.set(key, (rightCounts.get(key) || 0) + 1);
      }
      const rows = [];
      for (const row of leftResult.rows) {
        const key = JSON.stringify(row);
        if ((rightCounts.get(key) || 0) > 0) {
          rows.push(row);
          rightCounts.set(key, rightCounts.get(key) - 1);
        }
      }
      return { type: 'ROWS', rows };
    }
    
    const rightKeys = new Set(rightRemapped.map(r => JSON.stringify(r)));
    const seen = new Set();
    const rows = leftResult.rows.filter(row => {
      const key = JSON.stringify(row);
      if (rightKeys.has(key) && !seen.has(key)) {
        seen.add(key);
        return true;
      }
      return false;
    });
    return { type: 'ROWS', rows };
  }

  _except(ast) {
    const leftResult = this.execute_ast(ast.left);
    const rightResult = this.execute_ast(ast.right);
    const leftCols = leftResult.rows.length > 0 ? Object.keys(leftResult.rows[0]) : [];
    const rightRemapped = this._remapUnionColumns(rightResult.rows, leftCols);
    
    if (ast.all) {
      // Bag semantics: remove one copy per right row
      const rightCounts = new Map();
      for (const row of rightRemapped) {
        const key = JSON.stringify(row);
        rightCounts.set(key, (rightCounts.get(key) || 0) + 1);
      }
      const rows = [];
      for (const row of leftResult.rows) {
        const key = JSON.stringify(row);
        if ((rightCounts.get(key) || 0) > 0) {
          rightCounts.set(key, rightCounts.get(key) - 1);
        } else {
          rows.push(row);
        }
      }
      return { type: 'ROWS', rows };
    }
    
    const rightKeys = new Set(rightRemapped.map(r => JSON.stringify(r)));
    const seen = new Set();
    const rows = leftResult.rows.filter(row => {
      const key = JSON.stringify(row);
      if (!rightKeys.has(key) && !seen.has(key)) {
        seen.add(key);
        return true;
      }
      return false;
    });
    
    return { type: 'ROWS', rows };
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

  _explain(ast) {
    const stmt = ast.statement;
    const format = ast.format || 'text';

    // EXPLAIN COMPILED: show the compiled query plan
    if (ast.compiled) {
      return this._explainCompiled(stmt);
    }

    // EXPLAIN ANALYZE: execute the query and measure actual performance
    if (ast.analyze) {
      return this._explainAnalyze(stmt);
    }

    // Tree-structured plan (new system) — use for SELECT statements
    if (stmt.type === 'SELECT' && (format === 'tree' || format === 'json-tree' || format === 'html' || format === 'dot' || format === 'yaml')) {
      const builder = new PlanBuilder(this);
      const planTree = builder.buildPlan(stmt);
      if (format === 'json-tree') {
        const json = PlanFormatter.toJSON(planTree);
        return { type: 'PLAN', rows: [{ 'QUERY PLAN': JSON.stringify([json], null, 2) }] };
      }
      if (format === 'html') {
        const html = planToHTML(planTree);
        return { type: 'PLAN', rows: [{ 'QUERY PLAN': html }], html };
      }
      if (format === 'dot') {
        const dot = PlanFormatter.toDOT(planTree);
        return { type: 'PLAN', rows: [{ 'QUERY PLAN': dot }], dot };
      }
      if (format === 'yaml') {
        const yaml = PlanFormatter.toYAML(planTree);
        return { type: 'PLAN', rows: [{ 'QUERY PLAN': yaml }], yaml };
      }
      const lines = PlanFormatter.format(planTree);
      return { type: 'PLAN', rows: lines.map(l => ({ 'QUERY PLAN': l })) };
    }

    const plan = [];

    if (stmt.type !== 'SELECT') {
      return { type: 'PLAN', plan: [{ operation: 'UNKNOWN', detail: stmt.type }] };
    }

    // CTE analysis
    if (stmt.ctes) {
      for (const cte of stmt.ctes) {
        plan.push({ operation: 'CTE', name: cte.name, recursive: cte.recursive || false });
      }
    }

    const tableName = stmt.from?.table;
    const hasJoins = stmt.joins && stmt.joins.length > 0;

    // Check view
    if (tableName && this.views.has(tableName)) {
      plan.push({ operation: 'VIEW_SCAN', view: tableName });
    } else if (tableName && this.tables.has(tableName)) {
      const table = this.tables.get(tableName);

      // Determine scan type
      const estRows = this._estimateRowCount(table);
      const engine = table.heap instanceof BTreeTable ? 'btree' : 'heap';
      const filterEst = stmt.where ? this._estimateFilteredRows(tableName, stmt.where, estRows) : null;
      if (!hasJoins && stmt.where) {
        const indexScan = this._tryIndexScan(table, stmt.where, stmt.from.alias || tableName);
        if (indexScan !== null) {
          if (indexScan.btreeLookup) {
            plan.push({ operation: 'BTREE_PK_LOOKUP', table: tableName, engine, estimated_rows: filterEst?.estimated || 1, estimation_method: filterEst?.method });
          } else {
            const colName = this._findIndexedColumn(stmt.where);
            plan.push({ operation: 'INDEX_SCAN', table: tableName, index: colName, engine, estimated_rows: filterEst?.estimated || indexScan.rows.length, estimation_method: filterEst?.method });
          }
        } else {
          plan.push({ operation: 'TABLE_SCAN', table: tableName, engine, estimated_rows: estRows, filtered_estimate: filterEst?.estimated, estimation_method: filterEst?.method });
          plan.push({ operation: 'FILTER', condition: 'WHERE' });
        }
      } else {
        plan.push({ operation: 'TABLE_SCAN', table: tableName, engine, estimated_rows: estRows });
      }

      // Joins — show optimized order if applicable
      let joinList = stmt.joins || [];
      const originalOrder = joinList.map(j => j.table?.table || j.table);
      if (joinList.length >= 2 && tableName) {
        joinList = this._optimizeJoinOrder(tableName, joinList);
      }
      const optimizedOrder = joinList.map(j => j.table?.table || j.table);
      const wasReordered = JSON.stringify(originalOrder) !== JSON.stringify(optimizedOrder);
      
      if (wasReordered) {
        plan.push({
          operation: 'JOIN_REORDER',
          original: originalOrder.join(' → '),
          optimized: optimizedOrder.join(' → '),
          reason: 'cost-based (DP enumeration)',
        });
      }
      
      for (const join of joinList) {
        const joinTable = join.table?.table || join.table;
        const equiJoinKey = join.on ? this._extractEquiJoinKey(join.on, stmt.from.alias || tableName, join.alias || joinTable) : null;
        const isSelfJoin = joinTable === tableName;
        const joinEntry = {
          operation: equiJoinKey ? 'HASH_JOIN' : 'NESTED_LOOP_JOIN',
          type: join.type || 'INNER',
          table: joinTable,
          on: equiJoinKey ? `${equiJoinKey.leftKey} = ${equiJoinKey.rightKey}` : 'complex condition',
          selfJoin: isSelfJoin || undefined,
        };
        
        // Add cost estimate if stats available
        if (equiJoinKey) {
          const rightTbl = this.tables.get(joinTable);
          if (rightTbl) {
            const rightRows = this._estimateRowCount(rightTbl);
            joinEntry.estimated_right_rows = rightRows;
          }
        }
        
        plan.push(joinEntry);
      }
    }

    // WHERE (if not already noted)
    if (stmt.where && !plan.some(p => p.operation === 'FILTER')) {
      plan.push({ operation: 'FILTER', condition: 'WHERE' });
    }

    // GROUP BY
    if (stmt.groupBy) {
      // Estimate group count from ANALYZE ndistinct
      let groupEstimate = null;
      const stats = this._tableStats?.get(tableName);
      if (stats && stmt.groupBy.length > 0) {
        // For single column GROUP BY, use ndistinct
        const groupCols = stmt.groupBy.map(g => typeof g === 'string' ? g : g.name).filter(Boolean);
        if (groupCols.length === 1 && stats.columns[groupCols[0]]) {
          groupEstimate = stats.columns[groupCols[0]].distinct;
        } else if (groupCols.length > 1) {
          // Multi-column: product of ndistinct (capped at total rows)
          groupEstimate = groupCols.reduce((prod, c) => {
            return prod * (stats.columns[c]?.distinct || 10);
          }, 1);
          groupEstimate = Math.min(groupEstimate, estRows);
        }
      }
      plan.push({ operation: 'HASH_GROUP_BY', columns: stmt.groupBy, estimated_groups: groupEstimate });
    }

    // HAVING
    if (stmt.having) {
      plan.push({ operation: 'FILTER', condition: 'HAVING' });
    }

    // Window functions
    if (stmt.columns.some(c => c.type === 'window')) {
      plan.push({ operation: 'WINDOW_FUNCTION' });
    }

    // Aggregates
    if (stmt.columns.some(c => c.type === 'aggregate') && !stmt.groupBy) {
      plan.push({ operation: 'AGGREGATE' });
    }

    // ORDER BY
    if (stmt.orderBy) {
      if (this._canEliminateSort(stmt)) {
        plan.push({ operation: 'SORT_ELIMINATED', reason: 'BTree PK ordering', columns: stmt.orderBy.map(o => `${o.column} ${o.direction}`) });
      } else {
        plan.push({ operation: 'SORT', columns: stmt.orderBy.map(o => `${o.column} ${o.direction}`) });
      }
    }

    // DISTINCT
    if (stmt.distinct) {
      plan.push({ operation: 'DISTINCT' });
    }

    // LIMIT
    if (stmt.limit) {
      plan.push({ operation: 'LIMIT', count: stmt.limit });
    }

    return this._formatPlan(plan, format);
  }

  _formatPlan(plan, format) {
    switch (format) {
      case 'json': {
        const json = JSON.stringify(plan, null, 2);
        return { type: 'PLAN', rows: [{ 'QUERY PLAN': json }] };
      }
      case 'yaml': {
        const yaml = this._planToYaml(plan);
        return { type: 'PLAN', rows: [{ 'QUERY PLAN': yaml }] };
      }
      case 'dot': {
        const dot = this._planToDot(plan);
        return { type: 'PLAN', rows: [{ 'QUERY PLAN': dot }] };
      }
      case 'text':
      default: {
        // Format like PostgreSQL's EXPLAIN output with cost estimates
        const lines = [];
        let indent = 0;
        let runningCost = 0.00;
        const SEQ_PAGE_COST = 1.0;
        const CPU_TUPLE_COST = 0.01;
        const CPU_OPERATOR_COST = 0.0025;
        const SORT_COST_FACTOR = 2.0; // n log n factor

        for (const step of plan) {
          const prefix = indent > 0 ? '  '.repeat(indent) + '->  ' : '';
          switch (step.operation) {
            case 'TABLE_SCAN': {
              const rows = step.estimated_rows || 0;
              const startCost = runningCost;
              runningCost += rows * (SEQ_PAGE_COST + CPU_TUPLE_COST);
              const filteredRows = step.filtered_estimate || rows;
              const eng = step.engine ? ` engine=${step.engine}` : '';
              lines.push(`${prefix}Seq Scan on ${step.table}  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)} rows=${filteredRows} width=32${eng})`);
              if (step.estimation_method) {
                lines.push(`  ${'  '.repeat(indent)}  Estimation: ${step.estimation_method}`);
              }
              indent++;
              break;
            }
            case 'INDEX_SCAN': {
              const rows = step.estimated_rows || 1;
              const startCost = runningCost + 0.5; // index startup cost
              runningCost = startCost + rows * CPU_TUPLE_COST;
              lines.push(`${prefix}Index Scan using ${step.index || 'idx'} on ${step.table}  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)} rows=${rows} width=32)`);
              if (step.estimation_method) {
                lines.push(`  ${'  '.repeat(indent)}  Estimation: ${step.estimation_method}`);
              }
              indent++;
              break;
            }
            case 'BTREE_PK_LOOKUP': {
              const rows = step.estimated_rows || 1;
              const startCost = runningCost + 0.25;
              runningCost = startCost + rows * CPU_TUPLE_COST;
              lines.push(`${prefix}BTree PK Lookup on ${step.table}  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)} rows=${rows} width=32 engine=btree)`);
              indent++;
              break;
            }
            case 'HASH_JOIN': {
              const rightRows = step.estimated_right_rows || 100;
              const startCost = runningCost;
              const hashBuildCost = rightRows * CPU_TUPLE_COST;
              runningCost += hashBuildCost + rightRows * CPU_OPERATOR_COST;
              const selfTag = step.selfJoin ? ' [Self-Join]' : '';
              lines.push(`${prefix}Hash ${step.type} Join${selfTag}  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)} rows=${rightRows})`);
              lines.push(`  ${'  '.repeat(indent)}  Hash Cond: (${step.on})`);
              indent++;
              break;
            }
            case 'NESTED_LOOP_JOIN': {
              const startCost = runningCost;
              runningCost += (step.estimated_right_rows || 100) * CPU_TUPLE_COST * 10;
              lines.push(`${prefix}Nested Loop ${step.type} Join  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)})`);
              lines.push(`  ${'  '.repeat(indent)}  Join Filter: ${step.on}`);
              indent++;
              break;
            }
            case 'JOIN_REORDER':
              lines.push(`${prefix}Join Reorder: ${step.original} → ${step.optimized}  (${step.reason})`);
              break;
            case 'FILTER': {
              const filterCost = CPU_OPERATOR_COST;
              runningCost += filterCost;
              lines.push(`${prefix}Filter: ${step.condition}`);
              break;
            }
            case 'HASH_GROUP_BY': {
              const startCost = runningCost;
              const groups = step.estimated_groups || 10;
              runningCost += groups * CPU_TUPLE_COST;
              lines.push(`${prefix}HashAggregate  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)} rows=${groups})`);
              lines.push(`  ${'  '.repeat(indent)}  Group Key: ${step.columns.join(', ')}`);
              break;
            }
            case 'AGGREGATE': {
              const startCost = runningCost;
              runningCost += CPU_TUPLE_COST;
              lines.push(`${prefix}Aggregate  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)} rows=1)`);
              break;
            }
            case 'SORT': {
              const startCost = runningCost;
              const prevRows = plan.find(p => p.estimated_rows)?.estimated_rows || 100;
              const sortWork = prevRows > 1 ? prevRows * Math.log2(prevRows) * CPU_OPERATOR_COST * SORT_COST_FACTOR : 0;
              runningCost += sortWork;
              lines.push(`${prefix}Sort  (keys: ${step.columns.join(', ')}; cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)})`);
              break;
            }
            case 'SORT_ELIMINATED':
              lines.push(`${prefix}Sort Eliminated  (keys: ${step.columns.join(', ')}, reason: ${step.reason})`);
              break;
            case 'LIMIT': {
              const startCost = runningCost;
              lines.push(`${prefix}Limit  (cost=${startCost.toFixed(2)}..${startCost.toFixed(2)} rows=${step.count})`);
              break;
            }
            case 'DISTINCT':
              lines.push(`${prefix}Unique  (cost=${runningCost.toFixed(2)}..${runningCost.toFixed(2)})`);
              break;
            case 'WINDOW_FUNCTION': {
              const startCost = runningCost;
              runningCost += CPU_TUPLE_COST * 2;
              lines.push(`${prefix}WindowAgg  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)})`);
              break;
            }
            case 'CTE':
              lines.push(`${prefix}CTE Scan on ${step.name}${step.recursive ? ' (recursive)' : ''}`);
              indent++;
              break;
            case 'VIEW_SCAN':
              lines.push(`${prefix}Subquery Scan on ${step.view}`);
              indent++;
              break;
            default:
              lines.push(`${prefix}${step.operation}  ${JSON.stringify(step)}`);
          }
        }
        return { type: 'PLAN', plan, rows: lines.map(l => ({ 'QUERY PLAN': l })) };
      }
    }
  }

  _planToYaml(plan, indent = 0) {
    const lines = [];
    const prefix = '  '.repeat(indent);
    if (Array.isArray(plan)) {
      for (const item of plan) {
        if (typeof item === 'object' && item !== null) {
          lines.push(`${prefix}-`);
          for (const [key, value] of Object.entries(item)) {
            if (Array.isArray(value)) {
              lines.push(`${prefix}  ${key}:`);
              for (const v of value) {
                lines.push(`${prefix}    - ${v}`);
              }
            } else {
              lines.push(`${prefix}  ${key}: ${value}`);
            }
          }
        } else {
          lines.push(`${prefix}- ${item}`);
        }
      }
    } else if (typeof plan === 'object' && plan !== null) {
      for (const [key, value] of Object.entries(plan)) {
        if (typeof value === 'object' && value !== null) {
          lines.push(`${prefix}${key}:`);
          lines.push(this._planToYaml(value, indent + 1));
        } else {
          lines.push(`${prefix}${key}: ${value}`);
        }
      }
    }
    return lines.join('\n');
  }

  _planToDot(plan) {
    const lines = ['digraph QueryPlan {', '  rankdir=TB;', '  node [shape=record, fontname="Courier"];'];
    let nextId = 0;
    const nodes = Array.isArray(plan) ? plan : [plan];
    let prevId = null;
    for (const node of nodes) {
      const id = `n${nextId++}`;
      const op = node.operation || node.type || 'unknown';
      const details = Object.entries(node)
        .filter(([k]) => k !== 'operation' && k !== 'type')
        .map(([k, v]) => `${k}: ${Array.isArray(v) ? v.join(', ') : v}`)
        .join('\\n');
      const label = details ? `${op}|${details}` : op;
      lines.push(`  ${id} [label="{${label}}"];`);
      if (prevId !== null) {
        lines.push(`  ${prevId} -> ${id};`);
      }
      prevId = id;
    }
    lines.push('}');
    return lines.join('\n');
  }

  // Execute a recursive CTE: base UNION ALL recursive
  _executeRecursiveCTE(cte) {
    const MAX_ITERATIONS = 1000;

    // Split into base and recursive parts
    let baseQuery, recursiveQuery;
    if (cte.query.type === 'UNION') {
      baseQuery = cte.query.left;
      recursiveQuery = cte.query.right;
    } else {
      baseQuery = cte.query;
      recursiveQuery = cte.unionQuery;
    }

    // Step 1: Execute base query
    const baseResult = this._select(baseQuery);
    let columnNames = Object.keys(baseResult.rows[0] || {});
    
    // Apply CTE column aliases if provided: WITH RECURSIVE cnt(x) AS (...)
    if (cte.columns && cte.columns.length > 0) {
      // Determine mapping: if CTE has more column names than row keys, there
      // are duplicate column names in the base query. Use AST to resolve them.
      // Otherwise, use row keys (more reliable).
      const rowKeys = Object.keys(baseResult.rows[0] || {});
      let mappingKeys;
      if (rowKeys.length < cte.columns.length) {
        // Duplicate column names detected — use AST column order
        // Map AST column references to row keys
        mappingKeys = baseQuery.columns.map(c => {
          if (c.alias) return c.alias;
          if (c.name) return c.name;
          if (c.type === 'aggregate') return c.alias || `${c.func}(${c.arg || '*'})`;
          return c.alias || c.name || 'expr';
        });
      } else {
        mappingKeys = rowKeys;
      }
      const aliasedRows = baseResult.rows.map(row => {
        const aliased = {};
        for (let i = 0; i < cte.columns.length && i < mappingKeys.length; i++) {
          aliased[cte.columns[i]] = row[mappingKeys[i]];
        }
        return aliased;
      });
      baseResult.rows = aliasedRows;
      columnNames = cte.columns.slice(0, mappingKeys.length);
    }
    let allRows = [...baseResult.rows];
    let workingSet = [...baseResult.rows];

    // Initialize CYCLE tracking with base rows
    if (cte.cycle) {
      this._cycleVisited = new Set();
      const cycleCols = cte.cycle.columns;
      for (const row of allRows) {
        const key = cycleCols.map(c => String(row[c] ?? '')).join('|||');
        this._cycleVisited.add(key);
        row[cte.cycle.setCycleCol] = cte.cycle.defaultVal;
        row[cte.cycle.pathCol] = key;
      }
    }

    // Step 2: Iterate until fixed point
    for (let iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
      if (workingSet.length === 0) break;

      // Register current working set as the CTE view
      this.views.set(cte.name, { materializedRows: workingSet, isCTE: true });

      // Execute recursive part
      const recursiveResult = this._select(recursiveQuery);
      let newRows = recursiveResult.rows;

      if (newRows.length === 0) break;

      // Normalize column names to match base query
      if (columnNames.length > 0) {
        newRows = newRows.map(row => {
          const normalized = {};
          const rowKeys = Object.keys(row);
          for (let i = 0; i < columnNames.length && i < rowKeys.length; i++) {
            normalized[columnNames[i]] = row[rowKeys[i]];
          }
          return normalized;
        });
      }

      // CYCLE clause handling
      if (cte.cycle) {
        const { columns: cycleCols, setCycleCol, cycleMarkVal, defaultVal, pathCol } = cte.cycle;
        // Track visited states by cycle columns
        if (!this._cycleVisited) this._cycleVisited = new Set();
        
        // Compute cycle key for each new row
        const filteredNew = [];
        for (const row of newRows) {
          const cycleKey = cycleCols.map(c => String(row[c] ?? '')).join('|||');
          if (this._cycleVisited.has(cycleKey)) {
            // This row would create a cycle — mark it but don't recurse
            row[setCycleCol] = cycleMarkVal;
            row[pathCol] = '(cycle)';
            filteredNew.push(row); // Include the cycle row but don't add to working set
          } else {
            this._cycleVisited.add(cycleKey);
            row[setCycleCol] = defaultVal;
            row[pathCol] = cycleKey;
            filteredNew.push(row);
          }
        }
        
        // Only non-cycle rows continue recursion
        const nonCycleRows = filteredNew.filter(r => r[setCycleCol] !== cycleMarkVal);
        allRows.push(...filteredNew);
        workingSet = nonCycleRows;
        
        if (nonCycleRows.length === 0) {
          delete this._cycleVisited;
          break;
        }
        continue;
      }

      // Default cycle detection: check if any new row already exists in allRows
      const seenKeys = new Set(allRows.map(r => JSON.stringify(Object.values(r))));
      const uniqueNew = newRows.filter(r => !seenKeys.has(JSON.stringify(Object.values(r))));

      if (uniqueNew.length === 0) break;

      allRows.push(...uniqueNew);
      workingSet = uniqueNew;
    }

    return allRows;
  }

  _explainCompiled(stmt) {
    if (stmt.type !== 'SELECT') {
      return { type: 'COMPILED_PLAN', message: 'Only SELECT queries can be compiled' };
    }

    const engine = new CompiledQueryEngine(this);
    
    const plan = engine.planner.plan(stmt);
    const explainText = engine.explainCompiled(stmt);
    
    // Also check if it would actually compile
    const tableStats = engine.planner.getStats(stmt.from?.table);
    const wouldCompile = (tableStats?.rowCount || 0) >= 50;
    
    const lines = explainText.split('\n');
    lines.push('');
    lines.push(`Compilation: ${wouldCompile ? 'YES (table has ' + (tableStats?.rowCount || 0) + ' rows)' : 'NO (table too small)'}`);
    
    if (plan.joins?.length > 0) {
      lines.push(`Join strategies: ${plan.joins.map(j => j.type).join(', ')}`);
    }
    
    const aggInfo = engine._extractAggregation?.(stmt);
    if (aggInfo) {
      lines.push(`Aggregation: compiled (${aggInfo.aggregates.map(a => a.fn).join(', ')} with ${aggInfo.groupBy.length} group columns)`);
    }

    return {
      type: 'COMPILED_PLAN',
      plan: lines,
      message: lines.join('\n'),
      compiled: wouldCompile,
      estimatedCost: plan.estimatedCost || plan.totalCost,
    };
  }

  _explainAnalyze(stmt) {
    // Build tree-structured plan with estimates
    let planTree = null;
    try {
      const builder = new PlanBuilder(this);
      planTree = builder.buildPlan(stmt);
    } catch (e) {
      // Plan builder may fail — fall through to legacy
    }

    // Get planner estimates (legacy)
    let plannerEstimate = null;
    try {
      const planner = new QueryPlanner(this);
      plannerEstimate = planner.plan(stmt);
    } catch (e) {
      // Planner may fail for complex queries — proceed with execution only
    }

    // Execute the actual query with timing and I/O tracking
    const startTime = performance.now();
    
    // Track I/O statistics: heap scans, buffer reads, index lookups
    const ioStats = { heapScans: 0, bufferReads: 0, indexLookups: 0, rowsExamined: 0 };
    
    // Instrument the table to count heap scans
    const ioTableName = stmt.from?.table;
    let origScan = null;
    let origGet = null;
    if (ioTableName && this.tables.has(ioTableName)) {
      const table = this.tables.get(ioTableName);
      if (table.heap && table.heap.scan) {
        origScan = table.heap.scan.bind(table.heap);
        const origScanFn = table.heap.scan;
        table.heap.scan = function(...args) {
          ioStats.heapScans++;
          const iter = origScan(...args);
          // Wrap iterator to count rows
          return {
            [Symbol.iterator]() {
              const it = iter[Symbol.iterator] ? iter[Symbol.iterator]() : iter;
              return {
                next() {
                  const result = it.next();
                  if (!result.done) {
                    ioStats.bufferReads++;
                    ioStats.rowsExamined++;
                  }
                  return result;
                }
              };
            }
          };
        };
      }
      if (table.heap && table.heap.get) {
        origGet = table.heap.get.bind(table.heap);
        table.heap.get = function(...args) {
          ioStats.bufferReads++;
          ioStats.indexLookups++;
          return origGet(...args);
        };
      }
    }
    
    let result;
    try {
      result = this._select(stmt);
    } finally {
      // Restore original methods
      if (ioTableName && this.tables.has(ioTableName)) {
        const table = this.tables.get(ioTableName);
        if (origScan) table.heap.scan = origScan;
        if (origGet) table.heap.get = origGet;
      }
    }
    
    const executionTime = performance.now() - startTime;
    const actualRows = result.rows.length;

    // Fill in actuals on the tree plan
    if (planTree) {
      // Set actuals on root node
      planTree.setActuals(actualRows, executionTime);
      // Propagate scan-level actuals
      this._fillScanActuals(planTree, stmt, actualRows);
    }

    // Build analyze output
    const analysis = [];
    
    // Table scan info
    const tableName = stmt.from?.table;
    if (tableName && this.tables.has(tableName)) {
      const table = this.tables.get(tableName);
      const totalRows = table.heap.tupleCount || 0;
      
      const engine = table.heap instanceof BTreeTable ? 'btree' : 'heap';
      
      // Use _estimateFilteredRows for better WHERE clause estimates
      let outputEstimate = plannerEstimate?.estimatedRows || totalRows;
      if (stmt.where) {
        const filterEst = this._estimateFilteredRows(tableName, stmt.where, totalRows);
        if (filterEst) outputEstimate = filterEst.estimated;
      }
      
      // For GROUP BY queries, estimate output rows based on group cardinality
      if (stmt.groupBy) {
        const stats = this._tableStats?.get(tableName);
        if (stats) {
          const groupCols = stmt.groupBy.map(g => typeof g === 'string' ? g : g.name).filter(Boolean);
          if (groupCols.length === 1 && stats.columns[groupCols[0]]) {
            outputEstimate = stats.columns[groupCols[0]].distinct;
          }
        }
      }

      analysis.push({
        operation: plannerEstimate?.scanType || 'TABLE_SCAN',
        table: tableName,
        engine,
        estimated_rows: outputEstimate,
        actual_rows: actualRows,
        total_table_rows: totalRows,
        selectivity: totalRows > 0 ? (actualRows / totalRows).toFixed(4) : '?',
      });

      if (plannerEstimate?.indexColumn) {
        analysis[0].index = plannerEstimate.indexColumn;
      }
    }

    // Join info
    for (const join of stmt.joins || []) {
      const joinTable = join.table?.table || join.table;
      analysis.push({
        operation: 'JOIN',
        table: joinTable,
        type: join.joinType || 'INNER',
      });
    }

    // WHERE filter
    if (stmt.where) {
      analysis.push({ operation: 'FILTER', actual_rows_after: actualRows });
    }

    // GROUP BY
    if (stmt.groupBy) {
      // Estimate group cardinality from ANALYZE stats
      let estimatedGroups = null;
      if (tableName && this._tableStats?.has(tableName)) {
        const stats = this._tableStats.get(tableName);
        const groupCols = stmt.groupBy.map(g => typeof g === 'string' ? g : g.name).filter(Boolean);
        if (groupCols.length === 1 && stats.columns[groupCols[0]]) {
          estimatedGroups = stats.columns[groupCols[0]].distinct;
        }
      }
      analysis.push({ operation: 'GROUP_BY', groups: actualRows, estimated_groups: estimatedGroups });
    }

    // ORDER BY
    if (stmt.orderBy) {
      if (this._canEliminateSort(stmt)) {
        analysis.push({ operation: 'SORT_ELIMINATED', reason: 'BTree PK ordering', actual_rows: actualRows });
      } else {
        analysis.push({ operation: 'SORT', rows_sorted: actualRows });
      }
    }

    const analyzeResult = {
      type: 'ROWS',
      rows: [
        ...analysis.map(a => {
          let line = a.operation;
          if (a.table) line += ` on ${a.table}`;
          if (a.engine) line += ` (engine=${a.engine})`;
          const parts = [];
          if (a.estimated_rows !== undefined) parts.push(`est=${a.estimated_rows}`);
          if (a.actual_rows !== undefined) parts.push(`actual=${a.actual_rows}`);
          if (a.total_table_rows !== undefined) parts.push(`total=${a.total_table_rows}`);
          if (a.selectivity) parts.push(`sel=${a.selectivity}`);
          if (a.index) parts.push(`index=${a.index}`);
          if (a.cost !== undefined) parts.push(`cost=${a.cost.toFixed(1)}`);
          if (parts.length) line += `  (${parts.join(', ')})`;
          return { 'QUERY PLAN': line };
        }),
        { 'QUERY PLAN': '' },
        { 'QUERY PLAN': `Planning Time: ${(plannerEstimate ? 0.1 : 0).toFixed(3)} ms` },
        { 'QUERY PLAN': `Execution Time: ${executionTime.toFixed(3)} ms` },
        { 'QUERY PLAN': `Actual Rows: ${actualRows}` },
        { 'QUERY PLAN': `Buffers: heap_scans=${ioStats.heapScans} buffer_reads=${ioStats.bufferReads} index_lookups=${ioStats.indexLookups} rows_examined=${ioStats.rowsExamined}` },
      ],
      analysis,
      ioStats,
      execution_time_ms: parseFloat(executionTime.toFixed(3)),
      actual_rows: actualRows,
      planTree: planTree || null,
      planTreeText: planTree ? PlanFormatter.format(planTree, { analyze: true }) : null,
    };
    return analyzeResult;
  }

  _fillScanActuals(node, stmt, totalActualRows) {
    // Walk the tree and fill in scan-level actuals where we can
    if (node.type === 'Seq Scan' && node.table) {
      const table = this.tables.get(node.table);
      if (table) {
        const tableRows = table.heap?._rowCount || table.heap?.tupleCount || 0;
        node.setActuals(tableRows, 0); // Scan reads all rows
      }
    }
    for (const child of node.children) {
      this._fillScanActuals(child, stmt, totalActualRows);
    }
  }

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

  _computeWindowFunctions(columns, rows, windowDefs) {
    const windowCols = columns.filter(c => c.type === 'window');

    for (const col of windowCols) {
      const name = col.alias || `${col.func}(${col.arg || ''})`;
      
      // Resolve named window reference
      let overSpec = col.over;
      if (overSpec && overSpec.windowRef && windowDefs && windowDefs[overSpec.windowRef]) {
        overSpec = windowDefs[overSpec.windowRef];
      }
      const { partitionBy, orderBy, frame } = overSpec || {};

      // Partition rows
      const partitions = new Map();
      for (const row of rows) {
        const key = partitionBy
          ? partitionBy.map(c => typeof c === 'string' ? this._resolveColumn(c, row) : this._evalValue(c, row)).join('\0')
          : '__all__';
        if (!partitions.has(key)) partitions.set(key, []);
        partitions.get(key).push(row);
      }

      // Sort each partition
      for (const [, partition] of partitions) {
        if (orderBy) {
          partition.sort((a, b) => {
            for (const { column, direction } of orderBy) {
              const av = this._orderByValue(column, a);
              const bv = this._orderByValue(column, b);
              const cmp = av < bv ? -1 : av > bv ? 1 : 0;
              if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
            }
            return 0;
          });
        }

        // Compute window function values
        // Helper: get frame bounds for row at index i
        const getFrameBounds = (i, len) => {
          if (!frame) {
            // Default: with ORDER BY → UNBOUNDED PRECEDING to CURRENT ROW
            // Without ORDER BY → entire partition
            return orderBy ? [0, i] : [0, len - 1];
          }
          let start = 0, end = len - 1;
          // Start bound
          if (frame.start.type === 'UNBOUNDED' && frame.start.direction === 'PRECEDING') {
            start = 0;
          } else if (frame.start.type === 'CURRENT ROW') {
            start = i;
          } else if (frame.start.type === 'OFFSET') {
            if (frame.start.direction === 'PRECEDING') {
              start = Math.max(0, i - frame.start.offset);
            } else {
              start = Math.min(len - 1, i + frame.start.offset);
            }
          }
          // End bound
          if (frame.end.type === 'UNBOUNDED' && frame.end.direction === 'FOLLOWING') {
            end = len - 1;
          } else if (frame.end.type === 'CURRENT ROW') {
            end = i;
          } else if (frame.end.type === 'OFFSET') {
            if (frame.end.direction === 'PRECEDING') {
              end = Math.max(0, i - frame.end.offset);
            } else {
              end = Math.min(len - 1, i + frame.end.offset);
            }
          }
          return [start, end];
        };

        switch (col.func) {
          case 'ROW_NUMBER': {
            for (let i = 0; i < partition.length; i++) {
              partition[i][`__window_${name}`] = i + 1;
            }
            break;
          }
          case 'RANK': {
            let rank = 1;
            for (let i = 0; i < partition.length; i++) {
              if (i > 0 && orderBy) {
                const same = orderBy.every(({ column }) =>
                  this._resolveColumn(column, partition[i]) === this._resolveColumn(column, partition[i - 1])
                );
                if (!same) rank = i + 1;
              }
              partition[i][`__window_${name}`] = rank;
            }
            break;
          }
          case 'DENSE_RANK': {
            let rank = 1;
            for (let i = 0; i < partition.length; i++) {
              if (i > 0 && orderBy) {
                const same = orderBy.every(({ column }) =>
                  this._resolveColumn(column, partition[i]) === this._resolveColumn(column, partition[i - 1])
                );
                if (!same) rank++;
              }
              partition[i][`__window_${name}`] = rank;
            }
            break;
          }
          case 'COUNT': {
            for (let i = 0; i < partition.length; i++) {
              const [start, end] = getFrameBounds(i, partition.length);
              partition[i][`__window_${name}`] = end - start + 1;
            }
            break;
          }
          case 'SUM': {
            for (let i = 0; i < partition.length; i++) {
              const [start, end] = getFrameBounds(i, partition.length);
              let sum = 0;
              for (let j = start; j <= end; j++) {
                sum += (this._resolveColumn(col.arg, partition[j]) || 0);
              }
              partition[i][`__window_${name}`] = sum;
            }
            break;
          }
          case 'AVG': {
            for (let i = 0; i < partition.length; i++) {
              const [start, end] = getFrameBounds(i, partition.length);
              let sum = 0;
              const count = end - start + 1;
              for (let j = start; j <= end; j++) {
                sum += (this._resolveColumn(col.arg, partition[j]) || 0);
              }
              partition[i][`__window_${name}`] = count > 0 ? sum / count : null;
            }
            break;
          }
          case 'MIN': {
            for (let i = 0; i < partition.length; i++) {
              const [start, end] = getFrameBounds(i, partition.length);
              let min = Infinity;
              for (let j = start; j <= end; j++) {
                const v = this._resolveColumn(col.arg, partition[j]);
                if (v != null && v < min) min = v;
              }
              partition[i][`__window_${name}`] = min === Infinity ? null : min;
            }
            break;
          }
          case 'MAX': {
            for (let i = 0; i < partition.length; i++) {
              const [start, end] = getFrameBounds(i, partition.length);
              let max = -Infinity;
              for (let j = start; j <= end; j++) {
                const v = this._resolveColumn(col.arg, partition[j]);
                if (v != null && v > max) max = v;
              }
              partition[i][`__window_${name}`] = max === -Infinity ? null : max;
            }
            break;
          }
          case 'LAG': {
            const lagArg = typeof col.arg === 'object' && col.arg?.name ? col.arg.name : col.arg;
            const offset = col.offset || 1;
            const defaultVal = col.defaultValue ?? null;
            for (let i = 0; i < partition.length; i++) {
              const prevIdx = i - offset;
              if (prevIdx >= 0) {
                partition[i][`__window_${name}`] = this._resolveColumn(lagArg, partition[prevIdx]);
              } else {
                partition[i][`__window_${name}`] = defaultVal;
              }
            }
            break;
          }
          case 'LEAD': {
            const leadArg = typeof col.arg === 'object' && col.arg?.name ? col.arg.name : col.arg;
            const offset2 = col.offset || 1;
            const defaultVal2 = col.defaultValue ?? null;
            for (let i = 0; i < partition.length; i++) {
              const nextIdx = i + offset2;
              if (nextIdx < partition.length) {
                partition[i][`__window_${name}`] = this._resolveColumn(leadArg, partition[nextIdx]);
              } else {
                partition[i][`__window_${name}`] = defaultVal2;
              }
            }
            break;
          }
          case 'NTILE': {
            const nArg = typeof col.arg === 'object' && col.arg?.value ? col.arg.value : (col.arg || 4);
            const n = Math.min(nArg, partition.length);
            const baseSize = Math.floor(partition.length / n);
            const remainder = partition.length % n;
            let idx = 0;
            for (let tile = 1; tile <= n; tile++) {
              const size = baseSize + (tile <= remainder ? 1 : 0);
              for (let j = 0; j < size; j++) {
                partition[idx++][`__window_${name}`] = tile;
              }
            }
            break;
          }
          case 'FIRST_VALUE': {
            const fvArg = typeof col.arg === 'object' && col.arg?.name ? col.arg.name : col.arg;
            const firstVal = partition.length > 0 ? this._resolveColumn(fvArg, partition[0]) : null;
            for (const r of partition) r[`__window_${name}`] = firstVal;
            break;
          }
          case 'LAST_VALUE': {
            const lvArg = typeof col.arg === 'object' && col.arg?.name ? col.arg.name : col.arg;
            if (orderBy) {
              for (let i = 0; i < partition.length; i++) {
                partition[i][`__window_${name}`] = this._resolveColumn(lvArg, partition[i]);
              }
            } else {
              const lastVal = partition.length > 0 ? this._resolveColumn(lvArg, partition[partition.length - 1]) : null;
              for (const r of partition) r[`__window_${name}`] = lastVal;
            }
            break;
          }
          case 'NTH_VALUE': {
            // NTH_VALUE(col, n) — returns the value of col at the nth row in the partition
            const nvArg = typeof col.arg === 'object' && col.arg?.name ? col.arg.name : col.arg;
            const n = col.offset || 1; // offset stores the second argument
            if (partition.length >= n) {
              const nthVal = this._resolveColumn(nvArg, partition[n - 1]);
              for (let i = 0; i < partition.length; i++) {
                // With default frame (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                // NTH_VALUE returns null until the frame includes the nth row
                if (orderBy && i < n - 1) {
                  partition[i][`__window_${name}`] = null;
                } else {
                  partition[i][`__window_${name}`] = nthVal;
                }
              }
            } else {
              for (const r of partition) r[`__window_${name}`] = null;
            }
            break;
          }
          case 'CUME_DIST': {
            // CUME_DIST = fraction of rows with value <= current row's value
            // For ties, all tied rows get the same value (highest rank / total)
            const n = partition.length;
            if (n === 0) break;
            for (let i = 0; i < n; i++) {
              // Find last row with same ORDER BY value (ties)
              let lastTie = i;
              while (lastTie + 1 < n && this._windowOrderEqual(partition[i], partition[lastTie + 1], orderBy)) {
                lastTie++;
              }
              const cumeDist = (lastTie + 1) / n;
              for (let j = i; j <= lastTie; j++) {
                partition[j][`__window_${name}`] = cumeDist;
              }
              i = lastTie; // Skip ties
            }
            break;
          }
          case 'PERCENT_RANK': {
            // PERCENT_RANK = (rank - 1) / (N - 1), 0 for first row
            const n = partition.length;
            if (n <= 1) {
              for (const r of partition) r[`__window_${name}`] = 0;
              break;
            }
            let rank = 1;
            for (let i = 0; i < n; i++) {
              if (i > 0 && !this._windowOrderEqual(partition[i - 1], partition[i], orderBy)) {
                rank = i + 1;
              }
              partition[i][`__window_${name}`] = (rank - 1) / (n - 1);
            }
            break;
          }
        }
      }
    }

    return rows;
  }

  _windowOrderEqual(rowA, rowB, orderBy) {
    if (!orderBy || orderBy.length === 0) return true;
    for (const ob of orderBy) {
      const col = ob.column?.name || ob.column;
      const va = this._resolveColumn(col, rowA);
      const vb = this._resolveColumn(col, rowB);
      if (va !== vb) return false;
    }
    return true;
  }

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
  
  _selectWithGroupBy(ast, rows) {
    // Build alias→expression map from SELECT columns
    const aliasMap = new Map();
    // Also build alias→column name map for simple renames (name AS person)
    const columnAliasMap = new Map();
    for (const col of ast.columns) {
      if (col.alias) {
        if (col.type === 'column') columnAliasMap.set(col.alias, col.name);
        else if (col.type === 'expression' && col.expr) aliasMap.set(col.alias, col.expr);
        else if (col.type === 'function') aliasMap.set(col.alias, col);
        else if (col.type === 'case') aliasMap.set(col.alias, col);
      }
    }
    
    // Helper: resolve GROUP BY column (string or expression)
    // If string matches a SELECT alias, use that expression instead
    const resolveGroupKey = (col, row) => {
      if (typeof col === 'string') {
        if (aliasMap.has(col)) {
          const expr = aliasMap.get(col);
          if (expr.type === 'function') return this._evalFunction(expr.func, expr.args, row);
          if (expr.type === 'case') return this._evalCase(expr, row);
          return this._evalValue(expr, row);
        }
        // Simple column alias (e.g., name AS person → resolve to name)
        if (columnAliasMap.has(col)) {
          return this._resolveColumn(columnAliasMap.get(col), row);
        }
        return this._resolveColumn(col, row);
      }
      // Ordinal position: GROUP BY 1 → first SELECT column
      if (col.type === 'literal' && typeof col.value === 'number') {
        const idx = col.value - 1; // 1-based
        if (idx >= 0 && idx < ast.columns.length) {
          const selCol = ast.columns[idx];
          if (selCol.type === 'column') return this._resolveColumn(selCol.name, row);
          if (selCol.type === 'expression' && selCol.expr) return this._evalValue(selCol.expr, row);
          if (selCol.type === 'function') return this._evalFunction(selCol.func, selCol.args, row);
          if (selCol.type === 'case') return this._evalCase(selCol, row);
          return this._evalValue(selCol, row);
        }
      }
      return this._evalValue(col, row); // Expression
    };

    // Handle GROUPING SETS / ROLLUP / CUBE
    let groupingSets = null;
    let effectiveGroupBy = ast.groupBy;
    
    if (ast.groupBy && !Array.isArray(ast.groupBy)) {
      if (ast.groupBy.type === 'ROLLUP') {
        // ROLLUP(a, b, c) = GROUPING SETS ((a,b,c), (a,b), (a), ())
        const cols = ast.groupBy.columns;
        groupingSets = [];
        for (let i = cols.length; i >= 0; i--) {
          groupingSets.push(cols.slice(0, i));
        }
      } else if (ast.groupBy.type === 'CUBE') {
        // CUBE(a, b) = all subsets: (a,b), (a), (b), ()
        const cols = ast.groupBy.columns;
        groupingSets = [];
        for (let mask = (1 << cols.length) - 1; mask >= 0; mask--) {
          const set = [];
          for (let i = 0; i < cols.length; i++) {
            if (mask & (1 << (cols.length - 1 - i))) set.push(cols[i]);
          }
          groupingSets.push(set);
        }
      } else if (ast.groupBy.type === 'GROUPING_SETS') {
        groupingSets = ast.groupBy.sets;
      }
    }

    if (groupingSets) {
      // Execute query for each grouping set and UNION ALL
      const allCols = ast.groupBy.columns || groupingSets.flat().filter((v, i, a) => a.indexOf(v) === i);
      let allRows = [];
      // Remove ORDER BY from sub-queries to avoid re-sorting
      const baseAst = { ...ast, orderBy: null, limit: null, offset: null };
      for (const setCols of groupingSets) {
        const subAst = { ...baseAst, groupBy: setCols.length > 0 ? setCols : null };
        const subResult = this._select(subAst);
        // NULL out columns not in this grouping set
        for (const row of subResult.rows) {
          for (const col of allCols) {
            const colName = typeof col === 'string' ? col : (col.alias || col.name);
            if (!setCols.includes(col)) {
              row[colName] = null;
            }
          }
        }
        allRows = allRows.concat(subResult.rows);
      }
      // Apply ORDER BY and LIMIT to combined results
      if (ast.orderBy) {
        allRows.sort((a, b) => {
          for (const o of ast.orderBy) {
            const colName = typeof o.column === 'string' ? o.column : (o.column.name || o.column.alias);
            const av = a[colName], bv = b[colName];
            if (av === null && bv !== null) return o.direction === 'DESC' ? -1 : 1;
            if (av !== null && bv === null) return o.direction === 'DESC' ? 1 : -1;
            if (av < bv) return o.direction === 'DESC' ? 1 : -1;
            if (av > bv) return o.direction === 'DESC' ? -1 : 1;
          }
          return 0;
        });
      }
      if (ast.offset) allRows = allRows.slice(ast.offset);
      if (ast.limit != null) allRows = allRows.slice(0, ast.limit);
      return { rows: allRows, columns: allRows.length > 0 ? Object.keys(allRows[0]) : [] };
    }

    // Group rows by GROUP BY columns
    const groups = new Map();
    for (const row of rows) {
      const key = ast.groupBy.map(col => resolveGroupKey(col, row)).join('\0');
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(row);
    }

    // Compute aggregates per group
    let resultRows = [];
    for (const [, groupRows] of groups) {
      const result = {};

      // Add GROUP BY columns
      for (const col of ast.groupBy) {
        if (typeof col === 'string') {
          if (aliasMap.has(col)) {
            // Alias refers to a SELECT expression — evaluate and use alias as key
            const expr = aliasMap.get(col);
            let val;
            if (expr.type === 'function') val = this._evalFunction(expr.func, expr.args, groupRows[0]);
            else if (expr.type === 'case') val = this._evalCase(expr, groupRows[0]);
            else val = this._evalValue(expr, groupRows[0]);
            result[col] = val;
          } else if (columnAliasMap.has(col)) {
            // Simple column alias (name AS person) — resolve to real column, use alias as output key
            const realCol = columnAliasMap.get(col);
            const val = this._resolveColumn(realCol, groupRows[0]);
            result[col] = val;
          } else {
            const val = this._resolveColumn(col, groupRows[0]);
            result[col] = val;
            if (col.includes('.')) result[col.split('.').pop()] = val;
          }
        } else {
          // Ordinal position: GROUP BY 1 → resolve to SELECT column
          if (col.type === 'literal' && typeof col.value === 'number') {
            const idx = col.value - 1;
            if (idx >= 0 && idx < ast.columns.length) {
              const selCol = ast.columns[idx];
              const outKey = selCol.alias || selCol.name || `col_${col.value}`;
              let val;
              if (selCol.type === 'column') val = this._resolveColumn(selCol.name, groupRows[0]);
              else if (selCol.type === 'expression' && selCol.expr) val = this._evalValue(selCol.expr, groupRows[0]);
              else if (selCol.type === 'function') val = this._evalFunction(selCol.func, selCol.args, groupRows[0]);
              else if (selCol.type === 'case') val = this._evalCase(selCol, groupRows[0]);
              else val = this._evalValue(selCol, groupRows[0]);
              result[outKey] = val;
              continue;
            }
          }
          // Expression group key — evaluate and use the matching SELECT column alias
          const val = this._evalValue(col, groupRows[0]);
          // Find matching SELECT column alias for this expression
          let key;
          for (const selCol of ast.columns) {
            if (selCol.alias && selCol.type === 'expression') {
              // Check if the expressions match (by comparing stringified AST)
              const selExpr = selCol.expr || selCol;
              if (JSON.stringify(selExpr) === JSON.stringify(col)) {
                key = selCol.alias;
                break;
              }
            }
          }
          if (!key) {
            // No alias found — try to generate a readable name
            if (col.type === 'arith') {
              const left = typeof col.left === 'string' ? col.left : (col.left?.name || '?');
              const right = typeof col.right === 'object' ? (col.right?.value ?? '?') : col.right;
              key = `${left} ${col.op} ${right}`;
            } else {
              key = col.alias || `expr_${ast.groupBy.indexOf(col)}`;
            }
          }
          result[key] = val;
        }
      }

      // Helper to compute an aggregate on this group
      const computeAgg = (func, arg, distinct, extra = {}) => {
        let values;
        if (arg === '*') {
          values = groupRows;
        } else if (typeof arg === 'object') {
          // Expression argument (e.g., SUM(qty * price))
          values = groupRows.map(r => this._evalValue(arg, r)).filter(v => v != null);
        } else {
          values = groupRows.map(r => this._resolveColumn(arg, r)).filter(v => v != null);
        }
        switch (func) {
          case 'COUNT': {
            if (distinct && arg !== '*') return new Set(values).size;
            return arg === '*' ? groupRows.length : values.length;
          }
          case 'SUM': return values.length ? values.reduce((s, v) => s + v, 0) : null;
          case 'AVG': return values.length ? values.reduce((s, v) => s + v, 0) / values.length : null;
          case 'MIN': return values.length ? values.reduce((a, b) => a < b ? a : b) : null;
          case 'MAX': return values.length ? values.reduce((a, b) => a > b ? a : b) : null;
          case 'GROUP_CONCAT':
          case 'STRING_AGG': {
            const sep = extra.separator || ',';
            let items = distinct ? [...new Set(values)] : values;
            // Apply ORDER BY if specified inside the aggregate
            if (extra.aggOrderBy && extra.aggOrderBy.length > 0 && extra.groupRows) {
              const ordered = extra.groupRows.slice();
              ordered.sort((a, b) => {
                for (const ob of extra.aggOrderBy) {
                  const av = this._evalValue(ob.column, a);
                  const bv = this._evalValue(ob.column, b);
                  if (av < bv) return ob.direction === 'DESC' ? 1 : -1;
                  if (av > bv) return ob.direction === 'DESC' ? -1 : 1;
                }
                return 0;
              });
              items = ordered.map(r => {
                const v = typeof extra.aggArg === 'string' ? r[extra.aggArg] : this._evalValue(extra.aggArg, r);
                return v;
              }).filter(v => v != null);
            }
            const strs = items.map(String);
            return strs.length ? strs.join(sep) : null;
          }
          case 'JSON_AGG':
          case 'JSONB_AGG': {
            return JSON.stringify(distinct ? [...new Set(values)] : values);
          }
          case 'ARRAY_AGG': {
            return (distinct ? [...new Set(values)] : values);
          }
          case 'BOOL_AND':
          case 'EVERY': {
            const boolVals = (arg === '*' ? groupRows : values).filter(v => v != null);
            return boolVals.length === 0 ? null : boolVals.every(v => !!v);
          }
          case 'BOOL_OR': {
            const boolVals = (arg === '*' ? groupRows : values).filter(v => v != null);
            return boolVals.length === 0 ? null : boolVals.some(v => !!v);
          }
        }
      };

      // Add aggregate and non-aggregate columns
      for (const col of ast.columns) {
        if (col.type === 'aggregate') {
          const name = col.alias || `${col.func}(${col.arg})`;
          result[name] = computeAgg(col.func, col.arg, col.distinct, { separator: col.separator, aggOrderBy: col.aggOrderBy, groupRows, aggArg: col.arg });
          // Also store under canonical key for HAVING resolution
          const canonKey = `${col.func}(${col.arg})`;
          if (name !== canonKey) result[`__agg_${canonKey}`] = result[name];
        } else if (col.type === 'column') {
          const baseName = col.name.includes('.') ? col.name.split('.').pop() : col.name;
          const name = col.alias || baseName;
          result[name] = this._resolveColumn(col.name, groupRows[0]);
        } else if (col.type === 'expression') {
          // Expression columns (CASE, arithmetic, etc.) — evaluate with aggregate support
          const name = col.alias || 'expr';
          const expr = col.expr;
          // Check if expression contains aggregate references — if so, compute them
          result[name] = this._evalGroupExpr(expr, groupRows, result, computeAgg);
        }
      }

      // Pre-compute aggregates used in HAVING that aren't in SELECT
      if (ast.having) {
        this._collectAggregateExprs(ast.having).forEach(agg => {
          const argStr = this._serializeExpr(agg.arg);
          const key = `${agg.func}(${argStr})`;
          if (!(key in result) && !(`__agg_${key}` in result)) {
            result[`__agg_${key}`] = computeAgg(agg.func, agg.arg, agg.distinct);
          }
        });
      }

      resultRows.push(result);
    }

    // HAVING
    if (ast.having) {
      resultRows = resultRows.filter(row => this._evalExpr(ast.having, row));
    }

    // ORDER BY
    if (ast.orderBy) {
      resultRows.sort((a, b) => {
        for (const { column, direction } of ast.orderBy) {
          const av = this._orderByValue(column, a);
          const bv = this._orderByValue(column, b);
          const aNull = av === null || av === undefined;
          const bNull = bv === null || bv === undefined;
          if (aNull && bNull) continue;
          if (aNull) return direction === 'DESC' ? 1 : -1;
          if (bNull) return direction === 'DESC' ? -1 : 1;
          const cmp = av < bv ? -1 : av > bv ? 1 : 0;
          if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
        }
        return 0;
      });
    }

    // LIMIT
    if (ast.offset) resultRows = resultRows.slice(ast.offset);
    if (ast.limit != null) resultRows = resultRows.slice(0, ast.limit);

    // Strip internal __agg_ keys before returning
    resultRows = resultRows.map(row => {
      const clean = {};
      for (const [k, v] of Object.entries(row)) {
        if (!k.startsWith('__agg_')) clean[k] = v;
      }
      return clean;
    });

    return { type: 'ROWS', rows: resultRows };
  }

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
            const values = table.heap.get(rid.pageId, rid.slotIdx);
            if (values) {
              rows.push(this._valuesToRow(values, table.schema, tableAlias));
            }
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
            const values = table.heap.get(rid.pageId, rid.slotIdx);
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
            const values = table.heap.get(rid.pageId, rid.slotIdx);
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
            const values = table.heap.get(rid.pageId, rid.slotIdx);
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
      row[schema[i].name] = values[i];
      row[`${tableAlias}.${schema[i].name}`] = values[i];
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

  _collectAggregateExprs(expr) {
    if (!expr) return [];
    if (expr.type === 'aggregate_expr') return [expr];
    const results = [];
    for (const key of ['left', 'right', 'expr']) {
      if (expr[key]) results.push(...this._collectAggregateExprs(expr[key]));
    }
    return results;
  }

  /**
   * Resolve an ORDER BY column value from a row.
   * Handles string column names, numeric references, and expression nodes.
   */
  _orderByValue(column, row, selectCols) {
    if (typeof column === 'number') {
      if (selectCols && selectCols[column - 1]) {
        const selCol = selectCols[column - 1];
        const colName = selCol.alias || selCol.name;
        return row[colName] !== undefined ? row[colName] : this._resolveColumn(colName, row);
      }
      // Fallback: use unqualified keys
      const keys = Object.keys(row).filter(k => !k.includes('.'));
      const key = keys[column - 1];
      return key !== undefined ? row[key] : undefined;
    }
    if (typeof column === 'object' && column !== null) {
      return this._evalValue(column, row);
    }
    // String column name — try direct lookup first, then _resolveColumn
    if (row[column] !== undefined) return row[column];
    return this._resolveColumn(column, row);
  }

  /**
   * Evaluate an expression in GROUP BY context, resolving aggregate sub-expressions.
   */
  _evalGroupExpr(expr, groupRows, result, computeAgg) {
    if (!expr) return null;
    if (expr.type === 'literal') return expr.value;
    if (expr.type === 'column_ref') {
      // Try result first (already computed group-by / aggregate columns)
      if (result[expr.name] !== undefined) return result[expr.name];
      return this._resolveColumn(expr.name, groupRows[0]);
    }
    if (expr.type === 'aggregate_expr') {
      return computeAgg(expr.func, expr.arg, expr.distinct);
    }
    if (expr.type === 'case_expr') {
      for (const { condition, result: condResult } of expr.whens) {
        if (this._evalGroupCond(condition, groupRows, result, computeAgg)) {
          return this._evalGroupExpr(condResult, groupRows, result, computeAgg);
        }
      }
      return expr.elseResult ? this._evalGroupExpr(expr.elseResult, groupRows, result, computeAgg) : null;
    }
    if (expr.type === 'arith') {
      const left = this._evalGroupExpr(expr.left, groupRows, result, computeAgg);
      const right = this._evalGroupExpr(expr.right, groupRows, result, computeAgg);
      if (left == null || right == null) return null;
      switch (expr.op) {
        case '+': return left + right;
        case '-': return left - right;
        case '*': return left * right;
        case '/': return right === 0 ? null : left / right;
        case '%': return left % right;
      }
    }
    if (expr.type === 'unary_minus') {
      const val = this._evalGroupExpr(expr.operand, groupRows, result, computeAgg);
      if (val == null) return null;
      return val === 0 ? 0 : -val;
    }
    if (expr.type === 'function_call' || expr.type === 'function') {
      const args = (expr.args || []).map(a => this._evalGroupExpr(a, groupRows, result, computeAgg));
      return this._evalFunction(expr.func, args.map(v => ({ type: 'literal', value: v })), groupRows[0]);
    }
    // Fallback: try regular eval on first row
    return this._evalValue(expr, groupRows[0]);
  }

  _evalGroupCond(cond, groupRows, result, computeAgg) {
    if (!cond) return true;
    if (cond.type === 'COMPARE') {
      const left = this._evalGroupExpr(cond.left, groupRows, result, computeAgg);
      const right = this._evalGroupExpr(cond.right, groupRows, result, computeAgg);
      switch (cond.op) {
        case 'EQ': case '=': return left === right;
        case 'NE': case '!=': case '<>': return left !== right;
        case 'LT': case '<': return left < right;
        case 'GT': case '>': return left > right;
        case 'LE': case '<=': return left <= right;
        case 'GE': case '>=': return left >= right;
      }
    }
    if (cond.type === 'AND') return this._evalGroupCond(cond.left, groupRows, result, computeAgg) && this._evalGroupCond(cond.right, groupRows, result, computeAgg);
    if (cond.type === 'OR') return this._evalGroupCond(cond.left, groupRows, result, computeAgg) || this._evalGroupCond(cond.right, groupRows, result, computeAgg);
    if (cond.type === 'NOT') return !this._evalGroupCond(cond.expr, groupRows, result, computeAgg);
    // Fallback: eval as expression
    return !!this._evalGroupExpr(cond, groupRows, result, computeAgg);
  }

  /**
   * Extract a value from a JSON object using a path expression.
   * Supports: $.key, $.nested.key, $.array[0], $[0], $.key.array[1].nested
   */
  _jsonExtract(obj, path) {
    if (!path || !path.startsWith('$')) return null;
    const parts = path.substring(1); // Remove leading $
    if (!parts) return obj;
    
    let current = obj;
    // Tokenize path: split on . and [] 
    const tokens = parts.match(/\.([^.\[\]]+)|\[(\d+)\]/g);
    if (!tokens) return obj;
    
    for (const token of tokens) {
      if (current == null) return null;
      if (token.startsWith('.')) {
        const key = token.substring(1);
        if (typeof current !== 'object' || Array.isArray(current)) return null;
        current = current[key];
      } else if (token.startsWith('[')) {
        const idx = parseInt(token.slice(1, -1), 10);
        if (!Array.isArray(current)) return null;
        current = current[idx];
      }
    }
    
    // Return primitives directly, objects as JSON strings
    if (current === null || current === undefined) return null;
    if (typeof current === 'object') return JSON.stringify(current);
    return current;
  }

  /**
   * Evaluate an expression that contains aggregate functions against a set of rows.
   * Recursively evaluates aggregate_expr nodes against all rows, then computes arithmetic.
   */
  _evalAggregateExpr(expr, rows) {
    if (!expr) return null;
    if (expr.type === 'aggregate_expr') {
      // Compute the aggregate
      const func = expr.func;
      const isStarArg = expr.arg === '*' || (typeof expr.arg === 'object' && expr.arg?.name === '*');
      let values;
      if (isStarArg) {
        values = rows;
      } else if (typeof expr.arg === 'object') {
        values = rows.map(r => this._evalValue(expr.arg, r)).filter(v => v != null);
      } else {
        values = rows.map(r => this._resolveColumn(expr.arg, r)).filter(v => v != null);
      }
      if (expr.distinct) values = [...new Set(values)];
      
      switch (func) {
        case 'COUNT': return isStarArg ? rows.length : values.length;
        case 'SUM': return values.length ? values.reduce((s, v) => s + v, 0) : null;
        case 'AVG': return values.length ? values.reduce((s, v) => s + v, 0) / values.length : null;
        case 'MIN': return values.length ? values.reduce((a, b) => a < b ? a : b) : null;
        case 'MAX': return values.length ? values.reduce((a, b) => a > b ? a : b) : null;
        default: return null;
      }
    }
    if (expr.type === 'arith') {
      const left = this._evalAggregateExpr(expr.left, rows);
      const right = this._evalAggregateExpr(expr.right, rows);
      if (left == null || right == null) return null;
      switch (expr.op) {
        case '+': return left + right;
        case '-': return left - right;
        case '*': return left * right;
        case '/': return right !== 0 ? left / right : null;
        case '%': return right !== 0 ? left % right : null;
        default: return null;
      }
    }
    if (expr.type === 'literal') return expr.value;
    if (expr.type === 'number') return expr.value;
    // For non-aggregate expressions, evaluate against first row
    return rows.length ? this._evalValue(expr, rows[0]) : null;
  }

  /**
   * Check if an expression tree contains any aggregate function calls.
   */
  _exprContainsAggregate(expr) {
    if (!expr) return false;
    if (expr.type === 'aggregate_expr') return true;
    if (expr.type === 'arith') {
      return this._exprContainsAggregate(expr.left) || this._exprContainsAggregate(expr.right);
    }
    if (expr.type === 'function_call' && ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX'].includes(expr.func?.toUpperCase())) return true;
    return false;
  }

  _resolveColumn(name, row) {
    // Handle numeric column references (ORDER BY 1, 2, etc.)
    if (typeof name === 'number') {
      const keys = Object.keys(row);
      const idx = name - 1; // 1-based to 0-based
      if (idx >= 0 && idx < keys.length) return row[keys[idx]];
      return undefined;
    }
    if (name in row) return row[name];
    // Case-insensitive lookup
    const lowerName = name.toLowerCase();
    for (const key of Object.keys(row)) {
      if (key.toLowerCase() === lowerName) return row[key];
    }
    // Try without table prefix (e.g., t.a → a)
    for (const key of Object.keys(row)) {
      if (key.endsWith(`.${name}`)) return row[key];
      if (key.toLowerCase().endsWith(`.${lowerName}`)) return row[key];
    }
    // If name is qualified (contains '.'), try stripping the table alias
    if (name.includes('.')) {
      const colName = name.split('.').pop();
      const tablePrefix = name.substring(0, name.lastIndexOf('.'));
      const lowerColName = colName.toLowerCase();
      const lowerPrefix = tablePrefix.toLowerCase();
      
      // Check if this table prefix belongs to the current (inner) query scope
      const isInnerAlias = this._innerTableAliases && this._innerTableAliases.has(lowerPrefix);
      // Check if this table prefix belongs to the outer query scope
      const isOuterAlias = !isInnerAlias && this._outerRow;
      
      if (isOuterAlias) {
        // Resolve from outer row
        if (colName in this._outerRow) return this._outerRow[colName];
        for (const key of Object.keys(this._outerRow)) {
          if (key.toLowerCase() === lowerColName) return this._outerRow[key];
        }
      }
      
      // Resolve from inner row (strip alias)
      if (colName in row) return row[colName];
      for (const key of Object.keys(row)) {
        if (key.toLowerCase() === lowerColName) return row[key];
      }
    }
    // For correlated subqueries: check outer row
    if (this._outerRow) {
      if (name in this._outerRow) return this._outerRow[name];
      for (const key of Object.keys(this._outerRow)) {
        if (key.endsWith(`.${name}`)) return this._outerRow[key];
      }
      // If name is qualified, try stripping alias in outer row too
      if (name.includes('.')) {
        const colName = name.split('.').pop();
        if (colName in this._outerRow) return this._outerRow[colName];
        for (const key of Object.keys(this._outerRow)) {
          if (key.toLowerCase() === colName.toLowerCase()) return this._outerRow[key];
        }
      }
    }
    return undefined;
  }

  _evalExpr(expr, row) {
    if (!expr) return true;
    switch (expr.type) {
      case 'literal': return !!expr.value; // NULL/0/false → false, others → true
      case 'AND': return this._evalExpr(expr.left, row) && this._evalExpr(expr.right, row);
      case 'OR': return this._evalExpr(expr.left, row) || this._evalExpr(expr.right, row);
      case 'NOT': return !this._evalExpr(expr.expr, row);
      case 'MATCH_AGAINST': {
        // Find the fulltext index for this column
        const searchText = this._evalValue(expr.search, row);
        const column = expr.column;
        
        // Find a fulltext index that covers this column
        let ftIdx = null;
        for (const [, idx] of this.fulltextIndexes) {
          if (idx.column === column) { ftIdx = idx; break; }
        }
        if (!ftIdx) throw new Error(`No fulltext index found for column ${column}`);
        
        // Get the text from the current row
        const rowText = String(row[column] || '');
        const rowTokens = tokenize(rowText);
        const searchTokens = tokenize(String(searchText));
        
        // Check if all search terms appear in the row
        return searchTokens.every(st => rowTokens.includes(st));
      }
      case 'EXISTS': {
        const result = this._evalSubquery(expr.subquery, row);
        return result.length > 0;
      }
      case 'IN_SUBQUERY': {
        const leftVal = this._evalValue(expr.left, row);
        const result = this._evalSubquery(expr.subquery, row);
        return result.some(r => {
          const vals = Object.values(r);
          return vals.includes(leftVal);
        });
      }
      case 'IN_HASHSET': {
        const leftVal = this._evalValue(expr.left, row);
        const found = expr.hashSet.has(leftVal);
        return expr.negated ? !found : found;
      }
      case 'LITERAL_BOOL': {
        return expr.value;
      }
      case 'IN_LIST': {
        const leftVal = this._evalValue(expr.left, row);
        return expr.values.some(v => this._evalValue(v, row) === leftVal);
      }
      case 'IS_NULL': {
        const val = this._evalValue(expr.left, row);
        return val === null || val === undefined;
      }
      case 'IS_NOT_NULL': {
        const val = this._evalValue(expr.left, row);
        return val !== null && val !== undefined;
      }
      case 'LIKE': {
        const val = this._evalValue(expr.left, row);
        const pattern = this._evalValue(expr.pattern, row);
        if (val == null || pattern == null) return false;
        // Convert SQL LIKE pattern to regex: % → .*, _ → ., escape special chars
        // SQLite-compatible: case-insensitive by default for ASCII
        const regex = '^' + String(pattern)
          .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
          .replace(/%/g, '.*')
          .replace(/_/g, '.')
          + '$';
        return new RegExp(regex, 'i').test(String(val));
      }
      case 'ILIKE': {
        const val = this._evalValue(expr.left, row);
        const pattern = this._evalValue(expr.pattern, row);
        if (val == null || pattern == null) return false;
        const regex = '^' + String(pattern)
          .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
          .replace(/%/g, '.*')
          .replace(/_/g, '.')
          + '$';
        return new RegExp(regex, 'i').test(String(val));
      }
      case 'SIMILAR_TO': {
        const val = this._evalValue(expr.left, row);
        const pattern = this._evalValue(expr.pattern, row);
        if (val == null || pattern == null) return false;
        // SIMILAR TO: SQL standard regex with %, _, |, (), [], *, +
        // Convert to JS regex by only escaping non-SQL-regex chars
        let regex = '^';
        const p = String(pattern);
        for (let i = 0; i < p.length; i++) {
          const ch = p[i];
          if (ch === '%') regex += '.*';
          else if (ch === '_') regex += '.';
          else if (ch === '(' || ch === ')' || ch === '|' || ch === '[' || ch === ']' || ch === '+' || ch === '*') regex += ch;
          else if (ch === '\\' && i + 1 < p.length) { regex += '\\' + p[++i]; }
          else if ('.^${}?'.includes(ch)) regex += '\\' + ch;
          else regex += ch;
        }
        regex += '$';
        return new RegExp(regex).test(String(val));
      }
      case 'BETWEEN': {
        const val = this._evalValue(expr.left, row);
        let low = this._evalValue(expr.low, row);
        let high = this._evalValue(expr.high, row);
        if (val === null || val === undefined || low === null || low === undefined || high === null || high === undefined) return false;
        if (expr.symmetric && low > high) { const tmp = low; low = high; high = tmp; }
        return val >= low && val <= high;
      }
      case 'NATURAL_EQ': {
        // Compare column from left and right table in merged row
        // Use the RIGHT alias (qualified name preserved) and LEFT's original value
        // Left value: stored as __natural_left_<col> before merge
        const lVal = row[`__natural_left_${expr.column}`] ?? row[expr.column];
        const rVal = row[`${expr.rightAlias}.${expr.column}`] ?? row[expr.column];
        return lVal === rVal;
      }
      case 'QUANTIFIED_COMPARE': {
        // val op ANY/ALL (subquery)
        const leftVal = this._evalValue(expr.left, row);
        if (leftVal === null || leftVal === undefined) return false;
        const subRows = this._evalSubquery(expr.subquery, row);
        if (subRows.length === 0) {
          return expr.quantifier === 'ALL'; // ALL with empty set is true, ANY with empty set is false
        }
        const compare = (left, right) => {
          if (right === null || right === undefined) return null;
          switch (expr.op) {
            case 'EQ': return left === right;
            case 'NE': return left !== right;
            case 'LT': return left < right;
            case 'GT': return left > right;
            case 'LE': return left <= right;
            case 'GE': return left >= right;
          }
        };
        if (expr.quantifier === 'ANY') {
          return subRows.some(r => compare(leftVal, Object.values(r)[0]) === true);
        } else {
          return subRows.every(r => compare(leftVal, Object.values(r)[0]) === true);
        }
      }
      case 'COMPARE': {
        let left = this._evalValue(expr.left, row);
        let right = this._evalValue(expr.right, row);
        // SQL NULL semantics: any comparison with NULL returns false
        if (left === null || left === undefined || right === null || right === undefined) return false;
        // Implicit type coercion: if one is number and other is string, try numeric comparison
        if (typeof left === 'number' && typeof right === 'string') {
          const n = Number(right);
          if (!isNaN(n)) right = n;
        } else if (typeof left === 'string' && typeof right === 'number') {
          const n = Number(left);
          if (!isNaN(n)) left = n;
        }
        switch (expr.op) {
          case 'EQ': return left === right;
          case 'NE': return left !== right;
          case 'LT': return left < right;
          case 'GT': return left > right;
          case 'LE': return left <= right;
          case 'GE': return left >= right;
        }
      }
      default: {
        // For expression types not explicitly handled as boolean conditions
        // (arith, function_call, case_expr, etc.), evaluate as a value and
        // check truthiness. This handles WHERE 1+1, WHERE LENGTH('x'), etc.
        try {
          const val = this._evalValue(expr, row);
          return !!val; // NULL/0/false/'' → false, others → true
        } catch {
          return true; // If evaluation fails, default to true for safety
        }
      }
    }
  }

  _evalValue(node, row) {
    if (node.type === 'literal') return node.value;
    if (node.type === 'column_ref') return this._resolveColumn(node.name, row);
    if (node.type === 'MATCH_AGAINST') {
      // Return relevance score
      return this._evalExpr(node, row) ? 1 : 0;
    }
    if (node.type === 'SUBQUERY') {
      const result = this._evalSubquery(node.subquery, row);
      if (result.length === 0) return null;
      const firstRow = result[0];
      return Object.values(firstRow)[0];
    }
    if (node.type === 'function_call' || node.type === 'function') {
      return this._evalFunction(node.func, node.args, row);
    }
    if (node.type === 'cast') {
      const val = this._evalValue(node.expr, row);
      if (val == null) return null;
      switch (node.targetType) {
        case 'INT': case 'INTEGER': return parseInt(val, 10) || 0;
        case 'FLOAT': case 'REAL': case 'DOUBLE': return parseFloat(val) || 0;
        case 'TEXT': case 'VARCHAR': case 'CHAR': return String(val);
        case 'BOOLEAN': return Boolean(val);
        default: return val;
      }
    }
    if (node.type === 'case_expr') {
      for (const { condition, result } of node.whens) {
        if (this._evalExpr(condition, row)) {
          return this._evalValue(result, row);
        }
      }
      return node.elseResult ? this._evalValue(node.elseResult, row) : null;
    }
    if (node.type === 'interval') {
      return { __interval: true, value: node.value };
    }
    if (node.type === 'unary_minus') {
      const val = this._evalValue(node.operand, row);
      if (val == null) return null;
      const neg = -val;
      return neg === 0 ? 0 : neg; // avoid -0
    }
    if (node.type === 'arith') {
      const left = this._evalValue(node.left, row);
      const right = this._evalValue(node.right, row);
      if (left == null || right == null) return null;
      // Date arithmetic with INTERVAL
      if (right && right.__interval && (node.op === '+' || node.op === '-')) {
        return this._dateArith(left, right.value, node.op);
      }
      if (left && left.__interval && node.op === '+') {
        return this._dateArith(right, left.value, '+');
      }
      switch (node.op) {
        case '+': return left + right;
        case '-': return left - right;
        case '*': return left * right;
        case '/': {
          if (right === 0) return null;
          const result = left / right;
          // Integer division when both operands are integers
          if (Number.isInteger(left) && Number.isInteger(right)) return Math.trunc(result);
          return result;
        }
        case '%': return right === 0 ? null : left % right;
      }
    }
    if (node.type === 'aggregate_expr') {
      // In HAVING/ORDER BY context, look up the computed aggregate from the row
      const argStr = this._serializeExpr(node.arg);
      const key = `${node.func}(${argStr})`;
      if (key in row) return row[key];
      // Check prefixed aggregate keys (used for HAVING resolution)
      const prefixedKey = `__agg_${key}`;
      if (prefixedKey in row) return row[prefixedKey];
      // Try to find it with any alias pattern
      for (const k of Object.keys(row)) {
        if (k.toUpperCase().includes(node.func) && k.includes(argStr)) return row[k];
      }
      return null;
    }
    return null;
  }

  _dateArith(dateStr, intervalStr, op) {
    const d = new Date(String(dateStr));
    if (isNaN(d.getTime())) return null;
    const match = String(intervalStr).match(/^(\d+)\s*(year|month|day|hour|minute|second|week)s?$/i);
    if (!match) return null;
    const n = parseInt(match[1]) * (op === '-' ? -1 : 1);
    const unit = match[2].toLowerCase();
    switch (unit) {
      case 'year': d.setUTCFullYear(d.getUTCFullYear() + n); break;
      case 'month': d.setUTCMonth(d.getUTCMonth() + n); break;
      case 'day': d.setUTCDate(d.getUTCDate() + n); break;
      case 'week': d.setUTCDate(d.getUTCDate() + n * 7); break;
      case 'hour': d.setUTCHours(d.getUTCHours() + n); break;
      case 'minute': d.setUTCMinutes(d.getUTCMinutes() + n); break;
      case 'second': d.setUTCSeconds(d.getUTCSeconds() + n); break;
    }
    return d.toISOString();
  }

  _evalFunction(func, args, row) {
    switch (func) {
      case 'UPPER': { const v = this._evalValue(args[0], row); return v != null ? String(v).toUpperCase() : null; }
      case 'LOWER': { const v = this._evalValue(args[0], row); return v != null ? String(v).toLowerCase() : null; }
      case 'INITCAP': { const v = this._evalValue(args[0], row); return v != null ? String(v).replace(/\w\S*/g, t => t.charAt(0).toUpperCase() + t.slice(1).toLowerCase()) : null; }
      case 'LENGTH': case 'CHAR_LENGTH': { const v = this._evalValue(args[0], row); return v != null ? String(v).length : null; }
      case 'POSITION': {
        const substr = String(this._evalValue(args[0], row));
        const str = String(this._evalValue(args[1], row));
        const idx = str.indexOf(substr);
        return idx === -1 ? 0 : idx + 1;
      }
      case 'CONCAT': return args.map(a => { const v = this._evalValue(a, row); return v != null ? String(v) : ''; }).join('');
      case 'COALESCE': {
        for (const arg of args) {
          const v = this._evalValue(arg, row);
          if (v !== null && v !== undefined) return v;
        }
        return null;
      }
      case 'EXTRACT': {
        const field = String(this._evalValue(args[0], row)).toUpperCase();
        const dateStr = String(this._evalValue(args[1], row));
        const d = new Date(dateStr);
        if (isNaN(d.getTime())) return null;
        switch (field) {
          case 'YEAR': return d.getUTCFullYear();
          case 'MONTH': return d.getUTCMonth() + 1;
          case 'DAY': return d.getUTCDate();
          case 'HOUR': return d.getUTCHours();
          case 'MINUTE': return d.getUTCMinutes();
          case 'SECOND': return d.getUTCSeconds();
          case 'DOW': return d.getUTCDay();
          case 'DOY': { const start = new Date(Date.UTC(d.getUTCFullYear(), 0, 0)); return Math.floor((d - start) / 86400000); }
          case 'EPOCH': return Math.floor(d.getTime() / 1000);
          case 'QUARTER': return Math.ceil((d.getMonth() + 1) / 3);
          case 'WEEK': { const start = new Date(d.getFullYear(), 0, 1); return Math.ceil(((d - start) / 86400000 + start.getDay() + 1) / 7); }
          default: return null;
        }
      }
      case 'NULLIF': {
        const a = this._evalValue(args[0], row);
        const b = this._evalValue(args[1], row);
        return a === b ? null : a;
      }
      case 'SUBSTR':
      case 'SUBSTRING': {
        const str = this._evalValue(args[0], row);
        if (str == null) return null;
        const start = (this._evalValue(args[1], row) || 1) - 1; // SQL is 1-indexed
        const len = args[2] ? this._evalValue(args[2], row) : undefined;
        return String(str).substring(start, len !== undefined ? start + len : undefined);
      }
      case 'REPLACE': {
        const str = this._evalValue(args[0], row);
        if (str == null) return null;
        const search = this._evalValue(args[1], row);
        const replace = this._evalValue(args[2], row);
        return String(str).replaceAll(String(search), String(replace));
      }
      case 'TRIM': {
        const str = this._evalValue(args[0], row);
        return str != null ? String(str).trim() : null;
      }
      case 'ABS': {
        const val = this._evalValue(args[0], row);
        return val != null ? Math.abs(val) : null;
      }
      case 'ROUND': {
        const val = this._evalValue(args[0], row);
        if (val == null) return null;
        const decimals = args[1] ? this._evalValue(args[1], row) : 0;
        const factor = Math.pow(10, decimals);
        return Math.round(val * factor) / factor;
      }
      case 'CEIL': {
        const val = this._evalValue(args[0], row);
        return val != null ? Math.ceil(val) : null;
      }
      case 'FLOOR': {
        const val = this._evalValue(args[0], row);
        return val != null ? Math.floor(val) : null;
      }
      case 'IFNULL':
      case 'ISNULL':
      case 'NVL': {
        const val = this._evalValue(args[0], row);
        return val != null ? val : this._evalValue(args[1], row);
      }
      case 'IIF': {
        // IIF(condition, true_val, false_val) — but condition is an expression
        const cond = this._evalExpr(args[0], row);
        return cond ? this._evalValue(args[1], row) : this._evalValue(args[2], row);
      }
      case 'TYPEOF': {
        const val = this._evalValue(args[0], row);
        if (val === null || val === undefined) return 'null';
        if (typeof val === 'number') return Number.isInteger(val) ? 'integer' : 'real';
        if (typeof val === 'string') return 'text';
        if (typeof val === 'boolean') return 'integer';
        return 'blob';
      }
      case 'JSON_EXTRACT': {
        const json = this._evalValue(args[0], row);
        const path = this._evalValue(args[1], row);
        if (json == null) return null;
        try {
          const obj = typeof json === 'string' ? JSON.parse(json) : json;
          if (path === '$') return JSON.stringify(obj);
          const parts = path.replace(/^\$\.?/, '').split('.').filter(Boolean);
          let current = obj;
          for (const part of parts) {
            const arrMatch = part.match(/^(\w*)\[(\d+)\]$/);
            if (arrMatch) {
              if (arrMatch[1]) current = current[arrMatch[1]];
              current = current?.[parseInt(arrMatch[2])];
            } else {
              current = current?.[part];
            }
          }
          return current === undefined ? null : (typeof current === 'object' ? JSON.stringify(current) : current);
        } catch { return null; }
      }
      case 'JSON_EXTRACT_TEXT': {
        // Same as JSON_EXTRACT but always returns text/string
        const json = this._evalValue(args[0], row);
        const path = this._evalValue(args[1], row);
        if (json == null) return null;
        try {
          const obj = typeof json === 'string' ? JSON.parse(json) : json;
          const parts = path.replace(/^\$\.?/, '').split('.').filter(Boolean);
          let current = obj;
          for (const part of parts) {
            const arrMatch = part.match(/^(\w*)\[(\d+)\]$/);
            if (arrMatch) {
              if (arrMatch[1]) current = current[arrMatch[1]];
              current = current?.[parseInt(arrMatch[2])];
            } else {
              current = current?.[part];
            }
          }
          return current === undefined ? null : String(current);
        } catch { return null; }
      }
      case 'JSON_SET': {
        const json = this._evalValue(args[0], row);
        const path = this._evalValue(args[1], row);
        const value = this._evalValue(args[2], row);
        if (json == null) return null;
        try {
          const obj = typeof json === 'string' ? JSON.parse(json) : { ...json };
          const parts = path.replace(/^\$\.?/, '').split('.').filter(Boolean);
          let current = obj;
          for (let i = 0; i < parts.length - 1; i++) {
            if (!current[parts[i]]) current[parts[i]] = {};
            current = current[parts[i]];
          }
          current[parts[parts.length - 1]] = value;
          return JSON.stringify(obj);
        } catch { return null; }
      }
      case 'JSON_ARRAY_LENGTH': {
        const json = this._evalValue(args[0], row);
        if (json == null) return null;
        try {
          const arr = typeof json === 'string' ? JSON.parse(json) : json;
          return Array.isArray(arr) ? arr.length : null;
        } catch { return null; }
      }
      case 'JSON_TYPE': {
        const json = this._evalValue(args[0], row);
        if (json == null) return 'null';
        try {
          const val = typeof json === 'string' ? JSON.parse(json) : json;
          if (Array.isArray(val)) return 'array';
          if (typeof val === 'object') return 'object';
          return typeof val;
        } catch { return 'text'; }
      }
      // json_build_object(key1, val1, key2, val2, ...) → JSON string
      case 'JSON_BUILD_OBJECT': {
        const obj = {};
        for (let i = 0; i < args.length; i += 2) {
          const key = this._evalValue(args[i], row);
          const val = i + 1 < args.length ? this._evalValue(args[i + 1], row) : null;
          obj[key] = val;
        }
        return JSON.stringify(obj);
      }
      // json_build_array(val1, val2, ...) → JSON array string
      case 'JSON_BUILD_ARRAY': {
        const arr = args.map(a => this._evalValue(a, row));
        return JSON.stringify(arr);
      }
      // row_to_json(row) — we return the entire row as JSON
      case 'ROW_TO_JSON': {
        return JSON.stringify(row);
      }
      // to_json(value) — convert value to JSON
      case 'TO_JSON': {
        const v = this._evalValue(args[0], row);
        return JSON.stringify(v);
      }
      // json_object_keys(json) — for aggregate context
      case 'JSON_OBJECT_KEYS': {
        const json = this._evalValue(args[0], row);
        if (json == null) return null;
        try {
          const obj = typeof json === 'string' ? JSON.parse(json) : json;
          return JSON.stringify(Object.keys(obj));
        } catch { return null; }
      }
      // String functions
      case 'LEFT': { const v = this._evalValue(args[0], row); return v == null ? null : String(v).substring(0, this._evalValue(args[1], row)); }
      case 'RIGHT': { const v = this._evalValue(args[0], row); const n = this._evalValue(args[1], row); return v == null ? null : String(v).slice(-n); }
      case 'LPAD': {
        const str = String(this._evalValue(args[0], row) || '');
        const len = this._evalValue(args[1], row) || 0;
        const pad = args[2] ? String(this._evalValue(args[2], row)) : ' ';
        return str.length > len ? str.slice(0, len) : str.padStart(len, pad);
      }
      case 'RPAD': {
        const str = String(this._evalValue(args[0], row) || '');
        const len = this._evalValue(args[1], row) || 0;
        const pad = args[2] ? String(this._evalValue(args[2], row)) : ' ';
        return str.length > len ? str.slice(0, len) : str.padEnd(len, pad);
      }
      case 'REVERSE': { const v = this._evalValue(args[0], row); return v == null ? null : String(v).split('').reverse().join(''); }
      case 'REPEAT': { const v = this._evalValue(args[0], row); const n = this._evalValue(args[1], row); return v == null ? null : String(v).repeat(n || 0); }
      
      // Math functions
      case 'POWER': return Math.pow(this._evalValue(args[0], row), this._evalValue(args[1], row));
      case 'SQRT': return Math.sqrt(this._evalValue(args[0], row));
      case 'LOG': return args.length > 1 ? Math.log(this._evalValue(args[1], row)) / Math.log(this._evalValue(args[0], row)) : Math.log(this._evalValue(args[0], row));
      case 'EXP': return Math.exp(this._evalValue(args[0], row));
      case 'RANDOM': return Math.random();
      case 'GREATEST': { const vals = args.map(a => this._evalValue(a, row)).filter(v => v != null); return vals.length ? Math.max(...vals.map(Number)) : null; }
      case 'LEAST': { const vals = args.map(a => this._evalValue(a, row)).filter(v => v != null); return vals.length ? Math.min(...vals.map(Number)) : null; }
      case 'MOD': { const a = Number(this._evalValue(args[0], row)); const b = Number(this._evalValue(args[1], row)); return b === 0 ? null : a % b; }
      case 'LTRIM': { const v = this._evalValue(args[0], row); return v == null ? null : String(v).trimStart(); }
      case 'RTRIM': { const v = this._evalValue(args[0], row); return v == null ? null : String(v).trimEnd(); }
      
      // Regex functions
      case 'REGEXP_MATCHES': {
        const str = this._evalValue(args[0], row);
        const pattern = this._evalValue(args[1], row);
        if (str == null || pattern == null) return null;
        const flags = args[2] ? String(this._evalValue(args[2], row)) : '';
        try {
          const re = new RegExp(String(pattern), flags);
          const match = String(str).match(re);
          if (!match) return null;
          // If global flag, return all matches
          if (flags.includes('g')) {
            return [...String(str).matchAll(new RegExp(String(pattern), flags))].map(m => m[0]);
          }
          // Return capture groups (or full match if no groups)
          return match.length > 1 ? match.slice(1) : [match[0]];
        } catch (e) {
          throw new Error(`Invalid regex pattern: ${pattern}`);
        }
      }
      case 'REGEXP_REPLACE': {
        const str = this._evalValue(args[0], row);
        const pattern = this._evalValue(args[1], row);
        const replacement = this._evalValue(args[2], row);
        if (str == null) return null;
        const flags = args[3] ? String(this._evalValue(args[3], row)) : '';
        try {
          return String(str).replace(new RegExp(String(pattern), flags), String(replacement || ''));
        } catch (e) {
          throw new Error(`Invalid regex pattern: ${pattern}`);
        }
      }
      case 'REGEXP_COUNT': {
        const str = this._evalValue(args[0], row);
        const pattern = this._evalValue(args[1], row);
        if (str == null || pattern == null) return 0;
        const flags = args[2] ? String(this._evalValue(args[2], row)) : 'g';
        try {
          const matches = String(str).match(new RegExp(String(pattern), flags.includes('g') ? flags : flags + 'g'));
          return matches ? matches.length : 0;
        } catch (e) {
          throw new Error(`Invalid regex pattern: ${pattern}`);
        }
      }
      
      // Date/time functions
      case 'CURRENT_TIMESTAMP': case 'NOW': return new Date().toISOString();
      case 'CURRENT_DATE': return new Date().toISOString().split('T')[0];
      case 'NEXTVAL': {
        const seqName = String(this._evalValue(args[0], row)).toLowerCase();
        const seq = this.sequences.get(seqName);
        if (!seq) throw new Error(`Sequence ${seqName} not found`);
        seq.current += seq.increment;
        return seq.current;
      }
      case 'CURRVAL': {
        const seqName = String(this._evalValue(args[0], row)).toLowerCase();
        const seq = this.sequences.get(seqName);
        if (!seq) throw new Error(`Sequence ${seqName} not found`);
        return seq.current;
      }
      case 'SETVAL': {
        const seqName = String(this._evalValue(args[0], row)).toLowerCase();
        const seq = this.sequences.get(seqName);
        if (!seq) throw new Error(`Sequence ${seqName} not found`);
        seq.current = this._evalValue(args[1], row);
        return seq.current;
      }
      case 'DATE_ADD': {
        // DATE_ADD(date, interval, unit)
        const date = this._evalValue(args[0], row);
        const interval = this._evalValue(args[1], row);
        const unit = (this._evalValue(args[2], row) || 'day').toLowerCase();
        const d = new Date(date);
        switch (unit) {
          case 'day': case 'days': d.setDate(d.getDate() + interval); break;
          case 'month': case 'months': d.setMonth(d.getMonth() + interval); break;
          case 'year': case 'years': d.setFullYear(d.getFullYear() + interval); break;
          case 'hour': case 'hours': d.setHours(d.getHours() + interval); break;
          default: throw new Error(`Unknown date unit: ${unit}`);
        }
        return d.toISOString().split('T')[0];
      }
      case 'DATE_DIFF': {
        // DATE_DIFF(date1, date2, unit) — returns date1 - date2
        const d1 = new Date(this._evalValue(args[0], row));
        const d2 = new Date(this._evalValue(args[1], row));
        const unit = (this._evalValue(args[2], row) || 'day').toLowerCase();
        const diffMs = d1 - d2;
        switch (unit) {
          case 'day': case 'days': return Math.floor(diffMs / 86400000);
          case 'hour': case 'hours': return Math.floor(diffMs / 3600000);
          case 'month': case 'months': return (d1.getFullYear() - d2.getFullYear()) * 12 + d1.getMonth() - d2.getMonth();
          case 'year': case 'years': return d1.getFullYear() - d2.getFullYear();
          default: throw new Error(`Unknown date unit: ${unit}`);
        }
      }
      case 'REGEXP_MATCHES': {
        const str = String(this._evalValue(args[0], row));
        const pattern = String(this._evalValue(args[1], row));
        const flags = args.length > 2 ? String(this._evalValue(args[2], row)) : '';
        const regex = new RegExp(pattern, flags.includes('g') ? 'g' : '');
        const matches = str.match(regex);
        return matches ? JSON.stringify(matches) : null;
      }
      case 'DATE_TRUNC': {
        // DATE_TRUNC(unit, date)
        const unit = (this._evalValue(args[0], row) || 'day').toLowerCase();
        const d = new Date(this._evalValue(args[1], row));
        switch (unit) {
          case 'year': return `${d.getFullYear()}-01-01`;
          case 'month': return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-01`;
          case 'day': return d.toISOString().split('T')[0];
          default: throw new Error(`Unknown date trunc unit: ${unit}`);
        }
      }
      case 'STRFTIME': {
        const fmt = this._evalValue(args[0], row);
        const dateStr = args[1] ? this._evalValue(args[1], row) : new Date().toISOString();
        const d = new Date(dateStr);
        return String(fmt)
          .replace('%Y', String(d.getUTCFullYear()))
          .replace('%m', String(d.getUTCMonth() + 1).padStart(2, '0'))
          .replace('%d', String(d.getUTCDate()).padStart(2, '0'))
          .replace('%H', String(d.getUTCHours()).padStart(2, '0'))
          .replace('%M', String(d.getUTCMinutes()).padStart(2, '0'))
          .replace('%S', String(d.getUTCSeconds()).padStart(2, '0'));
      }
      
      // JSON functions
      case 'JSON_EXTRACT': case 'JSON_VALUE': {
        const jsonStr = this._evalValue(args[0], row);
        const path = this._evalValue(args[1], row);
        if (jsonStr == null || path == null) return null;
        try {
          const obj = typeof jsonStr === 'object' ? jsonStr : JSON.parse(String(jsonStr));
          return this._jsonExtract(obj, String(path));
        } catch (e) { return null; }
      }
      case 'JSON_ARRAY_LENGTH': {
        const jsonStr = this._evalValue(args[0], row);
        if (jsonStr == null) return null;
        try {
          const arr = typeof jsonStr === 'object' ? jsonStr : JSON.parse(String(jsonStr));
          return Array.isArray(arr) ? arr.length : null;
        } catch (e) { return null; }
      }
      case 'JSON_TYPE': {
        const jsonStr = this._evalValue(args[0], row);
        if (jsonStr == null) return 'null';
        try {
          const val = typeof jsonStr === 'object' ? jsonStr : JSON.parse(String(jsonStr));
          if (Array.isArray(val)) return 'array';
          if (val === null) return 'null';
          return typeof val; // 'object', 'number', 'string', 'boolean'
        } catch (e) { return 'text'; }
      }
      case 'JSON_OBJECT': {
        // JSON_OBJECT('key1', val1, 'key2', val2, ...)
        const obj = {};
        for (let i = 0; i < args.length; i += 2) {
          const key = String(this._evalValue(args[i], row));
          const val = i + 1 < args.length ? this._evalValue(args[i + 1], row) : null;
          obj[key] = val;
        }
        return JSON.stringify(obj);
      }
      case 'JSON_ARRAY': {
        const arr = args.map(a => this._evalValue(a, row));
        return JSON.stringify(arr);
      }
      case 'JSON_VALID': {
        const jsonStr = this._evalValue(args[0], row);
        if (jsonStr == null) return 0;
        try { JSON.parse(String(jsonStr)); return 1; } catch (e) { return 0; }
      }
      
      default: throw new Error(`Unknown function: ${func}`);
    }
  }

  _evalSubquery(subqueryAst, outerRow) {
    // Execute the subquery, passing outerRow for correlated references
    const savedOuterRow = this._outerRow;
    const savedInnerAliases = this._innerTableAliases;
    this._outerRow = outerRow;
    
    // Collect inner query's table aliases for qualified column resolution
    const aliases = new Set();
    if (subqueryAst.from) {
      const alias = (subqueryAst.from.alias || subqueryAst.from.table || '').toLowerCase();
      if (alias) aliases.add(alias);
    }
    if (subqueryAst.joins) {
      for (const join of subqueryAst.joins) {
        const alias = (join.alias || join.table || '').toLowerCase();
        if (alias) aliases.add(alias);
      }
    }
    this._innerTableAliases = aliases;
    
    const result = this._select(subqueryAst);
    this._outerRow = savedOuterRow;
    this._innerTableAliases = savedInnerAliases;
    return result.rows;
  }

  _computeSingleAggregate(func, arg, rows, distinct) {
    if (func === 'COUNT' && (arg === '*' || (arg && arg.type === 'literal' && arg.value === '*'))) return rows.length;
    let vals = rows.map(r => this._evalValue(arg, r)).filter(v => v != null);
    if (distinct) vals = [...new Set(vals)];
    switch (func) {
      case 'COUNT': return arg.type === 'literal' && arg.value === '*' ? rows.length : vals.length;
      case 'SUM': return vals.reduce((a, b) => Number(a) + Number(b), 0);
      case 'AVG': return vals.length > 0 ? vals.reduce((a, b) => Number(a) + Number(b), 0) / vals.length : null;
      case 'MAX': return vals.length > 0 ? vals.reduce((a, b) => a > b ? a : b) : null;
      case 'MIN': return vals.length > 0 ? vals.reduce((a, b) => a < b ? a : b) : null;
      default: return null;
    }
  }

  _computeAggregates(columns, rows) {
    const result = {};
    for (const col of columns) {
      // Handle expression columns that contain aggregates (e.g., SUM(a) / SUM(b))
      if (col.type === 'expression' && this._exprContainsAggregate(col.expr)) {
        const name = col.alias || 'expr';
        result[name] = this._evalAggregateExpr(col.expr, rows);
        continue;
      }
      if (col.type !== 'aggregate') continue;
      const argStr = typeof col.arg === 'object' ? 'expr' : col.arg;
      const name = col.alias || `${col.func}(${argStr})`;
      let values;
      if (col.arg === '*') {
        values = rows;
      } else if (typeof col.arg === 'object') {
        values = rows.map(r => this._evalValue(col.arg, r)).filter(v => v != null);
      } else {
        values = rows.map(r => this._resolveColumn(col.arg, r)).filter(v => v != null);
      }

      switch (col.func) {
        case 'COUNT': {
          if (col.distinct && col.arg !== '*') {
            result[name] = new Set(values).size;
          } else {
            result[name] = col.arg === '*' ? rows.length : values.length;
          }
          break;
        }
        case 'SUM': result[name] = values.length ? values.reduce((s, v) => s + v, 0) : null; break;
        case 'AVG': result[name] = values.length ? values.reduce((s, v) => s + v, 0) / values.length : null; break;
        case 'MIN': result[name] = values.length ? values.reduce((a, b) => a < b ? a : b) : null; break;
        case 'MAX': result[name] = values.length ? values.reduce((a, b) => a > b ? a : b) : null; break;
        case 'GROUP_CONCAT':
        case 'STRING_AGG': {
          const sep = col.separator || ',';
          const dedupValues = col.distinct ? [...new Set(values)] : values;
          result[name] = dedupValues.length ? dedupValues.map(String).join(sep) : null;
          break;
        }
        case 'JSON_AGG':
        case 'JSONB_AGG': {
          const vals = col.distinct ? [...new Set(values)] : values;
          result[name] = JSON.stringify(vals);
          break;
        }
        case 'ARRAY_AGG': {
          result[name] = col.distinct ? [...new Set(values)] : values;
          break;
        }
        case 'BOOL_AND':
        case 'EVERY': {
          // Returns TRUE if all values are true/truthy, NULL if all are null
          const boolVals = values.filter(v => v != null);
          result[name] = boolVals.length === 0 ? null : boolVals.every(v => !!v);
          break;
        }
        case 'BOOL_OR': {
          // Returns TRUE if any value is true/truthy, NULL if all are null
          const boolVals2 = values.filter(v => v != null);
          result[name] = boolVals2.length === 0 ? null : boolVals2.some(v => !!v);
          break;
        }
      }
    }
    return result;
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

  _escapeCsvField(val, delimiter) {
    if (val.includes(delimiter) || val.includes('"') || val.includes('\n')) {
      return '"' + val.replace(/"/g, '""') + '"';
    }
    return val;
  }
}
