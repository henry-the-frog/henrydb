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

export class Database {
  constructor(options = {}) {
    this.tables = new Map();  // name -> { heap, schema, indexes }
    this._prepared = new Map(); // name -> { ast, sql }
    this.catalog = [];
    this.indexCatalog = new Map();  // indexName -> { table, columns, unique }
    this.views = new Map();  // viewName -> { query (AST) }
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
  }

  execute(sql) {
    // Check plan cache first (only for SELECT)
    let ast = this._planCache.get(sql);
    if (!ast) {
      ast = parse(sql);
      // Only cache read-only queries (SELECT)
      if (ast.type === 'SELECT') {
        this._planCache.put(sql, ast);
      }
    }
    return this.execute_ast(ast);
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
    
    return {
      version: 1,
      tables,
      views,
      triggers: this.triggers,
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
      case 'DROP_TABLE': return this._dropTable(ast);
      case 'TRUNCATE_TABLE': {
        const table = this.tables.get(ast.table);
        if (!table) throw new Error(`Table ${ast.table} not found`);
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
      case 'UNION': return this._union(ast);
      case 'INTERSECT': return this._intersect(ast);
      case 'EXCEPT': return this._except(ast);
      case 'UPDATE': return this._update(ast);
      case 'DELETE': return this._delete(ast);
      case 'TRUNCATE': return this._truncate(ast);
      case 'SHOW_TABLES': return this._showTables();
      case 'DESCRIBE': return this._describe(ast);
      case 'EXPLAIN': return this._explain(ast);
      case 'BEGIN': this._inTransaction = true; return { type: 'OK', message: 'BEGIN' };
      case 'COMMIT': this._inTransaction = false; return { type: 'OK', message: 'COMMIT' };
      case 'ROLLBACK': this._inTransaction = false; return { type: 'OK', message: 'ROLLBACK' };
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
    const schema = ast.columns.map(c => ({
      name: c.name,
      type: c.type,
      primaryKey: c.primaryKey || false,
      notNull: c.notNull || false,
      check: c.check || null,
      defaultValue: c.defaultValue ?? null,
      references: c.references || null,
    }));
    // Choose storage engine: BTREE (clustered) or HEAP (default)
    let heap;
    const pkCol = schema.find(c => c.primaryKey);
    if (ast.engine === 'BTREE' && pkCol) {
      const pkIdx = schema.findIndex(c => c.primaryKey);
      heap = new BTreeTable(ast.table, { pkIndices: [pkIdx] });
    } else {
      heap = this._heapFactory(ast.table);
    }
    const indexes = new Map();

    // Create index for primary key
    if (pkCol) {
      indexes.set(pkCol.name, new BPlusTree(32));
    }

    this.tables.set(ast.table, { heap, schema, indexes });
    this.catalog.push({ name: ast.table, columns: schema });
    
    // Log DDL to WAL for crash recovery
    if (this._dataDir && this.wal && this.wal.logCreateTable) {
      this.wal.logCreateTable(ast.table, schema.map(c => ({ name: c.name, type: c.type })));
    }
    
    return { type: 'OK', message: `Table ${ast.table} created` };
  }

  _createTableAs(ast) {
    // Execute the query first
    const result = this._select(ast.query);
    if (!result.rows || result.rows.length === 0) {
      this.tables.set(ast.table, { schema: [], heap: this._heapFactory(ast.table), indexes: new Map() });
      return { type: 'OK', message: `Table ${ast.table} created (empty)` };
    }

    // Infer schema from first row
    const firstRow = result.rows[0];
    const schema = Object.keys(firstRow).filter(k => !k.includes('.')).map(name => ({
      name,
      type: typeof firstRow[name] === 'number' ? 'INT' : 'TEXT',
      primaryKey: false,
    }));

    const heap = this._heapFactory(ast.table);
    const indexes = new Map();
    const tableObj = { schema, heap, indexes };
    this.tables.set(ast.table, tableObj);

    // Insert all rows
    for (const row of result.rows) {
      const values = schema.map(col => row[col.name]);
      this._insertRow(tableObj, null, values);
    }

    return { type: 'OK', message: `Table ${ast.table} created with ${result.rows.length} rows` };
  }

  _alterTable(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

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
    // Remove any indexes for this table
    for (const [idxName, meta] of this.indexCatalog) {
      if (meta.table === ast.table) this.indexCatalog.delete(idxName);
    }
    this.tables.delete(ast.table);
    this.catalog = this.catalog.filter(t => t.name !== ast.table);
    return { type: 'OK', message: `Table ${ast.table} dropped` };
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

    // Populate from existing data
    const includeColIdxs = (ast.include || []).map(col => 
      table.schema.findIndex(c => c.name === col)
    ).filter(i => i >= 0);

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
    });
    this.indexCatalog.set(ast.name, {
      table: ast.table,
      columns: ast.columns,
      unique: ast.unique || false,
    });

    return { type: 'OK', message: `Index ${ast.name} created` };
  }

  _dropIndex(ast) {
    const meta = this.indexCatalog.get(ast.name);
    if (!meta) {
      if (ast.ifExists) return { type: 'OK', message: 'DROP INDEX' };
      throw new Error(`Index ${ast.name} not found`);
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
    if (this.views.has(ast.name)) throw new Error(`View ${ast.name} already exists`);
    this.views.set(ast.name, { query: ast.query });
    return { type: 'OK', message: `View ${ast.name} created` };
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
      const values = row.map(r => r.value);
      
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
              
              // Write back to heap
              if (existingRid) {
                const page = table.heap.pages[existingRid.pageId];
                if (page && page.updateTuple) {
                  page.updateTuple(existingRid.slotIdx, encodeTuple(newValues));
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
      const filteredRows = ast.returning === '*' ? returnedRows : returnedRows.map(row => {
        const filtered = {};
        for (const col of ast.returning) filtered[col] = row[col];
        return filtered;
      });
      return { type: 'ROWS', rows: filteredRows, count: inserted };
    }
    return { type: 'OK', message: `${inserted} row(s) inserted`, count: inserted };
  }

  _insertSelect(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    const result = this.execute_ast(ast.query);
    let inserted = 0;
    for (const row of result.rows) {
      // Extract values matching target table schema
      const values = [];
      if (ast.columns) {
        // Map by column names
        for (const col of ast.columns) {
          values.push(row[col] !== undefined ? row[col] : null);
        }
      } else {
        // Map by target schema column names
        for (const col of table.schema) {
          values.push(row[col.name] !== undefined ? row[col.name] : null);
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
          const av = a[column], bv = b[column];
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
          } else {
            result[alias] = this._evalValue(col, row);
          }
        }
        return result;
      });
    }
    return { type: 'ROWS', rows };
  }

  _orderValues(table, columns, values) {
    if (columns) {
      const ordered = new Array(table.schema.length).fill(null);
      for (let i = 0; i < table.schema.length; i++) {
        if (table.schema[i].defaultValue != null) ordered[i] = table.schema[i].defaultValue;
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
          orderedValues[i] = table.schema[i].defaultValue;
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
    // Handle CTEs — register as temporary views
    const tempViews = [];
    if (ast.ctes) {
      for (const cte of ast.ctes) {
        if (this.views.has(cte.name)) throw new Error(`CTE name ${cte.name} conflicts with existing view`);
        
        if (cte.recursive && (cte.unionQuery || cte.query.type === 'UNION')) {
          // Recursive CTE: iterate until fixed point
          const allRows = this._executeRecursiveCTE(cte);
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
      return this._selectInner(optimizedAst);
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

    // INNER/LEFT/RIGHT JOIN
    for (const leftRow of leftRows) {
      let matched = false;
      for (const rightRow of rightRows) {
        const combined = { ...leftRow, ...rightRow };
        if (!join.on || this._evalExpr(join.on, combined)) {
          result.push(combined);
          matched = true;
        }
      }
      if (!matched && (join.joinType === 'LEFT' || join.joinType === 'LEFT_OUTER')) {
        const nullRow = {};
        for (const key of Object.keys(rightRows[0] || {})) {
          nullRow[key] = null;
        }
        result.push({ ...leftRow, ...nullRow });
      }
    }
    return result;
  }

  _selectInner(ast) {
    // Handle SELECT without FROM (e.g., SELECT 1 AS n)
    if (!ast.from) {
      const row = {};
      for (const col of ast.columns) {
        if (col.type === 'expression') {
          const name = col.alias || 'expr';
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
      // Apply columns
      return this._applySelectColumns(ast, rows);
    }

    // Check if FROM is a subquery
    if (tableName === '__subquery') {
      const subResult = this._select(ast.from.subquery);
      let rows = subResult.rows || [];
      if (ast.where) rows = rows.filter(row => this._evalExpr(ast.where, row));
      for (const join of ast.joins || []) {
        rows = this._executeJoin(rows, join, ast.from.alias || '__subquery');
      }
      return this._applySelectColumns(ast, rows);
    }

    // Check if FROM references a view
    if (this.views.has(tableName)) {
      const viewDef = this.views.get(tableName);
      // Execute view query or use materialized rows (for recursive CTEs)
      let rows;
      if (viewDef.materializedRows) {
        rows = [...viewDef.materializedRows];
      } else {
        const viewResult = this._select(viewDef.query);
        rows = viewResult.rows;
      }

      // Apply WHERE
      if (ast.where) {
        rows = rows.filter(row => this._evalExpr(ast.where, row));
      }

      // Handle aggregates / GROUP BY on view results
      const hasAggregates = ast.columns.some(c => c.type === 'aggregate');
      if (ast.groupBy) {
        // Re-use groupby logic but with view rows
        return this._selectWithGroupBy(ast, rows);
      }
      if (hasAggregates) {
        return { type: 'ROWS', rows: [this._computeAggregates(ast.columns, rows)] };
      }

      // ORDER BY
      if (ast.orderBy) {
        rows.sort((a, b) => {
          for (const { column, direction } of ast.orderBy) {
            const av = a[column] !== undefined ? a[column] : this._resolveColumn(column, a);
            const bv = b[column] !== undefined ? b[column] : this._resolveColumn(column, b);
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
      if (ast.limit) rows = rows.slice(0, ast.limit);

      // Project if not star
      if (ast.columns[0]?.type !== 'star') {
        rows = rows.map(row => {
          const result = {};
          for (const col of ast.columns) {
            if (col.type === 'function') {
              const name = col.alias || `${col.func}(...)`;
              result[name] = this._evalFunction(col.func, col.args, row);
            } else if (col.type === 'expression') {
              const name = col.alias || 'expr';
              result[name] = this._evalValue(col.expr, row);
            } else {
              const name = col.alias || col.name;
              result[name] = row[col.name] !== undefined ? row[col.name] : row[name];
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
        // Full table scan
        for (const { pageId, slotIdx, values } of table.heap.scan()) {
          const row = this._valuesToRow(values, table.schema, ast.from.alias || ast.from.table);
          rows.push(row);
        }
        // WHERE filter
        if (ast.where) {
          rows = rows.filter(row => this._evalExpr(ast.where, row));
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

    // Handle JOINs (using the potentially modified AST with pushed filters)
    for (const join of workingAst.joins || []) {
      rows = this._executeJoin(rows, join, workingAst.from.alias || workingAst.from.table);
    }

    // WHERE filter after JOINs (only remaining predicates)
    if (hasJoins && workingAst.where) {
      rows = rows.filter(row => this._evalExpr(workingAst.where, row));
    }

    // Aggregates / GROUP BY / Window functions
    const hasAggregates = ast.columns.some(c => c.type === 'aggregate');
    const hasWindow = ast.columns.some(c => c.type === 'window');

    if (ast.groupBy) {
      return this._selectWithGroupBy(ast, rows);
    }
    if (hasAggregates && !hasWindow) {
      return { type: 'ROWS', rows: [this._computeAggregates(ast.columns, rows)] };
    }

    // Window functions: compute window values before projection
    if (hasWindow) {
      rows = this._computeWindowFunctions(ast.columns, rows);
    }

    // Build alias→expression map for ORDER BY resolution
    const aliasExprs = new Map();
    for (const col of ast.columns) {
      if (col.type === 'expression' && col.alias) {
        aliasExprs.set(col.alias, col.expr);
      } else if (col.type === 'function' && col.alias) {
        aliasExprs.set(col.alias, col);
      }
    }

    // ORDER BY
    if (ast.orderBy) {
      rows.sort((a, b) => {
        for (const { column, direction } of ast.orderBy) {
          let av, bv;
          if (aliasExprs.has(column)) {
            const expr = aliasExprs.get(column);
            if (expr.type === 'function') {
              av = this._evalFunction(expr.func, expr.args, a);
              bv = this._evalFunction(expr.func, expr.args, b);
            } else {
              av = this._evalValue(expr, a);
              bv = this._evalValue(expr, b);
            }
          } else {
            av = this._resolveColumn(column, a);
            bv = this._resolveColumn(column, b);
          }
          const cmp = av == null && bv == null ? 0 : av == null ? -1 : bv == null ? 1 : av < bv ? -1 : av > bv ? 1 : 0;
          if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
        }
        return 0;
      });
    }

    // OFFSET (before LIMIT, but LIMIT deferred until after DISTINCT)
    if (ast.offset && !ast.distinct) rows = rows.slice(ast.offset);

    // LIMIT (only apply before projection if no DISTINCT)
    if (ast.limit && !ast.distinct) rows = rows.slice(0, ast.limit);

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
      for (const col of ast.columns) {
        if (col.type === 'function') {
          const name = col.alias || `${col.func}(...)`;
          result[name] = this._evalFunction(col.func, col.args, row);
        } else if (col.type === 'expression') {
          const name = col.alias || 'expr';
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
        }
      }
      return result;
    });

    // DISTINCT
    let finalRows = projected;
    if (ast.distinct) {
      const seen = new Set();
      finalRows = projected.filter(row => {
        const key = JSON.stringify(row);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
      // Apply OFFSET and LIMIT after DISTINCT
      if (ast.offset) finalRows = finalRows.slice(ast.offset);
      if (ast.limit) finalRows = finalRows.slice(0, ast.limit);
    }

    return { type: 'ROWS', rows: finalRows };
  }

  _executeJoin(leftRows, join, leftAlias) {
    const rightTable = this.tables.get(join.table);
    const rightView = this.views.get(join.table);

    if (!rightTable && !rightView) throw new Error(`Table ${join.table} not found`);

    const rightAlias = join.alias || join.table;

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

    // RIGHT JOIN: swap logic
    if (join.joinType === 'RIGHT') {
      const rightMatchedSet = new Set();
      const rightRows = [];
      for (const { values } of rightTable.heap.scan()) {
        const row = this._valuesToRow(values, rightTable.schema, rightAlias);
        if (join.filter && !this._evalExpr(join.filter, row)) continue;
        rightRows.push(row);
      }

      for (const leftRow of leftRows) {
        for (let i = 0; i < rightRows.length; i++) {
          const combined = { ...leftRow, ...rightRows[i] };
          if (this._evalExpr(join.on, combined)) {
            result.push(combined);
            rightMatchedSet.add(i);
          }
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

    // Fallback: nested loop join
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

      const keyStr = String(leftVal);
      const matches = hashMap.get(keyStr);
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

  _update(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    let updated = 0;
    const toUpdate = [];

    for (const { pageId, slotIdx, values } of table.heap.scan()) {
      const row = this._valuesToRow(values, table.schema, ast.table);
      if (!ast.where || this._evalExpr(ast.where, row)) {
        toUpdate.push({ pageId, slotIdx, values: [...values] });
      }
    }

    const returnedRows = [];
    
    // Batch WAL: use a single transaction for all updates
    const batchTxId = this._currentTxId || this._nextTxId++;
    const isAutoCommit = !this._currentTxId;

    for (const item of toUpdate) {
      const newValues = [...item.values];
      const row = this._valuesToRow(item.values, table.schema, ast.table);
      for (const { column, value } of ast.assignments) {
        const colIdx = table.schema.findIndex(c => c.name === column);
        if (colIdx === -1) throw new Error(`Column ${column} not found`);
        newValues[colIdx] = this._evalValue(value, row);
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
      const filteredRows = ast.returning === '*' ? returnedRows : returnedRows.map(row => {
        const filtered = {};
        for (const col of ast.returning) filtered[col] = row[col];
        return filtered;
      });
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

  _delete(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    let deleted = 0;
    const toDelete = [];

    for (const { pageId, slotIdx, values } of table.heap.scan()) {
      const row = this._valuesToRow(values, table.schema, ast.table);
      if (!ast.where || this._evalExpr(ast.where, row)) {
        toDelete.push({ pageId, slotIdx });
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
      const filteredRows = ast.returning === '*' ? deletedRows : deletedRows.map(row => {
        const filtered = {};
        for (const col of ast.returning) filtered[col] = row[col];
        return filtered;
      });
      return { type: 'ROWS', rows: filteredRows, count: deleted };
    }

    return { type: 'OK', message: `${deleted} row(s) deleted`, count: deleted };
  }

  _truncate(ast) {
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    // Clear heap file
    const count = table.heap.rowCount || 0;
    table.heap = this._heapFactory();

    // Rebuild all indexes (empty)
    for (const [colName, oldIndex] of table.indexes) {
      table.indexes.set(colName, new BPlusTree(32, { unique: oldIndex.unique }));
    }

    return { type: 'OK', message: `${ast.table} truncated`, count };
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
    
    // Remap right columns to left column names for consistent comparison
    const leftCols = leftResult.rows.length > 0 ? Object.keys(leftResult.rows[0]) : [];
    const rightRemapped = this._remapUnionColumns(rightResult.rows, leftCols);
    
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
    
    // Remap right columns to left column names for consistent comparison
    const leftCols = leftResult.rows.length > 0 ? Object.keys(leftResult.rows[0]) : [];
    const rightRemapped = this._remapUnionColumns(rightResult.rows, leftCols);
    
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
    if (stmt.type === 'SELECT' && (format === 'tree' || format === 'json-tree' || format === 'html')) {
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
      if (!hasJoins && stmt.where) {
        const indexScan = this._tryIndexScan(table, stmt.where, stmt.from.alias || tableName);
        if (indexScan !== null) {
          if (indexScan.btreeLookup) {
            plan.push({ operation: 'BTREE_PK_LOOKUP', table: tableName, engine, estimated_rows: indexScan.rows.length });
          } else {
            const colName = this._findIndexedColumn(stmt.where);
            plan.push({ operation: 'INDEX_SCAN', table: tableName, index: colName, engine, estimated_rows: indexScan.rows.length });
          }
        } else {
          plan.push({ operation: 'TABLE_SCAN', table: tableName, engine, estimated_rows: estRows });
          plan.push({ operation: 'FILTER', condition: 'WHERE' });
        }
      } else {
        plan.push({ operation: 'TABLE_SCAN', table: tableName, engine, estimated_rows: estRows });
      }

      // Joins
      for (const join of stmt.joins || []) {
        const joinTable = join.table?.table || join.table;
        const equiJoinKey = join.on ? this._extractEquiJoinKey(join.on, stmt.from.alias || tableName, join.alias || joinTable) : null;
        plan.push({
          operation: equiJoinKey ? 'HASH_JOIN' : 'NESTED_LOOP_JOIN',
          type: join.type || 'INNER',
          table: joinTable,
          on: equiJoinKey ? `${equiJoinKey.leftKey} = ${equiJoinKey.rightKey}` : 'complex condition',
        });
      }
    }

    // WHERE (if not already noted)
    if (stmt.where && !plan.some(p => p.operation === 'FILTER')) {
      plan.push({ operation: 'FILTER', condition: 'WHERE' });
    }

    // GROUP BY
    if (stmt.groupBy) {
      plan.push({ operation: 'HASH_GROUP_BY', columns: stmt.groupBy });
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
        // Format like PostgreSQL's EXPLAIN output
        const lines = [];
        let indent = 0;
        for (const step of plan) {
          const prefix = '  '.repeat(indent) + (indent > 0 ? '->  ' : '');
          switch (step.operation) {
            case 'TABLE_SCAN':
              lines.push(`${prefix}Seq Scan on ${step.table}  (engine=${step.engine || 'heap'}, rows=${step.estimated_rows})`);
              indent++;
              break;
            case 'INDEX_SCAN':
              lines.push(`${prefix}Index Scan using ${step.index} on ${step.table}  (engine=${step.engine || 'heap'}, rows=${step.estimated_rows})`);
              indent++;
              break;
            case 'BTREE_PK_LOOKUP':
              lines.push(`${prefix}BTree PK Lookup on ${step.table}  (engine=btree, rows=${step.estimated_rows})`);
              indent++;
              break;
            case 'HASH_JOIN':
              lines.push(`${prefix}Hash ${step.type} Join  (on: ${step.on})`);
              indent++;
              break;
            case 'NESTED_LOOP_JOIN':
              lines.push(`${prefix}Nested Loop ${step.type} Join  (${step.on})`);
              indent++;
              break;
            case 'FILTER':
              lines.push(`${prefix}Filter: ${step.condition}`);
              break;
            case 'HASH_GROUP_BY':
              lines.push(`${prefix}HashAggregate  (keys: ${step.columns.join(', ')})`);
              break;
            case 'AGGREGATE':
              lines.push(`${prefix}Aggregate`);
              break;
            case 'SORT':
              lines.push(`${prefix}Sort  (keys: ${step.columns.join(', ')})`);
              break;
            case 'SORT_ELIMINATED':
              lines.push(`${prefix}Sort Eliminated  (keys: ${step.columns.join(', ')}, reason: ${step.reason})`);
              break;
            case 'LIMIT':
              lines.push(`${prefix}Limit  (count=${step.count})`);
              break;
            case 'DISTINCT':
              lines.push(`${prefix}Unique`);
              break;
            case 'WINDOW_FUNCTION':
              lines.push(`${prefix}WindowAgg`);
              break;
            case 'CTE':
              lines.push(`${prefix}CTE Scan on ${step.name}${step.recursive ? ' (recursive)' : ''}`);
              indent++;
              break;
            case 'VIEW_SCAN':
              lines.push(`${prefix}View Scan on ${step.view}`);
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
      const aliasedRows = baseResult.rows.map(row => {
        const aliased = {};
        const rowKeys = Object.keys(row);
        for (let i = 0; i < cte.columns.length && i < rowKeys.length; i++) {
          aliased[cte.columns[i]] = row[rowKeys[i]];
        }
        return aliased;
      });
      baseResult.rows = aliasedRows;
      columnNames = cte.columns.slice(0, columnNames.length);
    }
    let allRows = [...baseResult.rows];
    let workingSet = [...baseResult.rows];

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

      // Cycle detection: check if any new row already exists in allRows
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

    // Execute the actual query with timing
    const startTime = performance.now();
    const result = this._select(stmt);
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
      
      analysis.push({
        operation: plannerEstimate?.scanType || 'TABLE_SCAN',
        table: tableName,
        engine,
        estimated_rows: plannerEstimate?.estimatedRows || '?',
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
      analysis.push({ operation: 'GROUP_BY', groups: actualRows });
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
      ],
      analysis,
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

  _computeWindowFunctions(columns, rows) {
    const windowCols = columns.filter(c => c.type === 'window');

    for (const col of windowCols) {
      const name = col.alias || `${col.func}(${col.arg || ''})`;
      const { partitionBy, orderBy } = col.over;

      // Partition rows
      const partitions = new Map();
      for (const row of rows) {
        const key = partitionBy
          ? partitionBy.map(c => this._resolveColumn(c, row)).join('\0')
          : '__all__';
        if (!partitions.has(key)) partitions.set(key, []);
        partitions.get(key).push(row);
      }

      // Sort each partition
      for (const [, partition] of partitions) {
        if (orderBy) {
          partition.sort((a, b) => {
            for (const { column, direction } of orderBy) {
              const av = this._resolveColumn(column, a);
              const bv = this._resolveColumn(column, b);
              const cmp = av < bv ? -1 : av > bv ? 1 : 0;
              if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
            }
            return 0;
          });
        }

        // Compute window function values
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
            // With ORDER BY: running count, without: total count
            if (orderBy) {
              for (let i = 0; i < partition.length; i++) {
                partition[i][`__window_${name}`] = i + 1;
              }
            } else {
              for (let i = 0; i < partition.length; i++) {
                partition[i][`__window_${name}`] = partition.length;
              }
            }
            break;
          }
          case 'SUM': {
            if (orderBy) {
              // Running sum (default frame: UNBOUNDED PRECEDING to CURRENT ROW)
              let runningSum = 0;
              for (const r of partition) {
                runningSum += (this._resolveColumn(col.arg, r) || 0);
                r[`__window_${name}`] = runningSum;
              }
            } else {
              const total = partition.reduce((s, r) => s + (this._resolveColumn(col.arg, r) || 0), 0);
              for (const r of partition) r[`__window_${name}`] = total;
            }
            break;
          }
          case 'AVG': {
            if (orderBy) {
              // Running average
              let sum = 0;
              for (let i = 0; i < partition.length; i++) {
                sum += (this._resolveColumn(col.arg, partition[i]) || 0);
                partition[i][`__window_${name}`] = sum / (i + 1);
              }
            } else {
              const total = partition.reduce((s, r) => s + (this._resolveColumn(col.arg, r) || 0), 0);
              const avg = total / partition.length;
              for (const r of partition) r[`__window_${name}`] = avg;
            }
            break;
          }
          case 'MIN': {
            if (orderBy) {
              let min = Infinity;
              for (const r of partition) {
                const v = this._resolveColumn(col.arg, r);
                if (v != null && v < min) min = v;
                r[`__window_${name}`] = min === Infinity ? null : min;
              }
            } else {
              const values = partition.map(r => this._resolveColumn(col.arg, r)).filter(v => v != null);
              const min = values.length ? values.reduce((a, b) => a < b ? a : b) : null;
              for (const r of partition) r[`__window_${name}`] = min;
            }
            break;
          }
          case 'MAX': {
            if (orderBy) {
              let max = -Infinity;
              for (const r of partition) {
                const v = this._resolveColumn(col.arg, r);
                if (v != null && v > max) max = v;
                r[`__window_${name}`] = max === -Infinity ? null : max;
              }
            } else {
              const values = partition.map(r => this._resolveColumn(col.arg, r)).filter(v => v != null);
              const max = values.length ? values.reduce((a, b) => a > b ? a : b) : null;
              for (const r of partition) r[`__window_${name}`] = max;
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
            const bucketSize = Math.ceil(partition.length / nArg);
            for (let i = 0; i < partition.length; i++) {
              partition[i][`__window_${name}`] = Math.floor(i / bucketSize) + 1;
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
        }
      }
    }

    return rows;
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

  _selectWithGroupBy(ast, rows) {
    // Helper: resolve GROUP BY column (string or expression)
    const resolveGroupKey = (col, row) => {
      if (typeof col === 'string') return this._resolveColumn(col, row);
      return this._evalValue(col, row); // Expression
    };

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
          const val = this._resolveColumn(col, groupRows[0]);
          result[col] = val;
          if (col.includes('.')) result[col.split('.').pop()] = val;
        } else {
          // Expression group key — evaluate and use alias or stringify
          const val = this._evalValue(col, groupRows[0]);
          const key = col.alias || JSON.stringify(col).slice(0, 20);
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
          case 'GROUP_CONCAT': {
            const sep = extra.separator || ',';
            const strs = (distinct ? [...new Set(values)] : values).map(String);
            return strs.join(sep);
          }
        }
      };

      // Add aggregate and non-aggregate columns
      for (const col of ast.columns) {
        if (col.type === 'aggregate') {
          const name = col.alias || `${col.func}(${col.arg})`;
          result[name] = computeAgg(col.func, col.arg, col.distinct, { separator: col.separator });
          // Also store under canonical key for HAVING resolution
          const canonKey = `${col.func}(${col.arg})`;
          if (name !== canonKey) result[`__agg_${canonKey}`] = result[name];
        } else if (col.type === 'column') {
          const baseName = col.name.includes('.') ? col.name.split('.').pop() : col.name;
          const name = col.alias || baseName;
          result[name] = this._resolveColumn(col.name, groupRows[0]);
        }
      }

      // Pre-compute aggregates used in HAVING that aren't in SELECT
      if (ast.having) {
        this._collectAggregateExprs(ast.having).forEach(agg => {
          const argStr = typeof agg.arg === 'string' ? agg.arg : (agg.arg?.name || '*');
          const key = `${agg.func}(${argStr})`;
          if (!(key in result) && !(`__agg_${key}` in result)) {
            result[`__agg_${key}`] = computeAgg(agg.func, argStr, agg.distinct);
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
          const av = a[column] !== undefined ? a[column] : this._resolveColumn(column, a);
          const bv = b[column] !== undefined ? b[column] : this._resolveColumn(column, b);
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
    if (ast.limit) resultRows = resultRows.slice(0, ast.limit);

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
  _collectAggregateExprs(expr) {
    if (!expr) return [];
    if (expr.type === 'aggregate_expr') return [expr];
    const results = [];
    for (const key of ['left', 'right', 'expr']) {
      if (expr[key]) results.push(...this._collectAggregateExprs(expr[key]));
    }
    return results;
  }

  _resolveColumn(name, row) {
    if (name in row) return row[name];
    // Try without table prefix
    for (const key of Object.keys(row)) {
      if (key.endsWith(`.${name}`)) return row[key];
    }
    // For correlated subqueries: check outer row
    if (this._outerRow) {
      if (name in this._outerRow) return this._outerRow[name];
      for (const key of Object.keys(this._outerRow)) {
        if (key.endsWith(`.${name}`)) return this._outerRow[key];
      }
    }
    return undefined;
  }

  _evalExpr(expr, row) {
    if (!expr) return true;
    switch (expr.type) {
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
        const regex = '^' + String(pattern)
          .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
          .replace(/%/g, '.*')
          .replace(/_/g, '.')
          + '$';
        return new RegExp(regex).test(String(val));
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
      case 'BETWEEN': {
        const val = this._evalValue(expr.left, row);
        const low = this._evalValue(expr.low, row);
        const high = this._evalValue(expr.high, row);
        // SQL NULL semantics: BETWEEN with NULL always returns false
        if (val === null || val === undefined || low === null || low === undefined || high === null || high === undefined) return false;
        return val >= low && val <= high;
      }
      case 'COMPARE': {
        const left = this._evalValue(expr.left, row);
        const right = this._evalValue(expr.right, row);
        // SQL NULL semantics: any comparison with NULL returns false
        if (left === null || left === undefined || right === null || right === undefined) return false;
        switch (expr.op) {
          case 'EQ': return left === right;
          case 'NE': return left !== right;
          case 'LT': return left < right;
          case 'GT': return left > right;
          case 'LE': return left <= right;
          case 'GE': return left >= right;
        }
      }
      default: return true;
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
      const argStr = typeof node.arg === 'string' ? node.arg : (node.arg?.name || '*');
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
      case 'IFNULL': {
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
      case 'RANDOM': return Math.random();
      case 'GREATEST': { const vals = args.map(a => this._evalValue(a, row)).filter(v => v != null); return vals.length ? Math.max(...vals.map(Number)) : null; }
      case 'LEAST': { const vals = args.map(a => this._evalValue(a, row)).filter(v => v != null); return vals.length ? Math.min(...vals.map(Number)) : null; }
      case 'MOD': { const a = Number(this._evalValue(args[0], row)); const b = Number(this._evalValue(args[1], row)); return b === 0 ? null : a % b; }
      case 'LTRIM': { const v = this._evalValue(args[0], row); return v == null ? null : String(v).trimStart(); }
      case 'RTRIM': { const v = this._evalValue(args[0], row); return v == null ? null : String(v).trimEnd(); }
      
      // Date/time functions
      case 'CURRENT_TIMESTAMP': case 'NOW': return new Date().toISOString();
      case 'CURRENT_DATE': return new Date().toISOString().split('T')[0];
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
      
      default: throw new Error(`Unknown function: ${func}`);
    }
  }

  _evalSubquery(subqueryAst, outerRow) {
    // Execute the subquery, passing outerRow for correlated references
    const savedOuterRow = this._outerRow;
    this._outerRow = outerRow;
    const result = this._select(subqueryAst);
    this._outerRow = savedOuterRow;
    return result.rows;
  }

  _computeAggregates(columns, rows) {
    const result = {};
    for (const col of columns) {
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
        case 'GROUP_CONCAT': {
          const sep = col.separator || ',';
          result[name] = values.map(String).join(sep);
          break;
        }
      }
    }
    return result;
  }
}
