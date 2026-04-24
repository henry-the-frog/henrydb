// db.js — HenryDB query executor
// Ties together: HeapFile, BPlusTree, SQL parser

import { HeapFile, encodeTuple, decodeTuple } from './page.js';
import { BPlusTree } from './btree.js';
import { optimizeSelect } from './decorrelate.js';
import { QueryPlanner } from './planner.js';
import { makeCompositeKey } from './composite-key.js';
import { parse } from './sql.js';
import { WriteAheadLog } from './wal.js';
import { buildPlan as buildVolcanoPlan } from './volcano-planner.js';
import { CompiledQueryEngine } from './compiled-query.js';
import { InvertedIndex, tokenize } from './fulltext.js';
import { PlanCache } from './plan-cache.js';
import { MVCCManager } from './mvcc.js';
import { installPgCatalog } from './pg-catalog.js';
import { installSetOperations } from './set-operations.js';
import { installExpressionEvaluator } from './expression-evaluator.js';

export class Database {
  constructor(options = {}) {
    this.tables = new Map();  // name -> { heap, schema, indexes }
    this.catalog = [];
    this.indexCatalog = new Map();  // indexName -> { table, columns, unique }
    this.views = new Map();  // viewName -> { query (AST) }
    this.triggers = [];      // { name, timing, event, table, bodySql }
    
    // Storage factory: can be overridden for file-backed storage
    this._heapFactory = options.heapFactory || ((name) => new HeapFile(name));
    this.wal = new WriteAheadLog();
    this.fulltextIndexes = new Map(); // indexName → InvertedIndex
    this._nextTxId = 1;
    this._currentTxId = 0;  // 0 = auto-commit mode
    this._planCache = new PlanCache(256);
    
    // Vectorized execution engine (opt-in via { vectorized: true })
    this._useVectorized = !!options.vectorized;
    
    // MVCC support (opt-in)
    this._mvccEnabled = !!options.mvcc;
    this._mvcc = this._mvccEnabled ? new MVCCManager() : null;
    this._currentTx = null;  // Current MVCC transaction
    
    // pg_stat_statements tracking
    this._queryStats = new Map(); // normalized_query -> { query, calls, total_exec_time, min_exec_time, max_exec_time, rows }
    
    // User-defined functions catalog
    this._functions = new Map(); // name -> { params, returnType, body, language }
    
    // Prepared statements catalog
    this._preparedStatements = new Map();
    
    // Cursor catalog
    this._cursors = new Map();
    
    // Comments catalog
    this._comments = new Map(); // "TABLE.tablename" or "COLUMN.table.col" → comment
  }

  /** Case-insensitive table lookup */
  _getTable(name) {
    return this.tables.get(name) || this.tables.get(name.toLowerCase()) || this.tables.get(name.toUpperCase());
  }

  /** Normalize a SQL query for pg_stat_statements grouping: replace string and numeric literals with $? */
  _normalizeQuery(sql) {
    // Replace string literals (including escaped quotes)
    let normalized = sql.replace(/'(?:[^']|'')*'/g, '$?');
    // Replace numeric literals (integers and floats, but not identifiers)
    normalized = normalized.replace(/\b\d+(?:\.\d+)?\b/g, '$?');
    return normalized;
  }

  execute(sql) {
    const startTime = performance.now();
    
    // Check plan cache first (only for SELECT)
    let ast = this._planCache.get(sql);
    if (!ast) {
      ast = parse(sql);
      // Only cache read-only queries (SELECT)
      if (ast.type === 'SELECT') {
        this._planCache.put(sql, ast);
      }
    }
    const result = this.execute_ast(ast);
    
    // Track query stats
    const elapsed = performance.now() - startTime;
    const normalized = this._normalizeQuery(sql);
    const rowCount = result && result.rows ? result.rows.length : 
                     result && result.changes !== undefined ? result.changes : 0;
    
    let stats = this._queryStats.get(normalized);
    if (!stats) {
      stats = { query: normalized, calls: 0, total_exec_time: 0, min_exec_time: Infinity, max_exec_time: 0, rows: 0 };
      this._queryStats.set(normalized, stats);
    }
    stats.calls++;
    stats.total_exec_time += elapsed;
    stats.min_exec_time = Math.min(stats.min_exec_time, elapsed);
    stats.max_exec_time = Math.max(stats.max_exec_time, elapsed);
    stats.rows += rowCount;
    
    return result;
  }

  checkpoint() {
    return this.wal.checkpoint();
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
        foreignKeys: table.foreignKeys || [],
        tableChecks: table.tableChecks || null,
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
      const tableObj = { schema, heap, indexes, foreignKeys: tableData.foreignKeys || [], childFKs: [], tableChecks: tableData.tableChecks || null };
      db.tables.set(name, tableObj);
      
      // Insert rows
      for (const values of tableData.rows) {
        heap.insert(values);
      }
      
      // Rebuild indexes
      const indexMetaMap = tableData.indexMeta || {};
      for (const colName of tableData.indexes || []) {
        const meta = indexMetaMap[colName];
        const isUnique = meta?.unique || false;
        const index = new BPlusTree(32, { unique: isUnique });
        
        if (meta?.expressions && meta.expressions.some(e => e !== null)) {
          // Expression index: evaluate expressions for keys
          for (const { pageId, slotIdx, values } of heap.scan()) {
            const row = {};
            for (let j = 0; j < schema.length; j++) row[schema[j].name] = values[j];
            let key;
            if (meta.expressions.length === 1) {
              const expr = meta.expressions[0];
              key = db._evalValue(expr, row);
            } else {
              key = makeCompositeKey(meta.expressions.map((expr, i) => {
                if (expr) return db._evalValue(expr, row);
                return values[schema.findIndex(s => s.name === meta.columns[i])];
              }));
            }
            if (key !== null && key !== undefined) {
              index.insert(key, { pageId, slotIdx });
            }
          }
        } else {
          // Regular column index
          const colIdx = schema.findIndex(c => c.name === colName);
          if (colIdx >= 0) {
            for (const { pageId, slotIdx, values } of heap.scan()) {
              const key = values[colIdx];
              if (key !== null && key !== undefined) {
                index.insert(key, { pageId, slotIdx });
              }
            }
          }
        }
        indexes.set(colName, index);
      }
      
      // Restore index metadata
      if (tableData.indexMeta) {
        if (!tableObj.indexMeta) tableObj.indexMeta = new Map();
        for (const [key, meta] of Object.entries(tableData.indexMeta)) {
          tableObj.indexMeta.set(key, meta);
        }
      }
    }
    
    // Rebuild childFKs reverse references
    for (const [name, table] of db.tables) {
      for (const fk of table.foreignKeys || []) {
        const parentTable = db.tables.get(fk.refTable);
        if (parentTable) {
          if (!parentTable.childFKs) parentTable.childFKs = [];
          parentTable.childFKs.push({
            childTable: name,
            childColumns: fk.columns,
            parentColumns: fk.refColumns,
            onDelete: fk.onDelete,
            onUpdate: fk.onUpdate,
          });
        }
      }
      // Also rebuild from column-level REFERENCES
      for (const col of table.schema) {
        if (col.references) {
          const parentTable = db.tables.get(col.references.table);
          if (parentTable) {
            if (!parentTable.childFKs) parentTable.childFKs = [];
            parentTable.childFKs.push({
              childTable: name,
              childColumns: [col.name],
              parentColumns: [col.references.column],
              onDelete: col.references.onDelete || 'RESTRICT',
              onUpdate: col.references.onUpdate || 'RESTRICT',
            });
          }
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
      case 'CREATE_TABLE': this._planCache.clear(); return this._createTable(ast);
      case 'CREATE_TABLE_AS': this._planCache.clear(); return this._createTableAs(ast);
      case 'ALTER_TABLE': this._planCache.clear(); return this._alterTable(ast);
      case 'DROP_TABLE': return this._dropTable(ast);
      case 'TRUNCATE_TABLE': {
        const table = this._getTable(ast.table);
        if (!table) throw new Error(`Table ${ast.table} not found`);
        table.heap = this._heapFactory(ast.table);
        // Rebuild indexes (empty)
        for (const [colName] of table.indexes) {
          table.indexes.set(colName, new BPlusTree(32));
        }
        this._planCache.clear();
        return { type: 'OK', message: `Table ${ast.table} truncated` };
      }
      case 'RENAME_TABLE': {
        const table = this.tables.get(ast.from);
        if (!table) throw new Error(`Table ${ast.from} not found`);
        if (this.tables.has(ast.to)) throw new Error(`Table ${ast.to} already exists`);
        this.tables.set(ast.to, table);
        this.tables.delete(ast.from);
        if (table.heap) table.heap.name = ast.to;
        this._planCache.clear();
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
        const table = this._getTable(ast.table);
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
        const table = this._getTable(ast.table);
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
        const table = this._getTable(ast.table);
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
      case 'DROP_FUNCTION': {
        const name = ast.name.toLowerCase();
        if (!this._functions.has(name)) {
          if (ast.ifExists) return { type: 'OK', message: `NOTICE: function "${name}" does not exist, skipping` };
          throw new Error(`Function "${name}" does not exist`);
        }
        this._functions.delete(name);
        return { type: 'OK', message: `DROP FUNCTION ${name}` };
      }
      case 'INSERT': return this._insert(ast);
      case 'INSERT_SELECT': return this._insertSelect(ast);
      case 'SELECT': return this._select(ast);
      case 'UNION': return this._union(ast);
      case 'INTERSECT': return this._intersect(ast);
      case 'EXCEPT': return this._except(ast);
      case 'UPDATE': return this._update(ast);
      case 'DELETE': return this._delete(ast);
      case 'MERGE': return this._merge(ast);
      case 'VALUES_QUERY': return this._valuesQuery(ast);
      case 'TRUNCATE': return this._truncate(ast);
      case 'SHOW_TABLES': return this._showTables();
      case 'DESCRIBE': return this._describe(ast);
      case 'EXPLAIN': return this._explain(ast);
      case 'BEGIN': 
        this._inTransaction = true;
        this._txWriteLog = []; // Track writes for rollback
        // Take a snapshot of all table data for rollback
        this._txSnapshot = new Map();
        for (const [name, table] of this.tables) {
          const rows = [...table.heap.scan()].map(r => ({ ...r, values: [...r.values] }));
          const indexKeys = new Map();
          for (const [colName, index] of table.indexes) {
            // Save index state by storing all keys
            indexKeys.set(colName, { unique: index.unique });
          }
          this._txSnapshot.set(name, { rows, indexKeys });
        }
        if (this._mvccEnabled) {
          this._currentTx = this._mvcc.begin(ast.options || {});
          this._currentTxId = this._currentTx.txId;
        }
        return { type: 'OK', message: 'BEGIN' };
      case 'COMMIT': 
        this._inTransaction = false;
        if (this._currentTx) {
          this._currentTx.commit();
          this._currentTx = null;
          this._currentTxId = 0;
        }
        return { type: 'OK', message: 'COMMIT' };
      case 'ROLLBACK': 
        this._inTransaction = false;
        // Restore table state from snapshot (taken at BEGIN)
        if (this._txSnapshot) {
          for (const [tableName, snapshot] of this._txSnapshot) {
            const table = this._getTable(tableName);
            if (!table) continue;
            
            // Clear current heap by deleting all rows
            const currentRows = [...table.heap.scan()];
            for (const { pageId, slotIdx } of currentRows) {
              try { table.heap.delete(pageId, slotIdx); } catch {}
            }
            
            // Replace indexes with fresh instances
            for (const [colName, oldIndex] of table.indexes) {
              table.indexes.set(colName, new BPlusTree(32, { unique: oldIndex.unique }));
            }
            
            // Re-insert snapshot rows and rebuild indexes
            for (const row of snapshot.rows) {
              const rid = table.heap.insert(row.values);
              for (const [colName, index] of table.indexes) {
                const colIdx = table.schema.findIndex(c => c.name === colName);
                if (colIdx >= 0) {
                  const key = row.values[colIdx];
                  if (key !== null && key !== undefined) {
                    try { index.insert(key, rid); } catch {}
                  }
                }
              }
            }
          }
          this._txSnapshot = null;
          this._txWriteLog = null;
        }
        if (this._currentTx) {
          this._currentTx.rollback();
          this._currentTx = null;
          this._currentTxId = 0;
        }
        this._savepoints = null;
        return { type: 'OK', message: 'ROLLBACK' };
      case 'SAVEPOINT': return this._savepoint(ast);
      case 'RELEASE_SAVEPOINT': return this._releaseSavepoint(ast);
      case 'ROLLBACK_TO': return this._rollbackTo(ast);
      case 'VACUUM': return this._vacuum(ast);
      case 'PREPARE': return this._prepare(ast);
      case 'EXECUTE_PREPARED': return this._executePrepared(ast);
      case 'DEALLOCATE': return this._deallocate(ast);
      case 'COPY': return this._copy(ast);
      case 'LISTEN': return this._listen(ast);
      case 'NOTIFY': return this._notify(ast);
      case 'UNLISTEN': return this._unlisten(ast);
      case 'DECLARE_CURSOR': return this._declareCursor(ast);
      case 'FETCH': return this._fetch(ast);
      case 'CLOSE_CURSOR': return this._closeCursor(ast);
      case 'COPY': return this._executeCopy(ast);
      case 'COMMENT': return this._executeComment(ast);
      case 'CREATE_SEQUENCE': return this._createSequence(ast);
      case 'CREATE_FUNCTION': return this._createFunction(ast);
      case 'PREPARE': return this._prepare(ast);
      case 'EXECUTE': return this._executePrepared(ast);
      case 'DEALLOCATE': return this._deallocate(ast);
      case 'DROP_SEQUENCE': return this._dropSequence(ast);
      case 'TRUNCATE': return this._truncate(ast);
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
      generated: c.generated || null,
    }));
    const heap = this._heapFactory(ast.table);
    const indexes = new Map();

    // Create index for primary key
    const pkCol = schema.find(c => c.primaryKey);
    if (pkCol) {
      indexes.set(pkCol.name, new BPlusTree(32));
    }

    // Collect foreign key constraints
    const foreignKeys = [];
    // Column-level REFERENCES
    for (const col of schema) {
      if (col.references) {
        foreignKeys.push({
          columns: [col.name],
          refTable: col.references.table,
          refColumns: [col.references.column],
          onDelete: col.references.onDelete || 'RESTRICT',
          onUpdate: col.references.onUpdate || 'RESTRICT',
        });
      }
    }
    // Table-level FOREIGN KEY constraints
    if (ast.constraints) {
      for (const c of ast.constraints) {
        if (c.type === 'FOREIGN_KEY') {
          foreignKeys.push({
            columns: c.columns,
            refTable: c.references.table,
            refColumns: c.references.columns,
            onDelete: c.references.onDelete || 'RESTRICT',
            onUpdate: c.references.onUpdate || 'RESTRICT',
          });
        }
      }
    }

    // Table-level CHECK constraints
    const tableChecks = [];
    if (ast.constraints) {
      for (const c of ast.constraints) {
        if (c.type === 'CHECK') {
          tableChecks.push(c.expression);
        }
        if (c.type === 'UNIQUE') {
          // Create a unique index for each UNIQUE constraint
          for (const col of c.columns) {
            const index = new BPlusTree(32, { unique: true });
            indexes.set(col, index);
          }
        }
      }
    }

    this.tables.set(ast.table, { heap, schema, indexes, foreignKeys, childFKs: [], tableChecks: tableChecks.length > 0 ? tableChecks : null });
    this.catalog.push({ name: ast.table, columns: schema });

    // Register reverse FK references on parent tables for cascade lookups
    for (const fk of foreignKeys) {
      const parentTable = this.tables.get(fk.refTable);
      if (parentTable) {
        if (!parentTable.childFKs) parentTable.childFKs = [];
        parentTable.childFKs.push({
          childTable: ast.table,
          childColumns: fk.columns,
          parentColumns: fk.refColumns,
          onDelete: fk.onDelete,
          onUpdate: fk.onUpdate,
        });
      }
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
    const table = this._getTable(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    switch (ast.action) {
      case 'ADD_COLUMN': {
        // Check column doesn't already exist
        if (table.schema.find(c => c.name === ast.column)) {
          throw new Error(`Column ${ast.column} already exists`);
        }
        table.schema.push({ name: ast.column, type: ast.dataType, primaryKey: false });
        
        // Add default value to all existing rows
        // Use the original (non-MVCC-intercepted) scan/delete if available
        // This is necessary because ALTER TABLE runs as DDL (outside MVCC)
        const origScan = table.heap._origScan || table.heap.scan.bind(table.heap);
        const origDelete = table.heap._origDelete || table.heap.delete.bind(table.heap);
        
        const toUpdate = [];
        for (const { pageId, slotIdx, values } of origScan()) {
          toUpdate.push({ pageId, slotIdx, values: [...values] });
        }
        
        for (const entry of toUpdate) {
          const newValues = [...entry.values, ast.defaultValue ?? null];
          origDelete(entry.pageId, entry.slotIdx);
          table.heap.insert(newValues);
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

  _createTableAs(ast) {
    // Execute the query to get the schema and data
    const result = this.execute_ast(ast.query);
    
    let columns;
    if (!result.rows || result.rows.length === 0) {
      // Empty result — infer schema from query AST columns
      const queryCols = ast.query.columns;
      if (queryCols && queryCols.length > 0 && queryCols[0].type !== 'star') {
        columns = queryCols.map(col => {
          let name = col.alias || col.name || 'column';
          if (col.type === 'function') name = col.alias || `${col.func}`;
          if (col.type === 'expression') name = col.alias || 'expr';
          return { name, type: 'TEXT', primaryKey: false, notNull: false, check: null, defaultValue: null, references: null, generated: null };
        });
      } else {
        // Fallback: try to get column names from source table schema
        const fromTable = ast.query.from?.table || ast.query.from?.name;
        if (fromTable) {
          const srcTable = this._getTable(fromTable);
          if (srcTable) {
            columns = srcTable.schema.map(c => ({ ...c, primaryKey: false }));
          }
        }
        if (!columns) {
          throw new Error('CREATE TABLE AS with empty result set: cannot infer schema from SELECT *');
        }
      }
    } else {
      // Infer schema from first row
      const firstRow = result.rows[0];
      columns = Object.keys(firstRow).filter(k => !k.includes('.')).map(name => {
        const val = firstRow[name];
        let type = 'TEXT';
        if (typeof val === 'number') type = Number.isInteger(val) ? 'INTEGER' : 'REAL';
        else if (typeof val === 'boolean') type = 'INTEGER';
        return { name, type, primaryKey: false, notNull: false, check: null, defaultValue: null, references: null, generated: null };
      });
    }
    
    // Create the table
    const createAst = { type: 'CREATE_TABLE', table: ast.table, columns, ifNotExists: ast.ifNotExists };
    this._createTable(createAst);
    
    // Insert all rows
    const table = this._getTable(ast.table);
    for (const row of result.rows) {
      const values = columns.map(c => row[c.name] ?? null);
      this._insertRow(table, null, values);
    }
    
    return { type: 'OK', message: `Table ${ast.table} created with ${result.rows.length} rows` };
  }

  _valuesQuery(ast) {
    const rows = [];
    for (const valRow of ast.rows) {
      const row = {};
      for (let i = 0; i < valRow.length; i++) {
        row[`column${i + 1}`] = this._evalValue(valRow[i], {});
      }
      rows.push(row);
    }
    return { type: 'ROWS', rows };
  }

  _merge(ast) {
    const targetTable = this.tables.get(ast.target);
    if (!targetTable) throw new Error(`Table ${ast.target} not found`);
    
    // Get source rows — either from table or subquery
    let sourceRows;
    let sourceAlias = ast.sourceAlias;
    if (ast.sourceSubquery) {
      const result = this.execute_ast(ast.sourceSubquery);
      sourceRows = result.rows;
    } else {
      const sourceTable = this.tables.get(ast.source);
      if (!sourceTable) throw new Error(`Table ${ast.source} not found`);
      sourceRows = [...sourceTable.heap.scan()].map(entry => 
        this._valuesToRow(entry.values, sourceTable.schema, ast.source)
      );
    }
    
    let inserted = 0, updated = 0, deleted = 0;
    
    // For each source row, check if it matches any target row
    for (const sourceRow of sourceRows) {
      
      let matched = false;
      // Scan target for matching rows
      for (const targetEntry of targetTable.heap.scan()) {
        const targetRow = this._valuesToRow(targetEntry.values, targetTable.schema, ast.target);
        
        // Build combined row for condition evaluation
        const combined = {};
        for (const [k, v] of Object.entries(targetRow)) {
          combined[k] = v;
          combined[`${ast.targetAlias}.${k}`] = v;
        }
        for (const [k, v] of Object.entries(sourceRow)) {
          combined[`${ast.sourceAlias}.${k}`] = v;
        }
        
        if (this._evalExpr(ast.condition, combined)) {
          matched = true;
          // Apply WHEN MATCHED clauses
          for (const clause of ast.whenClauses) {
            if (!clause.matched) continue;
            if (clause.action === 'UPDATE') {
              // Build update AST and execute inline
              const newValues = [...targetEntry.values];
              for (const { column, value } of clause.assignments) {
                const colIdx = targetTable.schema.findIndex(c => c.name === column);
                if (colIdx === -1) throw new Error(`Column ${column} not found`);
                newValues[colIdx] = this._evalValue(value, combined);
              }
              // Delete old, insert new
              targetTable.heap.delete(targetEntry.pageId, targetEntry.slotIdx);
              targetTable.heap.insert(newValues);
              updated++;
            } else if (clause.action === 'DELETE') {
              targetTable.heap.delete(targetEntry.pageId, targetEntry.slotIdx);
              deleted++;
            }
            break;
          }
          break; // Only match first target row per source row
        }
      }
      
      if (!matched) {
        // Apply WHEN NOT MATCHED clauses
        for (const clause of ast.whenClauses) {
          if (clause.matched) continue;
          if (clause.action === 'INSERT') {
            const combined = {};
            for (const [k, v] of Object.entries(sourceRow)) {
              combined[`${ast.sourceAlias}.${k}`] = v;
              combined[k] = v;
            }
            const values = clause.values.map(v => this._evalValue(v, combined));
            this._insertRow(targetTable, clause.columns, values);
            inserted++;
          }
          break;
        }
      }
    }
    
    return { type: 'OK', message: `MERGE: ${inserted} inserted, ${updated} updated, ${deleted} deleted` };
  }

  _createIndex(ast) {
    const table = this._getTable(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    const hasExpressions = ast.expressions && ast.expressions.some(e => e !== null);

    // Validate columns exist (only for non-expression columns)
    if (!hasExpressions) {
      for (const col of ast.columns) {
        if (!table.schema.find(c => c.name === col)) {
          throw new Error(`Column ${col} not found in table ${ast.table}`);
        }
      }
    }

    // Build the index key name — use expression string representation for expression indexes
    const colName = hasExpressions
      ? ast.columns.map((c, i) => c || `expr_${i}`).join(',')
      : ast.columns.join(',');
    const isComposite = ast.columns.length > 1;
    const colIndices = hasExpressions ? null : ast.columns.map(c => table.schema.findIndex(s => s.name === c));

    const index = new BPlusTree(32, { unique: ast.unique || false });
    const colIdx = colIndices ? colIndices[0] : null;

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
      
      let key;
      if (hasExpressions) {
        const row = this._valuesToRow(values, table.schema, ast.table);
        if (ast.expressions.length === 1) {
          const expr = ast.expressions[0] || { type: 'column_ref', name: ast.columns[0] };
          key = this._evalValue(expr, row);
        } else {
          key = makeCompositeKey(ast.expressions.map((expr, i) => {
            if (expr) return this._evalValue(expr, row);
            return values[table.schema.findIndex(s => s.name === ast.columns[i])];
          }));
        }
      } else {
        key = isComposite
          ? makeCompositeKey(colIndices.map(i => values[i]))
          : values[colIdx];
      }
      if (ast.unique) {
        const existing = index.search(key);
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
      index.insert(key, rid);
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
      expressions: hasExpressions ? ast.expressions : null,
    });
    this.indexCatalog.set(ast.name, {
      table: ast.table,
      columns: ast.columns,
      unique: ast.unique || false,
      expressions: hasExpressions ? ast.expressions : null,
    });

    return { type: 'OK', message: `Index ${ast.name} created` };
  }

  _dropIndex(ast) {
    const meta = this.indexCatalog.get(ast.name);
    if (!meta) throw new Error(`Index ${ast.name} not found`);

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
    const table = this._getTable(ast.table);
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
        const tableData = this._getTable(ast.table);
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
    const table = this._getTable(ast.table) || this.tables.get(ast.table.toLowerCase());
    if (!table) throw new Error(`Table ${ast.table} not found`);

    let inserted = 0;
    const returnedRows = [];
    
    for (const row of ast.rows) {
      const values = row.map(r => {
        if (r.type === 'literal') return r.value;
        if (r.type === 'function_call') return this._evalFunction(r.func, r.args, {});
        return this._evalValue(r, {});
      });
      
      // INSERT OR REPLACE: delete conflicting row if exists
      if (ast.orReplace) {
        const pkIdx = table.schema.findIndex(c => c.primaryKey);
        if (pkIdx >= 0) {
          const orderedValues = this._orderValues(table, ast.columns, values);
          for (const tuple of table.heap.scan()) {
            if (tuple.values[pkIdx] === orderedValues[pkIdx]) {
              table.heap.delete(tuple.pageId, tuple.slotIdx);
              // Remove from indexes
              for (const [colName, index] of table.indexes) {
                const key = this._computeIndexKey(colName, tuple.values, table, ast.table);
                try { index.delete(key, { pageId: tuple.pageId, slotIdx: tuple.slotIdx }); } catch {}
              }
              break;
            }
          }
        }
      }
      
      // INSERT OR IGNORE: skip if conflict
      if (ast.orIgnore) {
        const pkIdx = table.schema.findIndex(c => c.primaryKey);
        if (pkIdx >= 0) {
          const orderedValues = this._orderValues(table, ast.columns, values);
          let conflict = false;
          for (const tuple of table.heap.scan()) {
            if (tuple.values[pkIdx] === orderedValues[pkIdx]) {
              conflict = true;
              break;
            }
          }
          if (conflict) continue;
        }
      }
      
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
      
      const { orderedValues: insertedValues } = this._insertRow(table, ast.columns, values);
      inserted++;
      
      if (ast.returning) {
        const retRow = {};
        table.schema.forEach((c, i) => { retRow[c.name] = insertedValues[i]; });
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
    const table = this._getTable(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    // Resolve CTEs if present
    const cteNames = [];
    if (ast.ctes) {
      for (const cte of ast.ctes) {
        const cteResult = this._select(cte.query);
        this.views.set(cte.name, { materializedRows: cteResult.rows, isCTE: true });
        cteNames.push(cte.name);
      }
    }

    try {
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
    } finally {
      // Clean up CTE views
      for (const name of cteNames) {
        this.views.delete(name);
      }
    }
  }

  // Validate column constraints (NOT NULL, CHECK) for a row
  _fireTriggers(timing, event, tableName, newRow, oldRow) {
    for (const trigger of this.triggers) {
      if (trigger.timing === timing && trigger.event === event && trigger.table === tableName) {
        try {
          let bodySql = trigger.bodySql;
          
          // Get table schema to resolve NEW.col / OLD.col references
          const table = this.tables.get(tableName);
          const schema = table?.schema || [];
          
          const resolveRow = (prefix, row) => {
            if (!row) return;
            for (let i = 0; i < schema.length; i++) {
              const colName = schema[i].name;
              const val = row[colName] ?? (Array.isArray(row) ? row[i] : null);
              const replacement = val === null || val === undefined ? 'NULL'
                : typeof val === 'string' ? `'${val.replace(/'/g, "''")}'`
                : String(val);
              const regex = new RegExp(`${prefix}\\.${colName}\\b`, 'gi');
              bodySql = bodySql.replace(regex, replacement);
            }
          };
          
          resolveRow('NEW', newRow);
          resolveRow('OLD', oldRow);
          
          this.execute(bodySql);
        } catch (e) {
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

    // Table-level foreign key constraints
    if (table.foreignKeys) {
      for (const fk of table.foreignKeys) {
        const fkValues = fk.columns.map(col => {
          const idx = table.schema.findIndex(c => c.name === col);
          return idx >= 0 ? values[idx] : null;
        });
        if (fkValues.some(v => v == null)) continue;
        
        const refTable = this.tables.get(fk.refTable);
        if (!refTable) throw new Error(`Referenced table ${fk.refTable} not found`);
        
        let found = false;
        for (const { values: refValues } of refTable.heap.scan()) {
          const refVals = fk.refColumns.map(col => {
            const idx = refTable.schema.findIndex(c => c.name === col);
            return idx >= 0 ? refValues[idx] : null;
          });
          if (fkValues.every((v, i) => v === refVals[i])) { found = true; break; }
        }
        if (!found) {
          throw new Error(`Foreign key constraint violated: (${fkValues.join(', ')}) not found in ${fk.refTable}(${fk.refColumns.join(', ')})`);
        }
      }
    }

    // Table-level CHECK constraints
    if (table.tableChecks) {
      const row = {};
      for (let i = 0; i < table.schema.length; i++) {
        row[table.schema[i].name] = values[i];
      }
      for (const check of table.tableChecks) {
        if (!this._evalExpr(check, row)) {
          throw new Error('Table-level CHECK constraint violated');
        }
      }
    }
  }

  _applySelectColumns(ast, rows) {
    // Apply ORDER BY
    if (ast.orderBy) {
              rows.sort((a, b) => {
        for (const { column, direction } of ast.orderBy) {
          const av = this._resolveOrderByValue(column, a, ast);
          const bv = this._resolveOrderByValue(column, b, ast);
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
        if (table.schema[i].defaultValue != null) ordered[i] = this._evalDefault(table.schema[i].defaultValue);
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
    // Validate no writes to generated columns
    this._validateNoGeneratedColumnWrites(table, columns);

    let orderedValues;
    if (columns) {
      orderedValues = new Array(table.schema.length).fill(null);
      // Apply default values first
      for (let i = 0; i < table.schema.length; i++) {
        if (table.schema[i].defaultValue !== undefined && table.schema[i].defaultValue !== null) {
          orderedValues[i] = this._evalDefault(table.schema[i].defaultValue);
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

    // Compute generated columns (STORED)
    this._computeGeneratedColumns(table, orderedValues);

    // Validate constraints
    this._validateConstraints(table, orderedValues);

    // BEFORE INSERT triggers
    const tableName = table.heap?.name || '';
    this._fireTriggers('BEFORE', 'INSERT', tableName, orderedValues);

    // Pre-check unique index constraints BEFORE heap insertion (atomicity)
    for (const [colName, index] of table.indexes) {
      const key = this._computeIndexKey(colName, orderedValues, table, tableName);
      if (index.unique && key !== null && key !== undefined) {
        const existing = index.range(key, key);
        if (existing.length > 0) {
          throw new Error(`Duplicate key '${key}' violates unique constraint on column '${colName}'`);
        }
      }
    }

    const rid = table.heap.insert(orderedValues);

    // Log for transaction rollback
    if (this._txWriteLog) {
      this._txWriteLog.push({ type: 'INSERT', table: tableName, rid, values: orderedValues });
    }

    // WAL: log the insert
    const txId = this._currentTxId || this._nextTxId++;
    this.wal.appendInsert(txId, tableName, rid.pageId, rid.slotIdx, orderedValues);
    if (!this._currentTxId) {
      // Auto-commit mode: immediately commit
      this.wal.appendCommit(txId);
    }

    // Update indexes (uniqueness already verified above)
    for (const [colName, index] of table.indexes) {
      const key = this._computeIndexKey(colName, orderedValues, table, tableName);
      if (key !== null && key !== undefined) {
        index.insert(key, rid);
      }
    }

    // AFTER INSERT triggers
    this._fireTriggers('AFTER', 'INSERT', tableName, orderedValues);

    return { rid, orderedValues };
  }

  _select(ast) {
    // Try vectorized fast path for simple single-table aggregate queries
    if (this._useVectorized) {
      const vectorResult = this._tryVectorizedSelect(ast);
      if (vectorResult) return vectorResult;
    }

    // Handle CTEs — register as temporary views
    const tempViews = [];
    if (ast.ctes) {
      for (const cte of ast.ctes) {
        if (this.views.has(cte.name)) throw new Error(`CTE name ${cte.name} conflicts with existing view`);
        
        if (cte.recursive && (cte.unionQuery || cte.query.type === 'UNION')) {
          // Recursive CTE: iterate until fixed point
          // Pass column list to recursive executor so it can rename at each iteration
          let allRows = this._executeRecursiveCTE(cte);
          this.views.set(cte.name, { materializedRows: allRows, isCTE: true });
        } else {
          if (cte.columnList) {
            // Non-recursive CTE with column list: materialize and rename
            const result = this._select(cte.query);
            const rows = this._renameCTEColumns(result.rows || [], cte.columnList);
            this.views.set(cte.name, { materializedRows: rows, isCTE: true });
          } else {
            this.views.set(cte.name, { query: cte.query, isCTE: true });
          }
        }
        tempViews.push(cte.name);
      }
    }

    try {
      // Optimize: decorrelate subqueries
      const optimizedAst = optimizeSelect(ast, this);
      
      // Try vectorized execution for eligible queries
      const vecResult = this._selectInnerCore(optimizedAst);
      if (vecResult) return vecResult;
      
      return this._selectInner(optimizedAst);
    } finally {
      // Clean up temporary CTE views
      for (const name of tempViews) {
        this.views.delete(name);
      }
    }
  }

  // LATERAL JOIN: for each outer row, execute subquery with outer row in scope
  _executeLateralJoin(leftRows, join) {
    const result = [];
    const rightAlias = join.alias || '__lateral';
    
    for (const leftRow of leftRows) {
      // Execute the subquery with the outer row's columns available
      // We do this by temporarily creating a "scope" that _evalExpr can access
      const savedLateralScope = this._lateralScope;
      this._lateralScope = leftRow;
      
      let subResult;
      try {
        subResult = this._select(join.subquery);
      } finally {
        this._lateralScope = savedLateralScope;
      }
      
      const rightRows = (subResult.rows || []).map(r => {
        const row = {};
        for (const [k, v] of Object.entries(r)) {
          row[k] = v;
          row[`${rightAlias}.${k}`] = v;
        }
        return row;
      });
      
      if (rightRows.length === 0) {
        if (join.joinType === 'LEFT') {
          // LEFT JOIN LATERAL with no results: add null right side
          const nullRow = {};
          // We don't know the columns, so just add the left row
          result.push({ ...leftRow });
        }
        // CROSS/INNER JOIN LATERAL with no results: skip this left row
      } else {
        for (const rightRow of rightRows) {
          const combined = { ...leftRow, ...rightRow };
          if (!join.on || this._evalExpr(join.on, combined)) {
            result.push(combined);
          }
        }
      }
    }
    
    return result;
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

  /**
   * Try to use an index for UPDATE's WHERE clause.
   * Returns array of {pageId, slotIdx, values} or null.
   */
  _tryIndexScanForUpdate(table, ast) {
    if (!ast.where) return null;
    
    // Check for simple equality: WHERE col = value
    let colName, value;
    if (ast.where.type === '=') {
      const left = ast.where.left;
      const right = ast.where.right;
      if ((left.type === 'column' || left.type === 'column_ref') && 
          (right.type === 'literal' || right.type === 'number' || right.type === 'string')) {
        colName = left.name;
        value = right.value;
      } else if ((right.type === 'column' || right.type === 'column_ref') && 
                 (left.type === 'literal' || left.type === 'number' || left.type === 'string')) {
        colName = right.name;
        value = left.value;
      }
    }
    
    if (!colName) return null;
    
    // Check for index on this column
    const idx = table.indexes?.get(colName) || table.indexes?.get(colName.toLowerCase()) || table.indexes?.get(colName.toUpperCase());
    if (!idx) return null;
    
    // Use index to find matching row IDs
    const rids = idx.search(value);
    if (!rids || rids.length === 0) return [];
    
    // Fetch rows by their rid
    const results = [];
    for (const rid of rids) {
      try {
        const entry = table.heap.get(rid);
        if (entry) {
          results.push({ pageId: entry.pageId, slotIdx: entry.slotIdx, values: entry.values });
        }
      } catch {
        // rid not valid, skip
      }
    }
    return results;
  }

  /**
   * Expand ROLLUP/CUBE/GROUPING SETS into multiple queries.
   * Returns combined result or null if not applicable.
   */
  _expandGroupingSets(ast) {
    if (!ast.groupBy || ast.groupBy.length === 0) return null;

    let groupingSets = null;
    const plainCols = [];
    
    for (const gb of ast.groupBy) {
      if (gb.type === 'function' || gb.type === 'function_call') {
        const func = (gb.func || gb.name || '').toUpperCase();
        const args = gb.args || [];
        const colNames = args.map(a => a.name || a.value || a);
        
        if (func === 'ROLLUP') {
          // ROLLUP(a, b) = GROUPING SETS ((a,b), (a), ())
          groupingSets = [];
          for (let i = colNames.length; i >= 0; i--) {
            groupingSets.push(colNames.slice(0, i));
          }
        } else if (func === 'CUBE') {
          // CUBE(a, b) = all subsets: (a,b), (a), (b), ()
          groupingSets = [];
          const n = colNames.length;
          for (let mask = (1 << n) - 1; mask >= 0; mask--) {
            const subset = [];
            for (let j = 0; j < n; j++) {
              if (mask & (1 << j)) subset.push(colNames[j]);
            }
            groupingSets.push(subset);
          }
        } else if (func === 'GROUPING_SETS' || (func === 'GROUPING' && (gb.func || '').toUpperCase() === 'GROUPING_SETS')) {
          // Explicit GROUPING SETS
          groupingSets = args.map(a => {
            if (Array.isArray(a)) return a.map(x => x.name || x.value || x);
            if (Array.isArray(a.args)) return a.args.map(x => x.name || x.value || x);
            return [a.name || a.value || a];
          });
        }
      } else {
        plainCols.push(gb.name || gb.column || gb);
      }
    }
    
    if (!groupingSets) return null;
    
    // Execute a separate query for each grouping set and UNION ALL results
    const allRows = [];
    const groupByColumns = [...new Set(groupingSets.flat())];
    
    for (const groupSet of groupingSets) {
      const modifiedAst = {
        ...ast,
        groupBy: groupSet.length > 0 ? groupSet : undefined,
      };
      
      // If no GROUP BY columns (the () set), remove groupBy entirely
      if (groupSet.length === 0) {
        delete modifiedAst.groupBy;
      }
      
      try {
        const result = this._selectInner(modifiedAst);
        if (result && result.rows) {
          for (const row of result.rows) {
            // Set NULL for columns not in this grouping set
            for (const col of groupByColumns) {
              if (!groupSet.includes(col)) {
                row[col] = null;
                // Also null any uppercase/lowercase variants
                const upper = col.toUpperCase();
                const lower = col.toLowerCase();
                if (upper !== col) row[upper] = null;
                if (lower !== col) row[lower] = null;
              }
            }
            allRows.push(row);
          }
        }
      } catch { /* skip failed grouping set */ }
    }
    
    return { rows: allRows, columns: ast.columns?.map(c => c.alias || c.name || c) };
  }

  _selectInnerCore(ast) {
    try {
      // Only for single-table SELECT without subqueries, GROUP BY, HAVING, UNION
      if (!ast.from?.table) return null;
      if (ast.joins?.length) return null;
      if (ast.groupBy || ast.having) return null;
      if (ast.type !== 'SELECT') return null;
      
      const table = this.tables.get(ast.from.table) || this.tables.get(ast.from.table.toLowerCase());
      if (!table) return null;
      
      // Need enough rows to benefit (vectorized has overhead for small tables)
      const stats = this._tableStats.get(ast.from.table);
      const rowCount = stats?.rowCount || table.heap.rowCount || 0;
      if (rowCount < 500) return null;
      
      const engine = new CompiledQueryEngine(this);
      return engine._tryVectorized(ast, table);
    } catch {
      return null;
    }
  }

  _selectInner(ast) {
    // Handle ROLLUP/CUBE by expanding to multiple GROUP BY queries
    if (ast.groupBy) {
      const rollupResult = this._expandGroupingSets(ast);
      if (rollupResult) return rollupResult;
    }
    // Handle SELECT without FROM (e.g., SELECT 1 AS n)
    if (!ast.from) {
      const row = {};
      for (const col of ast.columns) {
        if (col.type === 'expression') {
          let name = col.alias || (col.expr?.left?.type === 'column_ref' ? col.expr.left.name : 'expr');
          let base = name; let suf = 1;
          while (row.hasOwnProperty(name)) name = `${base}_${suf++}`;
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

    // Check if FROM is pg_catalog
    if (tableName.startsWith('pg_catalog.') || tableName.startsWith('pg_')) {
      const pgName = tableName.replace('pg_catalog.', '');
      const pgRows = this._getPgCatalog(pgName);
      if (pgRows !== null) {
        const alias = ast.from.alias || tableName;
        let rows = pgRows.map(r => {
          const row = { ...r };
          for (const [k, v] of Object.entries(r)) {
            if (!k.includes('.')) row[`${alias}.${k}`] = v;
          }
          return row;
        });
        for (const join of ast.joins || []) {
          rows = this._executeJoin(rows, join, alias);
        }
        if (ast.where) rows = rows.filter(row => this._evalExpr(ast.where, row));
        return this._applySelectColumns(ast, rows);
      }
    }

    // Check if FROM is information_schema
    if (tableName.startsWith('information_schema.') || tableName === 'information_schema') {
      const isRows = this._getInformationSchema(tableName);
      if (isRows !== null) {
        let rows = isRows;
        if (ast.where) rows = rows.filter(row => this._evalExpr(ast.where, row));
        for (const join of ast.joins || []) {
          rows = this._executeJoin(rows, join, ast.from.alias || tableName);
        }
        return this._applySelectColumns(ast, rows);
      }
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

      // Add alias-qualified column names so e.g. "ds.col" resolves
      const alias = ast.from.alias || tableName;
      if (alias) {
        rows = rows.map(row => {
          const newRow = { ...row };
          for (const [k, v] of Object.entries(row)) {
            if (!k.includes('.')) {
              newRow[`${alias}.${k}`] = v;
            }
          }
          return newRow;
        });
      }

      // Handle JOINs on view results
      if (ast.joins && ast.joins.length > 0) {
        for (const join of ast.joins) {
          rows = this._executeJoin(rows, join, alias);
        }
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
          for (const { column, direction, nulls } of ast.orderBy) {
            const av = this._resolveOrderByValue(column, a, ast);
            const bv = this._resolveOrderByValue(column, b, ast);
            
            // Handle nulls
            if (av == null && bv == null) continue;
            if (av == null) {
              const nullFirst = nulls === 'FIRST' || (!nulls && direction === 'DESC');
              return nullFirst ? -1 : 1;
            }
            if (bv == null) {
              const nullFirst = nulls === 'FIRST' || (!nulls && direction === 'DESC');
              return nullFirst ? 1 : -1;
            }
            
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
          for (const col of ast.columns) {
            if (col.type === 'function') {
              const name = col.alias || `${col.func}(...)`;
              result[name] = this._evalFunction(col.func, col.args, row);
            } else if (col.type === 'expression') {
              let name = col.alias;
              if (!name) {
                name = col.expr?.left?.type === 'column_ref' ? col.expr.left.name : 'expr';
                let base = name; let suf = 1;
                while (result.hasOwnProperty(name)) name = `${base}_${suf++}`;
              }
              result[name] = this._evalValue(col.expr, row);
            } else {
              let name = col.alias || col.name;
              const colName = String(col.name);
              const baseName = colName.includes('.') ? colName.split('.').pop() : colName;
              if (!col.alias) name = baseName;
              let base = name; let suf = 1;
              while (result.hasOwnProperty(name)) name = `${base}_${suf++}`;
              result[name] = row[col.name] !== undefined ? row[col.name] : row[baseName];
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
      // With JOINs: try predicate pushdown
      // Split WHERE into: (a) conditions on left table only, (b) rest
      const leftAlias = ast.from.alias || ast.from.table;
      const leftCols = new Set(table.schema.map(c => c.name));
      
      let leftPredicate = null;
      let residualPredicate = ast.where;
      
      if (ast.where) {
        const { left, rest } = this._splitPredicates(ast.where, leftAlias, leftCols);
        leftPredicate = left;
        residualPredicate = rest;
      }
      
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const row = this._valuesToRow(values, table.schema, leftAlias);
        // Apply pushed-down predicate on left table
        if (leftPredicate && !this._evalExpr(leftPredicate, row)) continue;
        rows.push(row);
      }
      
      // Override the WHERE for post-join filtering to only the residual
      if (residualPredicate !== ast.where) {
        ast = { ...ast, where: residualPredicate };
      }
    }

    // Handle JOINs — try Volcano engine first for performance
    if (ast.joins && ast.joins.length > 0) {
      // Resolve NATURAL joins: generate ON condition from common column names
      for (const join of ast.joins) {
        if (join.natural && !join.on) {
          const leftTable = ast.from.table || ast.from.name;
          const rightTable = join.table;
          const leftSchema = this.tables.get(leftTable)?.schema;
          const rightSchema = this.tables.get(rightTable)?.schema;
          if (leftSchema && rightSchema) {
            const leftCols = leftSchema.map(c => c.name);
            const rightCols = rightSchema.map(c => c.name);
            const commonCols = leftCols.filter(c => rightCols.includes(c));
            if (commonCols.length > 0) {
              const leftAlias = ast.from.alias || leftTable;
              const rightAlias = join.alias || rightTable;
              // Build AND chain of equality conditions
              let onExpr = null;
              for (const col of commonCols) {
                const eq = {
                  type: 'COMPARE', op: 'EQ',
                  left: { type: 'column_ref', name: `${leftAlias}.${col}` },
                  right: { type: 'column_ref', name: `${rightAlias}.${col}` },
                };
                onExpr = onExpr ? { type: 'AND', left: onExpr, right: eq } : eq;
              }
              join.on = onExpr;
            }
          }
        }
      }
      
      const volcanoRows = this._tryVolcanoJoin(ast, rows);
      if (volcanoRows !== null) {
        rows = volcanoRows;
      } else {
        // Fallback to nested loop
        for (const join of ast.joins) {
          rows = this._executeJoin(rows, join, ast.from.alias || ast.from.table);
        }
      }
    }

    // WHERE filter after JOINs
    if (hasJoins && ast.where) {
      rows = rows.filter(row => this._evalExpr(ast.where, row));
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
    const windowAliases = new Set();
    for (const col of ast.columns) {
      if (col.type === 'expression' && col.alias) {
        aliasExprs.set(col.alias, col.expr);
      } else if (col.type === 'function' && col.alias) {
        aliasExprs.set(col.alias, col);
      } else if (col.type === 'window') {
        const name = col.alias || col.func;
        windowAliases.add(name);
      }
    }

    // ORDER BY
    if (ast.orderBy) {
      rows.sort((a, b) => {
        for (const { column, direction, nulls } of ast.orderBy) {
          let av, bv;
          if (typeof column === 'number') {
            av = this._resolveOrderByValue(column, a, ast);
            bv = this._resolveOrderByValue(column, b, ast);
          } else if (typeof column === 'object') {
            av = this._evalValue(column, a);
            bv = this._evalValue(column, b);
          } else if (windowAliases.has(column)) {
            av = a[`__window_${column}`];
            bv = b[`__window_${column}`];
          } else if (aliasExprs.has(column)) {
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
          
          // Handle nulls
          if (av == null && bv == null) continue;
          if (av == null) {
            const nullFirst = nulls === 'FIRST' || (!nulls && direction === 'DESC');
            return nullFirst ? -1 : 1;
          }
          if (bv == null) {
            const nullFirst = nulls === 'FIRST' || (!nulls && direction === 'DESC');
            return nullFirst ? 1 : -1;
          }
          
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
      for (const col of ast.columns) {
        if (col.type === 'function') {
          const name = col.alias || `${col.func}(...)`;
          result[name] = this._evalFunction(col.func, col.args, row);
        } else if (col.type === 'expression') {
          // Generate unique name for unaliased expressions
          let name = col.alias;
          if (!name) {
            // Try to derive a meaningful name from the expression
            if (col.expr?.type === 'arith' && col.expr.left?.type === 'column_ref') {
              name = col.expr.left.name;
            } else {
              name = 'expr';
            }
            // Ensure uniqueness by appending suffix if key exists
            let baseName = name;
            let suffix = 1;
            while (result.hasOwnProperty(name)) {
              name = `${baseName}_${suffix++}`;
            }
          }
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
      if (ast.distinctOn) {
        // DISTINCT ON: keep first row per unique combination of ON columns
        finalRows = projected.filter(row => {
          const key = ast.distinctOn.map(col => {
            const val = row[col];
            return val === null ? 'NULL' : String(val);
          }).join('\0');
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });
      } else {
        finalRows = projected.filter(row => {
          const key = JSON.stringify(row);
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });
      }
      // Apply OFFSET and LIMIT after DISTINCT
      if (ast.offset) finalRows = finalRows.slice(ast.offset);
      if (ast.limit != null) finalRows = finalRows.slice(0, ast.limit);
    }

    return { type: 'ROWS', rows: finalRows };
  }

  _executeJoin(leftRows, join, leftAlias) {
    // LATERAL JOIN: re-execute subquery per outer row
    if (join.lateral && join.subquery) {
      return this._executeLateralJoin(leftRows, join);
    }
    
    // Check for pg_catalog virtual tables
    const joinTable = join.table;
    if (joinTable && (joinTable.startsWith('pg_catalog.') || joinTable.startsWith('pg_'))) {
      const pgName = joinTable.replace('pg_catalog.', '');
      const pgRows = this._getPgCatalog(pgName);
      if (pgRows !== null) {
        const rightAlias = join.alias || joinTable;
        const rightRows = pgRows.map(r => {
          const row = {};
          for (const [k, v] of Object.entries(r)) {
            row[k] = v;
            row[`${rightAlias}.${k}`] = v;
          }
          return row;
        });
        return this._executeJoinWithRows(leftRows, rightRows, join, rightAlias);
      }
    }
    
    // Check for information_schema virtual tables
    if (joinTable && (joinTable.startsWith('information_schema.') || joinTable === 'information_schema')) {
      const isRows = this._getInformationSchema(joinTable);
      if (isRows !== null) {
        const rightAlias = join.alias || joinTable;
        const rightRows = isRows.map(r => {
          const row = {};
          for (const [k, v] of Object.entries(r)) {
            row[k] = v;
            row[`${rightAlias}.${k}`] = v;
          }
          return row;
        });
        return this._executeJoinWithRows(leftRows, rightRows, join, rightAlias);
      }
    }

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
        rightRows.push(this._valuesToRow(values, rightTable.schema, rightAlias));
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
    for (const leftRow of leftRows) {
      let matched = false;
      for (const { values } of rightTable.heap.scan()) {
        const rightRow = this._valuesToRow(values, rightTable.schema, rightAlias);
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

  _update(ast) {
    const table = this._getTable(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    // Validate no writes to generated columns
    this._validateNoGeneratedColumnWrites(table, ast.assignments?.map(a => a.column));

    let updated = 0;
    const returnedRows = [];
    const toUpdate = [];

    if (ast.from) {
      // UPDATE ... FROM: join target with source table
      const fromTableObj = this.tables.get(ast.from.table);
      if (!fromTableObj) throw new Error(`Table ${ast.from.table} not found`);
      const fromAlias = ast.from.alias || ast.from.table;
      
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const targetRow = this._valuesToRow(values, table.schema, ast.table);
        
        // For each target row, find matching source rows
        for (const fromEntry of fromTableObj.heap.scan()) {
          const fromRow = this._valuesToRow(fromEntry.values, fromTableObj.schema, ast.from.table);
          // Merge rows with proper prefixes
          const combined = {};
          for (const [k, v] of Object.entries(targetRow)) {
            combined[k] = v;
            combined[`${ast.table}.${k}`] = v;
          }
          for (const [k, v] of Object.entries(fromRow)) {
            combined[`${fromAlias}.${k}`] = v;
          }
          
          if (!ast.where || this._evalExpr(ast.where, combined)) {
            toUpdate.push({ pageId, slotIdx, values: [...values], combined });
            break; // Only update once per target row
          }
        }
      }
    } else {
      // Try index-backed scan for WHERE clause on indexed column
      const indexScanResult = this._tryIndexScanForUpdate(table, ast);
      if (indexScanResult) {
        // Index scan found matching rows — no SeqScan needed
        for (const entry of indexScanResult) {
          const row = this._valuesToRow(entry.values, table.schema, ast.table);
          if (!ast.where || this._evalExpr(ast.where, row)) {
            toUpdate.push({ pageId: entry.pageId, slotIdx: entry.slotIdx, values: [...entry.values] });
          }
        }
      } else {
        // Fall back to SeqScan
        for (const { pageId, slotIdx, values } of table.heap.scan()) {
          const row = this._valuesToRow(values, table.schema, ast.table);
          if (!ast.where || this._evalExpr(ast.where, row)) {
            toUpdate.push({ pageId, slotIdx, values: [...values] });
          }
        }
      }
    }

    for (const item of toUpdate) {
      const newValues = [...item.values];
      const row = item.combined || this._valuesToRow(item.values, table.schema, ast.table);
      for (const { column, value } of ast.assignments) {
        const colIdx = table.schema.findIndex(c => c.name === column);
        if (colIdx === -1) throw new Error(`Column ${column} not found`);
        newValues[colIdx] = this._evalValue(value, row);
      }

      // Recompute generated columns
      this._computeGeneratedColumns(table, newValues);

      // Validate constraints (including FK) on the new values
      this._validateConstraints(table, newValues);

      // Determine if this is a HOT-eligible update (no indexed columns changed)
      const indexedColumns = new Set();
      for (const [colName] of table.indexes) {
        if (colName.includes(',')) {
          for (const c of colName.split(',').map(s => s.trim())) indexedColumns.add(c);
        } else {
          indexedColumns.add(colName);
        }
        // For expression indexes, also check referenced columns
        const meta = table.indexMeta && table.indexMeta.get(colName);
        if (meta && meta.expressions) {
          for (const expr of meta.expressions) {
            if (expr) this._collectColumnRefs(expr).forEach(c => indexedColumns.add(c.name || c));
          }
        }
      }
      
      const updatedColumns = ast.assignments.map(a => a.column);
      const indexedColumnsChanged = updatedColumns.some(col => indexedColumns.has(col));

      // Try HOT update if no indexed columns changed
      if (!indexedColumnsChanged && table.heap.hotUpdate) {
        const hotRid = table.heap.hotUpdate(item.pageId, item.slotIdx, newValues);
        if (hotRid) {
          // HOT update successful — no index changes needed!
          if (this._txWriteLog) {
            this._txWriteLog.push({ type: 'UPDATE', table: ast.table, rid: { pageId: item.pageId, slotIdx: item.slotIdx }, oldValues: item.values, newValues, hot: true });
          }
          const txId = this._currentTxId || this._nextTxId++;
          this.wal.appendUpdate(txId, ast.table, hotRid.pageId, hotRid.slotIdx, item.values, newValues);
          if (!this._currentTxId) this.wal.appendCommit(txId);

          updated++;
          if (ast.returning) {
            const retRow = {};
            table.schema.forEach((c, i) => { retRow[c.name] = newValues[i]; });
            returnedRows.push(retRow);
          }
          continue; // Skip regular update path
        }
        // HOT update failed (no space on page) — fall through to regular update
      }

      // Regular update: Remove old index entries
      for (const [colName, index] of table.indexes) {
        const oldKey = this._computeIndexKey(colName, item.values, table, ast.table);
        try { index.delete(oldKey, { pageId: item.pageId, slotIdx: item.slotIdx }); } catch {}
      }

      // Delete old, insert new
      table.heap.delete(item.pageId, item.slotIdx);
      const newRid = table.heap.insert(newValues);

      // Log for transaction rollback
      if (this._txWriteLog) {
        this._txWriteLog.push({ type: 'UPDATE', table: ast.table, rid: { pageId: item.pageId, slotIdx: item.slotIdx }, newRid, oldValues: item.values, newValues });
      }

      // WAL: log the update
      const txId = this._currentTxId || this._nextTxId++;
      this.wal.appendUpdate(txId, ast.table, newRid.pageId, newRid.slotIdx, item.values, newValues);
      if (!this._currentTxId) this.wal.appendCommit(txId);

      // Update indexes with new entries (and enforce uniqueness)
      for (const [colName, index] of table.indexes) {
        const newKey = this._computeIndexKey(colName, newValues, table, ast.table);
        if (index.unique && newKey !== null && newKey !== undefined) {
          const existing = index.range(newKey, newKey);
          if (existing.length > 0) {
            throw new Error(`Duplicate key '${newKey}' violates unique constraint on column '${colName}'`);
          }
        }
        if (newKey !== null && newKey !== undefined) {
          index.insert(newKey, newRid);
        }
      }

      updated++;
      
      if (ast.returning) {
        const retRow = {};
        table.schema.forEach((c, i) => { retRow[c.name] = newValues[i]; });
        returnedRows.push(retRow);
      }
    }

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
    // Check childFKs (registered during CREATE TABLE)
    if (parentTable.childFKs) {
      for (const fk of parentTable.childFKs) {
        const childTable = this.tables.get(fk.childTable);
        if (!childTable) continue;
        
        const parentVals = fk.parentColumns.map(col => {
          const idx = parentTable.schema.findIndex(c => c.name === col);
          return idx >= 0 ? parentValues[idx] : null;
        });
        
        const childColIndices = fk.childColumns.map(col =>
          childTable.schema.findIndex(c => c.name === col)
        );

        if (fk.onDelete === 'CASCADE') {
          const toDelete = [];
          for (const { pageId, slotIdx, values: childValues } of childTable.heap.scan()) {
            const childVals = childColIndices.map(i => childValues[i]);
            if (parentVals.every((v, i) => v === childVals[i])) {
              toDelete.push({ pageId, slotIdx, values: childValues });
            }
          }
          for (const { pageId, slotIdx, values: childValues } of toDelete) {
            this._handleForeignKeyDelete(fk.childTable, childTable, childValues);
            childTable.heap.delete(pageId, slotIdx);
            // Remove from indexes
            for (const [colName, index] of childTable.indexes) {
              const key = this._computeIndexKey(colName, childValues, childTable, fk.childTable);
              try { index.delete(key, { pageId, slotIdx }); } catch {}
            }
          }
        } else if (fk.onDelete === 'SET NULL') {
          for (const { pageId, slotIdx, values: childValues } of childTable.heap.scan()) {
            const childVals = childColIndices.map(i => childValues[i]);
            if (parentVals.every((v, i) => v === childVals[i])) {
              // Build UPDATE statement
              const setClauses = fk.childColumns.map(col => `${col} = NULL`).join(', ');
              // Build WHERE clause using all child columns for matching
              const whereParts = [];
              for (let ci = 0; ci < childTable.schema.length; ci++) {
                const v = childValues[ci];
                if (v === null) continue;
                const colName = childTable.schema[ci].name;
                whereParts.push(`${colName} = ${typeof v === 'string' ? `'${v}'` : v}`);
              }
              if (whereParts.length > 0) {
                this.execute(`UPDATE ${fk.childTable} SET ${setClauses} WHERE ${whereParts.join(' AND ')}`);
              }
              break; // Re-scan needed after update (iterator invalidated)
            }
          }
        } else {
          // RESTRICT / NO ACTION
          for (const { values: childValues } of childTable.heap.scan()) {
            const childVals = childColIndices.map(i => childValues[i]);
            if (parentVals.every((v, i) => v === childVals[i])) {
              throw new Error(`Cannot delete: row is referenced by ${fk.childTable}(${fk.childColumns.join(', ')})`);
            }
          }
        }
      }
    }

    // Also check column-level REFERENCES (legacy path)
    for (const [childTableName, childTable] of this.tables) {
      for (const col of childTable.schema) {
        if (col.references && col.references.table === parentTableName) {
          const parentColIdx = parentTable.schema.findIndex(c => c.name === col.references.column);
          const parentValue = parentValues[parentColIdx];
          const childColIdx = childTable.schema.findIndex(c => c.name === col.name);

          if (col.references.onDelete === 'CASCADE') {
            const toDelete = [];
            for (const { pageId, slotIdx, values: childValues } of childTable.heap.scan()) {
              if (childValues[childColIdx] === parentValue) {
                toDelete.push({ pageId, slotIdx, values: childValues });
              }
            }
            for (const { pageId, slotIdx, values: childValues } of toDelete) {
              this._handleForeignKeyDelete(childTableName, childTable, childValues);
              childTable.heap.delete(pageId, slotIdx);
            }
          } else if (col.references.onDelete === 'SET NULL') {
            this.execute(`UPDATE ${childTableName} SET ${col.name} = NULL WHERE ${col.name} = ${typeof parentValue === 'string' ? `'${parentValue}'` : parentValue}`);
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
    const table = this._getTable(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    let deleted = 0;
    const returnedRows = [];
    const toDelete = [];

    if (ast.using) {
      // DELETE ... USING: join target with source table
      const usingTableObj = this.tables.get(ast.using.table);
      if (!usingTableObj) throw new Error(`Table ${ast.using.table} not found`);
      const usingAlias = ast.using.alias || ast.using.table;
      
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const targetRow = this._valuesToRow(values, table.schema, ast.table);
        
        let matched = false;
        for (const usingEntry of usingTableObj.heap.scan()) {
          const usingRow = this._valuesToRow(usingEntry.values, usingTableObj.schema, ast.using.table);
          const combined = {};
          // Add target table columns with table prefix
          for (const [k, v] of Object.entries(targetRow)) {
            combined[k] = v;
            combined[`${ast.table}.${k}`] = v;
          }
          // Add using table columns with alias prefix
          for (const [k, v] of Object.entries(usingRow)) {
            combined[`${usingAlias}.${k}`] = v;
          }
          
          if (!ast.where || this._evalExpr(ast.where, combined)) {
            matched = true;
            break;
          }
        }
        if (matched) {
          toDelete.push({ pageId, slotIdx });
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

    for (const { pageId, slotIdx } of toDelete) {
      const values = table.heap.get(pageId, slotIdx);
      
      // BEFORE DELETE triggers
      if (values) {
        this._fireTriggers('BEFORE', 'DELETE', ast.table, null, values);
      }
      
      // Check foreign key constraints from child tables
      if (values) {
        this._handleForeignKeyDelete(ast.table, table, values);
      }
      
      table.heap.delete(pageId, slotIdx);
      
      // Log for transaction rollback
      if (this._txWriteLog && values) {
        this._txWriteLog.push({ type: 'DELETE', table: ast.table, rid: { pageId, slotIdx }, values });
      }
      
      // Remove from indexes
      if (values) {
        for (const [colName, index] of table.indexes) {
          const key = this._computeIndexKey(colName, values, table, ast.table);
          try { index.delete(key, { pageId, slotIdx }); } catch {}
        }
      }
      
      // WAL: log the delete
      if (values) {
        const txId = this._currentTxId || this._nextTxId++;
        this.wal.appendDelete(txId, ast.table, pageId, slotIdx, values);
        if (!this._currentTxId) this.wal.appendCommit(txId);
      }
      
      // AFTER DELETE triggers
      if (values) {
        this._fireTriggers('AFTER', 'DELETE', ast.table, null, values);
      }
      
      deleted++;
      
      if (ast.returning && values) {
        const retRow = {};
        table.schema.forEach((c, i) => { retRow[c.name] = values[i]; });
        returnedRows.push(retRow);
      }
    }

    if (ast.returning) {
      const filteredRows = ast.returning === '*' ? returnedRows : returnedRows.map(row => {
        const filtered = {};
        for (const col of ast.returning) filtered[col] = row[col];
        return filtered;
      });
      return { type: 'ROWS', rows: filteredRows, count: deleted };
    }
    return { type: 'OK', message: `${deleted} row(s) deleted`, count: deleted };
  }

  _showTables() {
    const rows = [];
    for (const [name] of this.tables) {
      rows.push({ table_name: name });
    }
    return { type: 'ROWS', rows };
  }

  _describe(ast) {
    const table = this._getTable(ast.table);
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
    const tables = ast.table ? [ast.table] : [...this.tables.keys()];
    let totalDead = 0, totalBytes = 0, totalPages = 0, totalPagesProcessed = 0;
    let allDone = true;
    const cursors = {};
    let hotPruned = 0;

    for (const tableName of tables) {
      const table = this.tables.get(tableName);
      if (!table) continue;
      
      // Prune HOT chains (works on regular heaps too)
      if (table.heap && table.heap.pruneHotChains) {
        hotPruned += table.heap.pruneHotChains();
      }

      if (!table.mvccHeap || !this._mvcc) continue;

      if (ast.incremental) {
        // Incremental VACUUM: process maxPages per table
        const maxPages = ast.maxPages || 10;
        const cursor = (this._vacuumCursors && this._vacuumCursors[tableName]) || 0;
        const result = table.mvccHeap.vacuumIncremental(this._mvcc, maxPages, cursor);
        totalDead += result.deadTuplesRemoved;
        totalBytes += result.bytesFreed;
        totalPages += result.pagesCompacted;
        totalPagesProcessed += result.pagesProcessed;
        cursors[tableName] = result.cursor;
        if (!result.done) allDone = false;
      } else {
        const result = table.mvccHeap.vacuum(this._mvcc);
        totalDead += result.deadTuplesRemoved;
        totalBytes += result.bytesFreed;
        totalPages += result.pagesCompacted;
      }
    }

    // Store cursors for resuming incremental VACUUM
    if (ast.incremental) {
      if (!this._vacuumCursors) this._vacuumCursors = {};
      Object.assign(this._vacuumCursors, cursors);
      if (allDone) this._vacuumCursors = {};
    }

    const mode = ast.incremental ? 'VACUUM INCREMENTAL' : 'VACUUM';
    return {
      type: 'OK',
      message: `${mode}: ${totalDead} dead tuples removed, ${totalBytes} bytes freed, ${totalPages} pages compacted${hotPruned ? `, ${hotPruned} HOT chains pruned` : ''}${ast.incremental ? `, ${totalPagesProcessed} pages processed, ${allDone ? 'COMPLETE' : 'IN PROGRESS'}` : ''}`,
      details: { 
        deadTuplesRemoved: totalDead, 
        bytesFreed: totalBytes, 
        pagesCompacted: totalPages,
        hotPruned,
        ...(ast.incremental && { pagesProcessed: totalPagesProcessed, done: allDone }),
      },
    };
  }

  // Prepared statements
  _copy(ast) {
    if (ast.direction === 'TO') {
      return this._copyTo(ast);
    } else if (ast.direction === 'FROM') {
      return this._copyFrom(ast);
    }
    throw new Error('COPY requires FROM or TO direction');
  }

  _copyTo(ast) {
    const { format, header, delimiter } = ast.options;
    let rows;
    
    if (ast.query) {
      // COPY (query) TO
      const result = this.execute(ast.query);
      rows = result.rows;
    } else if (ast.table) {
      const result = this.execute(`SELECT * FROM ${ast.table}`);
      rows = result.rows;
    } else {
      throw new Error('COPY TO requires a table or query');
    }

    if (!rows || rows.length === 0) {
      return { type: 'COPY', data: '', rowCount: 0 };
    }

    const columns = Object.keys(rows[0]);
    const lines = [];

    if (header) {
      lines.push(columns.join(delimiter));
    }

    for (const row of rows) {
      const values = columns.map(col => {
        const val = row[col];
        if (val === null || val === undefined) return '\\N';
        const str = String(val);
        if (format === 'csv' && (str.includes(delimiter) || str.includes('"') || str.includes('\n'))) {
          return `"${str.replace(/"/g, '""')}"`;
        }
        return str;
      });
      lines.push(values.join(delimiter));
    }

    return { type: 'COPY', data: lines.join('\n'), rowCount: rows.length };
  }

  _copyFrom(ast) {
    if (!ast.table) throw new Error('COPY FROM requires a table name');
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table '${ast.table}' does not exist`);

    const data = ast.source;
    if (!data || data === 'STDIN') {
      return { type: 'COPY', message: 'COPY FROM STDIN ready', rowCount: 0 };
    }

    const { format, header, delimiter } = ast.options;
    const lines = data.split('\n').filter(l => l.trim());
    
    let startIdx = 0;
    let columnNames = table.schema.map(c => c.name);
    
    if (header && lines.length > 0) {
      const headerLine = lines[0];
      columnNames = this._parseCsvLine(headerLine, delimiter);
      startIdx = 1;
    }

    let rowCount = 0;
    for (let i = startIdx; i < lines.length; i++) {
      const values = this._parseCsvLine(lines[i], delimiter);
      // Build INSERT statement
      const insertValues = values.map((v, idx) => {
        if (v === '\\N' || v === '') return 'NULL';
        const col = table.schema[idx];
        if (!col) return `'${v.replace(/'/g, "''")}'`;
        const type = (col.type || '').toUpperCase();
        if (['INT', 'INTEGER', 'FLOAT', 'DOUBLE', 'REAL', 'NUMERIC', 'DECIMAL', 'BIGINT', 'SMALLINT', 'SERIAL'].includes(type)) {
          const num = Number(v);
          return isNaN(num) ? `'${v.replace(/'/g, "''")}'` : String(num);
        }
        return `'${v.replace(/'/g, "''")}'`;
      }).join(', ');

      this.execute(`INSERT INTO ${ast.table} VALUES (${insertValues})`);
      rowCount++;
    }

    return { type: 'COPY', message: `COPY ${rowCount}`, rowCount };
  }

  // LISTEN/NOTIFY/UNLISTEN
  _listen(ast) {
    if (!this._channels) this._channels = new Map();
    if (!this._channels.has(ast.channel)) {
      this._channels.set(ast.channel, new Set());
    }
    // Track that this session/connection is listening
    // For now, just register the channel
    if (!this._notifications) this._notifications = [];
    return { type: 'OK', message: `LISTEN "${ast.channel}"` };
  }

  _notify(ast) {
    if (!this._channels) this._channels = new Map();
    const channel = ast.channel;
    const payload = ast.payload || '';
    
    // Queue notification for listeners
    if (!this._pendingNotifications) this._pendingNotifications = [];
    this._pendingNotifications.push({
      channel,
      payload,
      pid: process.pid || 0,
      timestamp: Date.now(),
    });

    // Emit to any registered callbacks
    const callbacks = this._channels.get(channel);
    if (callbacks) {
      for (const cb of callbacks) {
        try { cb({ channel, payload }); } catch (e) { /* ignore */ }
      }
    }

    return { type: 'OK', message: 'NOTIFY' };
  }

  _unlisten(ast) {
    if (!this._channels) this._channels = new Map();
    if (ast.channel === '*') {
      this._channels.clear();
    } else {
      this._channels.delete(ast.channel);
    }
    return { type: 'OK', message: `UNLISTEN "${ast.channel}"` };
  }

  // Register a callback for LISTEN notifications (programmatic API)
  onNotify(channel, callback) {
    if (!this._channels) this._channels = new Map();
    if (!this._channels.has(channel)) {
      this._channels.set(channel, new Set());
    }
    this._channels.get(channel).add(callback);
    return () => this._channels.get(channel)?.delete(callback); // Return unsubscribe function
  }

  // Get pending notifications (for polling mode)
  getNotifications() {
    const pending = this._pendingNotifications || [];
    this._pendingNotifications = [];
    return pending;
  }

    // TRUNCATE support
  _truncate(ast) {
    let totalRows = 0;
    for (const tableName of ast.tables) {
      const table = this.tables.get(tableName);
      if (!table) throw new Error(`Table '${tableName}' does not exist`);
      
      totalRows += table.heap.tupleCount;
      
      // Clear heap
      table.heap.pages = [];
      table.heap.nextPageId = 0;
      if (table.heap._hotChains) table.heap._hotChains.clear();
      if (table.heap._hotRedirected) table.heap._hotRedirected.clear();
      
      // Clear all indexes
      for (const [colName] of table.indexes) {
        table.indexes.set(colName, new BPlusTree());
      }
    }
    return { type: 'OK', message: `TRUNCATE TABLE (${totalRows} rows removed)` };
  }

  // SAVEPOINT support
  _savepoint(ast) {
    if (!this._inTransaction) {
      throw new Error('SAVEPOINT can only be used within a transaction');
    }
    if (!this._savepoints) this._savepoints = new Map();
    const name = ast.name.toLowerCase();
    
    // Snapshot current table data
    const snapshot = new Map();
    for (const [tableName, table] of this.tables) {
      const rows = [...table.heap.scan()].map(r => ({
        pageId: r.pageId,
        slotIdx: r.slotIdx,
        values: [...r.values],
      }));
      snapshot.set(tableName, {
        schema: [...table.schema],
        rows,
      });
    }
    
    this._savepoints.set(name, snapshot);
    return { type: 'OK', message: `SAVEPOINT "${name}"` };
  }

  _releaseSavepoint(ast) {
    if (!this._savepoints) this._savepoints = new Map();
    const name = ast.name.toLowerCase();
    if (!this._savepoints.has(name)) {
      throw new Error(`Savepoint "${name}" does not exist`);
    }
    this._savepoints.delete(name);
    return { type: 'OK', message: `RELEASE SAVEPOINT "${name}"` };
  }

  _rollbackTo(ast) {
    if (!this._savepoints) this._savepoints = new Map();
    const name = ast.savepoint.toLowerCase();
    if (!this._savepoints.has(name)) {
      throw new Error(`Savepoint "${name}" does not exist`);
    }
    
    const snapshot = this._savepoints.get(name);
    
    // Restore table data from snapshot
    for (const [tableName, tableSnapshot] of snapshot) {
      const table = this.tables.get(tableName);
      if (!table) continue;
      
      // Clear current heap
      table.heap.pages = [];
      table.heap.nextPageId = 0;
      if (table.heap._hotChains) table.heap._hotChains.clear();
      if (table.heap._hotRedirected) table.heap._hotRedirected.clear();
      
      // Re-insert snapshot rows (this will auto-update indexes through normal insert path)
      // First, temporarily disable indexes to avoid stale entries
      const savedIndexes = new Map(table.indexes);
      
      // Clear all indexes by recreating them
      for (const [colName] of table.indexes) {
        table.indexes.set(colName, new BPlusTree());
      }
      
      // Re-insert all rows
      for (const row of tableSnapshot.rows) {
        const rid = table.heap.insert(row.values);
        // Re-index each column
        for (const [colName, index] of table.indexes) {
          const colIdx = table.schema.findIndex(c => c.name === colName);
          if (colIdx !== -1) {
            index.insert(row.values[colIdx], rid);
          }
        }
      }
    }
    
    // Delete all savepoints AFTER this one (PostgreSQL behavior)
    const spNames = [...this._savepoints.keys()];
    const idx = spNames.indexOf(name);
    for (let i = idx + 1; i < spNames.length; i++) {
      this._savepoints.delete(spNames[i]);
    }
    
    return { type: 'OK', message: `ROLLBACK TO SAVEPOINT "${name}"` };
  }

  // CURSOR support

  // SEQUENCE support
  _createSequence(ast) {
    if (!this._sequences) this._sequences = new Map();
    const name = ast.name.toLowerCase();
    if (this._sequences.has(name)) {
      throw new Error(`Sequence "${name}" already exists`);
    }
    this._sequences.set(name, {
      current: ast.options.start - ast.options.increment, // Will be incremented on first nextval
      start: ast.options.start,
      increment: ast.options.increment,
      minValue: ast.options.minValue,
      maxValue: ast.options.maxValue,
      cycle: ast.options.cycle,
      called: false,
    });
    return { type: 'OK', message: `CREATE SEQUENCE "${name}"` };
  }

  _createFunction(ast) {
    const name = ast.name.toLowerCase();
    if (this._functions.has(name) && !ast.orReplace) {
      throw new Error(`Function "${name}" already exists`);
    }
    // Parse the body SQL expression once
    let bodyAst;
    try {
      bodyAst = parse(ast.body);
    } catch {
      // If it doesn't parse as a full statement, try wrapping in SELECT
      try {
        bodyAst = parse('SELECT ' + ast.body);
      } catch (e2) {
        throw new Error(`Invalid function body: ${e2.message}`);
      }
    }
    this._functions.set(name, {
      params: ast.params,
      returnType: ast.returnType,
      body: ast.body,
      bodyAst,
      language: ast.language || 'sql',
    });
    return { type: 'OK', message: `CREATE FUNCTION ${name}` };
  }

  _prepare(ast) {
    const name = ast.name.toLowerCase();
    if (this._preparedStatements.has(name)) {
      throw new Error(`Prepared statement "${name}" already exists`);
    }
    this._preparedStatements.set(name, {
      statement: ast.statement || (ast.sql ? parse(ast.sql) : null),
      paramTypes: ast.paramTypes || [],
    });
    return { type: 'OK', message: `PREPARE ${name}` };
  }

  _executePrepared(ast) {
    const name = ast.name.toLowerCase();
    const ps = this._preparedStatements.get(name);
    if (!ps) {
      throw new Error(`Prepared statement "${name}" does not exist`);
    }
    
    // Substitute $1, $2, etc. with parameter values
    const stmt = JSON.parse(JSON.stringify(ps.statement)); // Deep clone AST
    const paramValues = ast.params || [];
    
    // Walk the AST and replace parameter references
    this._substituteParams(stmt, paramValues);
    
    return this.execute_ast(stmt);
  }

  _substituteParams(node, params) {
    if (!node || typeof node !== 'object') return;
    
    // Replace PARAM nodes ($1, $2, etc.)
    if (node.type === 'PARAM' || node.type === 'param') {
      const idx = (node.index || node.number || 1) - 1;
      if (idx >= 0 && idx < params.length) {
        const val = this._evalValue(params[idx], {});
        Object.assign(node, { type: 'literal', value: val });
      }
      return;
    }
    
    for (const key of Object.keys(node)) {
      const val = node[key];
      if (Array.isArray(val)) {
        for (const item of val) {
          if (item && typeof item === 'object') this._substituteParams(item, params);
        }
      } else if (val && typeof val === 'object') {
        this._substituteParams(val, params);
      }
    }
  }

  _deallocate(ast) {
    if (ast.all || (ast.name && ast.name.toUpperCase() === 'ALL')) {
      this._preparedStatements.clear();
      return { type: 'OK', message: 'DEALLOCATE ALL' };
    }
    const name = ast.name.toLowerCase();
    if (!this._preparedStatements.has(name)) {
      throw new Error(`Prepared statement "${name}" does not exist`);
    }
    this._preparedStatements.delete(name);
    return { type: 'OK', message: `DEALLOCATE ${name}` };
  }

  _executeCopy(ast) {
    if (ast.direction === 'TO') {
      // COPY TO — export as CSV
      let rows, columns;
      if (ast.query) {
        const result = this.execute_ast(ast.query);
        rows = result.rows;
        columns = result.columns || Object.keys(rows[0] || {});
      } else {
        const table = this.tables.get(ast.table) || this.tables.get(ast.table.toLowerCase());
        if (!table) throw new Error(`Table "${ast.table}" does not exist`);
        const result = this.execute(`SELECT * FROM ${ast.table}`);
        rows = result.rows;
        columns = table.schema.map(c => c.name);
      }
      
      const delim = ast.delimiter || ',';
      const lines = [];
      if (ast.header) {
        lines.push(columns.join(delim));
      }
      for (const row of rows) {
        const vals = columns.map(c => {
          const v = row[c];
          if (v === null || v === undefined) return '';
          const s = String(v);
          // Quote if contains delimiter, newline, or quotes
          if (s.includes(delim) || s.includes('\n') || s.includes('"')) {
            return '"' + s.replace(/"/g, '""') + '"';
          }
          return s;
        });
        lines.push(vals.join(delim));
      }
      
      return { type: 'COPY', data: lines.join('\n'), rows: rows.length };
    }
    
    if (ast.direction === 'FROM' && ast.target) {
      // COPY FROM — import CSV (from provided data string)
      // Note: file system access not available in all contexts,
      // so we also support COPY FROM STDIN with data provided separately
      return { type: 'COPY_FROM', table: ast.table, header: ast.header, delimiter: ast.delimiter };
    }
    
    throw new Error('Unsupported COPY direction');
  }

  _evalDefault(val) {
    if (val && typeof val === 'object' && val.type === 'function_call') {
      return this._evalFunction(val.func, val.args || [], {});
    }
    return val;
  }

  _renameCTEColumns(rows, columnList) {
    if (!columnList || columnList.length === 0) return rows;
    return rows.map(row => {
      const keys = Object.keys(row);
      const newRow = {};
      for (let i = 0; i < keys.length; i++) {
        const newName = i < columnList.length ? columnList[i] : keys[i];
        newRow[newName] = row[keys[i]];
      }
      return newRow;
    });
  }

  _splitPredicates(expr, tableAlias, tableCols) {
    // Split an AND-connected predicate into parts that reference only the given table
    // and parts that reference other tables
    if (!expr) return { left: null, rest: null };
    
    if (expr.type === 'AND') {
      const leftSplit = this._splitPredicates(expr.left, tableAlias, tableCols);
      const rightSplit = this._splitPredicates(expr.right, tableAlias, tableCols);
      
      const left = leftSplit.left && rightSplit.left 
        ? { type: 'AND', left: leftSplit.left, right: rightSplit.left }
        : leftSplit.left || rightSplit.left;
      const rest = leftSplit.rest && rightSplit.rest
        ? { type: 'AND', left: leftSplit.rest, right: rightSplit.rest }
        : leftSplit.rest || rightSplit.rest;
      
      return { left, rest };
    }
    
    // Check if this expression only references columns from the given table
    const refs = this._collectColumnRefs(expr);
    const onlyLeft = refs.every(ref => {
      const name = ref.name || ref.column || '';
      const table = ref.table || '';
      // Matches if: (a) table-qualified to our alias, (b) unqualified and column exists in our table
      if (table && table.toUpperCase() === tableAlias.toUpperCase()) return true;
      if (!table && tableCols.has(name)) return true;
      if (!table && tableCols.has(name.toUpperCase())) return true;
      if (!table && tableCols.has(name.toLowerCase())) return true;
      return false;
    });
    
    if (onlyLeft) return { left: expr, rest: null };
    return { left: null, rest: expr };
  }

  _buildExplainPlan(stmt, analyze) {
    const tableName = stmt.from?.table;
    const plan = [{
      'Plan': {
        'Node Type': stmt.where ? 'Seq Scan' : 'Seq Scan',
        'Relation Name': tableName || '(derived)',
        'Alias': tableName || '(derived)',
        'Filter': stmt.where ? this._exprToString(stmt.where) : undefined,
      }
    }];
    
    if (analyze) {
      const t0 = performance.now();
      const result = this.execute_ast(stmt);
      const elapsed = performance.now() - t0;
      plan[0].Plan['Actual Rows'] = result.rows?.length || 0;
      plan[0].Plan['Actual Total Time'] = +elapsed.toFixed(3);
      plan[0]['Execution Time'] = +elapsed.toFixed(3);
    }
    
    return plan;
  }

  _exprToString(expr) {
    if (!expr) return '';
    if (expr.type === '=' || expr.type === '!=' || expr.type === '<' || expr.type === '>') {
      return `(${this._exprToString(expr.left)} ${expr.type} ${this._exprToString(expr.right)})`;
    }
    if (expr.type === 'AND') return `(${this._exprToString(expr.left)} AND ${this._exprToString(expr.right)})`;
    if (expr.type === 'OR') return `(${this._exprToString(expr.left)} OR ${this._exprToString(expr.right)})`;
    if (expr.type === 'arith') return `(${this._exprToString(expr.left)} ${expr.op} ${this._exprToString(expr.right)})`;
    if (expr.type === 'column' || expr.type === 'column_ref') return expr.name || `${expr.table}.${expr.column}`;
    if (expr.type === 'literal' || expr.type === 'number') return String(expr.value);
    if (expr.type === 'string') return `'${expr.value}'`;
    if (expr.type === 'function_call') return `${expr.func}(${(expr.args || []).map(a => this._exprToString(a)).join(', ')})`;
    return JSON.stringify(expr);
  }

  _executeComment(ast) {
    const key = ast.columnName 
      ? `${ast.objectType}.${ast.objectName}.${ast.columnName}`
      : `${ast.objectType}.${ast.objectName}`;
    if (ast.comment === null) {
      this._comments.delete(key);
    } else {
      this._comments.set(key, ast.comment);
    }
    return { type: 'OK', message: `COMMENT` };
  }

  _executeMerge(ast) {
    const targetTable = this.tables.get(ast.target) || this.tables.get(ast.target.toLowerCase());
    if (!targetTable) throw new Error(`Table "${ast.target}" does not exist`);
    
    // Get source rows
    let sourceRows;
    if (ast.source.type === 'subquery') {
      sourceRows = this.execute_ast(ast.source.query).rows;
    } else {
      sourceRows = this.execute(`SELECT * FROM ${ast.source.name}`).rows;
    }
    
    // Get target rows
    const targetRows = this.execute(`SELECT * FROM ${ast.target}`).rows;
    
    let inserted = 0, updated = 0, deleted = 0;
    
    for (const srcRow of sourceRows) {
      // Create combined row for condition evaluation
      const combinedRow = {};
      const srcAlias = ast.sourceAlias || ast.source.name || 'source';
      const tgtAlias = ast.targetAlias || ast.target;
      
      for (const [k, v] of Object.entries(srcRow)) {
        combinedRow[k] = v;
        combinedRow[`${srcAlias}.${k}`] = v;
      }
      
      // Find matching target row
      let matched = false;
      for (const tgtRow of targetRows) {
        const mergedRow = {};
        // Add source columns with all name variants
        for (const [k, v] of Object.entries(srcRow)) {
          mergedRow[k] = v;
          mergedRow[k.toUpperCase()] = v;
          mergedRow[k.toLowerCase()] = v;
          const sa = srcAlias;
          mergedRow[`${sa}.${k}`] = v;
          mergedRow[`${sa.toUpperCase()}.${k.toUpperCase()}`] = v;
          mergedRow[`${sa.toLowerCase()}.${k.toLowerCase()}`] = v;
        }
        // Add target columns
        for (const [k, v] of Object.entries(tgtRow)) {
          mergedRow[k] = v;
          mergedRow[k.toUpperCase()] = v;
          mergedRow[k.toLowerCase()] = v;
          const ta = tgtAlias;
          mergedRow[`${ta}.${k}`] = v;
          mergedRow[`${ta.toUpperCase()}.${k.toUpperCase()}`] = v;
          mergedRow[`${ta.toLowerCase()}.${k.toLowerCase()}`] = v;
        }
        
        if (this._evalExpr(ast.condition, mergedRow)) {
          matched = true;
          // Find WHEN MATCHED clause
          const matchedClause = ast.clauses.find(c => c.matched === true);
          if (matchedClause) {
            if (matchedClause.action === 'UPDATE') {
              // Evaluate SET expressions against the merged row
              const pkCol = targetTable.schema.find(c => c.primaryKey);
              if (pkCol) {
                const pkVal = tgtRow[pkCol.name];
                for (const s of matchedClause.sets) {
                  const newVal = this._evalValue(s.value, mergedRow);
                  const valStr = newVal === null ? 'NULL' : typeof newVal === 'string' ? "'" + newVal.replace(/'/g, "''") + "'" : newVal;
                  this.execute(`UPDATE ${ast.target} SET ${s.column} = ${valStr} WHERE ${pkCol.name} = ${typeof pkVal === 'string' ? "'" + pkVal + "'" : pkVal}`);
                }
                updated++;
              }
            } else if (matchedClause.action === 'DELETE') {
              const pkCol = targetTable.schema.find(c => c.primaryKey);
              if (pkCol) {
                const pkVal = tgtRow[pkCol.name];
                this.execute(`DELETE FROM ${ast.target} WHERE ${pkCol.name} = ${typeof pkVal === 'string' ? "'" + pkVal + "'" : pkVal}`);
                deleted++;
              }
            }
          }
          break;
        }
      }
      
      if (!matched) {
        // Find WHEN NOT MATCHED clause
        const notMatchedClause = ast.clauses.find(c => c.matched === false);
        if (notMatchedClause && notMatchedClause.action === 'INSERT') {
          const vals = notMatchedClause.values.map(v => this._evalValue(v, combinedRow));
          const valStr = vals.map(v => v === null ? 'NULL' : typeof v === 'string' ? "'" + v.replace(/'/g, "''") + "'" : v).join(', ');
          if (notMatchedClause.columns) {
            this.execute(`INSERT INTO ${ast.target} (${notMatchedClause.columns.join(', ')}) VALUES (${valStr})`);
          } else {
            this.execute(`INSERT INTO ${ast.target} VALUES (${valStr})`);
          }
          inserted++;
        }
      }
    }
    
    return { type: 'MERGE', inserted, updated, deleted };
  }

  _callUserFunction(funcDef, args, row) {
    // Evaluate arguments
    const argValues = args.map(a => this._evalValue(a, row));
    
    // Create a parameter binding row: param_name → value (both cases for case-insensitive matching)
    const paramRow = { ...row };
    for (let i = 0; i < funcDef.params.length; i++) {
      const val = argValues[i] !== undefined ? argValues[i] : null;
      const name = funcDef.params[i].name;
      paramRow[name] = val;
      paramRow[name.toUpperCase()] = val;
      paramRow[name.toLowerCase()] = val;
    }
    
    if (funcDef.language === 'sql') {
      // SQL function: evaluate the body expression with params as the row context
      const ast = funcDef.bodyAst;
      if (ast.type === 'SELECT' && !ast.from) {
        const col = ast.columns[0];
        // Resolve the column value depending on its type
        if (col.type === 'column' || col.type === 'column_ref') {
          const name = col.name;
          return paramRow[name] !== undefined ? paramRow[name] : null;
        }
        if (col.type === 'function') {
          return this._evalFunction(col.func, col.args, paramRow);
        }
        if (col.type === 'expression') {
          return this._evalValue(col.expr, paramRow);
        }
        return this._evalValue(col, paramRow);
      }
      // Full SELECT with FROM: execute as subquery
      const result = this._select(ast);
      if (!result.rows || result.rows.length === 0) return null;
      return Object.values(result.rows[0])[0];
    }
    
    throw new Error(`Unsupported function language: ${funcDef.language}`);
  }

  _dropSequence(ast) {
    if (!this._sequences) this._sequences = new Map();
    const name = ast.name.toLowerCase();
    if (!this._sequences.has(name) && !ast.ifExists) {
      throw new Error(`Sequence "${name}" does not exist`);
    }
    this._sequences.delete(name);
    return { type: 'OK', message: `DROP SEQUENCE "${name}"` };
  }

  _nextval(seqName) {
    if (!this._sequences) this._sequences = new Map();
    const name = seqName.toLowerCase();
    const seq = this._sequences.get(name);
    if (!seq) throw new Error(`Sequence "${name}" does not exist`);
    
    seq.current += seq.increment;
    if (seq.current > seq.maxValue) {
      if (seq.cycle) {
        seq.current = seq.minValue;
      } else {
        throw new Error(`Sequence "${name}" reached maximum value`);
      }
    }
    seq.called = true;
    return seq.current;
  }

  _currval(seqName) {
    if (!this._sequences) this._sequences = new Map();
    const name = seqName.toLowerCase();
    const seq = this._sequences.get(name);
    if (!seq) throw new Error(`Sequence "${name}" does not exist`);
    if (!seq.called) throw new Error(`currval of sequence "${name}" is not yet defined in this session`);
    return seq.current;
  }

  _setval(seqName, value) {
    if (!this._sequences) this._sequences = new Map();
    const name = seqName.toLowerCase();
    const seq = this._sequences.get(name);
    if (!seq) throw new Error(`Sequence "${name}" does not exist`);
    seq.current = value;
    seq.called = true;
    return value;
  }

  // CURSOR support
  _declareCursor(ast) {
    if (!this._cursors) this._cursors = new Map();
    const name = ast.name.toLowerCase();
    if (this._cursors.has(name)) {
      throw new Error(`Cursor "${name}" already exists`);
    }
    
    // Execute the query and materialize all rows
    const result = this.execute(ast.query);
    this._cursors.set(name, {
      rows: result.rows,
      columns: result.rows.length > 0 ? Object.keys(result.rows[0]) : [],
      position: 0,
    });
    
    return { type: 'OK', message: `DECLARE CURSOR "${name}"` };
  }

  _fetch(ast) {
    if (!this._cursors) this._cursors = new Map();
    const name = ast.name.toLowerCase();
    const cursor = this._cursors.get(name);
    if (!cursor) {
      throw new Error(`Cursor "${name}" does not exist`);
    }

    let rows;
    if (ast.direction === 'FIRST') {
      cursor.position = 0;
      rows = cursor.rows.slice(0, 1);
      cursor.position = 1;
    } else if (ast.count === Infinity) {
      // FETCH ALL
      rows = cursor.rows.slice(cursor.position);
      cursor.position = cursor.rows.length;
    } else {
      // FETCH n
      const count = ast.count || 1;
      rows = cursor.rows.slice(cursor.position, cursor.position + count);
      cursor.position += rows.length;
    }

    return {
      type: 'SELECT',
      rows,
      columns: cursor.columns,
    };
  }

  _closeCursor(ast) {
    if (!this._cursors) this._cursors = new Map();
    if (ast.name === 'ALL') {
      this._cursors.clear();
    } else {
      const name = ast.name.toLowerCase();
      if (!this._cursors.has(name)) {
        throw new Error(`Cursor "${name}" does not exist`);
      }
      this._cursors.delete(name);
    }
    return { type: 'OK', message: `CLOSE "${ast.name}"` };
  }

  _parseCsvLine(line, delimiter = ',') {
    const values = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (inQuotes) {
        if (ch === '"') {
          if (i + 1 < line.length && line[i + 1] === '"') {
            current += '"';
            i++;
          } else {
            inQuotes = false;
          }
        } else {
          current += ch;
        }
      } else {
        if (ch === '"') {
          inQuotes = true;
        } else if (ch === delimiter) {
          values.push(current);
          current = '';
        } else {
          current += ch;
        }
      }
    }
    values.push(current);
    return values;
  }

  // Serialization
  toJSON() {
    const data = {
      tables: {},
      views: {},
      sequences: {},
      indexCatalog: {},
    };

    for (const [name, table] of this.tables) {
      const rows = [...table.heap.scan()].map(r => r.values);
      data.tables[name] = {
        schema: table.schema,
        rows,
        indexes: [...table.indexes.keys()],
      };
    }

    for (const [name, view] of this.views) {
      data.views[name] = view;
    }

    if (this._sequences) {
      for (const [name, seq] of this._sequences) {
        data.sequences[name] = { ...seq };
      }
    }

    for (const [name, meta] of this.indexCatalog) {
      data.indexCatalog[name] = meta;
    }

    return data;
  }

  static fromJSON(json) {
    const db = new Database();
    
    // Recreate tables
    for (const [name, tableData] of Object.entries(json.tables || {})) {
      // Build CREATE TABLE SQL from schema
      const cols = tableData.schema.map(c => {
        let def = `${c.name} ${c.type || 'TEXT'}`;
        if (c.primaryKey) def += ' PRIMARY KEY';
        if (c.notNull) def += ' NOT NULL';
        if (c.unique) def += ' UNIQUE';
        if (c.defaultValue !== undefined && c.defaultValue !== null) def += ` DEFAULT ${typeof c.defaultValue === 'string' ? `'${c.defaultValue}'` : c.defaultValue}`;
        return def;
      }).join(', ');
      
      db.execute(`CREATE TABLE ${name} (${cols})`);
      
      // Insert rows
      for (const row of tableData.rows) {
        const vals = row.map(v => {
          if (v === null) return 'NULL';
          if (typeof v === 'string') return `'${v.replace(/'/g, "''")}'`;
          return String(v);
        }).join(', ');
        db.execute(`INSERT INTO ${name} VALUES (${vals})`);
      }
      
      // Recreate indexes
      for (const colName of tableData.indexes) {
        // Skip PK index (auto-created)
        const isPk = tableData.schema.some(c => c.name === colName && c.primaryKey);
        if (!isPk) {
          // Check indexCatalog for uniqueness info
          const isUnique = Object.values(json.indexCatalog || {}).some(
            meta => meta.table === name && meta.unique && (
              meta.columns === colName ||
              (Array.isArray(meta.columns) && meta.columns.length === 1 && meta.columns[0] === colName) ||
              (Array.isArray(meta.columns) && meta.columns.join(',') === colName)
            )
          );
          const uniqueKw = isUnique ? 'UNIQUE ' : '';
          db.execute(`CREATE ${uniqueKw}INDEX idx_${name}_${colName} ON ${name} (${colName})`);
        }
      }
    }

    // Recreate views
    for (const [name, viewData] of Object.entries(json.views || {})) {
      if (viewData.sql) {
        db.execute(`CREATE VIEW ${name} AS ${viewData.sql}`);
      } else if (viewData.query) {
        // Views stored as AST — restore directly
        db.views.set(name, viewData);
      }
    }

    // Recreate sequences
    for (const [name, seqData] of Object.entries(json.sequences || {})) {
      db.execute(`CREATE SEQUENCE ${name} START WITH ${seqData.start} INCREMENT BY ${seqData.increment}${seqData.cycle ? ' CYCLE' : ''}`);
      if (seqData.called) {
        db._sequences.get(name).current = seqData.current;
        db._sequences.get(name).called = true;
      }
    }

    return db;
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

  _explain(ast) {
    const stmt = ast.statement;

    // EXPLAIN (FORMAT JSON) — return structured plan
    if (ast.format === 'JSON') {
      const plan = this._buildExplainPlan(stmt, ast.analyze);
      return { rows: [{ 'QUERY PLAN': JSON.stringify(plan, null, 2) }], json: plan };
    }

    // EXPLAIN COMPILED: show the compiled query plan
    if (ast.compiled) {
      return this._explainCompiled(stmt);
    }

    // EXPLAIN ANALYZE: execute the query and measure actual performance
    if (ast.analyze) {
      return this._explainAnalyze(stmt);
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
      if (!hasJoins && stmt.where) {
        const indexScan = this._tryIndexScan(table, stmt.where, stmt.from.alias || tableName);
        if (indexScan !== null) {
          // Find which index was used
          const colName = this._findIndexedColumn(stmt.where);
          plan.push({ operation: 'INDEX_SCAN', table: tableName, index: colName, estimated_rows: indexScan.rows.length });
          if (indexScan.residual) {
            plan.push({ operation: 'FILTER', condition: 'residual' });
          }
        } else {
          plan.push({ operation: 'TABLE_SCAN', table: tableName, estimated_rows: table.heap.rowCount || '?' });
          plan.push({ operation: 'FILTER', condition: 'WHERE' });
        }
      } else {
        plan.push({ operation: 'TABLE_SCAN', table: tableName, estimated_rows: table.heap.rowCount || '?' });
      }

      // Joins
      for (const join of stmt.joins || []) {
        const joinTable = join.table?.table || join.table;
        plan.push({
          operation: 'NESTED_LOOP_JOIN',
          type: join.type || 'INNER',
          table: joinTable,
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
      plan.push({ operation: 'SORT', columns: stmt.orderBy.map(o => `${o.column} ${o.direction}`) });
    }

    // DISTINCT
    if (stmt.distinct) {
      plan.push({ operation: 'DISTINCT' });
    }

    // LIMIT
    if (stmt.limit) {
      plan.push({ operation: 'LIMIT', count: stmt.limit });
    }

    return { type: 'PLAN', plan };
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
    let baseRows = baseResult.rows;
    
    // Apply column list renaming if specified
    if (cte.columnList) {
      baseRows = this._renameCTEColumns(baseRows, cte.columnList);
    }
    
    const columnNames = Object.keys(baseRows[0] || {}).filter(k => !k.includes('.'));
    let allRows = [...baseRows];
    let workingSet = [...baseRows];

    // Step 2: Iterate until fixed point
    for (let iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
      if (workingSet.length === 0) break;

      // Register current working set as the CTE view
      this.views.set(cte.name, { materializedRows: workingSet, isCTE: true });

      // Execute recursive part
      const recursiveResult = this._select(recursiveQuery);
      let newRows = recursiveResult.rows;

      if (newRows.length === 0) break;

      // Normalize column names to match base query (positional mapping)
      if (columnNames.length > 0 && newRows.length > 0) {
        const recKeys = Object.keys(newRows[0]);
        // Only remap if column names differ
        const needsRemap = recKeys.some((k, i) => i < columnNames.length && k !== columnNames[i]);
        if (needsRemap) {
          newRows = newRows.map(row => {
            const normalized = {};
            const vals = Object.values(row);
            for (let i = 0; i < columnNames.length; i++) {
              normalized[columnNames[i]] = vals[i] !== undefined ? vals[i] : null;
            }
            return normalized;
          });
        }
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
    // Get planner estimates
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

    // Build analyze output — tree structure like PG
    const nodes = [];
    
    // Root node: scan type
    const tableName = stmt.from?.table;
    let scanNode = null;
    if (tableName && this.tables.has(tableName)) {
      const table = this.tables.get(tableName);
      const totalRows = table.heap.tupleCount || 0;
      const estimatedRows = plannerEstimate?.estimatedRows || totalRows;
      const scanType = plannerEstimate?.scanType || 'Seq Scan';
      
      scanNode = {
        node: scanType === 'INDEX_SCAN' ? 'Index Scan' : 'Seq Scan',
        relation: tableName,
        estimated_rows: estimatedRows,
        actual_rows: totalRows, // rows scanned (before filter)
        estimated_cost: plannerEstimate?.cost || '?',
        actual_time_ms: parseFloat(executionTime.toFixed(3)),
        rows_removed_by_filter: totalRows - actualRows,
      };

      if (plannerEstimate?.indexColumn) {
        scanNode.node = `Index Scan using idx_${tableName}_${plannerEstimate.indexColumn}`;
        scanNode.index_cond = `${plannerEstimate.indexColumn} = ?`;
      }

      if (stmt.where) {
        scanNode.filter = this._exprToString(stmt.where);
      }

      nodes.push(scanNode);
    }

    // Join nodes
    for (const join of stmt.joins || []) {
      const joinTable = join.table?.table || join.table;
      const joinNode = {
        node: `${(join.joinType || 'INNER').replace('_', ' ')} Join`,
        relation: joinTable,
        join_condition: join.on ? this._exprToString(join.on) : null,
      };
      nodes.push(joinNode);
    }

    // Aggregation
    if (stmt.groupBy) {
      nodes.push({
        node: 'HashAggregate',
        group_key: stmt.groupBy.map(g => g.name || g.value).join(', '),
        actual_groups: actualRows,
      });
    }

    // Sort
    if (stmt.orderBy) {
      const sortKeys = stmt.orderBy.map(o => `${o.column || o.value} ${o.direction || 'ASC'}`).join(', ');
      nodes.push({
        node: 'Sort',
        sort_key: sortKeys,
        actual_rows: actualRows,
      });
    }

    // LIMIT
    if (stmt.limit !== undefined && stmt.limit !== null) {
      nodes.push({
        node: 'Limit',
        limit: stmt.limit,
        actual_rows: actualRows,
      });
    }

    // Build text output like PG's EXPLAIN ANALYZE
    const textLines = [];
    const indent = (depth) => '  '.repeat(depth);
    for (let i = 0; i < nodes.length; i++) {
      const n = nodes[i];
      const prefix = i === 0 ? '' : '->  ';
      const depth = i;
      let line = `${indent(depth)}${prefix}${n.node}`;
      if (n.relation) line += ` on ${n.relation}`;
      if (n.estimated_cost !== undefined) line += `  (cost=${n.estimated_cost})`;
      if (n.estimated_rows !== undefined) line += `  (rows=${n.estimated_rows})`;
      textLines.push(line);
      
      if (n.actual_time_ms !== undefined) {
        textLines.push(`${indent(depth + 1)}actual time=${n.actual_time_ms}ms rows=${n.actual_rows || actualRows}`);
      }
      if (n.filter) {
        textLines.push(`${indent(depth + 1)}Filter: ${n.filter}`);
        if (n.rows_removed_by_filter > 0) {
          textLines.push(`${indent(depth + 1)}Rows Removed by Filter: ${n.rows_removed_by_filter}`);
        }
      }
      if (n.index_cond) {
        textLines.push(`${indent(depth + 1)}Index Cond: (${n.index_cond})`);
      }
      if (n.sort_key) {
        textLines.push(`${indent(depth + 1)}Sort Key: ${n.sort_key}`);
      }
      if (n.group_key) {
        textLines.push(`${indent(depth + 1)}Group Key: ${n.group_key}`);
      }
    }
    textLines.push(`Planning Time: ${Math.max(0.001, executionTime * 0.1).toFixed(3)} ms`);
    textLines.push(`Execution Time: ${executionTime.toFixed(3)} ms`);

    return {
      type: 'ANALYZE',
      plan: nodes,
      text: textLines.join('\n'),
      execution_time_ms: parseFloat(executionTime.toFixed(3)),
      actual_rows: actualRows,
      estimated_rows: plannerEstimate?.estimatedRows || '?',
      estimation_accuracy: plannerEstimate?.estimatedRows
        ? parseFloat((actualRows / plannerEstimate.estimatedRows).toFixed(3))
        : '?',
    };
  }

  // Convert an AST expression to a readable string (for EXPLAIN output)
  _exprToString(expr) {
    if (!expr) return '?';
    if (expr.type === 'column_ref') return expr.name;
    if (expr.type === 'literal') return JSON.stringify(expr.value);
    if (expr.type === 'COMPARE') {
      const opMap = { EQ: '=', NE: '!=', '<>': '!=', GT: '>', GE: '>=', LT: '<', LE: '<=' };
      const op = opMap[expr.op] || expr.op;
      return `(${this._exprToString(expr.left)} ${op} ${this._exprToString(expr.right)})`;
    }
    if (expr.type === 'AND') {
      return `(${this._exprToString(expr.left)} AND ${this._exprToString(expr.right)})`;
    }
    if (expr.type === 'OR') {
      return `(${this._exprToString(expr.left)} OR ${this._exprToString(expr.right)})`;
    }
    if (expr.type === 'NOT') {
      return `NOT ${this._exprToString(expr.expr)}`;
    }
    if (expr.type === 'BETWEEN') {
      return `(${this._exprToString(expr.expr)} BETWEEN ${this._exprToString(expr.low)} AND ${this._exprToString(expr.high)})`;
    }
    if (expr.type === 'IN') {
      return `(${this._exprToString(expr.left)} IN (...))`;
    }
    if (expr.type === 'LIKE') {
      return `(${this._exprToString(expr.left)} LIKE ${this._exprToString(expr.right)})`;
    }
    if (expr.type === 'function') {
      return `${expr.name}(...)`;
    }
    if (expr.type === 'arith') {
      return `(${this._exprToString(expr.left)} ${expr.op} ${this._exprToString(expr.right)})`;
    }
    if (expr.type === 'function_call') {
      return `${expr.func}(${(expr.args || []).map(a => this._exprToString(a)).join(', ')})`;
    }
    if (expr.type === '=' || expr.type === '!=' || expr.type === '<' || expr.type === '>') {
      return `(${this._exprToString(expr.left)} ${expr.type} ${this._exprToString(expr.right)})`;
    }
    return '?';
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
          case 'PERCENT_RANK': {
            // PERCENT_RANK = (rank - 1) / (total - 1)
            // Same ranking logic as RANK
            let rank = 1;
            for (let i = 0; i < partition.length; i++) {
              if (i > 0 && orderBy) {
                const prev = this._resolveColumn(orderBy[0].column || orderBy[0].expr?.name || orderBy[0], partition[i - 1]);
                const curr = this._resolveColumn(orderBy[0].column || orderBy[0].expr?.name || orderBy[0], partition[i]);
                if (prev !== curr) rank = i + 1;
              }
              partition[i][`__window_${name}`] = partition.length <= 1 ? 0 : (rank - 1) / (partition.length - 1);
            }
            break;
          }
          case 'CUME_DIST': {
            // CUME_DIST = number of rows <= current row / total rows
            for (let i = 0; i < partition.length; i++) {
              // Count how many rows have values <= current (in sorted order)
              let count = i + 1;
              // Include ties
              while (count < partition.length && orderBy) {
                const curr = this._resolveColumn(orderBy[0].column || orderBy[0].expr?.name || orderBy[0], partition[i]);
                const next = this._resolveColumn(orderBy[0].column || orderBy[0].expr?.name || orderBy[0], partition[count]);
                if (curr !== next) break;
                count++;
              }
              partition[i][`__window_${name}`] = count / partition.length;
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
          case 'LEAD': {
            // LEAD(column, offset, default) — value from a following row
            const offset = col.args && col.args.length > 1 ? this._evalValue(col.args[1], {}) : 1;
            const defaultVal = col.args && col.args.length > 2 ? this._evalValue(col.args[2], {}) : null;
            for (let i = 0; i < partition.length; i++) {
              const targetIdx = i + offset;
              if (targetIdx < partition.length) {
                partition[i][`__window_${name}`] = this._resolveColumn(col.arg, partition[targetIdx]);
              } else {
                partition[i][`__window_${name}`] = defaultVal;
              }
            }
            break;
          }
          case 'LAG': {
            // LAG(column, offset, default) — value from a preceding row
            const offset = col.args && col.args.length > 1 ? this._evalValue(col.args[1], {}) : 1;
            const defaultVal = col.args && col.args.length > 2 ? this._evalValue(col.args[2], {}) : null;
            for (let i = 0; i < partition.length; i++) {
              const targetIdx = i - offset;
              if (targetIdx >= 0) {
                partition[i][`__window_${name}`] = this._resolveColumn(col.arg, partition[targetIdx]);
              } else {
                partition[i][`__window_${name}`] = defaultVal;
              }
            }
            break;
          }
          case 'FIRST_VALUE': {
            const firstVal = this._resolveColumn(col.arg, partition[0]);
            for (const r of partition) r[`__window_${name}`] = firstVal;
            break;
          }
          case 'LAST_VALUE': {
            // Without frame specification, LAST_VALUE uses the whole partition
            const lastVal = this._resolveColumn(col.arg, partition[partition.length - 1]);
            for (const r of partition) r[`__window_${name}`] = lastVal;
            break;
          }
          case 'NTILE': {
            // NTILE(n) — divide partition into n roughly equal groups
            const n = col.args && col.args.length > 0 ? this._evalValue(col.args[0], {}) : 1;
            const size = partition.length;
            for (let i = 0; i < size; i++) {
              partition[i][`__window_${name}`] = Math.floor(i * n / size) + 1;
            }
            break;
          }
          case 'NTH_VALUE': {
            // NTH_VALUE(expr, n) — value of nth row in window frame
            const nth = col.args && col.args.length > 1 ? this._evalValue(col.args[1], {}) : 1;
            const idx = nth - 1; // 1-based to 0-based
            for (const r of partition) {
              r[`__window_${name}`] = idx >= 0 && idx < partition.length 
                ? this._resolveColumn(col.arg, partition[idx])
                : null;
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
          const key = col.alias || this._exprToString(col) || JSON.stringify(col).slice(0, 20);
          result[key] = val;
        }
      }

      // Helper to compute an aggregate on this group
      const computeAgg = (func, arg, distinct, extra = {}) => {
        let filteredGroupRows = groupRows;
        if (extra.filter) {
          filteredGroupRows = groupRows.filter(r => this._evalExpr(extra.filter, r));
        }
        let values;
        if (arg === '*') {
          values = filteredGroupRows;
        } else if (typeof arg === 'object') {
          // Expression argument (e.g., SUM(qty * price))
          values = filteredGroupRows.map(r => this._evalValue(arg, r)).filter(v => v != null);
        } else {
          values = filteredGroupRows.map(r => this._resolveColumn(arg, r)).filter(v => v != null);
        }
        switch (func) {
          case 'COUNT': {
            if (distinct && arg !== '*') return new Set(values).size;
            return arg === '*' ? filteredGroupRows.length : values.length;
          }
          case 'SUM': return values.reduce((s, v) => s + v, 0);
          case 'AVG': return values.length ? values.reduce((s, v) => s + v, 0) / values.length : null;
          case 'MIN': return values.length ? values.reduce((a, b) => a < b ? a : b) : null;
          case 'MAX': return values.length ? values.reduce((a, b) => a > b ? a : b) : null;
          case 'STDDEV': case 'STDDEV_SAMP': {
            if (values.length < 2) return null;
            const nums = values.map(Number);
            const mean = nums.reduce((s, v) => s + v, 0) / nums.length;
            return Math.sqrt(nums.reduce((s, v) => s + (v - mean) ** 2, 0) / (nums.length - 1));
          }
          case 'STDDEV_POP': {
            if (!values.length) return null;
            const nums = values.map(Number);
            const mean = nums.reduce((s, v) => s + v, 0) / nums.length;
            return Math.sqrt(nums.reduce((s, v) => s + (v - mean) ** 2, 0) / nums.length);
          }
          case 'VARIANCE': case 'VAR_SAMP': {
            if (values.length < 2) return null;
            const nums = values.map(Number);
            const mean = nums.reduce((s, v) => s + v, 0) / nums.length;
            return nums.reduce((s, v) => s + (v - mean) ** 2, 0) / (nums.length - 1);
          }
          case 'VAR_POP': {
            if (!values.length) return null;
            const nums = values.map(Number);
            const mean = nums.reduce((s, v) => s + v, 0) / nums.length;
            return nums.reduce((s, v) => s + (v - mean) ** 2, 0) / nums.length;
          }
          case 'MEDIAN': {
            if (!values.length) return null;
            const sorted = values.map(Number).sort((a, b) => a - b);
            const mid = Math.floor(sorted.length / 2);
            return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
          }
          case 'GROUP_CONCAT':
          case 'STRING_AGG': {
            const sep = extra.separator || ',';
            const strs = (distinct ? [...new Set(values)] : values).map(String);
            return strs.join(sep);
          }
          case 'ARRAY_AGG':
          case 'JSON_AGG': {
            const arr = distinct ? [...new Set(values)] : values;
            return JSON.stringify(arr);
          }
          case 'BOOL_AND': return values.every(v => v) ? 1 : 0;
          case 'BOOL_OR': return values.some(v => v) ? 1 : 0;
        }
      };

      // Add aggregate and non-aggregate columns
      for (const col of ast.columns) {
        if (col.type === 'aggregate') {
          const name = col.alias || `${col.func}(${col.arg})`;
          result[name] = computeAgg(col.func, col.arg, col.distinct, { separator: col.separator, filter: col.filter });
          // Also store under canonical key for HAVING resolution
          const canonKey = `${col.func}(${col.arg})`;
          if (name !== canonKey) result[canonKey] = result[name];
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
          if (!(key in result)) {
            result[key] = computeAgg(agg.func, argStr, agg.distinct);
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
          let av, bv;
          if (typeof column === 'number') {
            av = this._resolveOrderByValue(column, a, ast);
            bv = this._resolveOrderByValue(column, b, ast);
          } else if (typeof column === 'object') {
            av = this._evalValue(column, a);
            bv = this._evalValue(column, b);
          } else {
            av = a[column] !== undefined ? a[column] : this._resolveColumn(column, a);
            bv = b[column] !== undefined ? b[column] : this._resolveColumn(column, b);
          }
          const cmp = av < bv ? -1 : av > bv ? 1 : 0;
          if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
        }
        return 0;
      });
    }

    // LIMIT
    if (ast.offset) resultRows = resultRows.slice(ast.offset);
    if (ast.limit) resultRows = resultRows.slice(0, ast.limit);

    return { type: 'ROWS', rows: resultRows };
  }

  _tryIndexScan(table, where, tableAlias) {
    if (!where) return null;

    // Simple equality: col = literal where col is indexed
    if (where.type === 'COMPARE' && where.op === 'EQ') {
      const colRef = where.left.type === 'column_ref' ? where.left : (where.right.type === 'column_ref' ? where.right : null);
      const literal = where.left.type === 'literal' ? where.left : (where.right.type === 'literal' ? where.right : null);
      if (colRef && literal) {
        const colName = colRef.name.includes('.') ? colRef.name.split('.').pop() : colRef.name;
        const index = table.indexes.get(colName);
        if (index) {
          const entries = index.range(literal.value, literal.value);
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
          // If index lookup found entries but get() returned null for all (MVCC invisible),
          // fall back to scan — the visible version might be at a different slot
          if (rows.length === 0 && entries.length > 0) return null;
          return { rows, residual: null, indexOnly: rows.length > 0 && rows[0]?.includedValues !== undefined };
        }
      }

      // Expression index: check if either side of the comparison matches an expression index
      if (table.indexMeta && literal) {
        const exprSide = where.left.type !== 'literal' ? where.left : where.right;
        for (const [idxKey, meta] of table.indexMeta) {
          if (meta.expressions && meta.expressions.some(e => e !== null)) {
            const idxExpr = meta.expressions[0];
            if (idxExpr && this._exprMatchesIndex(exprSide, idxExpr)) {
              const index = table.indexes.get(idxKey);
              if (index) {
                const entries = index.range(literal.value, literal.value);
                const rows = [];
                for (const entry of entries) {
                  const rid = entry.value;
                  const values = table.heap.get(rid.pageId, rid.slotIdx);
                  if (values) {
                    rows.push(this._valuesToRow(values, table.schema, tableAlias));
                  }
                }
                return { rows, residual: null, expressionIndex: true };
              }
            }
          }
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
    // Compute VIRTUAL generated columns
    for (let i = 0; i < schema.length; i++) {
      if (schema[i].generated && schema[i].generated.mode === 'VIRTUAL') {
        const val = this._evalValue(schema[i].generated.expression, row);
        row[schema[i].name] = val;
        row[`${tableAlias}.${schema[i].name}`] = val;
      }
    }
    return row;
  }

  // Check if a WHERE expression matches an expression index definition (structural AST comparison)
  _exprMatchesIndex(whereExpr, indexExpr) {
    if (!whereExpr || !indexExpr) return false;
    if (whereExpr.type !== indexExpr.type) return false;
    
    switch (whereExpr.type) {
      case 'function_call':
        return (whereExpr.func || whereExpr.name || '').toUpperCase() === (indexExpr.func || indexExpr.name || '').toUpperCase() &&
          whereExpr.args?.length === indexExpr.args?.length &&
          (whereExpr.args || []).every((arg, i) => this._exprMatchesIndex(arg, indexExpr.args[i]));
      case 'column_ref':
        return (whereExpr.column || whereExpr.name) === (indexExpr.column || indexExpr.name);
      case 'BINARY':
      case 'arith':
        return whereExpr.op === indexExpr.op &&
          this._exprMatchesIndex(whereExpr.left, indexExpr.left) &&
          this._exprMatchesIndex(whereExpr.right, indexExpr.right);
      case 'literal':
        return whereExpr.value === indexExpr.value;
      default:
        return JSON.stringify(whereExpr) === JSON.stringify(indexExpr);
    }
  }

  // Resolve ORDER BY column: string name, integer position (1-based), or expression object
  _resolveOrderByValue(column, row, ast) {
    if (typeof column === 'number') {
      // Column position (1-based): try projected column names first, then raw row keys
      if (ast && ast.columns) {
        const col = ast.columns[column - 1];
        if (col) {
          const name = col.alias || col.name;
          if (name && row[name] !== undefined) return row[name];
        }
      }
      const keys = Object.keys(row).filter(k => !k.includes('.'));
      const colName = keys[column - 1];
      return colName ? row[colName] : null;
    }
    if (typeof column === 'object') {
      return this._evalValue(column, row);
    }
    // String column name
    return row[column] !== undefined ? row[column] : this._resolveColumn(column, row);
  }

  // Compute values for generated columns (STORED or both modes for pre-insert)
  _computeGeneratedColumns(table, values) {
    for (let i = 0; i < table.schema.length; i++) {
      const col = table.schema[i];
      if (col.generated && col.generated.mode === 'STORED') {
        const row = {};
        for (let j = 0; j < table.schema.length; j++) {
          row[table.schema[j].name] = values[j];
        }
        values[i] = this._evalValue(col.generated.expression, row);
      }
    }
  }

  // Validate that generated columns are not directly set
  _validateNoGeneratedColumnWrites(table, columns) {
    if (!columns) return;
    for (const colName of columns) {
      const col = table.schema.find(c => c.name === colName);
      if (col && col.generated) {
        throw new Error(`Cannot INSERT or UPDATE generated column '${colName}'`);
      }
    }
  }

  /** Collect all column_ref names from an expression AST */
  _collectColumnRefs(expr) {
    if (!expr) return [];
    if (expr.type === 'column_ref') {
      return [expr];
    }
    const refs = [];
    if (expr.left) refs.push(...this._collectColumnRefs(expr.left));
    if (expr.right) refs.push(...this._collectColumnRefs(expr.right));
    if (expr.args) for (const a of expr.args) refs.push(...this._collectColumnRefs(a));
    if (expr.expr) refs.push(...this._collectColumnRefs(expr.expr));
    return refs;
  }

  // Compute index key for a given set of values, handling both column and expression indexes
  _computeIndexKey(colName, values, table, tableName) {
    const meta = table.indexMeta && table.indexMeta.get(colName);
    if (meta && meta.expressions && meta.expressions.some(e => e !== null)) {
      const row = this._valuesToRow(values, table.schema, tableName);
      const exprs = meta.expressions;
      if (exprs.length === 1) {
        const expr = exprs[0] || { type: 'column_ref', name: meta.columns[0] };
        return this._evalValue(expr, row);
      }
      return makeCompositeKey(exprs.map((expr, i) => {
        if (expr) return this._evalValue(expr, row);
        return values[table.schema.findIndex(s => s.name === meta.columns[i])];
      }));
    }
    const colIdx = table.schema.findIndex(c => c.name === colName);
    return colIdx >= 0 ? values[colIdx] : null;
  }

  // Collect aggregate_expr nodes from an expression tree (for HAVING pre-computation)

  /**
   * Try to execute joins via Volcano engine for performance.
   * Returns materialized rows if successful, null to fall back to nested loop.
   */
  _tryVolcanoJoin(ast, leftRows) {
    try {
      // Only handle INNER and LEFT joins — fall back for RIGHT, FULL, CROSS
      for (const join of ast.joins) {
        const jt = (join.joinType || 'INNER').toUpperCase();
        if (jt !== 'INNER' && jt !== 'JOIN' && jt !== 'LEFT') return null;
        // Only handle equi-joins (ON clause with = comparison)
        if (!join.on) return null;
      }
      // Build a minimal join-only AST for Volcano
      const joinAst = {
        type: 'SELECT',
        columns: [{ type: 'star' }],
        from: ast.from,
        joins: ast.joins,
        where: null,   // WHERE applied separately by db.js
        groupBy: null,
        orderBy: null,
        limit: null,
      };
      
      const plan = buildVolcanoPlan(joinAst, this.tables, this.indexCatalog);
      const rows = [];
      plan.open();
      let row;
      while ((row = plan.next()) !== null) {
        rows.push(row);
      }
      plan.close();
      return rows;
    } catch (e) {
      // If Volcano can't handle the query, fall back silently
      return null;
    }
  }
}

// Install pg_catalog and information_schema virtual table methods
installPgCatalog(Database);

// Install UNION, INTERSECT, EXCEPT operations
installSetOperations(Database);

// Install expression evaluation methods
installExpressionEvaluator(Database);

// Install vectorized execution fast path
import { VSeqScan, VFilter, VHashAggregate } from './vector-engine.js';

/**
 * Try to use vectorized execution for simple queries.
 * Returns null if the query is too complex for the vectorized path.
 */
Database.prototype._tryVectorizedSelect = function(ast) {
  // VERY conservative: only simple aggregate queries
  // Only handle: SELECT agg(col) FROM table [GROUP BY col]
  // No WHERE, no HAVING, no ORDER BY, no LIMIT, no DISTINCT, no JOINs, no subqueries
  if (ast.ctes || ast.joins?.length > 0 || !ast.from?.table) return null;
  if (ast.where || ast.having || ast.orderBy || ast.limit != null || ast.distinct) return null;
  if (ast.from.alias) return null; // Aliased tables complicate column resolution
  
  const tableName = ast.from.table;
  const table = this.tables.get(tableName);
  if (!table) return null;
  if (this.views.has(tableName)) return null;
  
  // Must have aggregates
  const hasAggregates = ast.columns?.some(c => c.type === 'aggregate');
  if (!hasAggregates) return null;
  
  // All non-aggregate columns must be simple column references (for GROUP BY)
  for (const col of ast.columns) {
    if (col.type !== 'aggregate' && col.type !== 'column') return null;
  }
  
  const schema = table.schema;
  const colNames = schema.map(c => c.name);
  const hasGroupBy = ast.groupBy && ast.groupBy.length > 0;
  
  try {
    const scan = new VSeqScan(table.heap, colNames);
    
    const groupCols = hasGroupBy ? ast.groupBy.map(g => typeof g === 'string' ? g : (g.name || g.column)) : [];
    const aggregates = [];
    
    for (const col of ast.columns) {
      if (col.type === 'aggregate') {
        const aggCol = col.arg || col.args?.[0]?.name || '*';
        const fn = (col.func || col.fn || '').toUpperCase();
        const name = col.alias || `${fn}(${aggCol})`;
        aggregates.push({ name, fn, col: aggCol === '*' ? colNames[0] : aggCol });
      }
    }
    
    const agg = new VHashAggregate(scan, groupCols, aggregates);
    agg.open();
    const rows = [];
    let batch;
    while ((batch = agg.nextBatch()) !== null) {
      rows.push(...batch.toRows());
    }
    agg.close();
    
    if (rows.length === 0) return null; // Fall back to standard path
    
    return { rows, columns: Object.keys(rows[0]) };
  } catch {
    return null;
  }
};
