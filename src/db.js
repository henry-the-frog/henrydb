// db.js — HenryDB query executor
// Ties together: HeapFile, BPlusTree, SQL parser

import { HeapFile, encodeTuple, decodeTuple } from './page.js';
import { BPlusTree } from './btree.js';
import { optimizeSelect } from './decorrelate.js';
import { QueryPlanner } from './planner.js';
import { makeCompositeKey } from './composite-key.js';
import { parse } from './sql.js';
import { WriteAheadLog } from './wal.js';
import { CompiledQueryEngine } from './compiled-query.js';
import { InvertedIndex, tokenize } from './fulltext.js';
import { PlanCache } from './plan-cache.js';
import { MVCCManager } from './mvcc.js';

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
    const sourceTable = this.tables.get(ast.source);
    if (!sourceTable) throw new Error(`Table ${ast.source} not found`);
    
    let inserted = 0, updated = 0, deleted = 0;
    
    // For each source row, check if it matches any target row
    for (const sourceEntry of sourceTable.heap.scan()) {
      const sourceRow = this._valuesToRow(sourceEntry.values, sourceTable.schema, ast.source);
      
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
    // Validate no writes to generated columns
    this._validateNoGeneratedColumnWrites(table, columns);

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
      
      // Try vectorized execution for eligible queries
      const vecResult = this._tryVectorizedExecution(optimizedAst);
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
        } else if (func === 'GROUPING' && (gb.func || '').toUpperCase() === 'GROUPING_SETS') {
          // Explicit GROUPING SETS
          groupingSets = args.map(a => {
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
        groupBy: groupSet.length > 0 ? groupSet.map(c => ({ type: 'column', name: c })) : undefined,
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
              if (!groupSet.includes(col) && !(col in row)) {
                row[col] = null;
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
      // With JOINs: full scan, apply WHERE after JOINs
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const row = this._valuesToRow(values, table.schema, ast.from.alias || ast.from.table);
        rows.push(row);
      }
    }

    // Handle JOINs
    for (const join of ast.joins || []) {
      rows = this._executeJoin(rows, join, ast.from.alias || ast.from.table);
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
      if (ast.limit) finalRows = finalRows.slice(0, ast.limit);
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
            if (expr) this._collectColumnRefs(expr).forEach(c => indexedColumns.add(c));
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

  _truncate(ast) {
    const table = this._getTable(ast.table);
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
  _prepare(ast) {
    if (!this._prepared) this._prepared = new Map();
    const name = ast.name.toLowerCase();
    if (this._prepared.has(name)) {
      throw new Error(`Prepared statement "${name}" already exists`);
    }
    this._prepared.set(name, {
      sql: ast.sql,
      paramTypes: ast.paramTypes || [],
    });
    return { type: 'OK', message: 'PREPARE' };
  }

  _executePrepared(ast) {
    if (!this._prepared) this._prepared = new Map();
    const name = ast.name.toLowerCase();
    const prepared = this._prepared.get(name);
    if (!prepared) {
      throw new Error(`Prepared statement "${name}" does not exist`);
    }

    // Evaluate parameter expressions — extract literal values
    const params = (ast.params || []).map(p => {
      if (p.type === 'literal') return p.value;
      if (p.type === 'column_ref') return p.name; // treat bare identifiers as strings
      return this._evalValue(p, {});
    });
    
    // Substitute $1, $2, etc. in the SQL with actual values
    let sql = prepared.sql;
    for (let i = params.length; i >= 1; i--) {
      const val = params[i - 1];
      let replacement;
      if (val === null) replacement = 'NULL';
      else if (typeof val === 'string') replacement = `'${val.replace(/'/g, "''")}'`;
      else replacement = String(val);
      sql = sql.replace(new RegExp(`\\$${i}`, 'g'), replacement);
    }

    return this.execute(sql);
  }

  _deallocate(ast) {
    if (!this._prepared) this._prepared = new Map();
    if (ast.name === 'ALL') {
      this._prepared.clear();
    } else {
      const name = ast.name.toLowerCase();
      if (!this._prepared.has(name)) {
        throw new Error(`Prepared statement "${name}" does not exist`);
      }
      this._prepared.delete(name);
    }
    return { type: 'OK', message: 'DEALLOCATE' };
  }

  // COPY command: bulk import/export
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
      statement: ast.statement,
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
    
    return this._executeAst(stmt);
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
    if (ast.all) {
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

  _declareCursor(ast) {
    const name = ast.name.toLowerCase();
    if (this._cursors.has(name)) {
      throw new Error(`Cursor "${name}" already exists`);
    }
    // Execute the query and store the full result set
    const result = this._executeAst(ast.query);
    const rows = result && result.rows ? result.rows : [];
    this._cursors.set(name, { rows, position: 0, scroll: ast.scroll });
    return { type: 'OK', message: `DECLARE CURSOR ${name}` };
  }

  _fetch(ast) {
    const name = ast.name.toLowerCase();
    const cursor = this._cursors.get(name);
    if (!cursor) {
      throw new Error(`Cursor "${name}" does not exist`);
    }
    
    const count = ast.count === Infinity ? cursor.rows.length - cursor.position : ast.count;
    const start = cursor.position;
    const end = Math.min(start + count, cursor.rows.length);
    const rows = cursor.rows.slice(start, end);
    cursor.position = end;
    
    return { rows };
  }

  _closeCursor(ast) {
    if (ast.all) {
      this._cursors.clear();
      return { type: 'OK', message: 'CLOSE ALL' };
    }
    const name = ast.name.toLowerCase();
    if (!this._cursors.has(name)) {
      throw new Error(`Cursor "${name}" does not exist`);
    }
    this._cursors.delete(name);
    return { type: 'OK', message: `CLOSE ${name}` };
  }

  _executeCopy(ast) {
    if (ast.direction === 'TO') {
      // COPY TO — export as CSV
      let rows, columns;
      if (ast.query) {
        const result = this._executeAst(ast.query);
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

  _union(ast) {
    const leftResult = this.execute_ast(ast.left);
    const rightResult = this.execute_ast(ast.right);
    let rows = [...leftResult.rows, ...rightResult.rows];

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

    // ORDER BY on the combined result
    if (ast.orderBy) {
      rows.sort((a, b) => {
        for (const { column, direction } of ast.orderBy) {
          let av, bv;
          if (typeof column === 'number') {
            av = this._resolveOrderByValue(column, a, ast);
            bv = this._resolveOrderByValue(column, b, ast);
          } else if (typeof column === 'object') {
            av = this._evalValue(column, a);
            bv = this._evalValue(column, b);
          } else {
            av = this._resolveColumn(column, a);
            bv = this._resolveColumn(column, b);
          }
          const cmp = av < bv ? -1 : av > bv ? 1 : 0;
          if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
        }
        return 0;
      });
    }

    // LIMIT/OFFSET
    if (ast.limit != null || ast.offset != null) {
      const offset = ast.offset || 0;
      const limit = ast.limit != null ? ast.limit : rows.length;
      rows = rows.slice(offset, offset + limit);
    }

    return { type: 'ROWS', rows };
  }

  _intersect(ast) {
    const leftResult = this.execute_ast(ast.left);
    const rightResult = this.execute_ast(ast.right);
    
    if (ast.all) {
      // INTERSECT ALL: multiset intersection (count-based)
      const rightCounts = new Map();
      for (const row of rightResult.rows) {
        const key = JSON.stringify(row);
        rightCounts.set(key, (rightCounts.get(key) || 0) + 1);
      }
      const rows = [];
      for (const row of leftResult.rows) {
        const key = JSON.stringify(row);
        const count = rightCounts.get(key) || 0;
        if (count > 0) {
          rows.push(row);
          rightCounts.set(key, count - 1);
        }
      }
      return { type: 'ROWS', rows };
    }
    
    // INTERSECT (set): deduplicate
    const rightKeys = new Set(rightResult.rows.map(r => JSON.stringify(r)));
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
    
    if (ast.all) {
      // EXCEPT ALL: multiset difference (count-based)
      const rightCounts = new Map();
      for (const row of rightResult.rows) {
        const key = JSON.stringify(row);
        rightCounts.set(key, (rightCounts.get(key) || 0) + 1);
      }
      const rows = [];
      for (const row of leftResult.rows) {
        const key = JSON.stringify(row);
        const count = rightCounts.get(key) || 0;
        if (count > 0) {
          rightCounts.set(key, count - 1);
        } else {
          rows.push(row);
        }
      }
      return { type: 'ROWS', rows };
    }
    
    // EXCEPT (set): deduplicate
    const rightKeys = new Set(rightResult.rows.map(r => JSON.stringify(r)));
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

  _explain(ast) {
    const stmt = ast.statement;

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
    const columnNames = Object.keys(baseResult.rows[0] || {});
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
          case 'SUM': return values.reduce((s, v) => s + v, 0);
          case 'AVG': return values.length ? values.reduce((s, v) => s + v, 0) / values.length : null;
          case 'MIN': return values.length ? values.reduce((a, b) => a < b ? a : b) : null;
          case 'MAX': return values.length ? values.reduce((a, b) => a > b ? a : b) : null;
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
          result[name] = computeAgg(col.func, col.arg, col.distinct, { separator: col.separator });
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
    if (expr.type === 'column_ref') return [expr.name];
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
  _collectAggregateExprs(expr) {
    if (!expr) return [];
    if (expr.type === 'aggregate_expr') return [expr];
    const results = [];
    for (const key of ['left', 'right', 'expr']) {
      if (expr[key]) results.push(...this._collectAggregateExprs(expr[key]));
    }
    return results;
  }

  // PostgreSQL type OIDs for pg_type catalog
  static _PG_TYPE_OIDS = {
    'INT': 23, 'INTEGER': 23, 'SERIAL': 23, 'AUTOINCREMENT': 23,
    'BIGINT': 20, 'SMALLINT': 21,
    'FLOAT': 701, 'DOUBLE': 701, 'REAL': 700, 'NUMERIC': 1700, 'DECIMAL': 1700,
    'TEXT': 25, 'VARCHAR': 1043, 'CHAR': 1042, 'STRING': 25,
    'BOOLEAN': 16, 'BOOL': 16,
    'DATE': 1082, 'TIMESTAMP': 1114, 'TIME': 1083,
    'JSON': 114, 'JSONB': 3802,
    'BYTEA': 17, 'BLOB': 17,
    'UUID': 2950,
  };

  // pg_catalog virtual tables
  _getPgCatalog(tableName) {
    // Generate stable OIDs from table names
    const _oid = (name, salt = 0) => {
      let h = salt;
      for (let i = 0; i < name.length; i++) h = ((h << 5) - h + name.charCodeAt(i)) | 0;
      return Math.abs(h) % 1000000 + 16384; // Start above system OIDs
    };

    switch (tableName) {
      case 'pg_namespace': {
        return [
          { oid: 11, nspname: 'pg_catalog', nspowner: 10, 'pg_namespace.oid': 11, 'pg_namespace.nspname': 'pg_catalog', 'pg_namespace.nspowner': 10 },
          { oid: 2200, nspname: 'public', nspowner: 10, 'pg_namespace.oid': 2200, 'pg_namespace.nspname': 'public', 'pg_namespace.nspowner': 10 },
          { oid: 13200, nspname: 'information_schema', nspowner: 10, 'pg_namespace.oid': 13200, 'pg_namespace.nspname': 'information_schema', 'pg_namespace.nspowner': 10 },
        ];
      }

      case 'pg_class': {
        const rows = [];
        let oidCounter = 16384;
        for (const [name, table] of this.tables) {
          const tableOid = _oid(name);
          const row = {
            oid: tableOid,
            relname: name,
            relnamespace: 2200,
            reltype: 0,
            reloftype: 0,
            relowner: 10,
            relam: 2, // heap
            relfilenode: tableOid,
            reltablespace: 0,
            relpages: 1,
            reltuples: table.heap ? table.heap.scan().length : -1,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: (table.indexes && table.indexes.size > 0) ? true : false,
            relisshared: false,
            relpersistence: 'p',
            relkind: 'r', // ordinary table
            relnatts: table.schema.length,
            relchecks: 0,
            relhasrules: false,
            relhastriggers: (this._triggers && this._triggers.some(t => t.table === name)) || false,
            relhassubclass: false,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relispopulated: true,
            relreplident: 'd',
            relispartition: false,
          };
          // Add qualified names
          for (const [k, v] of Object.entries(row)) {
            row[`pg_class.${k}`] = v;
          }
          rows.push(row);
        }
        // Add indexes as pg_class entries (use indexCatalog for proper names)
        for (const [idxName, idxMeta] of this.indexCatalog) {
              const idxOid = _oid(idxName, 1);
              const row = {
                oid: idxOid,
                relname: idxName,
                relnamespace: 2200,
                reltype: 0,
                reloftype: 0,
                relowner: 10,
                relam: 403, // btree
                relfilenode: idxOid,
                reltablespace: 0,
                relpages: 1,
                reltuples: -1,
                relallvisible: 0,
                reltoastrelid: 0,
                relhasindex: false,
                relisshared: false,
                relpersistence: 'p',
                relkind: 'i', // index
                relnatts: 1,
                relchecks: 0,
                relhasrules: false,
                relhastriggers: false,
                relhassubclass: false,
                relrowsecurity: false,
                relforcerowsecurity: false,
                relispopulated: true,
                relreplident: 'n',
                relispartition: false,
              };
              for (const [k, v] of Object.entries(row)) {
                row[`pg_class.${k}`] = v;
              }
              rows.push(row);
        }
        // Add views
        for (const [name] of this.views) {
          const viewOid = _oid(name, 2);
          const row = {
            oid: viewOid,
            relname: name,
            relnamespace: 2200,
            reltype: 0,
            reloftype: 0,
            relowner: 10,
            relam: 0,
            relfilenode: 0,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: 'p',
            relkind: 'v', // view
            relnatts: 0,
            relchecks: 0,
            relhasrules: true,
            relhastriggers: false,
            relhassubclass: false,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relispopulated: true,
            relreplident: 'n',
            relispartition: false,
          };
          for (const [k, v] of Object.entries(row)) {
            row[`pg_class.${k}`] = v;
          }
          rows.push(row);
        }
        return rows;
      }

      case 'pg_attribute': {
        const rows = [];
        for (const [tblName, table] of this.tables) {
          const tableOid = _oid(tblName);
          for (let i = 0; i < table.schema.length; i++) {
            const col = table.schema[i];
            const typeUpper = (col.type || 'TEXT').toUpperCase();
            const typeOid = Database._PG_TYPE_OIDS[typeUpper] || 25;
            const row = {
              attrelid: tableOid,
              attname: col.name,
              atttypid: typeOid,
              attstattarget: -1,
              attlen: typeOid === 23 ? 4 : typeOid === 20 ? 8 : typeOid === 16 ? 1 : -1,
              attnum: i + 1,
              attndims: 0,
              attcacheoff: -1,
              atttypmod: -1,
              attbyval: [16, 21, 23].includes(typeOid),
              attalign: 'i',
              attstorage: 'p',
              attcompression: '',
              attnotnull: col.notNull || col.primaryKey || false,
              atthasdef: col.defaultValue !== undefined && col.defaultValue !== null,
              atthasmissing: false,
              attidentity: '',
              attgenerated: '',
              attisdropped: false,
              attislocal: true,
              attinhcount: 0,
              attcollation: 0,
            };
            for (const [k, v] of Object.entries(row)) {
              row[`pg_attribute.${k}`] = v;
            }
            rows.push(row);
          }
        }
        return rows;
      }

      case 'pg_type': {
        const typeEntries = [
          { oid: 16, typname: 'bool', typnamespace: 11, typlen: 1, typbyval: true, typtype: 'b', typcategory: 'B' },
          { oid: 20, typname: 'int8', typnamespace: 11, typlen: 8, typbyval: true, typtype: 'b', typcategory: 'N' },
          { oid: 21, typname: 'int2', typnamespace: 11, typlen: 2, typbyval: true, typtype: 'b', typcategory: 'N' },
          { oid: 23, typname: 'int4', typnamespace: 11, typlen: 4, typbyval: true, typtype: 'b', typcategory: 'N' },
          { oid: 25, typname: 'text', typnamespace: 11, typlen: -1, typbyval: false, typtype: 'b', typcategory: 'S' },
          { oid: 114, typname: 'json', typnamespace: 11, typlen: -1, typbyval: false, typtype: 'b', typcategory: 'U' },
          { oid: 700, typname: 'float4', typnamespace: 11, typlen: 4, typbyval: true, typtype: 'b', typcategory: 'N' },
          { oid: 701, typname: 'float8', typnamespace: 11, typlen: 8, typbyval: true, typtype: 'b', typcategory: 'N' },
          { oid: 1042, typname: 'bpchar', typnamespace: 11, typlen: -1, typbyval: false, typtype: 'b', typcategory: 'S' },
          { oid: 1043, typname: 'varchar', typnamespace: 11, typlen: -1, typbyval: false, typtype: 'b', typcategory: 'S' },
          { oid: 1082, typname: 'date', typnamespace: 11, typlen: 4, typbyval: true, typtype: 'b', typcategory: 'D' },
          { oid: 1083, typname: 'time', typnamespace: 11, typlen: 8, typbyval: true, typtype: 'b', typcategory: 'D' },
          { oid: 1114, typname: 'timestamp', typnamespace: 11, typlen: 8, typbyval: true, typtype: 'b', typcategory: 'D' },
          { oid: 1700, typname: 'numeric', typnamespace: 11, typlen: -1, typbyval: false, typtype: 'b', typcategory: 'N' },
          { oid: 2950, typname: 'uuid', typnamespace: 11, typlen: 16, typbyval: false, typtype: 'b', typcategory: 'U' },
          { oid: 3802, typname: 'jsonb', typnamespace: 11, typlen: -1, typbyval: false, typtype: 'b', typcategory: 'U' },
        ];
        return typeEntries.map(t => {
          const row = { ...t };
          for (const [k, v] of Object.entries(t)) {
            row[`pg_type.${k}`] = v;
          }
          return row;
        });
      }

      case 'pg_index': {
        const rows = [];
        for (const [idxName, idxMeta] of this.indexCatalog) {
          const table = this.tables.get(idxMeta.table);
          const colName = idxMeta.columns ? idxMeta.columns[0] : null;
          const row = {
            indexrelid: _oid(idxName, 1),
            indrelid: _oid(idxMeta.table),
            indnatts: idxMeta.columns ? idxMeta.columns.length : 1,
            indnkeyatts: idxMeta.columns ? idxMeta.columns.length : 1,
            indisunique: idxMeta.unique || false,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: true,
            indisclustered: false,
            indisvalid: true,
            indcheckxmin: false,
            indisready: true,
            indislive: true,
            indisreplident: false,
            indkey: colName && table ? String(table.schema.findIndex(c => c.name === colName) + 1) : '1',
          };
          for (const [k, v] of Object.entries(row)) {
            row[`pg_index.${k}`] = v;
          }
          rows.push(row);
        }
        // Also add PK indexes
        for (const [tblName, table] of this.tables) {
          const pkCol = table.schema.find(c => c.primaryKey);
          if (pkCol && table.indexes.has(pkCol.name)) {
            const pkIdxName = `${tblName}_pkey`;
            const row = {
              indexrelid: _oid(pkIdxName, 1),
              indrelid: _oid(tblName),
              indnatts: 1,
              indnkeyatts: 1,
              indisunique: true,
              indisprimary: true,
              indisexclusion: false,
              indimmediate: true,
              indisclustered: false,
              indisvalid: true,
              indcheckxmin: false,
              indisready: true,
              indislive: true,
              indisreplident: false,
              indkey: String(table.schema.indexOf(pkCol) + 1),
            };
            for (const [k, v] of Object.entries(row)) {
              row[`pg_index.${k}`] = v;
            }
            rows.push(row);
          }
        }
        return rows;
      }

      case 'pg_settings': {
        const settings = [];
        const costModel = this._costModel || {};
        const params = {
          'seq_page_cost': costModel.seqPageCost || 1.0,
          'random_page_cost': costModel.randomPageCost || 1.1,
          'cpu_tuple_cost': costModel.cpuTupleCost || 0.01,
          'cpu_index_tuple_cost': costModel.cpuIndexTupleCost || 0.005,
          'cpu_operator_cost': costModel.cpuOperatorCost || 0.0025,
          'effective_cache_size': '4GB',
          'work_mem': '4MB',
          'server_version': '16.0',
          'server_encoding': 'UTF8',
          'client_encoding': 'UTF8',
        };
        for (const [name, setting] of Object.entries(params)) {
          const row = {
            name,
            setting: String(setting),
            unit: typeof setting === 'number' ? '' : null,
            category: 'Query Tuning / Planner Cost Constants',
            short_desc: `${name} parameter`,
            extra_desc: null,
            context: 'user',
            vartype: typeof setting === 'number' ? 'real' : 'string',
            source: 'default',
            min_val: typeof setting === 'number' ? '0' : null,
            max_val: null,
            boot_val: String(setting),
            reset_val: String(setting),
          };
          for (const [k, v] of Object.entries(row)) {
            row[`pg_settings.${k}`] = v;
          }
          settings.push(row);
        }
        return settings;
      }

      case 'pg_stat_user_tables': {
        const rows = [];
        for (const [name, table] of this.tables) {
          const scanCount = table.heap ? [...table.heap.scan()].length : 0;
          const row = {
            relid: _oid(name),
            schemaname: 'public',
            relname: name,
            seq_scan: 0,
            seq_tup_read: 0,
            idx_scan: 0,
            idx_tup_fetch: 0,
            n_tup_ins: 0,
            n_tup_upd: 0,
            n_tup_del: 0,
            n_live_tup: scanCount,
            n_dead_tup: 0,
            last_vacuum: null,
            last_autovacuum: null,
            last_analyze: null,
          };
          for (const [k, v] of Object.entries(row)) {
            row[`pg_stat_user_tables.${k}`] = v;
          }
          rows.push(row);
        }
        return rows;
      }

      case 'pg_stat_statements': {
        const rows = [];
        for (const [, stats] of this._queryStats) {
          const row = {
            query: stats.query,
            calls: stats.calls,
            total_exec_time: Math.round(stats.total_exec_time * 1000) / 1000,
            mean_exec_time: stats.calls > 0 ? Math.round((stats.total_exec_time / stats.calls) * 1000) / 1000 : 0,
            min_exec_time: stats.min_exec_time === Infinity ? 0 : Math.round(stats.min_exec_time * 1000) / 1000,
            max_exec_time: Math.round(stats.max_exec_time * 1000) / 1000,
            rows: stats.rows,
          };
          for (const [k, v] of Object.entries(row)) {
            row[`pg_stat_statements.${k}`] = v;
          }
          rows.push(row);
        }
        return rows;
      }

      default:
        return null;
    }
  }

  // information_schema virtual tables
  _getInformationSchema(tableName) {
    const schema = tableName.replace('information_schema.', '');
    
    switch (schema) {
      case 'tables': {
        const rows = [];
        for (const [name] of this.tables) {
          rows.push({
            table_catalog: 'henrydb',
            table_schema: 'public',
            table_name: name,
            table_type: 'BASE TABLE',
            'information_schema.tables.table_catalog': 'henrydb',
            'information_schema.tables.table_schema': 'public',
            'information_schema.tables.table_name': name,
            'information_schema.tables.table_type': 'BASE TABLE',
          });
        }
        for (const [name] of this.views) {
          rows.push({
            table_catalog: 'henrydb',
            table_schema: 'public',
            table_name: name,
            table_type: 'VIEW',
            'information_schema.tables.table_catalog': 'henrydb',
            'information_schema.tables.table_schema': 'public',
            'information_schema.tables.table_name': name,
            'information_schema.tables.table_type': 'VIEW',
          });
        }
        return rows;
      }
      
      case 'columns': {
        const rows = [];
        for (const [tableName, table] of this.tables) {
          for (let i = 0; i < table.schema.length; i++) {
            const col = table.schema[i];
            const row = {
              table_catalog: 'henrydb',
              table_schema: 'public',
              table_name: tableName,
              column_name: col.name,
              ordinal_position: i + 1,
              column_default: col.defaultValue,
              is_nullable: col.notNull ? 'NO' : 'YES',
              data_type: col.type,
            };
            // Also add qualified names
            for (const [k, v] of Object.entries(row)) {
              row[`information_schema.columns.${k}`] = v;
            }
            rows.push(row);
          }
        }
        return rows;
      }
      
      case 'table_constraints': {
        const rows = [];
        for (const [tableName, table] of this.tables) {
          for (const col of table.schema) {
            if (col.primaryKey) {
              const row = {
                constraint_catalog: 'henrydb',
                constraint_schema: 'public',
                constraint_name: `${tableName}_${col.name}_pkey`,
                table_catalog: 'henrydb',
                table_schema: 'public',
                table_name: tableName,
                constraint_type: 'PRIMARY KEY',
              };
              for (const [k, v] of Object.entries(row)) {
                row[`information_schema.table_constraints.${k}`] = v;
              }
              rows.push(row);
            }
            if (col.notNull) {
              const row = {
                constraint_catalog: 'henrydb',
                constraint_schema: 'public',
                constraint_name: `${tableName}_${col.name}_notnull`,
                table_catalog: 'henrydb',
                table_schema: 'public',
                table_name: tableName,
                constraint_type: 'NOT NULL',
              };
              for (const [k, v] of Object.entries(row)) {
                row[`information_schema.table_constraints.${k}`] = v;
              }
              rows.push(row);
            }
            if (col.references) {
              const row = {
                constraint_catalog: 'henrydb',
                constraint_schema: 'public',
                constraint_name: `${tableName}_${col.name}_fkey`,
                table_catalog: 'henrydb',
                table_schema: 'public',
                table_name: tableName,
                constraint_type: 'FOREIGN KEY',
              };
              for (const [k, v] of Object.entries(row)) {
                row[`information_schema.table_constraints.${k}`] = v;
              }
              rows.push(row);
            }
          }
        }
        return rows;
      }
      
      case 'key_column_usage': {
        const rows = [];
        for (const [tableName, table] of this.tables) {
          for (let i = 0; i < table.schema.length; i++) {
            const col = table.schema[i];
            if (col.primaryKey) {
              const row = {
                constraint_catalog: 'henrydb',
                constraint_schema: 'public',
                constraint_name: `${tableName}_${col.name}_pkey`,
                table_catalog: 'henrydb',
                table_schema: 'public',
                table_name: tableName,
                column_name: col.name,
                ordinal_position: i + 1,
              };
              for (const [k, v] of Object.entries(row)) {
                row[`information_schema.key_column_usage.${k}`] = v;
              }
              rows.push(row);
            }
            if (col.references) {
              const row = {
                constraint_catalog: 'henrydb',
                constraint_schema: 'public',
                constraint_name: `${tableName}_${col.name}_fkey`,
                table_catalog: 'henrydb',
                table_schema: 'public',
                table_name: tableName,
                column_name: col.name,
                ordinal_position: i + 1,
              };
              for (const [k, v] of Object.entries(row)) {
                row[`information_schema.key_column_usage.${k}`] = v;
              }
              rows.push(row);
            }
          }
        }
        return rows;
      }
      
      default:
        return null;
    }
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
    // For LATERAL JOINs: check lateral scope (outer row)
    if (this._lateralScope) {
      if (name in this._lateralScope) return this._lateralScope[name];
      for (const key of Object.keys(this._lateralScope)) {
        if (key.endsWith(`.${name}`)) return this._lateralScope[key];
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
      case 'NOT_IN_HASHSET': {
        const leftVal = this._evalValue(expr.left, row);
        return !expr.hashSet.has(leftVal);
      }
      case 'IN_COMPOSITE_HASHSET': {
        const vals = expr.outerCols.map(col => this._evalValue({ type: 'column_ref', name: col }, row));
        return expr.hashSet.has(JSON.stringify(vals));
      }
      case 'NOT_IN_COMPOSITE_HASHSET': {
        const vals = expr.outerCols.map(col => this._evalValue({ type: 'column_ref', name: col }, row));
        return !expr.hashSet.has(JSON.stringify(vals));
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
      case 'LIKE':
      case 'ILIKE': {
        const val = this._evalValue(expr.left, row);
        const pattern = this._evalValue(expr.pattern, row);
        if (val == null || pattern == null) return false;
        const regex = '^' + String(pattern)
          .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
          .replace(/%/g, '.*')
          .replace(/_/g, '.')
          + '$';
        const flags = expr.type === 'ILIKE' ? 'i' : '';
        return new RegExp(regex, flags).test(String(val));
      }
      case 'BETWEEN': {
        const val = this._evalValue(expr.left, row);
        const low = this._evalValue(expr.low, row);
        const high = this._evalValue(expr.high, row);
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
    if (node.type === 'SUBQUERY' || node.type === 'subquery') {
      const subqueryAst = node.subquery || node.query;
      const result = this._evalSubquery(subqueryAst, row);
      if (result.length === 0) return null;
      const firstRow = result[0];
      return Object.values(firstRow)[0];
    }
    if (node.type === 'function_call') {
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
    if (node.type === 'arith') {
      const left = this._evalValue(node.left, row);
      const right = this._evalValue(node.right, row);
      if (left == null || right == null) return null;
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
      // Try to find it with any alias pattern
      for (const k of Object.keys(row)) {
        if (k.toUpperCase().includes(node.func) && k.includes(argStr)) return row[k];
      }
      return null;
    }
    return null;
  }

  _evalFunction(func, args, row) {
    switch (func) {
      case 'NEXTVAL': { const v = this._evalValue(args[0], row); return this._nextval(String(v)); }
      case 'CURRVAL': { const v = this._evalValue(args[0], row); return this._currval(String(v)); }
      case 'SETVAL': { const v = this._evalValue(args[0], row); const n = this._evalValue(args[1], row); return this._setval(String(v), Number(n)); }
      case 'PG_STAT_STATEMENTS_RESET': { this._queryStats.clear(); return true; }
      case 'COALESCE': { for (const arg of args) { const v = this._evalValue(arg, row); if (v !== null && v !== undefined) return v; } return null; }
      case 'NULLIF': { const a = this._evalValue(args[0], row); const b = this._evalValue(args[1], row); return a === b ? null : a; }
      case 'GREATEST': { return Math.max(...args.map(a => this._evalValue(a, row)).filter(v => v !== null)); }
      case 'LEAST': { return Math.min(...args.map(a => this._evalValue(a, row)).filter(v => v !== null)); }
      case 'UPPER': { const v = this._evalValue(args[0], row); return v != null ? String(v).toUpperCase() : null; }
      case 'LOWER': { const v = this._evalValue(args[0], row); return v != null ? String(v).toLowerCase() : null; }
      case 'LENGTH': { const v = this._evalValue(args[0], row); return v != null ? String(v).length : null; }
      case 'CONCAT': return args.map(a => { const v = this._evalValue(a, row); return v != null ? String(v) : ''; }).join('');
      case 'CONCAT_WS': {
        const sep = String(this._evalValue(args[0], row));
        return args.slice(1).map(a => this._evalValue(a, row)).filter(v => v != null).map(String).join(sep);
      }
      case 'REGEXP_REPLACE': {
        const str = String(this._evalValue(args[0], row));
        const pattern = String(this._evalValue(args[1], row));
        const replacement = String(this._evalValue(args[2], row));
        const flags = args[3] ? String(this._evalValue(args[3], row)) : 'g';
        return str.replace(new RegExp(pattern, flags), replacement);
      }
      case 'REGEXP_MATCH': {
        const str = String(this._evalValue(args[0], row));
        const pattern = String(this._evalValue(args[1], row));
        return new RegExp(pattern).test(str) ? 1 : 0;
      }
      case 'COALESCE': {
        for (const arg of args) {
          const v = this._evalValue(arg, row);
          if (v !== null && v !== undefined) return v;
        }
        return null;
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
      case 'LTRIM': {
        const str = this._evalValue(args[0], row);
        return str != null ? String(str).replace(/^\s+/, '') : null;
      }
      case 'RTRIM': {
        const str = this._evalValue(args[0], row);
        return str != null ? String(str).replace(/\s+$/, '') : null;
      }
      case 'INSTR': {
        const str = this._evalValue(args[0], row);
        const sub = this._evalValue(args[1], row);
        if (str == null || sub == null) return null;
        const idx = String(str).indexOf(String(sub));
        return idx >= 0 ? idx + 1 : 0; // SQL INSTR is 1-based, 0 if not found
      }
      case 'PRINTF': {
        // Simplified printf: supports %d, %s, %f, %0Nd
        const fmt = this._evalValue(args[0], row);
        const vals = args.slice(1).map(a => this._evalValue(a, row));
        if (fmt == null) return null;
        let i = 0;
        return String(fmt).replace(/%(\d*)([dsf%])/g, (m, width, type) => {
          if (type === '%') return '%';
          const v = vals[i++];
          if (type === 'd') return width ? String(v || 0).padStart(parseInt(width), '0') : String(v || 0);
          if (type === 's') return String(v ?? '');
          if (type === 'f') return String(v ?? 0);
          return m;
        });
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
        return str.padStart(len, pad);
      }
      case 'RPAD': {
        const str = String(this._evalValue(args[0], row) || '');
        const len = this._evalValue(args[1], row) || 0;
        const pad = args[2] ? String(this._evalValue(args[2], row)) : ' ';
        return str.padEnd(len, pad);
      }
      case 'REVERSE': { const v = this._evalValue(args[0], row); return v == null ? null : String(v).split('').reverse().join(''); }
      case 'REPEAT': { const v = this._evalValue(args[0], row); const n = this._evalValue(args[1], row); return v == null ? null : String(v).repeat(n || 0); }
      
      // Math functions
      case 'POWER': return Math.pow(this._evalValue(args[0], row), this._evalValue(args[1], row));
      case 'GREATEST': return Math.max(...args.map(a => this._evalValue(a, row)));
      case 'LEAST': return Math.min(...args.map(a => this._evalValue(a, row)));
      case 'SQRT': return Math.sqrt(this._evalValue(args[0], row));
      case 'LOG': return args.length > 1 ? Math.log(this._evalValue(args[1], row)) / Math.log(this._evalValue(args[0], row)) : Math.log(this._evalValue(args[0], row));
      case 'RANDOM': return Math.random();
      
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
      
      default: {
        // Check user-defined functions
        const udf = this._functions.get(func.toLowerCase());
        if (udf) {
          return this._callUserFunction(udf, args, row);
        }
        throw new Error(`Unknown function: ${func}`);
      }
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
        case 'SUM': result[name] = values.reduce((s, v) => s + v, 0); break;
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
