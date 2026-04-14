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
        const table = this.tables.get(ast.table);
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
      case 'MERGE': return this._merge(ast);
      case 'VALUES_QUERY': return this._valuesQuery(ast);
      case 'TRUNCATE': return this._truncate(ast);
      case 'SHOW_TABLES': return this._showTables();
      case 'DESCRIBE': return this._describe(ast);
      case 'EXPLAIN': return this._explain(ast);
      case 'BEGIN': 
        this._inTransaction = true;
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
        if (this._currentTx) {
          this._currentTx.rollback();
          this._currentTx = null;
          this._currentTxId = 0;
        }
        return { type: 'OK', message: 'ROLLBACK' };
      case 'VACUUM': return this._vacuum(ast);
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

  _createTableAs(ast) {
    // Execute the query to get the schema and data
    const result = this.execute(ast.query);
    if (!result.rows || result.rows.length === 0) {
      // Empty result — create table with no rows but infer schema from query
      // For now, create with no columns (this is a limitation)
      throw new Error('CREATE TABLE AS with empty result set requires at least one row to infer schema');
    }
    
    // Infer schema from first row
    const firstRow = result.rows[0];
    const columns = Object.keys(firstRow).filter(k => !k.includes('.')).map(name => {
      const val = firstRow[name];
      let type = 'TEXT';
      if (typeof val === 'number') type = Number.isInteger(val) ? 'INTEGER' : 'REAL';
      else if (typeof val === 'boolean') type = 'INTEGER';
      return { name, type, primaryKey: false, notNull: false, check: null, defaultValue: null, references: null, generated: null };
    });
    
    // Create the table
    const createAst = { type: 'CREATE_TABLE', table: ast.table, columns, ifNotExists: ast.ifNotExists };
    this._createTable(createAst);
    
    // Insert all rows
    const table = this.tables.get(ast.table);
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
    const table = this.tables.get(ast.table);
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
      
      this._insertRow(table, ast.columns, values);
      inserted++;
      
      if (ast.returning) {
        const orderedValues = this._orderValues(table, ast.columns, values);
        const retRow = {};
        table.schema.forEach((c, i) => { retRow[c.name] = orderedValues[i]; });
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
          for (const { column, direction } of ast.orderBy) {
            const av = this._resolveOrderByValue(column, a, ast);
            const bv = this._resolveOrderByValue(column, b, ast);
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
        for (const { column, direction } of ast.orderBy) {
          let av, bv;
          if (typeof column === 'number') {
            av = this._resolveOrderByValue(column, a, ast);
            bv = this._resolveOrderByValue(column, b, ast);
          } else if (typeof column === 'object') {
            av = this._evalValue(column, a);
            bv = this._evalValue(column, b);
          } else if (windowAliases.has(column)) {
            // Window function alias — values stored as __window_<name>
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
          const cmp = av == null && bv == null ? 0 : av == null ? 1 : bv == null ? -1 : av < bv ? -1 : av > bv ? 1 : 0;
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
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    // Validate no writes to generated columns
    this._validateNoGeneratedColumnWrites(table, ast.assignments?.map(a => a.column));

    let updated = 0;
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
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const row = this._valuesToRow(values, table.schema, ast.table);
        if (!ast.where || this._evalExpr(ast.where, row)) {
          toUpdate.push({ pageId, slotIdx, values: [...values] });
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

      // Remove old index entries
      for (const [colName, index] of table.indexes) {
        const oldKey = this._computeIndexKey(colName, item.values, table, ast.table);
        try { index.delete(oldKey, { pageId: item.pageId, slotIdx: item.slotIdx }); } catch {}
      }

      // Delete old, insert new
      table.heap.delete(item.pageId, item.slotIdx);
      const newRid = table.heap.insert(newValues);

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
    const table = this.tables.get(ast.table);
    if (!table) throw new Error(`Table ${ast.table} not found`);

    let deleted = 0;
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
    // If no MVCC manager attached, just return
    if (!this._mvccManager) {
      return { type: 'OK', message: 'VACUUM (no MVCC)' };
    }

    const tables = ast.table ? [ast.table] : [...this.tables.keys()];
    let totalDead = 0, totalBytes = 0, totalPages = 0;

    for (const tableName of tables) {
      const table = this.tables.get(tableName);
      if (!table || !table.mvccHeap) continue;

      const result = table.mvccHeap.vacuum(this._mvccManager);
      totalDead += result.deadTuplesRemoved;
      totalBytes += result.bytesFreed;
      totalPages += result.pagesCompacted;
    }

    return {
      type: 'OK',
      message: `VACUUM: ${totalDead} dead tuples removed, ${totalBytes} bytes freed, ${totalPages} pages compacted`,
      details: { deadTuplesRemoved: totalDead, bytesFreed: totalBytes, pagesCompacted: totalPages },
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

      // Normalize column names to match base query
      // Recursive query columns map positionally to base column names.
      // Re-evaluate expressions because SELECT column naming can collide.
      if (columnNames.length > 0 && recursiveQuery.columns) {
        const recCols = recursiveQuery.columns;
        
        // Get the raw working set rows (before SELECT transforms them)
        // We need to re-evaluate against the source rows, not the SELECT output
        const sourceView = this.views.get(cte.name);
        const sourceRows = sourceView?.materializedRows || workingSet;
        
        // Re-evaluate: for each source row that passes WHERE, compute each column
        const whereFilter = recursiveQuery.where;
        newRows = [];
        
        for (const srcRow of sourceRows) {
          if (whereFilter && !this._evalExpr(whereFilter, srcRow)) continue;
          
          const normalized = {};
          for (let i = 0; i < columnNames.length && i < recCols.length; i++) {
            const col = recCols[i];
            let val;
            if (col.type === 'expression' && col.expr) {
              val = this._evalValue(col.expr, srcRow);
            } else if (col.type === 'column') {
              val = this._resolveColumn(col.name, srcRow);
            } else if (col.type === 'aggregate') {
              val = null; // Aggregates not supported in recursive part
            }
            if (col.alias) normalized[col.alias] = val;
            normalized[columnNames[i]] = val;
          }
          newRows.push(normalized);
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

    // Build analyze output
    const analysis = [];
    
    // Table scan info
    const tableName = stmt.from?.table;
    if (tableName && this.tables.has(tableName)) {
      const table = this.tables.get(tableName);
      const totalRows = table.heap.tupleCount || 0;
      
      analysis.push({
        operation: plannerEstimate?.scanType || 'TABLE_SCAN',
        table: tableName,
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
      analysis.push({ operation: 'SORT', rows_sorted: actualRows });
    }

    return {
      type: 'ANALYZE',
      plan: analysis,
      execution_time_ms: parseFloat(executionTime.toFixed(3)),
      actual_rows: actualRows,
      estimated_rows: plannerEstimate?.estimatedRows || '?',
      estimation_accuracy: plannerEstimate?.estimatedRows
        ? parseFloat((actualRows / plannerEstimate.estimatedRows).toFixed(3))
        : '?',
    };
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
        return new RegExp(regex, 'i').test(String(val));
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
    if (node.type === 'SUBQUERY') {
      const result = this._evalSubquery(node.subquery, row);
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
      case 'UPPER': { const v = this._evalValue(args[0], row); return v != null ? String(v).toUpperCase() : null; }
      case 'LOWER': { const v = this._evalValue(args[0], row); return v != null ? String(v).toLowerCase() : null; }
      case 'LENGTH': { const v = this._evalValue(args[0], row); return v != null ? String(v).length : null; }
      case 'CONCAT': return args.map(a => { const v = this._evalValue(a, row); return v != null ? String(v) : ''; }).join('');
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
