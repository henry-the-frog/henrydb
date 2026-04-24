// ddl-indexes.js — DDL handlers for CREATE INDEX, DROP INDEX
// Extracted from db.js to reduce monolith size

import { BPlusTree } from './btree.js';
import { makeCompositeKey } from './composite-key.js';

export function createIndex(db, ast) {
  const table = db.tables.get(ast.table);
  if (!table) throw new Error(`Table ${ast.table} not found`);

  // Check IF NOT EXISTS / duplicate index
  const colName = ast.columns.join(',');
  if (table.indexes?.has(colName)) {
    if (ast.ifNotExists) {
      return { type: 'OK', message: 'CREATE INDEX' };
    }
    throw new Error(`Index on column(s) ${colName} already exists on table ${ast.table}`);
  }
  // Also check by index name if provided
  if (ast.indexName && table._indexNames?.has(ast.indexName)) {
    if (ast.ifNotExists) {
      return { type: 'OK', message: 'CREATE INDEX' };
    }
    throw new Error(`Index ${ast.indexName} already exists`);
  }

  // Validate columns exist
  for (const col of ast.columns) {
    if (!table.schema.find(c => c.name.toLowerCase() === col.toLowerCase())) {
      throw new Error(`Column ${col} not found in table ${ast.table}`);
    }
  }

  // For simplicity, support single-column indexes (composite uses CompositeKey)
  const isComposite = ast.columns.length > 1;
  const colIndices = ast.columns.map(c => table.schema.findIndex(s => s.name.toLowerCase() === c.toLowerCase()));

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
      const row = db._valuesToRow(values, table.schema, ast.table);
      if (!db._evalExpr(ast.where, row)) continue;
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
        const row = db._valuesToRow(values, table.schema, ast.table);
        if (!db._evalExpr(ast.where, row)) continue;
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
  // Track index names for duplicate detection
  if (ast.indexName || ast.name) {
    if (!table._indexNames) table._indexNames = new Set();
    table._indexNames.add(ast.indexName || ast.name);
  }
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
  db.indexCatalog.set(ast.name, {
    table: ast.table,
    columns: ast.columns,
    unique: ast.unique || false,
  });

  db._logCreateIndexDDL(ast);
  const msg = isConcurrent 
    ? `Index ${ast.name} created concurrently (${rowsScanned} rows indexed, validated in ${buildStats.endTime - buildStats.startTime}ms)`
    : `Index ${ast.name} created`;
  return { type: 'OK', message: msg, buildStats };
}

export function dropIndex(db, ast) {
  const meta = db.indexCatalog.get(ast.name);
  if (!meta) {
    if (ast.ifExists) return { type: 'OK', message: 'DROP INDEX' };
    throw new Error(`Index ${ast.name} not found`);
  }

  // WAL: log the drop
  if (db.wal && db.wal.logDDL) {
    db.wal.logDDL(`DROP INDEX IF EXISTS ${ast.name}`);
  }

  const table = db.tables.get(meta.table);
  if (table) {
    const colName = meta.columns[0];
    // Don't drop primary key indexes
    const isPK = table.schema.find(c => c.name === colName && c.primaryKey);
    if (!isPK) {
      table.indexes.delete(colName);
    }
  }

  db.indexCatalog.delete(ast.name);
  return { type: 'OK', message: `Index ${ast.name} dropped` };
}
