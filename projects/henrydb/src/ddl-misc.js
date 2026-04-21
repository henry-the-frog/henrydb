// ddl-misc.js — DDL handlers for views, ALTER TABLE, functions
// Extracted from db.js to reduce monolith size

export function createView(db, ast) {
  if (db.views.has(ast.name) && !ast.orReplace) throw new Error(`View ${ast.name} already exists`);
  db.views.set(ast.name, { query: ast.query });
  return { type: 'OK', message: `View ${ast.name} ${ast.orReplace ? 'replaced' : 'created'}` };
}

export function dropView(db, ast) {
  if (!db.views.has(ast.name)) {
    if (ast.ifExists) return { type: 'OK', message: `View ${ast.name} does not exist (IF EXISTS)` };
    throw new Error(`View ${ast.name} not found`);
  }
  db.views.delete(ast.name);
  return { type: 'OK', message: `View ${ast.name} dropped` };
}

export function alterTable(db, ast) {
  const table = db.tables.get(ast.table);
  if (!table) throw new Error(`Table ${ast.table} not found`);
  db._logAlterTableDDL(ast);

  switch (ast.action) {
    case 'ADD_COLUMN': {
      const col = ast.column;
      const colName = typeof col === 'string' ? col : col.name;
      const colType = typeof col === 'object' ? col.type : (ast.dataType || 'TEXT');
      if (table.schema.find(c => c.name === colName)) {
        throw new Error(`Column ${colName} already exists`);
      }
      const colDef = { name: colName, type: colType, primaryKey: false };
      const defaultVal = typeof col === 'object' && col.default ? col.default.value : (ast.defaultValue ?? null);
      if (defaultVal !== undefined && defaultVal !== null) {
        colDef.defaultValue = defaultVal;
      }
      table.schema.push(colDef);

      // Add default value to all existing rows
      const allRows = [];
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        allRows.push({ pageId, slotIdx, values });
      }
      if (table.heap.updateInPlace) {
        // FileBackedHeap: update tuples in-place without WAL logging.
        // Schema change is persisted via DDL WAL record + catalog save.
        // Recovery replays schema-only, so data doesn't need separate WAL entries.
        for (const row of allRows) {
          table.heap.updateInPlace(row.pageId, row.slotIdx, [...row.values, defaultVal]);
        }
      } else {
        // In-memory HeapFile: delete + re-insert (no WAL concerns)
        for (const row of allRows) {
          table.heap.delete(row.pageId, row.slotIdx);
          table.heap.insert([...row.values, defaultVal]);
        }
        // Rebuild all indexes (RIDs changed from delete+re-insert)
        db._rebuildIndexes(table);
      }

      // Update catalog
      const catEntry = db.catalog.find(c => c.name === ast.table);
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
        for (const [idxName, meta] of db.indexCatalog) {
          if (meta.table === ast.table && meta.columns.includes(col.name)) {
            db.indexCatalog.delete(idxName);
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
      if (table.heap.updateInPlace) {
        for (const row of allRows) {
          const newValues = [...row.values];
          newValues.splice(colIdx, 1);
          table.heap.updateInPlace(row.pageId, row.slotIdx, newValues);
        }
      } else {
        for (const row of allRows) {
          table.heap.delete(row.pageId, row.slotIdx);
          const newValues = [...row.values];
          newValues.splice(colIdx, 1);
          table.heap.insert(newValues);
        }
        // Rebuild remaining indexes (RIDs changed from delete+re-insert)
        db._rebuildIndexes(table);
      }

      // Update catalog
      const catEntry = db.catalog.find(c => c.name === ast.table);
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
      const catEntry = db.catalog.find(c => c.name === ast.table);
      if (catEntry) catEntry.columns = table.schema;

      return { type: 'OK', message: `Column ${oldName} renamed to ${newName}` };
    }

    case 'RENAME_TABLE': {
      const tableData = db.tables.get(ast.table);
      db.tables.delete(ast.table);
      db.tables.set(ast.newName, tableData);

      // Update catalog
      const catEntry = db.catalog.find(c => c.name === ast.table);
      if (catEntry) catEntry.name = ast.newName;

      // Update index catalog
      for (const [, meta] of db.indexCatalog) {
        if (meta.table === ast.table) meta.table = ast.newName;
      }

      return { type: 'OK', message: `Table ${ast.table} renamed to ${ast.newName}` };
    }

    default:
      throw new Error(`Unknown ALTER TABLE action: ${ast.action}`);
  }
}

export function createFunction(db, ast) {
  if (db._functions.has(ast.name) && !ast.orReplace) {
    throw new Error(`Function ${ast.name} already exists`);
  }
  db._functions.set(ast.name, {
    params: ast.params,
    returnType: ast.returnType,
    returnColumns: ast.returnColumns,
    language: ast.language,
    body: ast.body.trim(),
    volatility: ast.volatility,
    isProcedure: ast.isProcedure,
  });
  // Invalidate result cache — queries using this function may return different results
  db._resultCache.clear();
  const kind = ast.isProcedure ? 'Procedure' : 'Function';
  return { type: 'OK', message: `${kind} ${ast.name} ${ast.orReplace ? 'replaced' : 'created'}` };
}

export function dropFunction(db, ast) {
  if (!db._functions.has(ast.name)) {
    if (ast.ifExists) return { type: 'OK', message: `Function ${ast.name} does not exist (IF EXISTS)` };
    throw new Error(`Function ${ast.name} does not exist`);
  }
  db._functions.delete(ast.name);
  db._resultCache.clear();
  return { type: 'OK', message: `Function ${ast.name} dropped` };
}
