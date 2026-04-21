// ddl-tables.js — DDL handlers for CREATE TABLE, DROP TABLE
// Extracted from db.js to reduce monolith size

import { BPlusTree } from './btree.js';
import { BTreeTable } from './btree-table.js';

export function createTable(db, ast) {
  if (db.tables.has(ast.table)) {
    if (ast.ifNotExists) return { type: 'OK', message: `Table ${ast.table} already exists (IF NOT EXISTS)` };
    throw new Error(`Table ${ast.table} already exists`);
  }
  const schema = ast.columns.map(c => {
    // Handle SERIAL columns: create sequence and set default
    if (c.serial) {
      const seqName = `${ast.table}_${c.name}_seq`;
      db.sequences.set(seqName.toLowerCase(), {
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
    heap = db._heapFactory(ast.table);
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

  // Extract table-level CHECK constraints
  const tableChecks = (ast.tableConstraints || []).filter(c => c.type === 'CHECK').map(c => c.expr);

  db.tables.set(ast.table, { heap, schema, indexes, tableChecks, deadTupleCount: 0, liveTupleCount: 0 });
  db.catalog.push({ name: ast.table, columns: schema });
  
  // Create composite unique indexes
  if (ast.compositeUniques && ast.compositeUniques.length > 0) {
    for (const cols of ast.compositeUniques) {
      const idxName = `${ast.table}_${cols.join('_')}_unique`;
      db.execute(`CREATE UNIQUE INDEX ${idxName} ON ${ast.table}(${cols.join(', ')})`);
    }
  }
  
  // Log DDL to WAL for crash recovery
  if (db._dataDir && db.wal && db.wal.logCreateTable) {
    db.wal.logCreateTable(ast.table, schema.map(c => ({ name: c.name, type: c.type })));
  }
  
  return { type: 'OK', message: `Table ${ast.table} created` };
}

export function createTableAs(db, ast) {
  const result = db._select(ast.query);
  const rows = result.rows || [];
  
  if (rows.length === 0) {
    const cols = ast.query.columns || [];
    const schema = cols.map(c => ({
      name: c.alias || c.name || c.value || 'column',
      type: 'TEXT'
    }));
    createTable(db, {
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
  
  createTable(db, {
    type: 'CREATE_TABLE',
    table: ast.table,
    ifNotExists: false,
    columns: schema
  });
  
  // Insert all rows
  const table = db.tables.get(ast.table);
  for (const row of rows) {
    const values = schema.map(col => row[col.name]);
    table.heap.insert(values);
  }
  
  return { type: 'OK', count: rows.length };
}

export function dropTable(db, ast) {
  if (!db.tables.has(ast.table)) {
    if (ast.ifExists) return { type: 'OK', message: `Table ${ast.table} does not exist (IF EXISTS)` };
    throw new Error(`Table ${ast.table} not found`);
  }
  
  // Check for dependent views
  const dependentViews = [];
  for (const [viewName, view] of db.views) {
    if (view.isCTE) continue; // Skip CTEs
    // Check if the view references this table
    const viewDef = view.sql || view.definition || '';
    if (viewDef.toLowerCase().includes(ast.table.toLowerCase())) {
      dependentViews.push(viewName);
    }
  }
  
  // Check for foreign key references from other tables
  const dependentFKs = [];
  for (const [tableName, table] of db.tables) {
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
      db.views.delete(viewName);
      dropped.push(`view ${viewName}`);
    }
    // Remove FK references (set to null) in dependent tables
    for (const { table: depTable, column: depCol } of dependentFKs) {
      const table = db.tables.get(depTable);
      const col = table.schema.find(c => c.name === depCol);
      if (col) delete col.references;
      dropped.push(`FK ${depTable}.${depCol}`);
    }
  }
  
  // WAL: log the drop for crash recovery
  if (db.wal && db.wal.logDropTable) {
    db.wal.logDropTable(ast.table);
  }
  // Remove any indexes for this table
  for (const [idxName, meta] of db.indexCatalog) {
    if (meta.table === ast.table) db.indexCatalog.delete(idxName);
  }
  db.tables.delete(ast.table);
  db.catalog = db.catalog.filter(t => t.name !== ast.table);
  
  const cascadeMsg = dropped.length > 0 ? ` (also dropped: ${dropped.join(', ')})` : '';
  return { type: 'OK', message: `Table ${ast.table} dropped${cascadeMsg}` };
}
