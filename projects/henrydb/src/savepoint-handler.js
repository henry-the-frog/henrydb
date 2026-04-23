// savepoint-handler.js — Extracted from db.js (2026-04-23)
// SAVEPOINT, ROLLBACK TO, RELEASE SAVEPOINT handling

import { BTreeTable } from './btree-table.js';

/**
 * Handle SAVEPOINT name — create a snapshot of current table state.
 * @param {object} db - Database instance
 * @param {string} sql - Full SAVEPOINT SQL string
 * @returns {object} Result
 */
export function handleSavepoint(db, sql) {
  const match = sql.match(/SAVEPOINT\s+(\w+)/i);
  if (!match) throw new Error('Invalid SAVEPOINT syntax');
  const name = match[1].replace(/;$/, '');
  
  // Snapshot: deep-clone each table's row data directly from heap scan
  const snapshot = {};
  for (const [tableName, table] of db.tables) {
    const rows = [];
    for (const item of table.heap.scan()) {
      // Deep-clone row values to prevent mutation
      rows.push({ values: item.values.map(v => v) });
    }
    snapshot[tableName] = rows;
  }
  
  db._savepoints.push({ name, snapshot });
  return { type: 'OK', message: `Savepoint "${name}" created` };
}

/**
 * Handle ROLLBACK TO [SAVEPOINT] name — restore to savepoint.
 * @param {object} db - Database instance
 * @param {string} sql - Full ROLLBACK TO SQL string
 * @returns {object} Result
 */
export function handleRollbackToSavepoint(db, sql) {
  const match = sql.match(/ROLLBACK\s+TO\s+(?:SAVEPOINT\s+)?(\w+)/i);
  if (!match) throw new Error('Invalid ROLLBACK TO syntax');
  const name = match[1].replace(/;$/, '');
  
  // Find the savepoint
  const idx = db._savepoints.findLastIndex(sp => sp.name === name);
  if (idx === -1) throw new Error(`Savepoint "${name}" not found`);
  
  const { snapshot } = db._savepoints[idx];
  
  // Restore each table by replacing heap contents directly
  for (const [tableName, savedRows] of Object.entries(snapshot)) {
    const table = db.tables.get(tableName);
    if (!table) continue;
    
    // Create a fresh heap of the same type and re-insert saved rows
    const oldHeap = table.heap;
    if (oldHeap instanceof BTreeTable) {
      // BTreeTable: create fresh with same primary key column
      const pkCol = table.schema?.findIndex(c => c.primaryKey);
      table.heap = new BTreeTable(tableName, pkCol >= 0 ? pkCol : 0);
    } else {
      table.heap = db._heapFactory(tableName);
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
      for (const [indexName, idx2] of table.indexes) {
        if (idx2.clear) idx2.clear();
      }
      // Re-index all rows
      for (const item of table.heap.scan()) {
        for (const [indexName, idx2] of table.indexes) {
          if (idx2.insert) {
            try {
              const keyCol = idx2.column ?? idx2.columns?.[0];
              if (keyCol !== undefined) {
                const keyVal = typeof keyCol === 'number' ? item.values[keyCol] : item.values[table.schema.findIndex(c => c.name === keyCol)];
                idx2.insert(keyVal, { pageId: item.pageId, slotIdx: item.slotIdx });
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
  db._savepoints.splice(idx + 1);
  
  // Clear result cache — stale cached query results from before rollback
  if (db._resultCache) db._resultCache.clear();
  
  return { type: 'OK', message: `Rolled back to savepoint "${name}"` };
}

/**
 * Handle RELEASE [SAVEPOINT] name — remove savepoint.
 * @param {object} db - Database instance
 * @param {string} sql - Full RELEASE SQL string
 * @returns {object} Result
 */
export function handleReleaseSavepoint(db, sql) {
  const match = sql.match(/RELEASE\s+(?:SAVEPOINT\s+)?(\w+)/i);
  if (!match) throw new Error('Invalid RELEASE syntax');
  const name = match[1].replace(/;$/, '');
  
  const idx = db._savepoints.findLastIndex(sp => sp.name === name);
  if (idx === -1) throw new Error(`Savepoint "${name}" not found`);
  
  // Remove this and all later savepoints
  db._savepoints.splice(idx);
  return { type: 'OK', message: `Savepoint "${name}" released` };
}
