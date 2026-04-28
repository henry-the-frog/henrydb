// fk-cascade.js — Extracted from db.js (2026-04-23)
// Foreign key CASCADE, SET NULL, RESTRICT handling for DELETE and UPDATE

// Cache: tableName → boolean (has any child FK references)
const _fkRefCache = new WeakMap();

/**
 * Check if any table has a FK referencing the given parent table.
 * Caches the result per db instance for fast repeated checks.
 */
function _hasChildReferences(db, parentTableName) {
  let dbCache = _fkRefCache.get(db);
  if (!dbCache) {
    dbCache = new Map();
    _fkRefCache.set(db, dbCache);
  }
  if (dbCache.has(parentTableName)) return dbCache.get(parentTableName);
  
  let hasRef = false;
  for (const [name, table] of db.tables) {
    for (const col of table.schema) {
      if (col.references && col.references.table === parentTableName) {
        hasRef = true;
        break;
      }
    }
    if (hasRef) break;
  }
  dbCache.set(parentTableName, hasRef);
  return hasRef;
}

/**
 * Invalidate the FK reference cache for a given db.
 * Call when schema changes (ALTER TABLE, CREATE TABLE with FK, DROP TABLE).
 */
export function invalidateFkCache(db) {
  _fkRefCache.delete(db);
}

/**
 * Handle foreign key actions when a parent row is deleted.
 * Supports CASCADE, SET NULL, RESTRICT.
 * @param {object} db - Database instance
 * @param {string} parentTableName - Parent table name
 * @param {object} parentTable - Parent table object
 * @param {Array} parentValues - Deleted row values
 */
export function handleForeignKeyDelete(db, parentTableName, parentTable, parentValues) {
  // Quick check: skip if no tables reference this one
  if (!_hasChildReferences(db, parentTableName)) return;
  // Find all child tables that reference this table
  for (const [childTableName, childTable] of db.tables) {
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
            handleForeignKeyDelete(db, childTableName, childTable, childValues);
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

/**
 * Handle foreign key actions when a parent row's PK is updated.
 * Supports CASCADE, SET NULL.
 * @param {object} db - Database instance
 * @param {string} parentTableName - Parent table name
 * @param {object} parentTable - Parent table object
 * @param {Array} oldValues - Old row values
 * @param {Array} newValues - New row values
 */
export function handleForeignKeyUpdate(db, parentTableName, parentTable, oldValues, newValues) {
  // Quick check: skip if no tables reference this one
  if (!_hasChildReferences(db, parentTableName)) return;
  for (const [childTableName, childTable] of db.tables) {
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
