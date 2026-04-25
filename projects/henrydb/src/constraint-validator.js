// constraint-validator.js — Extracted from db.js (2026-04-23)
// Constraint validation for INSERT and UPDATE operations

/**
 * Validate all constraints for a new row (INSERT).
 * @param {object} db - Database instance (for _evalExpr, tables access)
 * @param {object} table - Table object with schema, heap, indexes, tableChecks
 * @param {Array} values - Row values to validate
 */
export function validateConstraints(db, table, values) {
  return validateConstraintsForUpdate(db, table, values, null);
}

/**
 * Validate constraints, optionally excluding a specific row (for UPDATE/UPSERT
 * where the old row should not trigger UNIQUE false positives).
 * @param {object} db - Database instance (for _evalExpr, tables access)
 * @param {object} table - Table object
 * @param {Array} values - Row values to validate
 * @param {object|null} excludeRid - Row ID to exclude from uniqueness checks
 * @param {Array|null} oldValues - Old row values (for UPDATE optimization)
 */
export function validateConstraintsForUpdate(db, table, values, excludeRid, oldValues) {
  for (let i = 0; i < table.schema.length; i++) {
    const col = table.schema[i];
    const val = values[i];

    // Skip UNIQUE check if value hasn't changed (UPDATE with same PK/UNIQUE value)
    if (excludeRid && oldValues && (col.unique || col.primaryKey) && val === oldValues[i]) {
      continue;
    }

    // NOT NULL constraint (PRIMARY KEY columns are implicitly NOT NULL)
    if ((col.notNull || col.primaryKey) && val == null) {
      throw new Error(`NOT NULL constraint violated for column ${col.name}`);
    }

    // CHECK constraint
    if (col.check) {
      const row = {};
      for (let j = 0; j < table.schema.length; j++) {
        row[table.schema[j].name] = values[j];
      }
      const colIdx = table.schema.indexOf(col);
      if (colIdx >= 0 && values[colIdx] == null) continue;
      const result = db._evalExpr(col.check, row);
      if (!result) {
        throw new Error(`CHECK constraint violated for column ${col.name}`);
      }
    }

    // Table-level CHECK constraints
    if (table.tableChecks && table.tableChecks.length > 0) {
      const row = {};
      for (let j = 0; j < table.schema.length; j++) {
        row[table.schema[j].name] = values[j];
      }
      for (const checkExpr of table.tableChecks) {
        const result = db._evalExpr(checkExpr, row);
        if (result === false) {
          throw new Error('CHECK constraint violated');
        }
      }
    }

    // UNIQUE and PRIMARY KEY uniqueness check
    if ((col.unique || col.primaryKey) && val != null) {
      const index = table.indexes?.get(col.name);
      if (index && typeof index.search === 'function') {
        const found = index.search(val);
        if (found !== undefined && found !== null) {
          const rids = Array.isArray(found) ? found : [found];
          const liveRids = rids.filter(r => {
            if (excludeRid && r.pageId === excludeRid.pageId && r.slotIdx === excludeRid.slotIdx) return false;
            try {
              const row = table.heap.get(r.pageId, r.slotIdx);
              return row !== null && row !== undefined;
            } catch { return false; }
          });
          if (liveRids.length > 0) {
            throw new Error(`UNIQUE constraint violated: duplicate value '${val}' for column ${col.name}`);
          }
        }
      } else {
        for (const tuple of table.heap.scan()) {
          if (excludeRid && tuple.pageId === excludeRid.pageId && tuple.slotIdx === excludeRid.slotIdx) continue;
          const tupleValues = tuple.values || tuple;
          if (tupleValues[i] === val) {
            throw new Error(`UNIQUE constraint violated: duplicate value '${val}' for column ${col.name}`);
          }
        }
      }
    }

    // FOREIGN KEY constraint
    if (col.references && val != null) {
      const refTable = db.tables.get(col.references.table);
      if (!refTable) throw new Error(`Referenced table ${col.references.table} not found`);
      const refColIdx = refTable.schema.findIndex(c => c.name === col.references.column);
      let found = false;
      let foundPageId = null, foundSlotIdx = null;
      for (const { pageId, slotIdx, values: refValues } of refTable.heap.scan()) {
        if (refValues[refColIdx] === val) {
          found = true;
          foundPageId = pageId;
          foundSlotIdx = slotIdx;
          break;
        }
      }
      if (!found) {
        throw new Error(`Foreign key constraint violated: ${val} not found in ${col.references.table}(${col.references.column})`);
      }
      // MVCC visibility check: if another active transaction has marked this row
      // for deletion (xmax set to another tx's id), reject the FK reference to prevent
      // orphaned references after both transactions commit.
      if (found && db._tdb) {
        const tdb = db._tdb;
        const refTableName = col.references.table;
        const vm = tdb._versionMaps && tdb._versionMaps.get(refTableName);
        const activeTx = tdb._activeTx;
        if (vm && activeTx) {
          const key = `${foundPageId}:${foundSlotIdx}`;
          const ver = vm.get(key);
          if (ver && ver.xmax !== 0 && ver.xmax !== activeTx.txId) {
            // Another transaction is deleting this row — check if it's still active
            const otherTx = activeTx.manager && activeTx.manager.activeTxns
              ? activeTx.manager.activeTxns.get(ver.xmax)
              : null;
            if (otherTx && !otherTx.committed && !otherTx.aborted) {
              throw new Error(
                `Foreign key constraint violated: referenced row in ${refTableName}(${col.references.column}) ` +
                `is being deleted by another transaction`
              );
            }
          }
        }
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
