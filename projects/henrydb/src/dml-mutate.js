// dml-mutate.js — UPDATE and DELETE handlers extracted from db.js

export function update(db, ast) {
  const table = db.tables.get(ast.table);
  if (!table) throw new Error(`Table ${ast.table} not found`);

  let updated = 0;
  const toUpdate = [];

  if (ast.from) {
    // UPDATE ... FROM: join with another table
    const fromTable = db.tables.get(ast.from);
    if (!fromTable) throw new Error(`Table ${ast.from} not found`);
    const fromAlias = ast.fromAlias || ast.from;
    
    for (const { pageId, slotIdx, values } of table.heap.scan()) {
      const row = db._valuesToRow(values, table.schema, ast.table);
      
      // For each from-table row, check WHERE
      for (const fromItem of fromTable.heap.scan()) {
        const fromRow = db._valuesToRow(fromItem.values, fromTable.schema, fromAlias);
        const merged = { ...row, ...fromRow };
        
        if (!ast.where || db._evalExpr(ast.where, merged)) {
          toUpdate.push({ pageId, slotIdx, values: [...values], mergedRow: merged });
          break; // Only update target row once per match
        }
      }
    }
  } else {
    // Try index scan for simple equality WHERE (e.g., WHERE id = 5)
    let usedIndex = false;
    if (ast.where && !ast.from && ast.where.type === 'COMPARE' && ast.where.op === 'EQ') {
      const left = ast.where.left;
      const right = ast.where.right;
      let colName = null, val = null;
      if (left.type === 'column_ref' && (right.type === 'number' || right.type === 'string' || right.type === 'literal')) {
        colName = left.name.includes('.') ? left.name.split('.').pop() : left.name;
        val = right.value;
      } else if (right.type === 'column_ref' && (left.type === 'number' || left.type === 'string' || left.type === 'literal')) {
        colName = right.name.includes('.') ? right.name.split('.').pop() : right.name;
        val = left.value;
      }
      if (colName) {
        const colNameLower = colName.toLowerCase();
        const index = table.indexes.get(colName) || table.indexes.get(colNameLower);
        if (index && typeof index.search === 'function') {
          // Use range() for non-unique indexes to get ALL matching rows
          // search() only returns one match which misses duplicates
          let rids;
          if (typeof index.range === 'function') {
            const rangeResults = index.range(val, val);
            rids = rangeResults.map(r => r.value || r);
          } else {
            const results = index.search(val);
            rids = results !== undefined && results !== null ? 
              (Array.isArray(results) ? results : [results]) : [];
          }
          if (rids.length > 0) {
            for (const rid of rids) {
              try {
                const tuple = table.heap.get(rid.pageId, rid.slotIdx);
                if (tuple) {
                  const values = Array.isArray(tuple) ? tuple : (tuple.values || tuple);
                  const row = db._valuesToRow(values, table.schema, ast.table);
                  if (db._evalExpr(ast.where, row)) {
                    toUpdate.push({ pageId: rid.pageId, slotIdx: rid.slotIdx, values: [...values], mergedRow: row });
                  }
                }
              } catch(e) { /* stale index entry */ }
            }
            usedIndex = true;
            // MVCC fix: if index returned RIDs but all were invisible
            // (heap.get returned null for all), fall through to full scan
            // so we find the version visible to this transaction's snapshot.
            if (toUpdate.length === 0) usedIndex = false;
          }
        }
      }
    }
    
    if (!usedIndex) {
      // Fallback: full table scan
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const row = db._valuesToRow(values, table.schema, ast.table);
        if (!ast.where || db._evalExpr(ast.where, row)) {
          toUpdate.push({ pageId, slotIdx, values: [...values], mergedRow: row });
        }
      }
    }
  }

  const returnedRows = [];
  
  // Batch WAL: use a single transaction for all updates
  const batchTxId = db._currentTxId || db._nextTxId++;
  const isAutoCommit = !db._currentTxId;

  for (const item of toUpdate) {
    const newValues = [...item.values];
    const row = item.mergedRow || db._valuesToRow(item.values, table.schema, ast.table);
    for (const { column, value } of ast.assignments) {
      const colLower = column.toLowerCase();
      const colIdx = table.schema.findIndex(c => c.name === column || c.name.toLowerCase() === colLower);
      if (colIdx === -1) throw new Error(`Column ${column} not found`);
      newValues[colIdx] = db._evalValue(value, row);
    }

    // Recompute generated columns
    for (let gi = 0; gi < table.schema.length; gi++) {
      if (table.schema[gi].generated) {
        const genRow = {};
        for (let gj = 0; gj < table.schema.length; gj++) {
          genRow[table.schema[gj].name] = newValues[gj];
        }
        newValues[gi] = db._evalValue(table.schema[gi].generated, genRow);
      }
    }

    // Remove old index entries
    for (const [colName, index] of table.indexes) {
      const colIdx = table.schema.findIndex(c => c.name === colName);
      // B+ tree doesn't have delete, so we rebuild affected indexes after
    }

    // BEFORE UPDATE triggers
    db._fireTriggers('BEFORE', 'UPDATE', ast.table, newValues, table.schema, item.values);

    // Validate constraints BEFORE modifying the heap
    // Pass the current row's RID so UNIQUE check skips it
    db._validateConstraintsForUpdate(table, newValues, { pageId: item.pageId, slotIdx: item.slotIdx }, item.values);
    // HOT chain detection: check if any indexed column values changed.
    // If no indexed columns changed, this is a HOT (Heap-Only Tuple) update:
    // we skip index updates and create a HOT chain pointer from old → new.
    let isHotUpdate = false;
    if (table.indexes.size > 0) {
      isHotUpdate = true;
      for (const [colName, index] of table.indexes) {
        const colIdx = table.schema.findIndex(c => c.name === colName);
        if (colIdx >= 0 && item.values[colIdx] !== newValues[colIdx]) {
          isHotUpdate = false;
          break;
        }
      }
    }

    // Check row-level locks before modifying
    const lockKey = `${ast.table}:${item.pageId}:${item.slotIdx}`;
    const existingLock = db._rowLocks.get(lockKey);
    if (existingLock && existingLock.txId !== (db._currentTxId || 0) && existingLock.mode === 'UPDATE') {
      throw new Error(`Cannot UPDATE: row locked by transaction ${existingLock.txId} in "${ast.table}"`);
    }

    // Try in-place update first (avoids tombstones and heap bloat)
    let newRid = null;
    if (isHotUpdate && table.heap.update) {
      newRid = table.heap.update(item.pageId, item.slotIdx, newValues);
    }
    
    if (!newRid) {
      // Fallback: delete old, insert new
      table.heap.delete(item.pageId, item.slotIdx);
      newRid = table.heap.insert(newValues);
    }

    // WAL: log the update
    db.wal.appendUpdate(batchTxId, ast.table, newRid.pageId, newRid.slotIdx, item.values, newValues);

    if (isHotUpdate && table.heap.addHotChain) {
      // HOT update: create chain pointer from old RID → new RID
      // Index entries still point to old RID; index scans follow the chain.
      table.heap.addHotChain(item.pageId, item.slotIdx, newRid.pageId, newRid.slotIdx);
    } else {
      // Non-HOT update: update all indexes with new entries
      for (const [colName, index] of table.indexes) {
        const colIdx = table.schema.findIndex(c => c.name === colName);
        index.insert(newValues[colIdx], newRid);
      }
    }

    // Handle ON UPDATE CASCADE for foreign keys
    db._handleForeignKeyUpdate(ast.table, table, item.values, newValues);

    // AFTER UPDATE triggers
    db._fireTriggers('AFTER', 'UPDATE', ast.table, newValues, table.schema, item.values);

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
  if (isAutoCommit && updated > 0) db.wal.appendCommit(batchTxId);

  if (ast.returning) {
    const filteredRows = db._resolveReturning(ast.returning, returnedRows);
    return { type: 'ROWS', rows: filteredRows, count: updated };
  }

  if (table.deadTupleCount !== undefined) table.deadTupleCount += updated;
  db._maybeAutoVacuum(ast.table, table);
  return { type: 'OK', message: `${updated} row(s) updated`, count: updated };
}

export function executeDelete(db, ast) {
  const table = db.tables.get(ast.table);
  if (!table) throw new Error(`Table ${ast.table} not found`);

  let deleted = 0;
  const toDelete = [];

  if (ast.using) {
    // DELETE ... USING: join with another table
    const usingTable = db.tables.get(ast.using);
    if (!usingTable) throw new Error(`Table ${ast.using} not found`);
    const usingAlias = ast.usingAlias || ast.using;
    
    for (const { pageId, slotIdx, values } of table.heap.scan()) {
      const row = db._valuesToRow(values, table.schema, ast.table);
      for (const usingItem of usingTable.heap.scan()) {
        const usingRow = db._valuesToRow(usingItem.values, usingTable.schema, usingAlias);
        const merged = { ...row, ...usingRow };
        if (!ast.where || db._evalExpr(ast.where, merged)) {
          toDelete.push({ pageId, slotIdx });
          break; // Only delete target row once per match
        }
      }
    }
  } else {
    // Try index scan for simple equality WHERE
    let usedIndex = false;
    if (ast.where && ast.where.type === 'COMPARE' && ast.where.op === 'EQ') {
      const left = ast.where.left;
      const right = ast.where.right;
      let colName = null, val = null;
      if (left.type === 'column_ref' && (right.type === 'number' || right.type === 'string' || right.type === 'literal')) {
        colName = left.name.includes('.') ? left.name.split('.').pop() : left.name;
        val = right.value;
      } else if (right.type === 'column_ref' && (left.type === 'number' || left.type === 'string' || left.type === 'literal')) {
        colName = right.name.includes('.') ? right.name.split('.').pop() : right.name;
        val = left.value;
      }
      if (colName) {
        const index = table.indexes.get(colName);
        if (index && typeof index.search === 'function') {
          // Use range() for non-unique indexes to get ALL matching rows
          let rids;
          if (typeof index.range === 'function') {
            const rangeResults = index.range(val, val);
            rids = rangeResults.map(r => r.value || r);
          } else {
            const results = index.search(val);
            rids = results !== undefined && results !== null ? 
              (Array.isArray(results) ? results : [results]) : [];
          }
          if (rids.length > 0) {
            for (const rid of rids) {
              try {
                const tuple = table.heap.get(rid.pageId, rid.slotIdx);
                if (tuple) {
                  const values = Array.isArray(tuple) ? tuple : (tuple.values || tuple);
                  const row = db._valuesToRow(values, table.schema, ast.table);
                  if (db._evalExpr(ast.where, row)) {
                    toDelete.push({ pageId: rid.pageId, slotIdx: rid.slotIdx });
                  }
                }
              } catch(e) { /* stale index entry */ }
            }
            usedIndex = true;
            // MVCC fix: if index returned RIDs but all were invisible
            // (heap.get returned null for all), fall through to full scan
            if (toDelete.length === 0) usedIndex = false;
          }
        }
      }
    }
    
    if (!usedIndex) {
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const row = db._valuesToRow(values, table.schema, ast.table);
        if (!ast.where || db._evalExpr(ast.where, row)) {
          toDelete.push({ pageId, slotIdx });
        }
      }
    }
  }

  const deletedRows = [];
  
  // Batch WAL: use a single transaction for all deletes
  const batchTxId = db._currentTxId || db._nextTxId++;
  const isAutoCommit = !db._currentTxId;

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
      db._handleForeignKeyDelete(ast.table, table, values);
    }

    // BEFORE DELETE triggers
    if (values) {
      db._fireTriggers('BEFORE', 'DELETE', ast.table, null, table.schema, values);
    }
    
    // Check row-level locks before deleting
    const delLockKey = `${ast.table}:${pageId}:${slotIdx}`;
    const delLock = db._rowLocks.get(delLockKey);
    if (delLock && delLock.txId !== (db._currentTxId || 0) && delLock.mode === 'UPDATE') {
      throw new Error(`Cannot DELETE: row locked by transaction ${delLock.txId} in "${ast.table}"`);
    }
    
    table.heap.delete(pageId, slotIdx);
    
    // WAL: log the delete
    if (values) {
      db.wal.appendDelete(batchTxId, ast.table, pageId, slotIdx, values);
    }

    // AFTER DELETE triggers
    if (values) {
      db._fireTriggers('AFTER', 'DELETE', ast.table, null, table.schema, values);
    }
    
    deleted++;
  }

  // Single WAL commit for all deletes
  if (isAutoCommit && deleted > 0) db.wal.appendCommit(batchTxId);

  if (ast.returning) {
    const filteredRows = db._resolveReturning(ast.returning, deletedRows);
    return { type: 'ROWS', rows: filteredRows, count: deleted };
  }

  if (table.deadTupleCount !== undefined) table.deadTupleCount += deleted;
  if (table.liveTupleCount !== undefined) table.liveTupleCount -= deleted;
  db._maybeAutoVacuum(ast.table, table);
  return { type: 'OK', message: `${deleted} row(s) deleted`, count: deleted };
}
