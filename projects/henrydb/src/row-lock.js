// row-lock.js — Extracted from db.js (2026-04-23)
// Row-level locking for SELECT FOR UPDATE/SHARE

/**
 * Acquire row locks for SELECT FOR UPDATE/SHARE.
 * Implements NOWAIT and SKIP LOCKED semantics.
 * @param {object} db - Database instance
 * @param {object} ast - Parsed SELECT AST with forUpdate
 * @param {Array} rows - Result rows to lock
 */
export function acquireRowLocks(db, ast, rows) {
  const tableName = ast.from?.table || ast.from?.name;
  if (!tableName || tableName.startsWith('__')) return;
  
  const forMode = ast.forUpdate.includes('SHARE') ? 'SHARE' : 'UPDATE';
  const nowait = ast.forUpdate.includes('NOWAIT');
  const skipLocked = ast.forUpdate.includes('SKIP LOCKED');
  const txId = db._currentTxId || 0;
  
  const table = db.tables.get(tableName);
  if (!table) return;
  
  const pkIndices = table.schema
    .map((c, i) => c.primaryKey ? i : -1)
    .filter(i => i >= 0);
  
  for (let i = rows.length - 1; i >= 0; i--) {
    const row = rows[i];
    
    let rid = null;
    for (const item of table.heap.scan()) {
      const vals = item.values || item;
      let match = true;
      for (const pi of pkIndices) {
        if (vals[pi] !== row[table.schema[pi].name]) { match = false; break; }
      }
      if (match) {
        rid = { pageId: item.pageId, slotIdx: item.slotIdx };
        break;
      }
    }
    
    if (!rid) continue;
    
    const lockKey = `${tableName}:${rid.pageId}:${rid.slotIdx}`;
    const existingLock = db._rowLocks.get(lockKey);
    
    if (existingLock && existingLock.txId !== txId) {
      if (existingLock.mode === 'UPDATE' || forMode === 'UPDATE') {
        if (skipLocked) {
          rows.splice(i, 1);
          continue;
        }
        if (nowait) {
          throw new Error(`Could not obtain lock on row in "${tableName}": locked by transaction ${existingLock.txId}`);
        }
        throw new Error(`Row locked by transaction ${existingLock.txId} in "${tableName}"`);
      }
    }
    
    db._rowLocks.set(lockKey, { txId, mode: forMode });
  }
}

/**
 * Release all row locks held by a transaction.
 * @param {object} db - Database instance
 * @param {number} txId - Transaction ID
 */
export function releaseRowLocks(db, txId) {
  for (const [key, lock] of db._rowLocks) {
    if (lock.txId === txId) {
      db._rowLocks.delete(key);
    }
  }
}
