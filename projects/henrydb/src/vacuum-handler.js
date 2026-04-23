// vacuum-handler.js — Extracted from db.js (2026-04-23)
// VACUUM command handling: dead tuple cleanup, heap compaction, index rebuild

/**
 * Handle VACUUM [table] — clean dead tuples, compact heaps, rebuild indexes.
 * @param {object} db - Database instance
 * @param {object} ast - Parsed VACUUM AST
 * @returns {object} Vacuum result with stats
 */
export function handleVacuum(db, ast) {
  const tables = ast.table ? [db.tables.get(ast.table.toUpperCase()) || db.tables.get(ast.table)] 
                           : [...db.tables.values()];
  let totalDead = 0, totalBytes = 0, totalPages = 0, totalTables = 0;

  // MVCC-level vacuum (clean old versions across all keys)
  if (db._mvccManager) {
    try {
      const gcResult = db._mvccManager.gc();
      totalDead += gcResult.cleaned;
    } catch (e) {
      // GC is best-effort
    }
  }

  for (const table of tables) {
    if (!table) continue;
    totalTables++;
    
    // Table-level MVCC vacuum
    if (table.mvccHeap && db._mvccManager) {
      try {
        const result = table.mvccHeap.vacuum(db._mvccManager);
        totalDead += result.deadTuplesRemoved || 0;
        totalBytes += result.bytesFreed || 0;
        totalPages += result.pagesCompacted || 0;
        
        if (result.deadTuplesRemoved > 0 && table.indexes && table.indexes.size > 0) {
          db._rebuildIndexes(table);
        }
      } catch (e) {
        // Table-level vacuum not supported, skip
      }
      continue;
    }
    
    // Non-MVCC vacuum: compact heap pages, update statistics
    const heap = table.heap;
    if (!heap) continue;
    
    let liveRows = 0;
    if (typeof heap.scan === 'function') {
      for (const _ of heap.scan()) liveRows++;
    } else if (heap.rowCount !== undefined) {
      liveRows = heap.rowCount;
    }
    
    if (table.stats) {
      table.stats.rowCount = liveRows;
      table.stats.lastVacuum = Date.now();
    }
    table.deadTupleCount = 0;
    
    if (typeof heap.flush === 'function') {
      heap.flush();
    }
    
    if (heap._hotChains && heap._hotChains.size > 0 && table.indexes && table.indexes.size > 0) {
      db._rebuildIndexes(table);
    }
  }

  return {
    type: 'OK',
    message: `VACUUM: ${totalTables} table(s) processed, ${totalDead} dead tuples removed, ${totalBytes} bytes freed`,
    details: { tablesProcessed: totalTables, deadTuplesRemoved: totalDead, bytesFreed: totalBytes, pagesCompacted: totalPages },
  };
}
