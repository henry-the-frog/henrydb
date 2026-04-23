// merge-executor.js — Extracted from db.js (2026-04-23)
// MERGE (UPSERT) statement execution

/**
 * Execute a MERGE statement (SQL:2003 MERGE INTO).
 * @param {object} db - Database instance
 * @param {object} ast - Parsed MERGE AST
 * @returns {object} Result with updated/inserted counts
 */
export function merge(db, ast) {
  const targetTable = db.tables.get(ast.target);
  if (!targetTable) throw new Error(`Table ${ast.target} not found`);
  const sourceTable = db.tables.get(ast.source);
  if (!sourceTable) throw new Error(`Table ${ast.source} not found`);
  
  const targetAlias = ast.targetAlias || ast.target;
  const sourceAlias = ast.sourceAlias || ast.source;
  
  let updated = 0, inserted = 0;
  
  // For each source row, check if it matches any target row
  for (const sourceItem of sourceTable.heap.scan()) {
    const sourceRow = db._valuesToRow(sourceItem.values, sourceTable.schema, sourceAlias);
    
    let matched = false;
    
    // Check against all target rows
    for (const targetItem of targetTable.heap.scan()) {
      const targetRow = db._valuesToRow(targetItem.values, targetTable.schema, targetAlias);
      const mergedRow = { ...targetRow, ...sourceRow };
      
      if (db._evalExpr(ast.on, mergedRow)) {
        matched = true;
        
        // Find WHEN MATCHED clause
        const matchClause = ast.whenClauses.find(c => c.type === 'MATCHED');
        if (matchClause && matchClause.action === 'UPDATE') {
          const newValues = [...targetItem.values];
          for (const assignment of matchClause.assignments) {
            const colIdx = targetTable.schema.findIndex(c => c.name === assignment.column);
            if (colIdx >= 0) {
              newValues[colIdx] = db._evalValue(assignment.value, mergedRow);
            }
          }
          // Validate constraints before modifying
          db._validateConstraintsForUpdate(targetTable, newValues, { pageId: targetItem.pageId, slotIdx: targetItem.slotIdx }, targetItem.values);
          targetTable.heap.delete(targetItem.pageId, targetItem.slotIdx);
          targetTable.heap.insert(newValues);
          updated++;
        }
        break; // Only match once per source row
      }
    }
    
    if (!matched) {
      // Find WHEN NOT MATCHED clause
      const notMatchClause = ast.whenClauses.find(c => c.type === 'NOT_MATCHED');
      if (notMatchClause && notMatchClause.action === 'INSERT') {
        const values = notMatchClause.values.map(v => db._evalValue(v, sourceRow));
        db._validateConstraints(targetTable, values);
        targetTable.heap.insert(values);
        inserted++;
      }
    }
  }
  
  // Invalidate cache
  if (db._resultCache) db._resultCache.clear();
  
  return { type: 'OK', message: `MERGE: ${updated} updated, ${inserted} inserted`, updated, inserted };
}
