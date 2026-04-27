// dml-insert.js — INSERT handlers extracted from db.js
// Note: These functions take 'db' as first parameter (database context)

export function insert(db, ast) {
  const table = db.tables.get(ast.table);
  if (!table) {
    // Check if it's a view with INSTEAD OF INSERT trigger
    const view = db.views.get(ast.table);
    if (view) {
      const insteadTrigger = db.triggers.find(
        t => t.timing === 'INSTEAD OF' && t.event === 'INSERT' && t.table === ast.table
      );
      if (insteadTrigger) {
        return _executeInsteadOfInsert(db, ast, view, insteadTrigger);
      }
      throw new Error(`Cannot INSERT into view ${ast.table} (no INSTEAD OF trigger)`);
    }
    throw new Error(`Table ${ast.table} not found`);
  }

  // Handle INSERT OR REPLACE/IGNORE: synthesize onConflict
  if (ast.conflictAction && !ast.onConflict) {
    if (ast.conflictAction === 'REPLACE') {
      // REPLACE = ON CONFLICT (pk) DO UPDATE SET all columns = new values
      const pkCol = table.schema.find(c => c.primaryKey);
      if (pkCol) {
        ast.onConflict = {
          column: pkCol.name,
          action: 'UPDATE',
          sets: table.schema.map(c => ({
            column: c.name,
            value: { type: 'column_ref', name: `excluded.${c.name}` }
          }))
        };
      }
    } else if (ast.conflictAction === 'IGNORE') {
      const pkCol = table.schema.find(c => c.primaryKey);
      ast.onConflict = {
        column: pkCol?.name || null,
        action: 'NOTHING'
      };
    }
  }

  let inserted = 0;
  const returnedRows = [];
  
  // Batch autocommit: use a single WAL transaction for all rows
  const isBatch = ast.rows.length > 1 && !db._currentTxId;
  let batchTxId;
  if (isBatch) {
    batchTxId = db._nextTxId++;
    db._batchTxId = batchTxId;
  }
  
  for (const row of ast.rows) {
    const values = row.map((r, colIdx) => {
      if (r.type === 'literal') return r.value;
      // Handle DEFAULT keyword
      if (r.type === 'column_ref' && r.name === 'DEFAULT') {
        // Resolve the column's default value
        const schema = ast.columns ? 
          table.schema.find(s => s.name === ast.columns[colIdx]) :
          table.schema[colIdx];
        if (schema?.defaultValue != null) return db._resolveDefault(schema.defaultValue);
        return null;
      }
      // Evaluate expression (for INSERT VALUES with arithmetic, CASE, etc.)
      try { return db._evalValue(r, {}); } catch { return r.value; }
    });
    
    // UPSERT: ON CONFLICT handling
    if (ast.onConflict) {
      const orderedValues = db._orderValues(table, ast.columns, values);
      
      // Determine conflict column(s) — could be PK or specified UNIQUE column
      let conflictIdx = -1;
      if (ast.onConflict.column) {
        // ON CONFLICT (column) — use specified column
        conflictIdx = table.schema.findIndex(c => c.name.toLowerCase() === ast.onConflict.column.toLowerCase());
      } else if (ast.onConflict.columns && ast.onConflict.columns.length > 0) {
        // ON CONFLICT (col1, col2, ...) — use first column for now
        conflictIdx = table.schema.findIndex(c => c.name.toLowerCase() === ast.onConflict.columns[0].toLowerCase());
      }
      if (conflictIdx < 0) {
        // Fallback to PK
        conflictIdx = table.schema.findIndex(c => c.primaryKey);
      }
      
      if (conflictIdx >= 0 && orderedValues[conflictIdx] != null) {
        // Check if conflict column value already exists
        let existing = null;
        let existingRid = null;
        for (const tuple of table.heap.scan()) {
          const tupleValues = tuple.values || tuple;
          if (tupleValues[conflictIdx] === orderedValues[conflictIdx]) { 
            existing = tupleValues; 
            existingRid = { pageId: tuple.pageId, slotIdx: tuple.slotIdx };
            break; 
          }
        }
        
        if (existing) {
          if (ast.onConflict.action === 'NOTHING') {
            continue; // Skip this row
          }
          if (ast.onConflict.action === 'UPDATE') {
            // Build row object for expression evaluation
            const existingRow = {};
            table.schema.forEach((c, i) => { existingRow[c.name] = existing[i]; });
            // Also expose excluded.* (the values that would have been inserted)
            table.schema.forEach((c, i) => { existingRow[`excluded.${c.name}`] = orderedValues[i]; });
            
            // Evaluate SET expressions
            const newValues = [...existing];
            for (const set of ast.onConflict.sets) {
              const colIdx = table.schema.findIndex(c => c.name.toLowerCase() === set.column.toLowerCase());
              if (colIdx >= 0) {
                newValues[colIdx] = db._evalValue(set.value, existingRow);
              }
            }
            
            // Validate constraints BEFORE modifying the heap (CHECK, NOT NULL, FK)
            // For UNIQUE/PK: we need to exclude the row being updated
            db._validateConstraintsForUpdate(table, newValues, existingRid, existing);
            
            // Write back to heap: delete old row, insert new one
            if (existingRid) {
              table.heap.delete(existingRid.pageId, existingRid.slotIdx);
              const newRid = table.heap.insert(newValues);
              
              // Update conflict column index
              const pkIdx = table.schema.findIndex(c => c.primaryKey);
              if (pkIdx >= 0 && table.indexes.has(table.schema[pkIdx].name)) {
                const idx = table.indexes.get(table.schema[pkIdx].name);
                idx.insert(newValues[pkIdx], newRid);
              }
              if (conflictIdx !== pkIdx && table.indexes.has(table.schema[conflictIdx].name)) {
                const idx = table.indexes.get(table.schema[conflictIdx].name);
                idx.insert(newValues[conflictIdx], newRid);
              }
            }
            
            if (ast.returning) {
              const retRow = {};
              table.schema.forEach((c, i) => { retRow[c.name] = newValues[i]; });
              returnedRows.push(retRow);
            }
            inserted++;
            continue;
          }
        }
      }
    }
    
    // Check UNIQUE constraints (including PRIMARY KEY columns)
    // Skip if UPSERT (ON CONFLICT) is in play — conflicts are handled above
    if (!ast.onConflict) {
    // Must use schema-ordered values, not insert-column-ordered values
    const orderedValsForCheck = db._orderValues(table, ast.columns, values);
    // Apply SERIAL auto-increment for the check
    for (let ci = 0; ci < table.schema.length; ci++) {
      if (table.schema[ci].serial && (orderedValsForCheck[ci] === null || orderedValsForCheck[ci] === undefined)) {
        // Skip SERIAL columns that haven't been assigned yet — they'll be unique
        continue;
      }
      if ((table.schema[ci].unique || table.schema[ci].primaryKey) && orderedValsForCheck[ci] != null) {
        for (const tuple of table.heap.scan()) {
          const tv = tuple.values || tuple;
          if (tv[ci] === orderedValsForCheck[ci]) {
            throw new Error(`UNIQUE constraint violated on column ${table.schema[ci].name}: duplicate value '${orderedValsForCheck[ci]}'`);
          }
        }
      }
    }
    }

    // Check UNIQUE INDEX constraints from indexCatalog
    // Skip if UPSERT (ON CONFLICT) is in play — conflicts are handled above
    if (!ast.onConflict) {
    for (const [idxName, meta] of db.indexCatalog) {
      if (meta.table === ast.table && meta.unique) {
        const idxTable = db.tables.get(meta.table);
        if (!idxTable) continue;
        const colIndices = meta.columns.map(col => idxTable.schema.findIndex(c => c.name === col));
        if (colIndices.some(i => i < 0)) continue;
        // Get the ordered values (map from ast.columns to schema order)
        const orderedVals = new Array(idxTable.schema.length).fill(null);
        if (ast.columns) {
          for (let i = 0; i < ast.columns.length; i++) {
            const ci = idxTable.schema.findIndex(c => c.name === ast.columns[i]);
            if (ci >= 0) orderedVals[ci] = values[i];
          }
        } else {
          for (let i = 0; i < values.length; i++) orderedVals[i] = values[i];
        }
        const keyValues = colIndices.map(i => orderedVals[i]);
        // Skip NULL keys (SQL standard: NULLs are not equal)
        if (keyValues.some(v => v === null || v === undefined)) continue;
        // Scan existing data for duplicate (index-independent check)
        for (const tuple of idxTable.heap.scan()) {
          const tv = tuple.values || tuple;
          const existingKey = colIndices.map(i => tv[i]);
          if (keyValues.every((v, i) => v === existingKey[i])) {
            throw new Error(`UNIQUE constraint violated on index ${idxName}: duplicate key '${keyValues.join(', ')}'`);
          }
        }
      }
    }
    } // end if (!ast.onConflict) for UNIQUE INDEX check
    
    // When ON CONFLICT DO NOTHING is active without specific columns,
    // wrap insert in try/catch to handle constraint violations gracefully
    if (ast.onConflict && ast.onConflict.action === 'NOTHING') {
      try {
        const rid = db._insertRow(table, ast.columns, values);
        inserted++;
        if (ast.returning) {
          const lastTuple = [...table.heap.scan()].pop();
          const actualValues = lastTuple?.values || lastTuple || [];
          const retRow = {};
          table.schema.forEach((c, i) => { retRow[c.name] = actualValues[i]; });
          returnedRows.push(retRow);
        }
      } catch (e) {
        if (e.message && e.message.includes('constraint')) {
          continue; // Silently skip on constraint violation
        }
        throw e; // Re-throw non-constraint errors
      }
    } else {
    const rid = db._insertRow(table, ast.columns, values);
    inserted++;
    
    if (ast.returning) {
      // Read actual inserted values (including SERIAL-assigned IDs)
      const lastTuple = [...table.heap.scan()].pop();
      const actualValues = lastTuple?.values || lastTuple || [];
      const retRow = {};
      table.schema.forEach((c, i) => { retRow[c.name] = actualValues[i]; });
      returnedRows.push(retRow);
    }
    } // end else (non-NOTHING upsert or normal insert)
  }

  // Batch autocommit: commit the batch transaction after all rows
  if (isBatch && batchTxId !== undefined) {
    db.wal.appendCommit(batchTxId);
    db._batchTxId = undefined;
  }

  if (ast.returning) {
    const filteredRows = db._resolveReturning(ast.returning, returnedRows);
    return { type: 'ROWS', rows: filteredRows, count: inserted };
  }
  if (table.liveTupleCount !== undefined) table.liveTupleCount += inserted;
  db._changes = inserted;
  return { type: 'OK', message: `${inserted} row(s) inserted`, count: inserted };
}

export function insertSelect(db, ast) {
  const table = db.tables.get(ast.table);
  if (!table) throw new Error(`Table ${ast.table} not found`);

  const result = db.execute_ast(ast.query);
  let inserted = 0;
  const returnedRows = [];
  
  // Batch autocommit: use a single WAL transaction for all rows
  const isBatch = result.rows.length > 1 && !db._currentTxId;
  let batchTxId;
  if (isBatch) {
    batchTxId = db._nextTxId++;
    db._batchTxId = batchTxId;
  }
  
  // Determine how many SELECT columns we expect
  const selectCols = ast.query?.columns?.length || table.schema.length;
  
  for (const row of result.rows) {
    const values = [];
    if (ast.columns) {
      // Explicit column list: INSERT INTO t (col1, col2) SELECT ...
      // Map SELECT results POSITIONALLY (by order, not by name)
      const rowKeys = Object.keys(row);
      for (let i = 0; i < ast.columns.length; i++) {
        if (i < rowKeys.length) {
          values.push(row[rowKeys[i]]);
        } else {
          values.push(null);
        }
      }
    } else {
      // No explicit column list: map SELECT result to table schema by column name or position
      const rowKeys = Object.keys(row);
      // Try name-based mapping first
      let nameMatch = true;
      for (const col of table.schema) {
        if (row[col.name] === undefined && row[col.name] !== null) {
          nameMatch = false;
          break;
        }
      }
      if (nameMatch && table.schema.every(col => col.name in row)) {
        // Name-based mapping: match by column name
        for (const col of table.schema) {
          values.push(row[col.name] !== undefined ? row[col.name] : null);
        }
      } else {
        // Position-based mapping: use all keys in order
        for (let i = 0; i < table.schema.length; i++) {
          if (i < rowKeys.length) {
            values.push(row[rowKeys[i]]);
          } else {
            values.push(null);
          }
        }
      }
    }
    db._insertRow(table, ast.columns || null, values);
    if (ast.returning) {
      // Build the row from ordered values mapped to schema
      const retRow = {};
      if (ast.columns) {
        // Map values to specified columns, fill rest with defaults/null
        for (let i = 0; i < table.schema.length; i++) {
          const colIdx = ast.columns.indexOf(table.schema[i].name);
          retRow[table.schema[i].name] = colIdx >= 0 && colIdx < values.length ? values[colIdx] : null;
        }
      } else {
        const rowKeys = Object.keys(row);
        for (let i = 0; i < table.schema.length; i++) {
          retRow[table.schema[i].name] = i < rowKeys.length ? row[rowKeys[i]] : null;
        }
      }
      returnedRows.push(retRow);
    }
    inserted++;
  }

  // Batch autocommit: commit the batch transaction after all rows
  if (isBatch && batchTxId !== undefined) {
    db.wal.appendCommit(batchTxId);
    db._batchTxId = undefined;
  }

  if (ast.returning) {
    const filteredRows = db._resolveReturning(ast.returning, returnedRows);
    return { type: 'ROWS', rows: filteredRows, count: inserted };
  }
  if (table.liveTupleCount !== undefined) table.liveTupleCount += inserted;
  return { type: 'OK', message: `${inserted} row(s) inserted`, count: inserted };
}

const MAX_TRIGGER_DEPTH = 32;
let _triggerDepth = 0;

export function fireTriggers(db, timing, event, tableName, rowValues, schema, oldRowValues) {
  _triggerDepth++;
  if (_triggerDepth > MAX_TRIGGER_DEPTH) {
    _triggerDepth--;
    throw new Error(`Trigger recursion depth exceeded (max ${MAX_TRIGGER_DEPTH})`);
  }
  try {
  for (const trigger of db.triggers) {
    if (trigger.timing === timing && trigger.event === event && trigger.table === tableName) {
      // UPDATE OF columns check: only fire if at least one specified column changed
      if (trigger.columns && trigger.columns.length > 0 && event === 'UPDATE' && schema && rowValues && oldRowValues) {
        const changed = trigger.columns.some(colName => {
          const idx = schema.findIndex(c => c.name.toUpperCase() === colName.toUpperCase());
          return idx >= 0 && rowValues[idx] !== oldRowValues[idx];
        });
        if (!changed) continue;
      }

      // WHEN clause check: evaluate condition with NEW/OLD bindings
      if (trigger.whenClause) {
        try {
          let whenSql = trigger.whenClause;
          if (schema && rowValues) {
            for (let i = 0; i < schema.length; i++) {
              const colName = schema[i].name;
              const val = rowValues[i];
              const sqlVal = val === null || val === undefined ? 'NULL'
                : typeof val === 'string' ? `'${val.replace(/'/g, "''")}'`
                : String(val);
              whenSql = whenSql.replace(new RegExp(`NEW\\.${colName}\\b`, 'gi'), sqlVal);
            }
          }
          if (schema && oldRowValues) {
            for (let i = 0; i < schema.length; i++) {
              const colName = schema[i].name;
              const val = oldRowValues[i];
              const sqlVal = val === null || val === undefined ? 'NULL'
                : typeof val === 'string' ? `'${val.replace(/'/g, "''")}'`
                : String(val);
              whenSql = whenSql.replace(new RegExp(`OLD\\.${colName}\\b`, 'gi'), sqlVal);
            }
          }
          const result = db.execute(`SELECT CASE WHEN (${whenSql}) THEN 1 ELSE 0 END AS v`);
          if (!result.rows || result.rows.length === 0 || result.rows[0].v !== 1) continue;
        } catch (e) {
          // If WHEN evaluation fails, skip the trigger
          continue;
        }
      }

      try {
        // Build NEW and OLD row objects from values + schema
        let bodySql = trigger.bodySql;
        if (schema && rowValues) {
          for (let i = 0; i < schema.length; i++) {
            const colName = schema[i].name;
            const val = rowValues[i];
            const sqlVal = val === null || val === undefined ? 'NULL' 
              : typeof val === 'string' ? `'${val.replace(/'/g, "''")}'` 
              : String(val);
            // Replace NEW.column references
            bodySql = bodySql.replace(new RegExp(`NEW\\.${colName}\\b`, 'gi'), sqlVal);
          }
        }
        if (schema && oldRowValues) {
          for (let i = 0; i < schema.length; i++) {
            const colName = schema[i].name;
            const val = oldRowValues[i];
            const sqlVal = val === null || val === undefined ? 'NULL'
              : typeof val === 'string' ? `'${val.replace(/'/g, "''")}'`
              : String(val);
            // Replace OLD.column references
            bodySql = bodySql.replace(new RegExp(`OLD\\.${colName}\\b`, 'gi'), sqlVal);
          }
        }
        // Strip BEGIN...END wrapper
        let cleanSql = bodySql.trim();
        if (cleanSql.toUpperCase().startsWith('BEGIN')) cleanSql = cleanSql.slice(5).trim();
        if (cleanSql.toUpperCase().endsWith('END')) cleanSql = cleanSql.slice(0, -3).trim();
        
        // Split on semicolons for multi-statement triggers
        const statements = cleanSql.split(';').map(s => s.trim()).filter(Boolean);
        for (const stmt of statements) {
          db.execute(stmt);
        }
      } catch (e) {
        // Trigger errors propagate
        throw new Error(`Trigger ${trigger.name} failed: ${e.message}`);
      }
    }
  }
  } finally {
    _triggerDepth--;
  }
}

// Execute INSTEAD OF INSERT trigger for a view
function _executeInsteadOfInsert(db, ast, view, trigger) {
  // For INSTEAD OF INSERT, we need to:
  // 1. Get the column names from the INSERT statement (or from the view definition)
  // 2. Evaluate the VALUES expressions to get actual values
  // 3. Substitute NEW.col references in the trigger body
  // 4. Execute the trigger body

  const results = [];
  
  // Get column names from INSERT (or view columns)
  let columnNames = ast.columns;
  if (!columnNames && view.query && view.query.columns) {
    columnNames = view.query.columns
      .filter(c => c.type !== 'star')
      .map(c => c.alias || c.name || (c.expr && c.expr.name));
  }

  // Process each VALUES row
  for (const valueExprs of (ast.rows || ast.values || [])) {
    // Evaluate each expression to get literal values
    const newValues = {};
    for (let i = 0; i < valueExprs.length; i++) {
      const expr = valueExprs[i];
      const colName = columnNames && columnNames[i] ? columnNames[i] : `col${i}`;
      // Simple expression evaluation for common cases
      if (expr.type === 'literal') {
        newValues[colName] = expr.value;
      } else if (expr.type === 'null') {
        newValues[colName] = null;
      } else {
        // For complex expressions, evaluate via SQL
        try {
          const r = db.execute(`SELECT ${_exprToSql(expr)} AS v`);
          newValues[colName] = r.rows[0]?.v;
        } catch {
          newValues[colName] = null;
        }
      }
    }

    // Substitute NEW.col references in trigger body
    let bodySql = trigger.bodySql;
    for (const [colName, val] of Object.entries(newValues)) {
      const sqlVal = val === null || val === undefined ? 'NULL'
        : typeof val === 'string' ? `'${val.replace(/'/g, "''")}'`
        : String(val);
      bodySql = bodySql.replace(new RegExp(`NEW\\.${colName}\\b`, 'gi'), sqlVal);
    }

    // Execute trigger body
    let cleanSql = bodySql.trim();
    if (cleanSql.toUpperCase().startsWith('BEGIN')) cleanSql = cleanSql.slice(5).trim();
    if (cleanSql.toUpperCase().endsWith('END')) cleanSql = cleanSql.slice(0, -3).trim();

    const statements = cleanSql.split(';').map(s => s.trim()).filter(Boolean);
    for (const stmt of statements) {
      db.execute(stmt);
    }
    results.push(newValues);
  }

  return { type: 'INSERT', rowCount: results.length, rows: results };
}

// Helper: convert expression AST back to SQL string
function _exprToSql(expr) {
  if (!expr) return 'NULL';
  if (expr.type === 'literal') {
    if (typeof expr.value === 'string') return `'${expr.value.replace(/'/g, "''")}'`;
    return String(expr.value);
  }
  if (expr.type === 'null') return 'NULL';
  if (expr.type === 'column_ref') return expr.name;
  if (expr.type === 'COMPARE') return `${_exprToSql(expr.left)} ${expr.op} ${_exprToSql(expr.right)}`;
  if (expr.type === 'BINARY_OP') return `(${_exprToSql(expr.left)} ${expr.op} ${_exprToSql(expr.right)})`;
  return 'NULL';
}
