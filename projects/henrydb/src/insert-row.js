// insert-row.js — Extracted from db.js (2026-04-23)
// Core row insertion logic with constraint validation, triggers, WAL, and index maintenance

/**
 * Insert a single row into a table.
 * Handles: column ordering, defaults, constraint validation, triggers, WAL, indexes.
 * @param {object} db - Database instance
 * @param {object} table - Table object
 * @param {Array|null} columns - Column names (null for positional insert)
 * @param {Array} values - Row values
 * @returns {object} RID of inserted row
 */

export function insertRow(db, table, columns, values) {
  let orderedValues;
  if (columns) {
    orderedValues = new Array(table.schema.length).fill(null);
    // Apply default values first
    for (let i = 0; i < table.schema.length; i++) {
      if (table.schema[i].defaultValue !== undefined && table.schema[i].defaultValue !== null) {
        orderedValues[i] = db._resolveDefault(table.schema[i].defaultValue);
      }
    }
    for (let i = 0; i < columns.length; i++) {
      const colIdx = table.schema.findIndex(c => c.name.toLowerCase() === columns[i].toLowerCase());
      if (colIdx === -1) throw new Error(`Column ${columns[i]} not found`);
      orderedValues[colIdx] = values[i];
    }
  } else {
    orderedValues = values;
    // Pad short value arrays with defaults (e.g., after ALTER TABLE ADD COLUMN)
    if (orderedValues.length < table.schema.length) {
      orderedValues = [...orderedValues];
      for (let i = orderedValues.length; i < table.schema.length; i++) {
        if (table.schema[i].defaultValue !== undefined && table.schema[i].defaultValue !== null) {
          orderedValues[i] = db._resolveDefault(table.schema[i].defaultValue);
        } else {
          orderedValues[i] = null;
        }
      }
    }
}

  // SERIAL auto-increment: assign next value for SERIAL columns with null value
  for (let i = 0; i < table.schema.length; i++) {
    if (table.schema[i].type === 'SERIAL' && (orderedValues[i] === null || orderedValues[i] === undefined)) {
      if (!table._serialCounters) table._serialCounters = {};
      if (!table._serialCounters[i]) {
        // Find max existing value
        let max = 0;
        for (const tuple of table.heap.scan()) {
          const v = tuple.values ? tuple.values[i] : tuple[i];
          if (typeof v === 'number' && v > max) max = v;
        }
        table._serialCounters[i] = max;
      }
      table._serialCounters[i]++;
      orderedValues[i] = table._serialCounters[i];
    } else if (table.schema[i].type === 'SERIAL' && typeof orderedValues[i] === 'number') {
      // Explicit value provided — update counter to at least this value
      if (!table._serialCounters) table._serialCounters = {};
      if (!table._serialCounters[i] || orderedValues[i] > table._serialCounters[i]) {
        table._serialCounters[i] = orderedValues[i];
      }
    }
}

  // Validate constraints
  // Handle SERIAL columns: auto-increment if null
  for (let i = 0; i < table.schema.length; i++) {
    if (table.schema[i].serial && (orderedValues[i] === null || orderedValues[i] === undefined)) {
      const seqName = table.schema[i].serial.toLowerCase();
      const seq = db.sequences.get(seqName);
      if (seq) {
        seq.current += seq.increment;
        orderedValues[i] = seq.current;
      }
    } else if (table.schema[i].serial) {
      // Explicit value — advance sequence past it
      if (typeof orderedValues[i] === 'number') {
        const seqName = table.schema[i].serial.toLowerCase();
        const seq = db.sequences.get(seqName);
        if (seq && orderedValues[i] > seq.current) {
          seq.current = orderedValues[i];
        }
      }
    }
}

  // Compute generated/computed columns
  for (let i = 0; i < table.schema.length; i++) {
    if (table.schema[i].generated) {
      // Build a row object for expression evaluation
      const row = {};
      for (let j = 0; j < table.schema.length; j++) {
        row[table.schema[j].name] = orderedValues[j];
      }
      orderedValues[i] = db._evalValue(table.schema[i].generated, row);
    }
}

  db._validateConstraints(table, orderedValues);

  // BEFORE INSERT triggers
  const tableName = table.heap?.name || '';
  db._fireTriggers('BEFORE', 'INSERT', tableName, orderedValues, table.schema);

  const rid = table.heap.insert(orderedValues);

  // WAL: log the insert
  const txId = db._currentTxId || db._batchTxId || db._nextTxId++;
  db.wal.appendInsert(txId, tableName, rid.pageId, rid.slotIdx, orderedValues);
  if (!db._currentTxId && !db._batchTxId) {
    // Auto-commit mode (single row): immediately commit
    db.wal.appendCommit(txId);
}

  // Update indexes
  for (const [colName, index] of table.indexes) {
    const colIdx = table.schema.findIndex(c => c.name === colName);
    if (index._isHash) {
      const existing = index.get(orderedValues[colIdx]);
      if (existing !== undefined) {
        const arr = Array.isArray(existing) ? existing : [existing];
        arr.push(rid);
        index.insert(orderedValues[colIdx], arr);
      } else {
        index.insert(orderedValues[colIdx], rid);
      }
    } else {
      index.insert(orderedValues[colIdx], rid);
    }
}

  // AFTER INSERT triggers
  db._fireTriggers('AFTER', 'INSERT', tableName, orderedValues, table.schema);

  return rid;
}