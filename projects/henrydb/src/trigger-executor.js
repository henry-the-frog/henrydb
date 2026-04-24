// trigger-executor.js — Execute triggers (BEFORE/AFTER INSERT/UPDATE/DELETE)
//
// Triggers are stored as { name, timing, event, table, bodySql }.
// bodySql contains SQL with NEW.col and OLD.col references that need
// to be substituted with actual row values before execution.

/**
 * Fire matching triggers for a DML operation.
 * @param {Database} db
 * @param {string} timing - 'BEFORE' or 'AFTER'
 * @param {string} event - 'INSERT', 'UPDATE', or 'DELETE'
 * @param {string} tableName - Table name (uppercased)
 * @param {Object} newRow - New row values { col: value } (for INSERT/UPDATE)
 * @param {Object} oldRow - Old row values { col: value } (for DELETE/UPDATE)
 */
export function fireTriggers(db, timing, event, tableName, newRow = null, oldRow = null) {
  const matching = db.triggers.filter(t =>
    t.timing.toUpperCase() === timing &&
    t.event.toUpperCase() === event &&
    t.table.toUpperCase() === tableName.toUpperCase()
  );

  for (const trigger of matching) {
    executeTriggerBody(db, trigger.bodySql, newRow, oldRow);
  }
}

/**
 * Execute a trigger's body SQL with NEW/OLD substitution.
 */
function executeTriggerBody(db, bodySql, newRow, oldRow) {
  // Strip BEGIN ... END wrapper
  let sql = bodySql.trim();
  if (sql.toUpperCase().startsWith('BEGIN')) sql = sql.slice(5).trim();
  if (sql.toUpperCase().endsWith('END')) sql = sql.slice(0, -3).trim();

  // Split on semicolons for multi-statement triggers
  const statements = sql.split(';').map(s => s.trim()).filter(Boolean);

  for (let stmt of statements) {
    // Substitute NEW.column references
    if (newRow) {
      stmt = substituteRowRefs(stmt, 'NEW', newRow);
    }
    // Substitute OLD.column references
    if (oldRow) {
      stmt = substituteRowRefs(stmt, 'OLD', oldRow);
    }

    // Execute the substituted statement
    if (stmt.trim()) {
      db.execute(stmt);
    }
  }
}

/**
 * Replace PREFIX.column references with actual values.
 * Handles: NEW.name → 'Alice', NEW.id → 42, NEW.col → NULL
 */
function substituteRowRefs(sql, prefix, row) {
  // Match PREFIX.identifier patterns (case-insensitive)
  const pattern = new RegExp(`\\b${prefix}\\s*\\.\\s*([A-Za-z_][A-Za-z0-9_]*)`, 'gi');
  
  return sql.replace(pattern, (match, colName) => {
    // Try exact case, then uppercase, then lowercase
    let value = row[colName];
    if (value === undefined) value = row[colName.toUpperCase()];
    if (value === undefined) value = row[colName.toLowerCase()];
    if (value === undefined) return 'NULL';
    
    if (value === null) return 'NULL';
    if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`;
    return String(value);
  });
}
