// table-stats.js — Table statistics collector for HenryDB
// Collects row counts, column stats (min, max, nulls, distinct), and more.

/**
 * Collect comprehensive statistics for a table.
 * @param {Database} db - HenryDB instance
 * @param {string} table - Table name
 * @returns {Object} Table statistics
 */
export function collectTableStats(db, table) {
  const stats = {
    table,
    rowCount: 0,
    columns: {},
    sizeEstimate: 0,
  };

  // Row count
  try {
    const r = db.execute(`SELECT COUNT(*) as cnt FROM ${table}`);
    stats.rowCount = r.rows[0].cnt;
  } catch(e) {
    stats.error = e.message;
    return stats;
  }

  if (stats.rowCount === 0) return stats;

  // Get column names from first row
  const peek = db.execute(`SELECT * FROM ${table} LIMIT 1`);
  const columns = Object.keys(peek.rows[0]);

  for (const col of columns) {
    const colStats = {
      name: col,
      type: typeof peek.rows[0][col],
    };

    try {
      // Basic stats
      const r = db.execute(`SELECT 
        COUNT(${col}) as non_null,
        COUNT(DISTINCT ${col}) as distinct_values,
        MIN(${col}) as min_val,
        MAX(${col}) as max_val
        FROM ${table}
      `);
      
      const row = r.rows[0];
      colStats.nonNull = row.non_null;
      colStats.nullCount = stats.rowCount - row.non_null;
      colStats.distinctValues = row.distinct_values;
      colStats.min = row.min_val;
      colStats.max = row.max_val;
      colStats.nullRate = +((colStats.nullCount / stats.rowCount) * 100).toFixed(1);
      colStats.selectivity = stats.rowCount > 0 ? +(row.distinct_values / stats.rowCount).toFixed(4) : 0;

      // Numeric stats
      if (typeof peek.rows[0][col] === 'number') {
        const numR = db.execute(`SELECT AVG(${col}) as avg_val, SUM(${col}) as sum_val FROM ${table}`);
        colStats.avg = +numR.rows[0].avg_val?.toFixed(4);
        colStats.sum = numR.rows[0].sum_val;
      }
    } catch(e) {
      colStats.error = e.message;
    }

    stats.columns[col] = colStats;
  }

  // Rough size estimate (bytes)
  stats.sizeEstimate = stats.rowCount * columns.length * 20; // ~20 bytes per cell avg

  return stats;
}

/**
 * Collect statistics for all tables in a database.
 */
export function collectAllStats(db) {
  const result = {};
  try {
    const tables = db.execute('SHOW TABLES');
    for (const row of tables.rows) {
      const name = row.table_name || row.name || Object.values(row)[0];
      if (!name.startsWith('_')) {
        result[name] = collectTableStats(db, name);
      }
    }
  } catch(e) {}
  return result;
}

/**
 * Format stats as a human-readable report.
 */
export function formatStatsReport(stats) {
  const lines = [];
  lines.push(`Table: ${stats.table}`);
  lines.push(`Rows: ${stats.rowCount.toLocaleString()}`);
  lines.push(`Size estimate: ${(stats.sizeEstimate / 1024).toFixed(1)} KB`);
  lines.push('');
  
  if (Object.keys(stats.columns).length > 0) {
    lines.push(`${'Column'.padEnd(20)} ${'Type'.padEnd(10)} ${'Distinct'.padStart(10)} ${'Nulls'.padStart(8)} ${'Min'.padStart(12)} ${'Max'.padStart(12)}`);
    lines.push('─'.repeat(75));
    
    for (const col of Object.values(stats.columns)) {
      const type = (col.type || '?').substring(0, 8);
      const distinct = String(col.distinctValues || '?').padStart(10);
      const nulls = col.nullRate !== undefined ? `${col.nullRate}%`.padStart(8) : '?'.padStart(8);
      const min = String(col.min ?? '').substring(0, 12).padStart(12);
      const max = String(col.max ?? '').substring(0, 12).padStart(12);
      lines.push(`${col.name.padEnd(20)} ${type.padEnd(10)} ${distinct} ${nulls} ${min} ${max}`);
    }
  }
  
  return lines.join('\n');
}
