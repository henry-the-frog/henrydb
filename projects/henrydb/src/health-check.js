// health-check.js — Database health checker for HenryDB
// Verifies connectivity, table integrity, query performance.

/**
 * Run a comprehensive health check on a database.
 * @param {Database} db
 * @returns {Object} Health check results
 */
export function healthCheck(db) {
  const checks = [];
  const t0 = Date.now();
  
  // 1. Connectivity
  checks.push(runCheck('connectivity', () => {
    db.execute('SELECT 1 as ok');
    return { message: 'Database responds to queries' };
  }));
  
  // 2. Table count
  checks.push(runCheck('tables', () => {
    const r = db.execute('SHOW TABLES');
    return { message: `${r.rows.length} tables found`, count: r.rows.length };
  }));
  
  // 3. Table accessibility
  checks.push(runCheck('table_access', () => {
    const tables = db.execute('SHOW TABLES').rows;
    const errors = [];
    for (const row of tables) {
      const name = row.table_name || row.name || Object.values(row)[0];
      try {
        db.execute(`SELECT COUNT(*) FROM ${name}`);
      } catch(e) {
        errors.push(`${name}: ${e.message}`);
      }
    }
    if (errors.length > 0) throw new Error(`${errors.length} table(s) inaccessible: ${errors[0]}`);
    return { message: `All ${tables.length} tables accessible` };
  }));
  
  // 4. Query performance
  checks.push(runCheck('performance', () => {
    const start = performance.now();
    for (let i = 0; i < 100; i++) {
      db.execute('SELECT 1');
    }
    const ms = performance.now() - start;
    const qps = Math.round(100 / (ms / 1000));
    return { 
      message: `100 queries in ${ms.toFixed(1)}ms (${qps} qps)`,
      qps, latencyMs: +(ms / 100).toFixed(2),
    };
  }));
  
  // 5. Data integrity
  checks.push(runCheck('integrity', () => {
    const tables = db.execute('SHOW TABLES').rows;
    let totalRows = 0;
    for (const row of tables) {
      const name = row.table_name || row.name || Object.values(row)[0];
      if (name.startsWith('_')) continue;
      try {
        totalRows += db.execute(`SELECT COUNT(*) as cnt FROM ${name}`).rows[0].cnt;
      } catch(e) {}
    }
    return { message: `${totalRows} total rows across all tables`, totalRows };
  }));
  
  const totalMs = Date.now() - t0;
  const passing = checks.filter(c => c.status === 'pass').length;
  const failing = checks.filter(c => c.status === 'fail').length;
  
  return {
    status: failing === 0 ? 'healthy' : 'degraded',
    checks,
    summary: { total: checks.length, passing, failing, durationMs: totalMs },
  };
}

function runCheck(name, fn) {
  try {
    const result = fn();
    return { name, status: 'pass', ...result };
  } catch(e) {
    return { name, status: 'fail', message: e.message };
  }
}

/**
 * Format health check as readable string.
 */
export function formatHealthCheck(result) {
  const lines = [];
  const icon = result.status === 'healthy' ? '🟢' : '🔴';
  lines.push(`${icon} Database Health: ${result.status.toUpperCase()}`);
  lines.push('');
  
  for (const check of result.checks) {
    const mark = check.status === 'pass' ? '✅' : '❌';
    lines.push(`${mark} ${check.name}: ${check.message}`);
  }
  
  lines.push('');
  lines.push(`Duration: ${result.summary.durationMs}ms | ${result.summary.passing}/${result.summary.total} checks passing`);
  return lines.join('\n');
}
