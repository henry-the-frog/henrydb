// batch-executor.js — Execute SQL scripts with error handling and reporting
// Handles multi-statement scripts, progress tracking, and transactional execution.

/**
 * Execute a SQL script (multiple statements) with reporting.
 * @param {Database} db
 * @param {string} script - SQL script (semicolon-separated statements)
 * @param {Object} options - { stopOnError, transaction, onProgress }
 * @returns {Object} Execution report
 */
export function executeBatch(db, script, options = {}) {
  const { stopOnError = true, transaction = false, onProgress } = options;
  
  // Split into statements (respecting comments)
  const statements = parseStatements(script);
  
  const report = {
    total: statements.length,
    executed: 0,
    succeeded: 0,
    failed: 0,
    results: [],
    errors: [],
    duration: 0,
  };
  
  const t0 = Date.now();
  
  if (transaction) {
    try { db.execute('BEGIN'); } catch(e) {}
  }
  
  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    const stmtT0 = Date.now();
    
    try {
      const result = db.execute(stmt);
      const duration = Date.now() - stmtT0;
      report.executed++;
      report.succeeded++;
      report.results.push({
        index: i,
        sql: stmt.substring(0, 100),
        status: 'success',
        rows: result.rows?.length || 0,
        duration,
      });
    } catch(e) {
      const duration = Date.now() - stmtT0;
      report.executed++;
      report.failed++;
      const error = {
        index: i,
        sql: stmt.substring(0, 100),
        status: 'error',
        message: e.message,
        duration,
      };
      report.results.push(error);
      report.errors.push(error);
      
      if (stopOnError) {
        if (transaction) {
          try { db.execute('ROLLBACK'); } catch(e2) {}
        }
        break;
      }
    }
    
    if (onProgress) {
      onProgress({
        current: i + 1,
        total: statements.length,
        percent: +((i + 1) / statements.length * 100).toFixed(1),
      });
    }
  }
  
  if (transaction && report.failed === 0) {
    try { db.execute('COMMIT'); } catch(e) {}
  }
  
  report.duration = Date.now() - t0;
  return report;
}

/**
 * Parse SQL script into individual statements.
 */
function parseStatements(script) {
  return script
    .split('\n')
    .filter(line => !line.trim().startsWith('--'))
    .join('\n')
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0);
}

/**
 * Format execution report as readable string.
 */
export function formatReport(report) {
  const lines = [];
  lines.push(`Batch Execution Report`);
  lines.push(`Statements: ${report.total} total, ${report.succeeded} succeeded, ${report.failed} failed`);
  lines.push(`Duration: ${report.duration}ms`);
  
  if (report.errors.length > 0) {
    lines.push('\nErrors:');
    for (const err of report.errors) {
      lines.push(`  [${err.index}] ${err.message}`);
      lines.push(`      SQL: ${err.sql}...`);
    }
  }
  
  return lines.join('\n');
}
