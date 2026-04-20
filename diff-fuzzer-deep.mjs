import { Database } from './src/db.js';
import { execSync } from 'child_process';

function sqliteQuery(sql) {
  try {
    const result = execSync(`sqlite3 :memory: "${sql.replace(/"/g, '\\"')}"`, { 
      encoding: 'utf8', timeout: 10000 
    }).trim();
    return { ok: true, result };
  } catch (e) {
    return { ok: false, error: e.message?.split('\n')[0] };
  }
}

function henryQuery(db, sql) {
  try {
    const result = db.execute(sql);
    if (result?.rows) {
      // Format like SQLite output (pipe-separated)
      return { ok: true, result: result.rows.map(r => Object.values(r).join('|')).join('\n') };
    }
    return { ok: true, result: String(result?.rowCount ?? '') };
  } catch (e) {
    return { ok: false, error: e.message?.split('\n')[0] };
  }
}

function compareResults(henry, sqlite, label) {
  // Both error = skip (syntax differences)
  if (!henry.ok && !sqlite.ok) return null;
  // Henry errors but SQLite succeeds = BUG
  if (!henry.ok && sqlite.ok) return { type: 'henry_error', label, henry: henry.error, sqlite: sqlite.result };
  // Henry succeeds but SQLite errors = possible extension (not a bug)
  if (henry.ok && !sqlite.ok) return null;
  // Both succeed — compare values
  const h = henry.result.trim();
  const s = sqlite.result.trim();
  if (h === s) return null;
  
  // Try numeric comparison (floating point tolerance)
  const hLines = h.split('\n');
  const sLines = s.split('\n');
  if (hLines.length !== sLines.length) {
    return { type: 'row_count_mismatch', label, henry: h, sqlite: s, henryRows: hLines.length, sqliteRows: sLines.length };
  }
  
  for (let i = 0; i < hLines.length; i++) {
    const hVals = hLines[i].split('|');
    const sVals = sLines[i].split('|');
    for (let j = 0; j < Math.max(hVals.length, sVals.length); j++) {
      const hv = hVals[j]?.trim() ?? '';
      const sv = sVals[j]?.trim() ?? '';
      if (hv === sv) continue;
      // Numeric tolerance
      const hn = parseFloat(hv), sn = parseFloat(sv);
      if (!isNaN(hn) && !isNaN(sn) && Math.abs(hn - sn) < 0.001) continue;
      return { type: 'value_mismatch', label, row: i, col: j, henry: hv, sqlite: sv, full_henry: h, full_sqlite: s };
    }
  }
  return null; // Close enough
}

function run() {
  const db = new Database();
  
  // Shared schema
  const setupSQL = [
    'CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)',
    'CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER, amount REAL)',
  ];
  
  const insertSQL = [];
  for (let i = 1; i <= 50; i++) {
    insertSQL.push(`INSERT INTO t1 VALUES (${i}, ${i * 10 + (i % 7)}, 'name${i}')`);
  }
  for (let i = 1; i <= 100; i++) {
    insertSQL.push(`INSERT INTO t2 VALUES (${i}, ${1 + i % 50}, ${(i * 3.14).toFixed(2)})`);
  }
  
  // Setup both databases
  const sqliteSetup = [...setupSQL, ...insertSQL].join('; ');
  execSync(`sqlite3 :memory: "${sqliteSetup.replace(/"/g, '\\"')}"`, { encoding: 'utf8' });
  
  for (const sql of [...setupSQL, ...insertSQL]) db.execute(sql);
  
  console.log('Setup complete: t1 (50 rows), t2 (100 rows)\n');
  
  // Test queries — these should produce identical results in both
  const testQueries = [
    // Basic SELECT
    'SELECT COUNT(*) FROM t1',
    'SELECT SUM(val) FROM t1',
    'SELECT AVG(val) FROM t1',
    'SELECT MIN(val), MAX(val) FROM t1',
    'SELECT val, COUNT(*) FROM t1 GROUP BY val ORDER BY val LIMIT 10',
    
    // WHERE
    'SELECT COUNT(*) FROM t1 WHERE val > 50',
    'SELECT COUNT(*) FROM t1 WHERE val BETWEEN 20 AND 80',
    'SELECT COUNT(*) FROM t1 WHERE name LIKE \'name1%\'',
    'SELECT COUNT(*) FROM t1 WHERE val IN (10, 20, 30, 40, 50)',
    
    // GROUP BY + HAVING
    'SELECT val % 10 as bucket, COUNT(*) as cnt FROM t1 GROUP BY val % 10 HAVING COUNT(*) > 1 ORDER BY bucket',
    
    // Subqueries
    'SELECT COUNT(*) FROM t1 WHERE val > (SELECT AVG(val) FROM t1)',
    'SELECT COUNT(*) FROM t1 WHERE id IN (SELECT t1_id FROM t2 WHERE amount > 100)',
    'SELECT COUNT(*) FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)',
    'SELECT COUNT(*) FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id AND t2.amount > 500)',
    
    // JOIN  
    'SELECT COUNT(*) FROM t1 JOIN t2 ON t1.id = t2.t1_id',
    'SELECT t1.id, SUM(t2.amount) as total FROM t1 JOIN t2 ON t1.id = t2.t1_id GROUP BY t1.id ORDER BY t1.id LIMIT 10',
    'SELECT COUNT(*) FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id WHERE t2.id IS NULL',
    
    // Expressions
    'SELECT 1 + 2 * 3',
    'SELECT (1 + 2) * 3',
    'SELECT 10 / 3',  // Integer division difference?
    'SELECT 10.0 / 3',
    'SELECT CAST(10 AS REAL) / 3',
    'SELECT ABS(-42)',
    'SELECT COALESCE(NULL, NULL, 42)',
    'SELECT NULLIF(1, 1)',
    'SELECT NULLIF(1, 2)',
    
    // CASE
    'SELECT CASE WHEN 1 > 2 THEN \'yes\' ELSE \'no\' END',
    'SELECT SUM(CASE WHEN val > 50 THEN 1 ELSE 0 END) FROM t1',
    
    // ORDER BY
    'SELECT id FROM t1 ORDER BY val DESC LIMIT 5',
    'SELECT id FROM t1 ORDER BY val ASC, id DESC LIMIT 5',
    
    // DISTINCT
    'SELECT DISTINCT val % 10 FROM t1 ORDER BY val % 10',
    'SELECT COUNT(DISTINCT val % 10) FROM t1',
    
    // UNION
    'SELECT id FROM t1 WHERE id <= 3 UNION SELECT id FROM t1 WHERE id >= 48 ORDER BY id',
    'SELECT id FROM t1 WHERE id <= 3 UNION ALL SELECT id FROM t1 WHERE id <= 3 ORDER BY id',
    
    // NULL handling  
    'SELECT NULL IS NULL',
    'SELECT NULL = NULL',
    'SELECT NULL <> NULL',
    'SELECT 1 + NULL',
    
    // String functions
    'SELECT LENGTH(\'hello\')',
    'SELECT UPPER(\'hello\')',
    'SELECT LOWER(\'HELLO\')',
    'SELECT SUBSTR(\'hello\', 2, 3)',
    'SELECT REPLACE(\'hello world\', \'world\', \'earth\')',
    'SELECT TRIM(\'  hello  \')',
    
    // Math  
    'SELECT ABS(-5)',
    'SELECT MAX(1, 2, 3)',
    'SELECT MIN(1, 2, 3)',
    
    // Complex
    'SELECT t1.id, t1.val, (SELECT COUNT(*) FROM t2 WHERE t2.t1_id = t1.id) as t2_count FROM t1 WHERE t1.id <= 5 ORDER BY t1.id',
  ];
  
  let passed = 0, discrepancies = [], errors = [], skipped = 0;
  
  for (const sql of testQueries) {
    // For SQLite, we need to set up the data per query (since :memory: is fresh)
    const fullSqlite = `${sqliteSetup}; ${sql}`;
    const s = sqliteQuery(fullSqlite);
    const h = henryQuery(db, sql);
    
    const diff = compareResults(h, s, sql);
    if (diff === null) {
      if (!h.ok && !s.ok) {
        skipped++;
      } else {
        passed++;
      }
    } else {
      if (diff.type === 'henry_error') {
        errors.push(diff);
        console.log(`❌ ERROR: ${sql}`);
        console.log(`   Henry: ${diff.henry}`);
        console.log(`   SQLite: ${diff.sqlite}\n`);
      } else {
        discrepancies.push(diff);
        console.log(`⚠️  MISMATCH: ${sql}`);
        console.log(`   Henry: ${diff.henry || diff.full_henry}`);
        console.log(`   SQLite: ${diff.sqlite || diff.full_sqlite}\n`);
      }
    }
  }
  
  console.log(`\n--- Results: ${passed} passed, ${errors.length} errors, ${discrepancies.length} mismatches, ${skipped} skipped ---`);
  
  if (errors.length) {
    console.log('\nErrors (Henry fails, SQLite succeeds):');
    errors.forEach(e => console.log(`  ${e.label}: ${e.henry}`));
  }
  if (discrepancies.length) {
    console.log('\nMismatches (different results):');
    discrepancies.forEach(d => console.log(`  ${d.label}: henry=${d.henry||'(multi)'} sqlite=${d.sqlite||'(multi)'}`));
  }
}

run();
