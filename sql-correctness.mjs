import { Database } from './src/db.js';

function check(label, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) {
    console.log(`✅ ${label}`);
    return true;
  }
  console.log(`❌ ${label}`);
  console.log(`   Expected: ${e}`);
  console.log(`   Actual:   ${a}`);
  return false;
}

function run() {
  const db = new Database();
  
  // Setup
  db.execute('CREATE TABLE t1 (id INT, val INT, grp TEXT)');
  db.execute("INSERT INTO t1 VALUES (1, 10, 'A'), (2, NULL, 'A'), (3, 30, 'B'), (4, NULL, 'B'), (5, 50, NULL)");
  
  db.execute('CREATE TABLE t2 (id INT, t1_id INT, label TEXT)');
  db.execute("INSERT INTO t2 VALUES (1, 1, 'x'), (2, 1, 'y'), (3, 3, 'z'), (4, 99, 'orphan')");
  
  let passed = 0, failed = 0;
  
  // ========== NULL Semantics ==========
  console.log('\n=== NULL Semantics ===');
  
  // NULL in WHERE comparisons
  const r1 = db.execute('SELECT COUNT(*) as c FROM t1 WHERE val = NULL').rows[0];
  passed += check('NULL = NULL in WHERE returns no rows', r1, { c: 0 }) ? 1 : 0; failed += r1.c !== 0 ? 0 : 0;
  
  const r2 = db.execute('SELECT COUNT(*) as c FROM t1 WHERE val IS NULL').rows[0];
  check('IS NULL finds NULL rows', r2, { c: 2 }) ? passed++ : failed++;
  
  const r3 = db.execute('SELECT COUNT(*) as c FROM t1 WHERE val IS NOT NULL').rows[0];
  check('IS NOT NULL excludes NULL rows', r3, { c: 3 }) ? passed++ : failed++;
  
  // NULL in aggregates
  const r4 = db.execute('SELECT COUNT(*) as all_rows, COUNT(val) as non_null FROM t1').rows[0];
  check('COUNT(*) includes NULL, COUNT(val) excludes NULL', r4, { all_rows: 5, non_null: 3 }) ? passed++ : failed++;
  
  const r5 = db.execute('SELECT SUM(val) as s, AVG(val) as a FROM t1').rows[0];
  // SUM should skip NULLs: 10+30+50=90, AVG: 90/3=30
  check('SUM and AVG skip NULL values', { s: r5.s, a: r5.a }, { s: 90, a: 30 }) ? passed++ : failed++;
  
  // NULL in GROUP BY
  const r6 = db.execute('SELECT grp, COUNT(*) as c FROM t1 GROUP BY grp ORDER BY grp').rows;
  // NULL group should be separate
  check('GROUP BY treats NULL as a group', r6.length >= 3, true) ? passed++ : failed++;
  
  // NULL in COALESCE
  const r7 = db.execute("SELECT COALESCE(val, -1) as c FROM t1 WHERE id = 2").rows[0];
  check('COALESCE replaces NULL', r7, { c: -1 }) ? passed++ : failed++;
  
  // ========== OUTER JOIN ==========
  console.log('\n=== OUTER JOIN ===');
  
  // LEFT JOIN preserves left rows
  const r8 = db.execute('SELECT t1.id, t2.label FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.id').rows;
  check('LEFT JOIN preserves all left rows', r8.length >= 5, true) ? passed++ : failed++;
  
  // LEFT JOIN NULL fill
  const noMatch = r8.find(r => r.id === 5 || r['t1.id'] === 5);
  check('LEFT JOIN fills NULL for non-matching', noMatch?.label === null || noMatch?.['t2.label'] === null, true) ? passed++ : failed++;
  
  // LEFT JOIN with IS NULL (anti-join pattern)
  const r9 = db.execute('SELECT t1.id FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id WHERE t2.id IS NULL').rows;
  check('LEFT JOIN + IS NULL = anti-join', r9.length, 3) ? passed++ : failed++; // ids 2,4,5 have no match
  
  // ========== GROUP BY + HAVING ==========
  console.log('\n=== GROUP BY + HAVING ===');
  
  // HAVING with aggregate
  const r10 = db.execute("SELECT grp, COUNT(*) as c FROM t1 WHERE grp IS NOT NULL GROUP BY grp HAVING COUNT(*) >= 2 ORDER BY grp").rows;
  check('HAVING COUNT(*) >= 2', r10.length, 2) ? passed++ : failed++; // A:2, B:2
  
  // HAVING with expression
  const r11 = db.execute("SELECT grp, AVG(val) as avg_val FROM t1 WHERE grp IS NOT NULL GROUP BY grp HAVING AVG(val) > 20 ORDER BY grp").rows;
  // A: avg(10,NULL)=10, B: avg(30,NULL)=30, so only B passes
  check('HAVING AVG > 20', r11.length, 1) ? passed++ : failed++;
  
  // GROUP BY without aggregate in SELECT
  const r12 = db.execute('SELECT DISTINCT grp FROM t1 ORDER BY grp').rows;
  check('DISTINCT returns unique values including NULL', r12.length >= 3, true) ? passed++ : failed++;
  
  // ========== Edge Cases ==========
  console.log('\n=== Edge Cases ===');
  
  // Empty result + aggregate
  const r13 = db.execute('SELECT COUNT(*) as c, SUM(val) as s, AVG(val) as a FROM t1 WHERE 1 = 0').rows[0];
  check('Aggregate on empty: COUNT=0, SUM=NULL', { c: r13.c, s: r13.s }, { c: 0, s: null }) ? passed++ : failed++;
  
  // LIMIT 0
  const r14 = db.execute('SELECT * FROM t1 LIMIT 0').rows;
  check('LIMIT 0 returns no rows', r14.length, 0) ? passed++ : failed++;
  
  // Self-join
  const r15 = db.execute('SELECT a.id, b.id FROM t1 a JOIN t1 b ON a.grp = b.grp AND a.id < b.id ORDER BY a.id, b.id').rows;
  check('Self-join works', r15.length > 0, true) ? passed++ : failed++;
  
  // UNION removes duplicates
  const r16 = db.execute('SELECT id FROM t1 WHERE id <= 2 UNION SELECT id FROM t1 WHERE id <= 3 ORDER BY id').rows;
  check('UNION removes duplicates', r16.length, 3) ? passed++ : failed++;
  
  // UNION ALL keeps duplicates
  const r17 = db.execute('SELECT id FROM t1 WHERE id <= 2 UNION ALL SELECT id FROM t1 WHERE id <= 2 ORDER BY id').rows;
  check('UNION ALL keeps duplicates', r17.length, 4) ? passed++ : failed++;
  
  // ORDER BY expression
  const r18 = db.execute('SELECT id, val FROM t1 WHERE val IS NOT NULL ORDER BY val DESC').rows;
  check('ORDER BY DESC', r18[0]?.val, 50) ? passed++ : failed++;
  
  // Nested subquery
  const r19 = db.execute('SELECT id FROM t1 WHERE val = (SELECT MAX(val) FROM t1)').rows;
  check('Scalar subquery in WHERE', r19.length, 1) ? passed++ : failed++;
  check('Scalar subquery returns correct row', r19[0]?.id, 5) ? passed++ : failed++;
  
  console.log(`\n--- Results: ${passed} passed, ${failed} failed ---`);
}

run();
