// count-star-expr.test.js — Regression tests for COUNT(*) in arithmetic expressions
// Bug: COUNT(*) inside expressions (e.g., COUNT(*) - COUNT(v)) returned 0
// because the parser produces {type:'column_ref', name:'*'} but computeAgg
// only checked arg === '*' (string literal).

import { Database } from './db.js';
import { strict as assert } from 'assert';

let db, pass = 0, fail = 0;

function test(name, fn) {
  db = new Database();
  try { fn(); pass++; console.log(`  ✅ ${name}`); }
  catch (e) { fail++; console.log(`  ❌ ${name}: ${e.message}`); }
}

console.log('\n🧪 COUNT(*) in expressions');

test('COUNT(*) - COUNT(col) gives null count per group', () => {
  db.execute('CREATE TABLE t(g INT, v INT)');
  db.execute("INSERT INTO t VALUES (1, 10), (1, NULL), (1, 20), (2, NULL), (2, 30)");
  const r = db.execute('SELECT g, COUNT(*) - COUNT(v) as null_count FROM t GROUP BY g').rows;
  assert.equal(r.length, 2);
  const g1 = r.find(x => x.g === 1);
  const g2 = r.find(x => x.g === 2);
  assert.equal(g1.null_count, 1); // 3 rows - 2 non-null = 1
  assert.equal(g2.null_count, 1); // 2 rows - 1 non-null = 1
});

test('COUNT(*) + 0 equals COUNT(*)', () => {
  db.execute('CREATE TABLE t(g INT, v INT)');
  db.execute("INSERT INTO t VALUES (1, 10), (1, NULL), (2, 30)");
  const r = db.execute('SELECT g, COUNT(*) + 0 as x FROM t GROUP BY g').rows;
  assert.equal(r.find(x => x.g === 1).x, 2);
  assert.equal(r.find(x => x.g === 2).x, 1);
});

test('COUNT(*) * 2 doubles row count', () => {
  db.execute('CREATE TABLE t(g INT)');
  db.execute("INSERT INTO t VALUES (1), (1), (1), (2), (2)");
  const r = db.execute('SELECT g, COUNT(*) * 2 as x FROM t GROUP BY g').rows;
  assert.equal(r.find(x => x.g === 1).x, 6);
  assert.equal(r.find(x => x.g === 2).x, 4);
});

test('COUNT(*) / COUNT(col) gives ratio', () => {
  db.execute('CREATE TABLE t(g INT, v INT)');
  db.execute("INSERT INTO t VALUES (1, 10), (1, NULL), (1, 20), (1, NULL)");
  const r = db.execute('SELECT g, COUNT(*) / COUNT(v) as ratio FROM t GROUP BY g').rows;
  assert.equal(r[0].ratio, 2); // 4 / 2 = 2
});

test('SUM(col) / COUNT(*) gives true average including NULLs denominator', () => {
  db.execute('CREATE TABLE t(g INT, v INT)');
  db.execute("INSERT INTO t VALUES (1, 10), (1, NULL), (1, 20)");
  const r = db.execute('SELECT g, SUM(v) / COUNT(*) as true_avg FROM t GROUP BY g').rows;
  assert.equal(r[0].true_avg, 10); // 30 / 3 = 10
});

test('COUNT(*) in CASE expression', () => {
  db.execute('CREATE TABLE t(g INT)');
  db.execute("INSERT INTO t VALUES (1), (1), (1), (2)");
  const r = db.execute("SELECT g, CASE WHEN COUNT(*) > 2 THEN 'many' ELSE 'few' END as size FROM t GROUP BY g").rows;
  assert.equal(r.find(x => x.g === 1).size, 'many');
  assert.equal(r.find(x => x.g === 2).size, 'few');
});

test('Multiple COUNT(*) arithmetic in single SELECT', () => {
  db.execute('CREATE TABLE t(g INT, v INT)');
  db.execute("INSERT INTO t VALUES (1, 10), (1, NULL), (2, 20), (2, 30), (2, NULL)");
  const r = db.execute('SELECT g, COUNT(*) - COUNT(v) as nulls, COUNT(*) + COUNT(v) as total FROM t GROUP BY g').rows;
  const g1 = r.find(x => x.g === 1);
  const g2 = r.find(x => x.g === 2);
  assert.equal(g1.nulls, 1);  // 2 - 1
  assert.equal(g1.total, 3);  // 2 + 1
  assert.equal(g2.nulls, 1);  // 3 - 2
  assert.equal(g2.total, 5);  // 3 + 2
});

test('COUNT(*) without GROUP BY in expression', () => {
  db.execute('CREATE TABLE t(v INT)');
  db.execute("INSERT INTO t VALUES (1), (NULL), (3)");
  const r = db.execute('SELECT COUNT(*) - COUNT(v) as null_count FROM t').rows;
  assert.equal(r[0].null_count, 1); // 3 - 2 = 1
});

console.log(`\n  ${pass} passed, ${fail} failed\n`);
process.exit(fail > 0 ? 1 : 0);
