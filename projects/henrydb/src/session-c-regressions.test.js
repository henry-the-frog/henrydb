// session-c-regressions.test.js — Regression tests for bugs fixed in Session C (2026-04-19)
// 
// Bugs fixed:
// 1. COUNT(*) in expressions returned 0 (isStar normalization)
// 2. UPDATE/DELETE via non-unique index only affected first match
// 3. pg-server 'options is not defined' crash (one-line typo)
// 4. UPDATE SET with keyword column names (case-insensitive matching)
// 5. || operator didn't propagate NULL
// 6. LIKE ESCAPE clause not implemented
// 7. BTree/BPlusTree API inconsistency (search/get)
// 8. Correlated IN subquery not decorrelated

import { Database } from './db.js';
import { strict as assert } from 'assert';

let db, pass = 0, fail = 0;

function test(name, fn) {
  db = new Database();
  try { fn(); pass++; console.log(`  ✅ ${name}`); }
  catch (e) { fail++; console.log(`  ❌ ${name}: ${e.message}`); }
}

console.log('\n🧪 Session C Regression Tests');

// --- Bug 1: COUNT(*) in expressions ---

test('[BUG-1] COUNT(*) - COUNT(col) in GROUP BY', () => {
  db.execute('CREATE TABLE t(g INT, v INT)');
  db.execute("INSERT INTO t VALUES (1,10),(1,NULL),(1,20),(2,NULL),(2,30)");
  const r = db.execute('SELECT g, COUNT(*) - COUNT(v) as null_count FROM t GROUP BY g').rows;
  assert.equal(r.find(x => x.g === 1).null_count, 1);
  assert.equal(r.find(x => x.g === 2).null_count, 1);
});

test('[BUG-1] COUNT(*) * 2 in GROUP BY', () => {
  db.execute('CREATE TABLE t(g INT)');
  db.execute("INSERT INTO t VALUES (1),(1),(1),(2),(2)");
  const r = db.execute('SELECT g, COUNT(*) * 2 as x FROM t GROUP BY g').rows;
  assert.equal(r.find(x => x.g === 1).x, 6);
  assert.equal(r.find(x => x.g === 2).x, 4);
});

// --- Bug 2: UPDATE/DELETE via non-unique index ---

test('[BUG-2] UPDATE via non-unique index updates ALL matches', () => {
  db.execute('CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, score INT)');
  db.execute('CREATE INDEX idx ON t(cat)');
  db.execute("INSERT INTO t VALUES (1,'A',100),(2,'A',200),(3,'B',300)");
  db.execute("UPDATE t SET score = score + 50 WHERE cat = 'A'");
  const r = db.execute('SELECT * FROM t ORDER BY id').rows;
  assert.equal(r[0].score, 150);
  assert.equal(r[1].score, 250);
  assert.equal(r[2].score, 300);
});

test('[BUG-2] DELETE via non-unique index deletes ALL matches', () => {
  db.execute('CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)');
  db.execute('CREATE INDEX idx ON t(cat)');
  db.execute("INSERT INTO t VALUES (1,'A'),(2,'A'),(3,'B')");
  db.execute("DELETE FROM t WHERE cat = 'A'");
  const r = db.execute('SELECT * FROM t').rows;
  assert.equal(r.length, 1);
  assert.equal(r[0].cat, 'B');
});

// --- Bug 4: UPDATE SET keyword column names ---

test('[BUG-4] UPDATE SET depth (keyword) column', () => {
  db.execute('CREATE TABLE t(id INT PRIMARY KEY, depth INT)');
  db.execute('INSERT INTO t VALUES (1, 10), (2, 20)');
  db.execute('UPDATE t SET depth = depth * 2');
  const r = db.execute('SELECT * FROM t ORDER BY id').rows;
  assert.equal(r[0].depth, 20);
  assert.equal(r[1].depth, 40);
});

test('[BUG-4] UPDATE SET cycle (keyword) column', () => {
  db.execute('CREATE TABLE t(id INT PRIMARY KEY, cycle INT)');
  db.execute('INSERT INTO t VALUES (1, 5)');
  db.execute('UPDATE t SET cycle = cycle + 1');
  assert.equal(db.execute('SELECT cycle FROM t').rows[0].cycle, 6);
});

test('[BUG-4] UPDATE SET window (keyword) column', () => {
  db.execute('CREATE TABLE t(id INT PRIMARY KEY, window INT)');
  db.execute('INSERT INTO t VALUES (1, 100)');
  db.execute('UPDATE t SET window = 200');
  assert.equal(db.execute('SELECT window FROM t').rows[0].window, 200);
});

// --- Bug 5: || NULL propagation ---

test('[BUG-5] || with NULL returns NULL', () => {
  const r = db.execute("SELECT 'hello' || NULL || 'world' as result").rows[0];
  assert.equal(r.result, null);
});

test('[BUG-5] CONCAT() with NULL treats as empty string', () => {
  const r = db.execute("SELECT CONCAT('hello', NULL, 'world') as result").rows[0];
  assert.equal(r.result, 'helloworld');
});

test('[BUG-5] || without NULL works normally', () => {
  const r = db.execute("SELECT 'hello' || ' ' || 'world' as result").rows[0];
  assert.equal(r.result, 'hello world');
});

// --- Bug 6: LIKE ESCAPE ---

test('[BUG-6] LIKE ESCAPE literal percent', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('100%'), ('100'), ('50%')");
  const r = db.execute("SELECT val FROM t WHERE val LIKE '%\\%' ESCAPE '\\'").rows;
  assert.deepEqual(r.map(x => x.val).sort(), ['100%', '50%']);
});

test('[BUG-6] NOT LIKE ESCAPE', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('100%'), ('100'), ('50%')");
  const r = db.execute("SELECT val FROM t WHERE val NOT LIKE '%\\%' ESCAPE '\\'").rows;
  assert.deepEqual(r.map(x => x.val), ['100']);
});

// --- Bug 8: Correlated IN decorrelation ---

test('[BUG-8] Correlated IN subquery returns correct results', () => {
  db.execute('CREATE TABLE products(id INT, cat TEXT, price INT)');
  db.execute("INSERT INTO products VALUES (1,'A',10),(2,'A',20),(3,'A',30),(4,'B',5),(5,'B',25)");
  const r = db.execute(`
    SELECT p.id FROM products p
    WHERE p.price IN (
      SELECT p2.price FROM products p2 WHERE p2.cat = p.cat AND p2.price > 15
    )
    ORDER BY p.id
  `).rows;
  assert.deepEqual(r.map(x => x.id), [2, 3, 5]);
});

console.log(`\n  ${pass} passed, ${fail} failed\n`);
process.exit(fail > 0 ? 1 : 0);
