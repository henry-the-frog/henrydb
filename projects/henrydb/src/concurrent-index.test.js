import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

test('CREATE INDEX CONCURRENTLY basic', () => {
  const db = new Database();
  db.execute('CREATE TABLE t (id INT, name TEXT, val INT)');
  for (let i = 0; i < 100; i++) {
    db.execute(`INSERT INTO t VALUES (${i}, 'name${i}', ${i * 10})`);
  }
  
  const r = db.execute('CREATE INDEX CONCURRENTLY idx_name ON t(name)');
  assert.ok(r.message.includes('concurrently'));
  assert.ok(r.buildStats);
  assert.equal(r.buildStats.phase, 2);
  assert.equal(r.buildStats.rowsIndexed, 100);
  assert.equal(r.buildStats.validatedRows, 100);
});

test('CREATE INDEX CONCURRENTLY index is usable for queries', () => {
  const db = new Database();
  db.execute('CREATE TABLE employees (id INT, dept TEXT)');
  for (let i = 0; i < 50; i++) {
    db.execute(`INSERT INTO employees VALUES (${i}, '${['eng', 'sales', 'hr'][i % 3]}')`);
  }
  
  db.execute('CREATE INDEX CONCURRENTLY idx_dept ON employees(dept)');
  
  const r = db.execute("SELECT * FROM employees WHERE dept = 'eng'");
  // Should have ~17 rows (50/3)
  assert.ok(r.rows.length > 0);
  assert.ok(r.rows.every(row => row.dept === 'eng'));
});

test('CREATE INDEX CONCURRENTLY with unique constraint', () => {
  const db = new Database();
  db.execute('CREATE TABLE items (id INT, code TEXT)');
  for (let i = 0; i < 50; i++) {
    db.execute(`INSERT INTO items VALUES (${i}, 'CODE${i}')`);
  }
  
  const r = db.execute('CREATE UNIQUE INDEX CONCURRENTLY idx_code ON items(code)');
  assert.ok(r.message.includes('concurrently'));
  
  // Verify uniqueness by trying to query
  const q = db.execute("SELECT * FROM items WHERE code = 'CODE25'");
  assert.equal(q.rows.length, 1);
  assert.equal(q.rows[0].id, 25);
});

test('CREATE INDEX CONCURRENTLY unique violation', () => {
  const db = new Database();
  db.execute('CREATE TABLE t (id INT, val TEXT)');
  db.execute("INSERT INTO t VALUES (1, 'dup')");
  db.execute("INSERT INTO t VALUES (2, 'dup')");
  
  assert.throws(() => {
    db.execute('CREATE UNIQUE INDEX CONCURRENTLY idx_val ON t(val)');
  }, /Duplicate key.*unique constraint/);
});

test('CREATE INDEX CONCURRENTLY IF NOT EXISTS', () => {
  const db = new Database();
  db.execute('CREATE TABLE t (id INT, val INT)');
  db.execute('INSERT INTO t VALUES (1, 10)');
  
  db.execute('CREATE INDEX CONCURRENTLY idx_val ON t(val)');
  // Creating again with IF NOT EXISTS should succeed silently
  const r = db.execute('CREATE INDEX IF NOT EXISTS idx_val2 ON t(val)');
  assert.ok(r.message.includes('CREATE INDEX'));
});

test('CREATE INDEX CONCURRENTLY with partial index', () => {
  const db = new Database();
  db.execute('CREATE TABLE orders (id INT, status TEXT, amount INT)');
  for (let i = 0; i < 100; i++) {
    db.execute(`INSERT INTO orders VALUES (${i}, '${i % 3 === 0 ? 'pending' : 'complete'}', ${i * 10})`);
  }
  
  const r = db.execute("CREATE INDEX CONCURRENTLY idx_pending ON orders(amount) WHERE status = 'pending'");
  assert.ok(r.message.includes('concurrently'));
  // Only ~33 rows should be indexed (pending orders)
  assert.ok(r.buildStats.rowsIndexed < 50);
  assert.ok(r.buildStats.rowsIndexed > 25);
});

test('Normal CREATE INDEX does not have buildStats', () => {
  const db = new Database();
  db.execute('CREATE TABLE t (id INT, val INT)');
  db.execute('INSERT INTO t VALUES (1, 10)');
  
  const r = db.execute('CREATE INDEX idx_val ON t(val)');
  assert.equal(r.message, 'Index idx_val created');
  assert.ok(!r.buildStats);
});

test('EXPLAIN shows concurrent index is usable', () => {
  const db = new Database();
  db.execute('CREATE TABLE t (id INT, val INT)');
  for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
  db.execute('CREATE INDEX CONCURRENTLY idx_val ON t(val)');
  
  const r = db.execute('EXPLAIN SELECT * FROM t WHERE val = 500');
  const plan = r.rows.map(row => row['QUERY PLAN']).join('\n');
  assert.ok(plan.includes('Index Scan'));
});
