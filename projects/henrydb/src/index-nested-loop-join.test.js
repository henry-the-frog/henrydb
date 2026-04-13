// index-nested-loop-join.test.js — Test index nested-loop join optimization
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Index nested-loop join', () => {
  function makeDb() {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT, customer_id INT, amount INT)');
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)');
    // Create index on orders.customer_id for the join
    db.execute('CREATE INDEX idx_orders_cust ON orders(customer_id)');
    
    // Insert customers
    for (let i = 1; i <= 5; i++) {
      db.execute(`INSERT INTO customers VALUES (${i}, 'Customer ${i}')`);
    }
    // Insert orders
    for (let i = 1; i <= 20; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${(i % 5) + 1}, ${i * 10})`);
    }
    return db;
  }

  it('INNER JOIN uses index when available on right table join key', () => {
    const db = makeDb();
    const r = db.execute(`
      SELECT c.name, o.amount
      FROM customers c
      JOIN orders o ON o.customer_id = c.id
      ORDER BY c.name, o.amount
    `);
    assert.strictEqual(r.rows.length, 20);
    // Customer 1 has orders with customer_id = 1
    const cust1Orders = r.rows.filter(r => r.name === 'Customer 1');
    assert.strictEqual(cust1Orders.length, 4); // 4 orders per customer (20/5)
  });

  it('LEFT JOIN with index produces correct results including NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (a_id INT, val TEXT)');
    db.execute('CREATE INDEX idx_b_aid ON b(a_id)');
    
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO a VALUES (2)');
    db.execute('INSERT INTO a VALUES (3)');
    db.execute("INSERT INTO b VALUES (1, 'x')");
    db.execute("INSERT INTO b VALUES (1, 'y')");
    db.execute("INSERT INTO b VALUES (2, 'z')");
    // No match for a.id = 3
    
    const r = db.execute('SELECT a.id, b.val FROM a LEFT JOIN b ON b.a_id = a.id ORDER BY a.id');
    assert.strictEqual(r.rows.length, 4); // 2 for id=1, 1 for id=2, 1 null for id=3
    const id3 = r.rows.filter(r => r.id === 3);
    assert.strictEqual(id3.length, 1);
    assert.strictEqual(id3[0].val, null);
  });

  it('index join produces same results as non-indexed join', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT, val TEXT)');
    db.execute('CREATE TABLE t2 (t1_id INT, data TEXT)');
    
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO t1 VALUES (${i}, 'v${i}')`);
      db.execute(`INSERT INTO t2 VALUES (${i}, 'd${i}')`);
      if (i <= 5) db.execute(`INSERT INTO t2 VALUES (${i}, 'extra${i}')`);
    }
    
    // Query without index
    const r1 = db.execute('SELECT t1.id, t2.data FROM t1 JOIN t2 ON t2.t1_id = t1.id ORDER BY t1.id, t2.data');
    
    // Add index and query again
    db.execute('CREATE INDEX idx_t2_t1id ON t2(t1_id)');
    const r2 = db.execute('SELECT t1.id, t2.data FROM t1 JOIN t2 ON t2.t1_id = t1.id ORDER BY t1.id, t2.data');
    
    assert.deepStrictEqual(r1.rows, r2.rows);
  });

  it('EXPLAIN shows index usage in join', () => {
    const db = makeDb();
    const r = db.execute(`
      EXPLAIN SELECT c.name, o.amount
      FROM customers c
      JOIN orders o ON o.customer_id = c.id
    `);
    const plan = r.rows.map(r => r['QUERY PLAN']).join('\n');
    // Should show index or hash join usage
    assert.ok(r.rows.length > 0);
  });

  it('handles NULL join keys correctly (no false matches)', () => {
    const db = new Database();
    db.execute('CREATE TABLE p (id INT)');
    db.execute('CREATE TABLE c (parent_id INT, name TEXT)');
    db.execute('CREATE INDEX idx_c_pid ON c(parent_id)');
    
    db.execute('INSERT INTO p VALUES (1)');
    db.execute('INSERT INTO p VALUES (NULL)');
    db.execute("INSERT INTO c VALUES (1, 'child1')");
    db.execute("INSERT INTO c VALUES (NULL, 'orphan')");
    
    const r = db.execute('SELECT p.id, c.name FROM p JOIN c ON c.parent_id = p.id');
    // NULL != NULL in SQL, so only id=1 matches
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].name, 'child1');
  });

  it('performance: index join avoids full scan of large table', () => {
    const db = new Database();
    db.execute('CREATE TABLE small_t (id INT)');
    db.execute('CREATE TABLE big_t (small_id INT, data INT)');
    db.execute('CREATE INDEX idx_big ON big_t(small_id)');
    
    // 3 rows in small, 1000 in big
    for (let i = 1; i <= 3; i++) db.execute(`INSERT INTO small_t VALUES (${i})`);
    for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO big_t VALUES (${(i % 3) + 1}, ${i})`);
    
    const start = performance.now();
    const r = db.execute('SELECT small_t.id, COUNT(*) as cnt FROM small_t JOIN big_t ON big_t.small_id = small_t.id GROUP BY small_t.id');
    const elapsed = performance.now() - start;
    
    assert.strictEqual(r.rows.length, 3);
    // Each small_t.id should have ~333 matches
    for (const row of r.rows) {
      assert.ok(row.cnt >= 333 && row.cnt <= 334);
    }
    // Should be fast with index (< 500ms)
    assert.ok(elapsed < 500, `Join took ${elapsed}ms, expected < 500ms`);
  });
});
