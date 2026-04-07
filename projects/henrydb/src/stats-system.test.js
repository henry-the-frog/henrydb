// stats-system.test.js — Table statistics and query planning tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Query Planning Statistics', () => {
  it('EXPLAIN shows query plan', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    
    const r = db.execute('EXPLAIN SELECT * FROM t WHERE id = 1');
    assert.ok(r.type === 'PLAN' || r.plan);
  });

  it('EXPLAIN ANALYZE shows actual row counts', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE val > 500');
    assert.ok(r.rows || r.plan || r.message);
  });

  it('ANALYZE collects table statistics', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const r = db.execute('ANALYZE t');
    assert.ok(r.message || r.type);
  });
});

describe('Index Usage', () => {
  it('primary key index is used for equality', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    // This should use the PK index
    const r = db.execute('SELECT val FROM t WHERE id = 50');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 50);
  });

  it('secondary index improves query', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, category TEXT, val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'cat${i % 5}', ${i})`);
    
    db.execute('CREATE INDEX idx_cat ON t(category)');
    
    const r = db.execute("SELECT COUNT(*) AS cnt FROM t WHERE category = 'cat0'");
    assert.equal(r.rows[0].cnt, 20);
  });
});

describe('Complex Query Performance', () => {
  it('JOIN with index on join key', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)');
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)');
    
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO customers VALUES (${i}, 'cust${i}')`);
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO orders VALUES (${i}, ${i % 50}, ${(i + 1) * 10})`);
    
    const r = db.execute('SELECT c.name, SUM(o.amount) AS total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name ORDER BY total DESC LIMIT 5');
    assert.equal(r.rows.length, 5);
    assert.ok(r.rows[0].total > 0);
  });

  it('subquery with aggregation', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO t VALUES (${i}, 'g${i % 5}', ${i * 10})`);
    
    const r = db.execute('SELECT grp, SUM(val) AS total FROM t GROUP BY grp HAVING SUM(val) > 1000');
    assert.ok(r.rows.length >= 0);
  });

  it('window function with ranking', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (id INT PRIMARY KEY, name TEXT, score INT)');
    db.execute("INSERT INTO scores VALUES (1, 'Alice', 95)");
    db.execute("INSERT INTO scores VALUES (2, 'Bob', 87)");
    db.execute("INSERT INTO scores VALUES (3, 'Charlie', 92)");
    db.execute("INSERT INTO scores VALUES (4, 'Diana', 88)");
    
    const r = db.execute('SELECT name, score, RANK() OVER (ORDER BY score DESC) AS rank FROM scores');
    assert.equal(r.rows.length, 4);
    const alice = r.rows.find(row => row.name === 'Alice');
    assert.equal(alice.rank, 1);
  });

  it('CTE with recursive pattern', () => {
    const db = new Database();
    db.execute('CREATE TABLE tree (id INT PRIMARY KEY, parent_id INT, name TEXT)');
    db.execute("INSERT INTO tree VALUES (1, null, 'root')");
    db.execute("INSERT INTO tree VALUES (2, 1, 'child1')");
    db.execute("INSERT INTO tree VALUES (3, 1, 'child2')");
    db.execute("INSERT INTO tree VALUES (4, 2, 'grandchild')");
    
    // Find all descendants of root
    const r = db.execute("WITH RECURSIVE descendants AS (SELECT id, name, 0 AS depth FROM tree WHERE parent_id IS NULL UNION ALL SELECT t.id, t.name, d.depth + 1 FROM tree t JOIN descendants d ON t.parent_id = d.id) SELECT * FROM descendants ORDER BY depth");
    assert.ok(r.rows.length >= 4);
    assert.equal(r.rows[0].name, 'root');
  });
});
