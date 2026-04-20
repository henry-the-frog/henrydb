// index-usage.test.js — Index selection and usage verification
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Index Usage', () => {
  it('PK index used for equality lookup', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'val${i}')`);
    
    const explain = db.execute('EXPLAIN SELECT * FROM t WHERE id = 50');
    const plan = explain.rows.map(r => r['QUERY PLAN']).join('\n').toLowerCase();
    assert.ok(plan.includes('index'), 'PK lookup should use index');
  });

  it('secondary index used for equality', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, category TEXT)');
    db.execute('CREATE INDEX idx_cat ON t(category)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'cat${i % 10}')`);
    
    const explain = db.execute("EXPLAIN SELECT * FROM t WHERE category = 'cat5'");
    const plan = explain.rows.map(r => r['QUERY PLAN']).join('\n').toLowerCase();
    assert.ok(plan.includes('index'), 'Secondary index should be used');
  });

  it('sequential scan for non-indexed column', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const explain = db.execute('EXPLAIN SELECT * FROM t WHERE val > 500');
    const plan = explain.rows.map(r => r['QUERY PLAN']).join('\n').toLowerCase();
    assert.ok(plan.includes('scan'), 'Non-indexed lookup should scan');
  });

  it('index gives correct results', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, 'val${i}')`);
    
    const r = db.execute('SELECT val FROM t WHERE id = 25');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 'val25');
  });

  it('index range scan', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const r = db.execute('SELECT COUNT(*) as c FROM t WHERE id BETWEEN 20 AND 30');
    assert.equal(r.rows[0].c, 11);
  });

  it('index used after UPDATE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    
    db.execute("UPDATE t SET val = 'updated' WHERE id = 25");
    
    const r = db.execute('SELECT val FROM t WHERE id = 25');
    assert.equal(r.rows[0].val, 'updated');
  });

  it('index survives DELETE and INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    
    db.execute('DELETE FROM t WHERE id = 5');
    assert.equal(db.execute('SELECT * FROM t WHERE id = 5').rows.length, 0);
    
    db.execute("INSERT INTO t VALUES (5, 'new_v5')");
    assert.equal(db.execute('SELECT val FROM t WHERE id = 5').rows[0].val, 'new_v5');
  });
});
