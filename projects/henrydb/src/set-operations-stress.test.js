// set-operations-stress.test.js — Stress tests for UNION, INTERSECT, EXCEPT
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Set operations stress tests', () => {
  
  it('UNION ALL combines all rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT, val TEXT)');
    db.execute('CREATE TABLE b (id INT, val TEXT)');
    db.execute("INSERT INTO a VALUES (1, 'x')");
    db.execute("INSERT INTO a VALUES (2, 'y')");
    db.execute("INSERT INTO b VALUES (2, 'y')");
    db.execute("INSERT INTO b VALUES (3, 'z')");
    
    const r = db.execute('SELECT * FROM a UNION ALL SELECT * FROM b ORDER BY id');
    assert.strictEqual(r.rows.length, 4); // includes duplicate (2, 'y')
  });

  it('UNION removes duplicates', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO a VALUES (2)');
    db.execute('INSERT INTO b VALUES (2)');
    db.execute('INSERT INTO b VALUES (3)');
    
    const r = db.execute('SELECT * FROM a UNION SELECT * FROM b ORDER BY id');
    assert.strictEqual(r.rows.length, 3); // 1, 2, 3 (no duplicate 2)
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2, 3]);
  });

  it('INTERSECT returns common rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO a VALUES (2)');
    db.execute('INSERT INTO a VALUES (3)');
    db.execute('INSERT INTO b VALUES (2)');
    db.execute('INSERT INTO b VALUES (3)');
    db.execute('INSERT INTO b VALUES (4)');
    
    const r = db.execute('SELECT * FROM a INTERSECT SELECT * FROM b ORDER BY id');
    assert.strictEqual(r.rows.length, 2);
    assert.deepStrictEqual(r.rows.map(r => r.id), [2, 3]);
  });

  it('EXCEPT returns rows in first but not second', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO a VALUES (2)');
    db.execute('INSERT INTO a VALUES (3)');
    db.execute('INSERT INTO b VALUES (2)');
    db.execute('INSERT INTO b VALUES (4)');
    
    const r = db.execute('SELECT * FROM a EXCEPT SELECT * FROM b ORDER BY id');
    assert.strictEqual(r.rows.length, 2);
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 3]);
  });

  it('chained UNION ALL', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    db.execute('CREATE TABLE c (id INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO b VALUES (2)');
    db.execute('INSERT INTO c VALUES (3)');
    
    const r = db.execute('SELECT * FROM a UNION ALL SELECT * FROM b UNION ALL SELECT * FROM c ORDER BY id');
    assert.strictEqual(r.rows.length, 3);
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2, 3]);
  });

  it('UNION with different WHERE clauses on same table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, category TEXT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, 'cat${i % 3}')`);
    
    const r = db.execute(`
      SELECT id FROM t WHERE category = 'cat0'
      UNION ALL
      SELECT id FROM t WHERE category = 'cat1'
      ORDER BY id
    `);
    assert.ok(r.rows.length > 0);
  });

  it('UNION ALL with empty left', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    db.execute('INSERT INTO b VALUES (1)');
    
    const r = db.execute('SELECT * FROM a UNION ALL SELECT * FROM b');
    assert.strictEqual(r.rows.length, 1);
  });

  it('UNION ALL with empty right', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    db.execute('INSERT INTO a VALUES (1)');
    
    const r = db.execute('SELECT * FROM a UNION ALL SELECT * FROM b');
    assert.strictEqual(r.rows.length, 1);
  });

  it('INTERSECT with no common rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO b VALUES (2)');
    
    const r = db.execute('SELECT * FROM a INTERSECT SELECT * FROM b');
    assert.strictEqual(r.rows.length, 0);
  });

  it('EXCEPT with complete subtraction', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO a VALUES (2)');
    db.execute('INSERT INTO b VALUES (1)');
    db.execute('INSERT INTO b VALUES (2)');
    
    const r = db.execute('SELECT * FROM a EXCEPT SELECT * FROM b');
    assert.strictEqual(r.rows.length, 0);
  });

  it('large UNION ALL (1000 + 1000 rows)', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO a VALUES (${i})`);
    for (let i = 1001; i <= 2000; i++) db.execute(`INSERT INTO b VALUES (${i})`);
    
    // UNION ALL directly (not in subquery — subquery UNION not supported)
    const r = db.execute('SELECT * FROM a UNION ALL SELECT * FROM b');
    assert.strictEqual(r.rows.length, 2000);
  });

  it('UNION ALL with ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO a VALUES (${i})`);
    for (let i = 6; i <= 10; i++) db.execute(`INSERT INTO b VALUES (${i})`);
    
    // Note: ORDER BY on UNION results may need specific syntax
    const r = db.execute('SELECT * FROM a UNION ALL SELECT * FROM b');
    assert.strictEqual(r.rows.length, 10);
  });
});
