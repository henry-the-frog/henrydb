// dml-edge-cases.test.js — DML operations with complex patterns

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CTE with DML', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT, grp TEXT)');
    db.execute("INSERT INTO t VALUES (1, 10, 'A'), (2, 20, 'B'), (3, 30, 'A'), (4, 40, 'B'), (5, 50, 'C')");
  });

  it('CTE + DELETE', () => {
    const r = db.execute('WITH targets AS (SELECT id FROM t WHERE val > 30) DELETE FROM t WHERE id IN (SELECT id FROM targets)');
    assert.equal(r.count, 2); // ids 4 and 5
    const remaining = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(remaining.rows.length, 3);
  });

  it('CTE + UPDATE', () => {
    const r = db.execute("WITH bonus AS (SELECT id FROM t WHERE grp = 'A') UPDATE t SET val = val * 2 WHERE id IN (SELECT id FROM bonus)");
    assert.equal(r.count, 2); // ids 1 and 3
    const updated = db.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(updated.rows[0].val, 20);
  });

  it('CTE + INSERT SELECT', () => {
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, val INT, grp TEXT)');
    const r = db.execute("WITH src AS (SELECT * FROM t WHERE grp = 'A') INSERT INTO t2 SELECT * FROM src");
    assert.equal(r.count, 2);
    const inserted = db.execute('SELECT * FROM t2 ORDER BY id');
    assert.equal(inserted.rows.length, 2);
  });

  it('recursive CTE + DELETE', () => {
    db.execute('CREATE TABLE tree (id INT PRIMARY KEY, parent_id INT, name TEXT)');
    db.execute("INSERT INTO tree VALUES (1, NULL, 'root'), (2, 1, 'child1'), (3, 1, 'child2'), (4, 2, 'grandchild')");
    
    // Delete entire subtree of id=2
    const r = db.execute(`
      WITH RECURSIVE subtree AS (
        SELECT id FROM tree WHERE id = 2
        UNION ALL
        SELECT t.id FROM tree t JOIN subtree s ON t.parent_id = s.id
      )
      DELETE FROM tree WHERE id IN (SELECT id FROM subtree)
    `);
    assert.equal(r.count, 2); // id=2 and id=4
    const remaining = db.execute('SELECT * FROM tree ORDER BY id');
    assert.equal(remaining.rows.length, 2);
    assert.deepStrictEqual(remaining.rows.map(r => r.id), [1, 3]);
  });
});

describe('UPDATE Patterns', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT, category TEXT, stock INT)');
    db.execute("INSERT INTO products VALUES (1, 'Widget', 100, 'A', 50)");
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 200, 'B', 30)");
    db.execute("INSERT INTO products VALUES (3, 'Thing', 150, 'A', 0)");
    db.execute("INSERT INTO products VALUES (4, 'Doohickey', 75, 'C', 100)");
  });

  it('UPDATE with subquery in SET', () => {
    db.execute('UPDATE products SET price = (SELECT MAX(price) FROM products) WHERE id = 4');
    const r = db.execute('SELECT price FROM products WHERE id = 4');
    assert.equal(r.rows[0].price, 200);
  });

  it('UPDATE with subquery in WHERE', () => {
    const r = db.execute("UPDATE products SET stock = stock + 10 WHERE category IN (SELECT category FROM products WHERE stock = 0)");
    assert.equal(r.count, 2); // category A: ids 1 and 3
  });

  it('UPDATE with CASE expression', () => {
    db.execute("UPDATE products SET price = CASE WHEN category = 'A' THEN price + 50 WHEN category = 'B' THEN price - 50 ELSE price END");
    const a = db.execute("SELECT price FROM products WHERE id = 1");
    const b = db.execute("SELECT price FROM products WHERE id = 2");
    assert.equal(a.rows[0].price, 150);
    assert.equal(b.rows[0].price, 150);
  });

  it('UPDATE multiple columns', () => {
    db.execute("UPDATE products SET price = 999, stock = 0, category = 'Z' WHERE id = 1");
    const r = db.execute('SELECT * FROM products WHERE id = 1');
    assert.equal(r.rows[0].price, 999);
    assert.equal(r.rows[0].stock, 0);
    assert.equal(r.rows[0].category, 'Z');
  });

  it('UPDATE with arithmetic in SET', () => {
    db.execute('UPDATE products SET price = price * 2 + 10 WHERE id = 1');
    const r = db.execute('SELECT price FROM products WHERE id = 1');
    assert.equal(r.rows[0].price, 210);
  });

  it('UPDATE RETURNING', () => {
    const r = db.execute("UPDATE products SET price = price + 100 WHERE category = 'A' RETURNING id, name, price");
    assert.equal(r.rows.length, 2);
    assert.ok(r.rows.every(row => row.price > 100));
  });
});

describe('DELETE Patterns', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, val INT, active INT)');
    for (let i = 1; i <= 20; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, ${i * 10}, ${i % 3 === 0 ? 0 : 1})`);
    }
  });

  it('DELETE with complex WHERE', () => {
    const r = db.execute('DELETE FROM items WHERE active = 0 AND val > 50');
    const count = db.execute('SELECT COUNT(*) as cnt FROM items');
    assert.ok(r.count > 0);
    assert.ok(count.rows[0].cnt < 20);
  });

  it('DELETE with subquery', () => {
    const r = db.execute('DELETE FROM items WHERE val > (SELECT AVG(val) FROM items)');
    assert.ok(r.count > 0);
    // All remaining should be <= original average
    const remaining = db.execute('SELECT MAX(val) as max_val FROM items');
    assert.ok(remaining.rows[0].max_val <= 105); // avg of 10,20,...,200 = 105
  });

  it('DELETE RETURNING', () => {
    const r = db.execute('DELETE FROM items WHERE id <= 3 RETURNING id, val');
    assert.equal(r.rows.length, 3);
    assert.deepStrictEqual(r.rows.map(r => r.id).sort(), [1, 2, 3]);
  });

  it('DELETE all rows', () => {
    const r = db.execute('DELETE FROM items');
    assert.equal(r.count, 20);
    const remaining = db.execute('SELECT COUNT(*) as cnt FROM items');
    assert.equal(remaining.rows[0].cnt, 0);
  });

  it('DELETE with no matching rows', () => {
    const r = db.execute('DELETE FROM items WHERE val > 9999');
    assert.equal(r.count, 0);
  });
});

describe('INSERT Patterns', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE target (id INT PRIMARY KEY, val INT, label TEXT)');
  });

  it('INSERT ... SELECT', () => {
    db.execute('CREATE TABLE source (id INT PRIMARY KEY, val INT, label TEXT)');
    db.execute("INSERT INTO source VALUES (1, 10, 'a'), (2, 20, 'b')");
    const r = db.execute('INSERT INTO target SELECT * FROM source');
    assert.equal(r.count, 2);
  });

  it('INSERT ON CONFLICT DO UPDATE (upsert)', () => {
    db.execute("INSERT INTO target VALUES (1, 10, 'original')");
    db.execute("INSERT INTO target VALUES (1, 99, 'updated') ON CONFLICT (id) DO UPDATE SET val = 99, label = 'updated'");
    const r = db.execute('SELECT * FROM target WHERE id = 1');
    assert.equal(r.rows[0].val, 99);
    assert.equal(r.rows[0].label, 'updated');
  });

  it('INSERT ON CONFLICT DO NOTHING', () => {
    db.execute("INSERT INTO target VALUES (1, 10, 'first')");
    db.execute("INSERT INTO target VALUES (1, 99, 'ignored') ON CONFLICT (id) DO NOTHING");
    const r = db.execute('SELECT * FROM target WHERE id = 1');
    assert.equal(r.rows[0].val, 10); // Unchanged
    assert.equal(r.rows[0].label, 'first');
  });

  it('INSERT RETURNING', () => {
    const r = db.execute("INSERT INTO target VALUES (1, 10, 'test') RETURNING *");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[0].label, 'test');
  });

  it('multi-row INSERT', () => {
    db.execute("INSERT INTO target VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')");
    const r = db.execute('SELECT COUNT(*) as cnt FROM target');
    assert.equal(r.rows[0].cnt, 3);
  });
});
