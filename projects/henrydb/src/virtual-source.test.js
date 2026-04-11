// virtual-source.test.js — Test all query features with virtual sources
// (GENERATE_SERIES and subqueries should behave identically to regular tables)
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Virtual Source: GENERATE_SERIES', () => {
  let db;
  before(() => { db = new Database(); });

  it('basic', () => {
    const r = db.execute('SELECT * FROM GENERATE_SERIES(1, 5)');
    assert.strictEqual(r.rows.length, 5);
    assert.deepStrictEqual(r.rows.map(r => r.value), [1, 2, 3, 4, 5]);
  });

  it('with step', () => {
    const r = db.execute('SELECT * FROM GENERATE_SERIES(0, 10, 2)');
    assert.deepStrictEqual(r.rows.map(r => r.value), [0, 2, 4, 6, 8, 10]);
  });

  it('WHERE', () => {
    const r = db.execute('SELECT value FROM GENERATE_SERIES(1, 10) WHERE value > 7');
    assert.deepStrictEqual(r.rows.map(r => r.value), [8, 9, 10]);
  });

  it('ORDER BY', () => {
    const r = db.execute('SELECT value FROM GENERATE_SERIES(1, 5) ORDER BY value DESC');
    assert.deepStrictEqual(r.rows.map(r => r.value), [5, 4, 3, 2, 1]);
  });

  it('LIMIT', () => {
    const r = db.execute('SELECT value FROM GENERATE_SERIES(1, 100) LIMIT 3');
    assert.strictEqual(r.rows.length, 3);
  });

  it('OFFSET', () => {
    const r = db.execute('SELECT value FROM GENERATE_SERIES(1, 10) ORDER BY value LIMIT 3 OFFSET 5');
    assert.deepStrictEqual(r.rows.map(r => r.value), [6, 7, 8]);
  });

  it('COUNT', () => {
    const r = db.execute('SELECT COUNT(*) as cnt FROM GENERATE_SERIES(1, 100)');
    assert.strictEqual(r.rows[0].cnt, 100);
  });

  it('SUM', () => {
    const r = db.execute('SELECT SUM(value) as total FROM GENERATE_SERIES(1, 10)');
    assert.strictEqual(r.rows[0].total, 55);
  });

  it('AVG', () => {
    const r = db.execute('SELECT AVG(value) as avg_val FROM GENERATE_SERIES(1, 10)');
    assert.strictEqual(r.rows[0].avg_val, 5.5);
  });

  it('MIN/MAX', () => {
    const r = db.execute('SELECT MIN(value) as mn, MAX(value) as mx FROM GENERATE_SERIES(5, 15)');
    assert.strictEqual(r.rows[0].mn, 5);
    assert.strictEqual(r.rows[0].mx, 15);
  });

  it('GROUP BY expression', () => {
    const r = db.execute('SELECT value % 3 as grp, COUNT(*) as cnt FROM GENERATE_SERIES(1, 30) GROUP BY value % 3');
    assert.strictEqual(r.rows.length, 3);
    for (const row of r.rows) assert.strictEqual(row.cnt, 10);
  });

  it('HAVING', () => {
    const r = db.execute('SELECT value % 5 as grp, SUM(value) as total FROM GENERATE_SERIES(1, 20) GROUP BY value % 5 HAVING SUM(value) > 40');
    assert.ok(r.rows.every(row => row.total > 40));
  });

  it('window: SUM OVER', () => {
    const r = db.execute('SELECT value, SUM(value) OVER () as total FROM GENERATE_SERIES(1, 5)');
    assert.ok(r.rows.every(row => row.total === 15));
  });

  it('window: ROW_NUMBER', () => {
    const r = db.execute('SELECT value, ROW_NUMBER() OVER (ORDER BY value) as rn FROM GENERATE_SERIES(1, 5)');
    assert.deepStrictEqual(r.rows.map(r => r.rn), [1, 2, 3, 4, 5]);
  });

  it('window: running SUM', () => {
    const r = db.execute('SELECT value, SUM(value) OVER (ORDER BY value) as running FROM GENERATE_SERIES(1, 5)');
    assert.deepStrictEqual(r.rows.map(r => r.running), [1, 3, 6, 10, 15]);
  });

  it('expression in SELECT', () => {
    const r = db.execute('SELECT value * 2 as doubled FROM GENERATE_SERIES(1, 3)');
    assert.deepStrictEqual(r.rows.map(r => r.doubled), [2, 4, 6]);
  });

  it('CASE expression', () => {
    const r = db.execute("SELECT value, CASE WHEN value <= 3 THEN 'low' ELSE 'high' END as label FROM GENERATE_SERIES(1, 5)");
    assert.strictEqual(r.rows[0].label, 'low');
    assert.strictEqual(r.rows[4].label, 'high');
  });
});

describe('Virtual Source: Subquery', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price INT, category TEXT)');
    for (let i = 1; i <= 20; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, 'item_${i}', ${i * 10}, 'cat-${i % 4}')`);
    }
  });

  it('basic subquery', () => {
    const r = db.execute('SELECT * FROM (SELECT * FROM items WHERE price > 100) sq LIMIT 3');
    assert.strictEqual(r.rows.length, 3);
    assert.ok(r.rows.every(row => row.price > 100));
  });

  it('aggregate over subquery', () => {
    const r = db.execute('SELECT COUNT(*) as cnt, SUM(price) as total FROM (SELECT * FROM items WHERE category = \'cat-0\') sq');
    assert.strictEqual(r.rows[0].cnt, 5); // ids 4, 8, 12, 16, 20
    assert.strictEqual(r.rows[0].total, 600); // 40+80+120+160+200
  });

  it('GROUP BY over subquery', () => {
    const r = db.execute('SELECT category, COUNT(*) as cnt FROM (SELECT * FROM items) sq GROUP BY category');
    assert.strictEqual(r.rows.length, 4);
  });

  it('nested subquery', () => {
    const r = db.execute('SELECT MAX(price) as max_price FROM (SELECT price FROM (SELECT * FROM items WHERE price > 100) sq1) sq2');
    assert.strictEqual(r.rows[0].max_price, 200);
  });

  it('window over subquery', () => {
    const r = db.execute('SELECT id, price, SUM(price) OVER () as total FROM (SELECT * FROM items WHERE id <= 3) sq');
    assert.strictEqual(r.rows.length, 3);
    assert.ok(r.rows.every(row => row.total === 60)); // 10+20+30
  });

  it('ORDER BY + LIMIT over subquery', () => {
    const r = db.execute('SELECT name, price FROM (SELECT * FROM items) sq ORDER BY price DESC LIMIT 3');
    assert.strictEqual(r.rows[0].price, 200);
    assert.strictEqual(r.rows.length, 3);
  });

  it('HAVING over subquery', () => {
    const r = db.execute('SELECT category, SUM(price) as total FROM (SELECT * FROM items) sq GROUP BY category HAVING SUM(price) > 250');
    assert.ok(r.rows.length > 0);
    assert.ok(r.rows.every(row => row.total > 250));
  });
});
