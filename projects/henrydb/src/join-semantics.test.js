// join-semantics.test.js — Test all JOIN types for correctness

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('JOIN Semantics', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE left_t (id INT PRIMARY KEY, name TEXT, val INT)');
    db.execute('CREATE TABLE right_t (id INT PRIMARY KEY, left_id INT, label TEXT)');
    db.execute("INSERT INTO left_t VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
    db.execute("INSERT INTO right_t VALUES (10, 1, 'x'), (20, 2, 'y'), (30, 99, 'z')");
  });

  it('INNER JOIN — only matching rows', () => {
    const r = db.execute('SELECT l.name, r.label FROM left_t l JOIN right_t r ON l.id = r.left_id');
    assert.equal(r.rows.length, 2);
    assert.deepStrictEqual(r.rows.map(r => r.name).sort(), ['A', 'B']);
  });

  it('LEFT JOIN — preserves left side', () => {
    const r = db.execute('SELECT l.name, r.label FROM left_t l LEFT JOIN right_t r ON l.id = r.left_id');
    assert.equal(r.rows.length, 3);
    const c = r.rows.find(r => r.name === 'C');
    assert.equal(c.label, null, 'Unmatched left row gets NULLs');
  });

  it('RIGHT JOIN — preserves right side', () => {
    const r = db.execute('SELECT l.name, r.label FROM left_t l RIGHT JOIN right_t r ON l.id = r.left_id');
    assert.equal(r.rows.length, 3);
    const z = r.rows.find(r => r.label === 'z');
    assert.equal(z.name, null, 'Unmatched right row gets NULLs');
  });

  it('FULL OUTER JOIN — preserves both sides', () => {
    const r = db.execute('SELECT l.name, r.label FROM left_t l FULL OUTER JOIN right_t r ON l.id = r.left_id');
    assert.equal(r.rows.length, 4); // 2 matched + 1 left-only + 1 right-only
    const nullLeft = r.rows.find(r => r.name === null);
    const nullRight = r.rows.find(r => r.label === null);
    assert.ok(nullLeft, 'Right-only row exists');
    assert.ok(nullRight, 'Left-only row exists');
  });

  it('CROSS JOIN — cartesian product', () => {
    const r = db.execute('SELECT l.id as l_id, r.id as r_id FROM left_t l CROSS JOIN right_t r');
    assert.equal(r.rows.length, 9); // 3 × 3
  });

  it('self-join with different aliases', () => {
    const r = db.execute('SELECT a.name, b.name as b_name FROM left_t a JOIN left_t b ON a.id < b.id ORDER BY a.id, b.id');
    assert.equal(r.rows.length, 3); // (1,2), (1,3), (2,3)
    assert.equal(r.rows[0].name, 'A');
    assert.equal(r.rows[0].b_name, 'B');
  });

  it('NATURAL JOIN', () => {
    db.execute('CREATE TABLE nat1 (id INT PRIMARY KEY, code TEXT)');
    db.execute('CREATE TABLE nat2 (id INT PRIMARY KEY, code TEXT, extra INT)');
    db.execute("INSERT INTO nat1 VALUES (1, 'XX'), (2, 'YY')");
    db.execute("INSERT INTO nat2 VALUES (1, 'XX', 100), (2, 'ZZ', 200)");
    const r = db.execute('SELECT * FROM nat1 NATURAL JOIN nat2');
    assert.equal(r.rows.length, 1); // Only id=1 matches on both id and code
    assert.equal(r.rows[0].extra, 100);
  });

  it('JOIN USING — single column', () => {
    db.execute('CREATE TABLE u1 (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE u2 (id INT PRIMARY KEY, data TEXT)');
    db.execute("INSERT INTO u1 VALUES (1, 'a'), (2, 'b')");
    db.execute("INSERT INTO u2 VALUES (2, 'x'), (3, 'y')");
    const r = db.execute('SELECT * FROM u1 JOIN u2 USING (id)');
    assert.equal(r.rows.length, 1); // Only id=2 matches
    assert.equal(r.rows[0].id, 2);
  });

  it('anti-join pattern: LEFT JOIN + IS NULL', () => {
    const r = db.execute('SELECT l.name FROM left_t l LEFT JOIN right_t r ON l.id = r.left_id WHERE r.id IS NULL');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'C');
  });

  it('semi-join pattern: WHERE EXISTS', () => {
    const r = db.execute('SELECT l.name FROM left_t l WHERE EXISTS (SELECT 1 FROM right_t r WHERE r.left_id = l.id)');
    assert.equal(r.rows.length, 2);
    assert.deepStrictEqual(r.rows.map(r => r.name).sort(), ['A', 'B']);
  });

  it('multi-condition ON clause', () => {
    db.execute('CREATE TABLE mc1 (a INT, b INT)');
    db.execute('CREATE TABLE mc2 (a INT, b INT, val INT)');
    db.execute('INSERT INTO mc1 VALUES (1, 10), (2, 20)');
    db.execute('INSERT INTO mc2 VALUES (1, 10, 100), (1, 20, 200), (2, 20, 300)');
    const r = db.execute('SELECT mc1.a, mc2.val FROM mc1 JOIN mc2 ON mc1.a = mc2.a AND mc1.b = mc2.b');
    assert.equal(r.rows.length, 2); // (1,10)→100 and (2,20)→300
  });

  it('implicit join (comma syntax)', () => {
    const r = db.execute('SELECT l.name, r.label FROM left_t l, right_t r WHERE l.id = r.left_id ORDER BY l.id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'A');
  });

  it('3-way join correctness', () => {
    db.execute('CREATE TABLE j3 (id INT PRIMARY KEY, right_id INT, data TEXT)');
    db.execute("INSERT INTO j3 VALUES (1, 10, 'extra1'), (2, 20, 'extra2')");
    const r = db.execute(`
      SELECT l.name, r.label, j.data
      FROM left_t l
      JOIN right_t r ON l.id = r.left_id
      JOIN j3 j ON r.id = j.right_id
      ORDER BY l.id
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'A');
    assert.equal(r.rows[0].label, 'x');
    assert.equal(r.rows[0].data, 'extra1');
  });
});
