// cross-join-stress.test.js — Tests for CROSS JOIN and implicit cross join
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CROSS JOIN stress tests', () => {
  
  it('explicit CROSS JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (val TEXT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO a VALUES (2)');
    db.execute("INSERT INTO b VALUES ('x')");
    db.execute("INSERT INTO b VALUES ('y')");
    const r = db.execute('SELECT a.id, b.val FROM a CROSS JOIN b ORDER BY a.id, b.val');
    assert.strictEqual(r.rows.length, 4); // 2 × 2
    assert.deepStrictEqual(r.rows.map(r => [r.id, r.val]), [[1,'x'],[1,'y'],[2,'x'],[2,'y']]);
  });

  it('implicit cross join (comma syntax)', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (val INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO a VALUES (2)');
    db.execute('INSERT INTO b VALUES (10)');
    db.execute('INSERT INTO b VALUES (20)');
    const r = db.execute('SELECT a.id, b.val FROM a, b WHERE a.id = 1 ORDER BY b.val');
    assert.strictEqual(r.rows.length, 2);
  });

  it('cross join with WHERE filter', () => {
    const db = new Database();
    db.execute('CREATE TABLE sizes (name TEXT)');
    db.execute('CREATE TABLE colors (name TEXT)');
    db.execute("INSERT INTO sizes VALUES ('S')");
    db.execute("INSERT INTO sizes VALUES ('M')");
    db.execute("INSERT INTO sizes VALUES ('L')");
    db.execute("INSERT INTO colors VALUES ('red')");
    db.execute("INSERT INTO colors VALUES ('blue')");
    const r = db.execute("SELECT s.name as size, c.name as color FROM sizes s CROSS JOIN colors c WHERE s.name != 'L' ORDER BY s.name, c.name");
    assert.strictEqual(r.rows.length, 4); // S,M × red,blue
  });

  it('cross join single row tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (x INT)');
    db.execute('CREATE TABLE b (y INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO b VALUES (2)');
    const r = db.execute('SELECT x, y, x + y as sum FROM a CROSS JOIN b');
    assert.strictEqual(r.rows[0].sum, 3);
  });
});
