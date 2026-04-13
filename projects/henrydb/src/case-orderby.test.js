// case-orderby.test.js — Test CASE expression in ORDER BY
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CASE expression in ORDER BY', () => {
  function makeDb() {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT, score INT, category TEXT)');
    db.execute("INSERT INTO t VALUES ('alice', 90, 'A')");
    db.execute("INSERT INTO t VALUES ('bob', 50, 'B')");
    db.execute("INSERT INTO t VALUES ('carol', 30, 'C')");
    db.execute("INSERT INTO t VALUES ('dave', 70, 'A')");
    db.execute("INSERT INTO t VALUES ('eve', 85, 'B')");
    return db;
  }

  it('ORDER BY CASE WHEN for custom sort order', () => {
    const db = makeDb();
    const r = db.execute(`
      SELECT name, score FROM t
      ORDER BY CASE WHEN score >= 60 THEN 0 ELSE 1 END, score DESC
    `);
    // Pass first (score >= 60) sorted by score desc, then fail sorted by score desc
    assert.deepStrictEqual(r.rows.map(r => r.name), ['alice', 'eve', 'dave', 'bob', 'carol']);
  });

  it('ORDER BY CASE for custom category ordering', () => {
    const db = makeDb();
    const r = db.execute(`
      SELECT name, category FROM t
      ORDER BY CASE category WHEN 'C' THEN 1 WHEN 'B' THEN 2 WHEN 'A' THEN 3 END
    `);
    // C first, then B, then A
    assert.strictEqual(r.rows[0].category, 'C');
    assert.strictEqual(r.rows[1].category, 'B');
  });

  it('ORDER BY expression: score * 2 + 1', () => {
    const db = makeDb();
    const r = db.execute('SELECT name, score FROM t ORDER BY score * -1');
    // Highest score first (since *-1 makes it most negative)
    assert.strictEqual(r.rows[0].name, 'alice');
    assert.strictEqual(r.rows[4].name, 'carol');
  });

  it('ORDER BY function call: LENGTH(name)', () => {
    const db = makeDb();
    const r = db.execute('SELECT name FROM t ORDER BY LENGTH(name)');
    // bob(3), eve(3), dave(4), alice(5), carol(5)
    assert.ok(r.rows[0].name.length <= r.rows[4].name.length);
  });

  it('ORDER BY with alias still works', () => {
    const db = makeDb();
    const r = db.execute('SELECT name, score AS s FROM t ORDER BY s DESC');
    assert.strictEqual(r.rows[0].name, 'alice');
    assert.strictEqual(r.rows[0].s, 90);
  });

  it('ORDER BY numeric column reference still works', () => {
    const db = makeDb();
    const r = db.execute('SELECT name, score FROM t ORDER BY 2 DESC');
    assert.strictEqual(r.rows[0].score, 90);
  });
});
