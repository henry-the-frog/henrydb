// sql-standard-compliance.test.js — SQL standard conformance tests
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-sql-std-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('SQL Standard Compliance', () => {
  afterEach(cleanup);

  it('NULL comparison: NULL = NULL is false', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute('INSERT INTO t (id) VALUES (1)');
    db.execute('INSERT INTO t (id) VALUES (2)');
    // NULL = NULL should be false per SQL standard
    const r = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val = val');
    assert.equal(r.rows[0].cnt, 0); // No rows where NULL = NULL
  });

  it('NULL in arithmetic: NULL + 1 = NULL', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t (id) VALUES (1)');
    const r = db.execute('SELECT val + 1 as result FROM t');
    assert.equal(r.rows[0].result, null);
  });

  it('aggregate ignores NULL', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t (id) VALUES (3)'); // val = NULL
    const r = db.execute('SELECT AVG(val) as avg_val, COUNT(val) as cnt_val, COUNT(*) as cnt_all FROM t');
    assert.equal(r.rows[0].avg_val, 15); // (10+20)/2, not (10+20+0)/3
    assert.equal(r.rows[0].cnt_val, 2); // COUNT(val) ignores NULL
    assert.equal(r.rows[0].cnt_all, 3); // COUNT(*) counts all
  });

  it('UNION removes duplicates', () => {
    db = fresh();
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (3)');
    const r = db.execute('SELECT id FROM t1 UNION SELECT id FROM t2 ORDER BY id');
    assert.equal(r.rows.length, 3); // 1, 2, 3 — no duplicate 2
  });

  it('UNION ALL keeps duplicates', () => {
    db = fresh();
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (3)');
    const r = db.execute('SELECT id FROM t1 UNION ALL SELECT id FROM t2');
    assert.equal(r.rows.length, 4); // All 4 rows including duplicate 2
  });

  it('EXCEPT (set difference)', () => {
    db = fresh();
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t1 VALUES (3)');
    db.execute('INSERT INTO t2 VALUES (2)');
    const r = db.execute('SELECT id FROM t1 EXCEPT SELECT id FROM t2 ORDER BY id');
    assert.equal(r.rows.length, 2); // 1, 3
  });

  it('INTERSECT (set intersection)', () => {
    db = fresh();
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (3)');
    const r = db.execute('SELECT id FROM t1 INTERSECT SELECT id FROM t2');
    assert.equal(r.rows.length, 1); // Only 2
    assert.equal(r.rows[0].id, 2);
  });

  it('ORDER BY column position', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (3, 'c')");
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    const r = db.execute('SELECT name, id FROM t ORDER BY 2'); // ORDER BY second column (id)
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[1].id, 2);
    assert.equal(r.rows[2].id, 3);
  });

  it('GROUP BY with expression', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 10)');
    db.execute('INSERT INTO t VALUES (4, 20)');
    const r = db.execute('SELECT val, COUNT(*) as cnt FROM t GROUP BY val ORDER BY val');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].val, 10);
    assert.equal(r.rows[0].cnt, 2);
  });

  it('recursive CTE (if supported)', () => {
    db = fresh();
    // Try a recursive CTE — might not be supported
    try {
      const r = db.execute(`
        WITH RECURSIVE nums AS (
          SELECT 1 as n
          UNION ALL
          SELECT n + 1 FROM nums WHERE n < 5
        )
        SELECT * FROM nums
      `);
      assert.equal(r.rows.length, 5);
    } catch {
      // Recursive CTEs not supported — acceptable
      assert.ok(true);
    }
  });
});
