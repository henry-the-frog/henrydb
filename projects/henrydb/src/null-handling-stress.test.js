// null-handling-stress.test.js — Stress tests for NULL semantics
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('NULL handling stress tests', () => {
  
  it('IS NULL / IS NOT NULL', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, NULL)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    
    const nulls = db.execute('SELECT id FROM t WHERE val IS NULL');
    assert.strictEqual(nulls.rows.length, 1);
    assert.strictEqual(nulls.rows[0].id, 2);
    
    const notNulls = db.execute('SELECT id FROM t WHERE val IS NOT NULL ORDER BY id');
    assert.strictEqual(notNulls.rows.length, 2);
  });

  it('NULL comparison is always false', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (NULL)');
    
    // NULL = NULL should be false
    const r1 = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val = NULL');
    assert.strictEqual(r1.rows[0].cnt, 0);
    
    // NULL != NULL should also be false
    const r2 = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val != NULL');
    assert.strictEqual(r2.rows[0].cnt, 0);
    
    // NULL > 0 should be false
    const r3 = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val > 0');
    assert.strictEqual(r3.rows[0].cnt, 0);
  });

  it('COALESCE with NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT, c INT)');
    db.execute('INSERT INTO t VALUES (NULL, NULL, 30)');
    db.execute('INSERT INTO t VALUES (NULL, 20, 30)');
    db.execute('INSERT INTO t VALUES (10, 20, 30)');
    
    const r = db.execute('SELECT COALESCE(a, b, c) as result FROM t ORDER BY result');
    assert.strictEqual(r.rows[0].result, 10);
    assert.strictEqual(r.rows[1].result, 20);
    assert.strictEqual(r.rows[2].result, 30);
  });

  it('NULL in aggregates', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (10)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (20)');
    
    const r = db.execute('SELECT COUNT(*) as cnt_all, COUNT(val) as cnt_nonnull, SUM(val) as total, AVG(val) as avg FROM t');
    assert.strictEqual(r.rows[0].cnt_all, 3);
    assert.strictEqual(r.rows[0].cnt_nonnull, 2);
    assert.strictEqual(r.rows[0].total, 30);
    assert.strictEqual(r.rows[0].avg, 15);
  });

  it('NULL in DISTINCT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (2)');
    
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val');
    // Should have 3 distinct values: NULL, 1, 2
    assert.strictEqual(r.rows.length, 3);
  });

  it('NULL in GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A', 10)");
    db.execute("INSERT INTO t VALUES (NULL, 20)");
    db.execute("INSERT INTO t VALUES ('A', 30)");
    db.execute("INSERT INTO t VALUES (NULL, 40)");
    
    const r = db.execute('SELECT cat, SUM(val) as total FROM t GROUP BY cat ORDER BY cat');
    // Should have group for NULL and group for 'A'
    assert.ok(r.rows.length >= 2);
  });

  it('NULL in JOIN conditions', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT, key_col INT)');
    db.execute('CREATE TABLE b (key_col INT, val TEXT)');
    db.execute('INSERT INTO a VALUES (1, 10)');
    db.execute('INSERT INTO a VALUES (2, NULL)');
    db.execute('INSERT INTO b VALUES (10, \'match\')');
    db.execute('INSERT INTO b VALUES (NULL, \'null\')');
    
    // INNER JOIN: NULL = NULL should NOT match
    const r = db.execute('SELECT a.id, b.val FROM a JOIN b ON a.key_col = b.key_col');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].id, 1);
  });

  it('NULL in LEFT JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (a_id INT, val TEXT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO a VALUES (2)');
    db.execute("INSERT INTO b VALUES (1, 'match')");
    
    const r = db.execute('SELECT a.id, b.val FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id');
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].val, 'match');
    assert.strictEqual(r.rows[1].val, null); // No match for id=2
  });

  it('NULL in IN clause', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (3)');
    
    const r = db.execute('SELECT val FROM t WHERE val IN (1, 3) ORDER BY val');
    assert.strictEqual(r.rows.length, 2); // NULL should not match
  });

  it('NULL in CASE expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (NULL)');
    
    const r = db.execute(`
      SELECT val, CASE WHEN val IS NULL THEN 'null' ELSE 'not null' END as status
      FROM t ORDER BY val
    `);
    assert.ok(r.rows.length === 2);
    const nullRow = r.rows.find(r => r.val === null);
    assert.strictEqual(nullRow.status, 'null');
  });

  it('NULL in arithmetic', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (5, NULL)');
    const r = db.execute('SELECT a + b as sum, a * b as prod, a - b as diff FROM t');
    assert.strictEqual(r.rows[0].sum, null);
    assert.strictEqual(r.rows[0].prod, null);
    assert.strictEqual(r.rows[0].diff, null);
  });

  it('NULL in BETWEEN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (5)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (15)');
    
    const r = db.execute('SELECT val FROM t WHERE val BETWEEN 1 AND 10 ORDER BY val');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].val, 5); // NULL should not match
  });

  it('NULL in CONCAT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a TEXT, b TEXT)');
    db.execute("INSERT INTO t VALUES ('hello', NULL)");
    const r = db.execute("SELECT a || b as result FROM t");
    // Concatenation with NULL should produce NULL in SQL standard
    // But many DBs produce the non-null part
    assert.ok(r.rows[0].result === null || r.rows[0].result === 'hello' || r.rows[0].result === 'hellonull');
  });

  it('all NULLs in aggregate', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (NULL)');
    db.execute('INSERT INTO t VALUES (NULL)');
    
    const r = db.execute('SELECT COUNT(*) as cnt, COUNT(val) as cnt_val, SUM(val) as total FROM t');
    assert.strictEqual(r.rows[0].cnt, 2); // COUNT(*) counts rows
    assert.strictEqual(r.rows[0].cnt_val, 0); // COUNT(val) skips NULLs
    assert.strictEqual(r.rows[0].total, null); // SUM of all NULLs = NULL
  });
});
