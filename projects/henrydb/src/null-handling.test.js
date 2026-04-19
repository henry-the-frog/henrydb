import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('NULL Handling Comprehensive (2026-04-19)', () => {
  let db;

  function setup() {
    db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 10, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, NULL, 'Bob')");
    db.execute("INSERT INTO t VALUES (3, 30, NULL)");
    db.execute("INSERT INTO t VALUES (4, NULL, NULL)");
    return db;
  }

  describe('Comparison operators with NULL', () => {
    it('NULL = NULL is NULL (not true)', () => {
      setup();
      const r = db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val = NULL');
      assert.equal(r.rows[0].cnt, 0);  // NULL = NULL is NULL, filters out
    });

    it('NULL <> x is NULL', () => {
      setup();
      const r = db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val <> 10');
      // Only id=3 (val=30) matches. id=2,4 have NULL val which <> returns NULL
      assert.equal(r.rows[0].cnt, 1);
    });

    it('NULL < x is NULL', () => {
      setup();
      const r = db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val < 20');
      // Only id=1 (val=10) matches
      assert.equal(r.rows[0].cnt, 1);
    });

    it('NULL > x is NULL', () => {
      setup();
      const r = db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val > 0');
      // id=1 (10) and id=3 (30) match
      assert.equal(r.rows[0].cnt, 2);
    });
  });

  describe('IS NULL / IS NOT NULL', () => {
    it('IS NULL finds NULLs', () => {
      setup();
      const r = db.execute('SELECT id FROM t WHERE val IS NULL ORDER BY id');
      assert.equal(r.rows.length, 2);
      assert.equal(r.rows[0].id, 2);
      assert.equal(r.rows[1].id, 4);
    });

    it('IS NOT NULL excludes NULLs', () => {
      setup();
      const r = db.execute('SELECT id FROM t WHERE val IS NOT NULL ORDER BY id');
      assert.equal(r.rows.length, 2);
      assert.equal(r.rows[0].id, 1);
      assert.equal(r.rows[1].id, 3);
    });
  });

  describe('NULL in aggregates', () => {
    it('COUNT(*) counts all rows including NULL', () => {
      setup();
      assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 4);
    });

    it('COUNT(column) excludes NULL', () => {
      setup();
      assert.equal(db.execute('SELECT COUNT(val) AS cnt FROM t').rows[0].cnt, 2);
    });

    it('SUM ignores NULL', () => {
      setup();
      assert.equal(db.execute('SELECT SUM(val) AS total FROM t').rows[0].total, 40);
    });

    it('AVG ignores NULL', () => {
      setup();
      assert.equal(db.execute('SELECT AVG(val) AS avg FROM t').rows[0].avg, 20);  // (10+30)/2
    });

    it('MAX/MIN ignore NULL', () => {
      setup();
      assert.equal(db.execute('SELECT MAX(val) AS mx FROM t').rows[0].mx, 30);
      assert.equal(db.execute('SELECT MIN(val) AS mn FROM t').rows[0].mn, 10);
    });
  });

  describe('NULL in expressions', () => {
    it('NULL + number is NULL', () => {
      setup();
      const r = db.execute('SELECT val + 5 AS result FROM t WHERE id = 2');
      assert.equal(r.rows[0].result, null);
    });

    it('COALESCE picks first non-NULL', () => {
      setup();
      const r = db.execute('SELECT id, COALESCE(val, 0) AS safe_val FROM t ORDER BY id');
      assert.equal(r.rows[0].safe_val, 10);
      assert.equal(r.rows[1].safe_val, 0);
    });

    it('CASE handles NULL', () => {
      setup();
      const r = db.execute("SELECT id, CASE WHEN val IS NULL THEN 'missing' ELSE 'present' END AS status FROM t ORDER BY id");
      assert.equal(r.rows[1].status, 'missing');
      assert.equal(r.rows[0].status, 'present');
    });
  });

  describe('NULL in three-valued logic', () => {
    it('NULL AND true is NULL', () => {
      setup();
      const r = db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val > 5 AND name IS NOT NULL');
      // id=1: 10>5=true AND name NOT NULL=true → true
      // id=2: NULL>5=NULL AND name NOT NULL=true → NULL → filtered
      // id=3: 30>5=true AND name NOT NULL=false → false
      assert.equal(r.rows[0].cnt, 1);
    });

    it('NULL OR true is true', () => {
      setup();
      const r = db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val > 5 OR name IS NULL');
      // id=1: true OR false → true
      // id=2: NULL OR false → NULL → filtered
      // id=3: true OR true → true
      // id=4: NULL OR true → true
      assert.equal(r.rows[0].cnt, 3);
    });

    it('NOT NULL is NULL', () => {
      setup();
      const r = db.execute('SELECT COUNT(*) AS cnt FROM t WHERE NOT (val > 100)');
      // id=1: NOT(10>100=false) → true
      // id=2: NOT(NULL) → NULL → filtered
      // id=3: NOT(30>100=false) → true
      // id=4: NOT(NULL) → NULL → filtered
      assert.equal(r.rows[0].cnt, 2);
    });
  });

  describe('NULL in JOINs', () => {
    it('NULL values do not match in JOIN condition', () => {
      db = new Database();
      db.execute('CREATE TABLE a (id INT, val INT)');
      db.execute('CREATE TABLE b (id INT, val INT)');
      db.execute('INSERT INTO a VALUES (1, 10), (2, NULL)');
      db.execute('INSERT INTO b VALUES (1, 10), (2, NULL)');
      const r = db.execute('SELECT a.id FROM a JOIN b ON a.val = b.val');
      assert.equal(r.rows.length, 1);  // Only id=1 matches (10=10)
      // id=2 doesn't match because NULL = NULL is NULL, not true
    });
  });
});
