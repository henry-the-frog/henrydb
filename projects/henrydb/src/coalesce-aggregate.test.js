import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Function-wrapped aggregates (2026-04-19)', () => {
  let db;

  function setup() {
    db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT, grp TEXT)');
    db.execute("INSERT INTO t VALUES (1,NULL,'a'),(2,20,'a'),(3,NULL,'b'),(4,40,'b'),(5,10,'a')");
    return db;
  }

  describe('Without GROUP BY (whole-table aggregate)', () => {
    it('COALESCE(SUM(val), 0) with nulls', () => {
      setup();
      const r = db.execute('SELECT COALESCE(SUM(val), 0) AS total FROM t');
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].total, 70);  // 20+40+10
    });

    it('COALESCE(SUM(val), 0) all nulls', () => {
      db = new Database();
      db.execute('CREATE TABLE t2 (val INT)');
      db.execute('INSERT INTO t2 VALUES (NULL), (NULL)');
      const r = db.execute('SELECT COALESCE(SUM(val), 0) AS total FROM t2');
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].total, 0);
    });

    it('COALESCE(AVG(val), 0)', () => {
      setup();
      const r = db.execute('SELECT COALESCE(AVG(val), 0) AS avg FROM t');
      assert.equal(r.rows.length, 1);
      // avg of 20,40,10 = 23.33...
      assert.ok(Math.abs(r.rows[0].avg - 23.33) < 1);
    });
  });

  describe('With GROUP BY', () => {
    it('COALESCE(SUM(val), 0) per group', () => {
      setup();
      const r = db.execute('SELECT grp, COALESCE(SUM(val), 0) AS total FROM t GROUP BY grp');
      assert.equal(r.rows.length, 2);
      const a = r.rows.find(r => r.grp === 'a');
      const b = r.rows.find(r => r.grp === 'b');
      assert.equal(a.total, 30);  // 20+10
      assert.equal(b.total, 40);
    });

    it('ROUND(AVG(val), 2) per group', () => {
      setup();
      const r = db.execute('SELECT grp, ROUND(AVG(val), 2) AS avg FROM t GROUP BY grp');
      assert.equal(r.rows.length, 2);
      const a = r.rows.find(r => r.grp === 'a');
      assert.equal(a.avg, 15);  // avg of 20,10 = 15
    });

    it('COALESCE(COUNT(val), 0) per group', () => {
      setup();
      const r = db.execute('SELECT grp, COALESCE(COUNT(val), 0) AS cnt FROM t GROUP BY grp');
      assert.equal(r.rows.length, 2);
      const a = r.rows.find(r => r.grp === 'a');
      assert.equal(a.cnt, 2);  // 2 non-null values in group a
    });

    it('multiple function-wrapped aggregates', () => {
      setup();
      const r = db.execute('SELECT grp, COALESCE(SUM(val), 0) AS total, ROUND(AVG(val), 1) AS avg FROM t GROUP BY grp');
      assert.equal(r.rows.length, 2);
      const a = r.rows.find(r => r.grp === 'a');
      assert.equal(a.total, 30);
      assert.equal(a.avg, 15);
    });
  });

  describe('Mixed aggregate and non-aggregate columns', () => {
    it('regular aggregate + COALESCE aggregate', () => {
      setup();
      const r = db.execute('SELECT grp, COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS total FROM t GROUP BY grp');
      assert.equal(r.rows.length, 2);
      const a = r.rows.find(r => r.grp === 'a');
      assert.equal(a.cnt, 3);  // 3 rows in group a
      assert.equal(a.total, 30);
    });
  });
});
