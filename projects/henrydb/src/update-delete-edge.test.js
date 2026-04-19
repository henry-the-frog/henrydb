import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('UPDATE/DELETE Edge Cases (2026-04-19)', () => {
  let db;

  function setup() {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT, tag TEXT)');
    db.execute("INSERT INTO t VALUES (1,10,'a'),(2,20,'b'),(3,30,'a'),(4,40,'b'),(5,50,'a')");
    return db;
  }

  describe('UPDATE edge cases', () => {
    it('self-referencing SET (val = val * val)', () => {
      setup();
      db.execute('UPDATE t SET val = val * val WHERE id = 1');
      const r = db.execute('SELECT val FROM t WHERE id = 1');
      assert.equal(r.rows[0].val, 100);
    });

    it('UPDATE with CASE expression', () => {
      setup();
      db.execute("UPDATE t SET tag = CASE WHEN val > 30 THEN 'high' ELSE 'low' END");
      const r = db.execute('SELECT * FROM t ORDER BY id');
      assert.equal(r.rows[0].tag, 'low');   // val=10
      assert.equal(r.rows[3].tag, 'high');  // val=40
    });

    it('UPDATE RETURNING', () => {
      setup();
      const r = db.execute("UPDATE t SET val = val + 1 WHERE tag = 'a' RETURNING id, val");
      assert.equal(r.rows.length, 3);  // 3 rows with tag='a'
    });

    it('UPDATE with correlated subquery', () => {
      setup();
      db.execute('UPDATE t SET val = (SELECT MAX(val) FROM t t2 WHERE t2.tag = t.tag)');
      const r = db.execute("SELECT val FROM t WHERE tag = 'a'");
      assert.ok(r.rows.every(row => row.val === 50));  // max of group a
    });

    it('UPDATE with FROM clause', () => {
      setup();
      db.execute('CREATE TABLE multipliers (tag TEXT, factor INT)');
      db.execute("INSERT INTO multipliers VALUES ('a', 2), ('b', 3)");
      db.execute('UPDATE t SET val = val * m.factor FROM multipliers m WHERE t.tag = m.tag');
      const r = db.execute("SELECT val FROM t WHERE id = 1");
      assert.equal(r.rows[0].val, 20);  // 10 * 2
    });
  });

  describe('DELETE edge cases', () => {
    it('DELETE with subquery', () => {
      setup();
      db.execute("DELETE FROM t WHERE id IN (SELECT id FROM t WHERE tag = 'b')");
      const r = db.execute('SELECT COUNT(*) AS cnt FROM t');
      assert.equal(r.rows[0].cnt, 3);
    });

    it('DELETE RETURNING', () => {
      setup();
      const r = db.execute("DELETE FROM t WHERE tag = 'a' RETURNING id, val");
      assert.equal(r.rows.length, 3);
    });

    it('DELETE with complex WHERE', () => {
      setup();
      db.execute('DELETE FROM t WHERE val > (SELECT AVG(val) FROM t)');
      const r = db.execute('SELECT * FROM t ORDER BY id');
      assert.ok(r.rows.every(row => row.val <= 30));  // avg is 30, so <=30 remain
    });

    it('DELETE all rows', () => {
      setup();
      db.execute('DELETE FROM t');
      const r = db.execute('SELECT COUNT(*) AS cnt FROM t');
      assert.equal(r.rows[0].cnt, 0);
    });

    it('DELETE non-matching WHERE returns 0 rows', () => {
      setup();
      const r = db.execute('DELETE FROM t WHERE val > 1000');
      assert.equal(r.count, 0);
      const cnt = db.execute('SELECT COUNT(*) AS cnt FROM t');
      assert.equal(cnt.rows[0].cnt, 5);
    });
  });
});
