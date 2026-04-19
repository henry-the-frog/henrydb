import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Aggregate with Expression Arguments (2026-04-19)', () => {
  let db;

  function setup() {
    db = new Database();
    db.execute('CREATE TABLE t (id INT, a INT, b INT, grp TEXT)');
    db.execute("INSERT INTO t VALUES (1,10,20,'x'),(2,30,5,'x'),(3,15,15,'y'),(4,25,30,'y')");
    return db;
  }

  describe('SUM with expression arg', () => {
    it('SUM(CASE) in GROUP BY', () => {
      setup();
      const r = db.execute(`
        SELECT grp, SUM(CASE WHEN a > b THEN 1 ELSE 0 END) AS wins
        FROM t GROUP BY grp
      `);
      const x = r.rows.find(r => r.grp === 'x');
      const y = r.rows.find(r => r.grp === 'y');
      assert.equal(x.wins, 1);  // id=2: 30>5
      assert.equal(y.wins, 0);  // none
    });

    it('SUM(CASE) without GROUP BY', () => {
      setup();
      const r = db.execute('SELECT SUM(CASE WHEN a > b THEN 1 ELSE 0 END) AS wins FROM t');
      assert.equal(r.rows[0].wins, 1);  // only id=2
    });

    it('SUM(arithmetic) in GROUP BY', () => {
      setup();
      const r = db.execute('SELECT grp, SUM(a * b) AS product_sum FROM t GROUP BY grp');
      const x = r.rows.find(r => r.grp === 'x');
      assert.equal(x.product_sum, 350);  // 10*20 + 30*5 = 200 + 150
    });

    it('COUNT(CASE) counts non-NULL only', () => {
      setup();
      const r = db.execute('SELECT COUNT(CASE WHEN a > b THEN 1 END) AS wins FROM t');
      assert.equal(r.rows[0].wins, 1);  // only id=2 returns non-NULL
    });

    it('AVG(arithmetic)', () => {
      setup();
      const r = db.execute('SELECT AVG(a + b) AS avg_sum FROM t');
      // (30 + 35 + 30 + 55) / 4 = 37.5
      assert.equal(r.rows[0].avg_sum, 37.5);
    });
  });

  describe('Window with expression arg', () => {
    it('SUM(a*b) OVER', () => {
      setup();
      const r = db.execute('SELECT id, SUM(a * b) OVER (ORDER BY id) AS running FROM t');
      assert.equal(r.rows[0].running, 200);    // 10*20
      assert.equal(r.rows[1].running, 350);    // + 30*5
      assert.equal(r.rows[2].running, 575);    // + 15*15
      assert.equal(r.rows[3].running, 1325);   // + 25*30
    });

    it('SUM(CASE) OVER partition', () => {
      setup();
      const r = db.execute(`
        SELECT id, grp,
          SUM(CASE WHEN a > b THEN 1 ELSE 0 END) OVER (PARTITION BY grp) AS group_wins
        FROM t ORDER BY id
      `);
      assert.equal(r.rows[0].group_wins, 1);  // group x: id=2 wins
      assert.equal(r.rows[2].group_wins, 0);  // group y: no wins
    });
  });
});
