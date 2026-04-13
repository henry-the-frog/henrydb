// case-groupby.test.js — Test CASE expressions with aggregates in GROUP BY context
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CASE expression with aggregates in GROUP BY', () => {
  it('CASE WHEN SUM(col) > threshold THEN ... ELSE ... END', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (region TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES ('A', 100)");
    db.execute("INSERT INTO sales VALUES ('A', 200)");
    db.execute("INSERT INTO sales VALUES ('B', 50)");
    
    const r = db.execute(`
      SELECT region, SUM(amount) as total,
        CASE WHEN SUM(amount) >= 200 THEN 'high' ELSE 'low' END as tier
      FROM sales GROUP BY region ORDER BY region
    `);
    assert.strictEqual(r.rows[0].tier, 'high'); // A: 300 >= 200
    assert.strictEqual(r.rows[1].tier, 'low');  // B: 50 < 200
  });

  it('CASE with COUNT in GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (cat TEXT, name TEXT)');
    db.execute("INSERT INTO items VALUES ('x', 'a')");
    db.execute("INSERT INTO items VALUES ('x', 'b')");
    db.execute("INSERT INTO items VALUES ('x', 'c')");
    db.execute("INSERT INTO items VALUES ('y', 'd')");
    
    const r = db.execute(`
      SELECT cat, COUNT(*) as cnt,
        CASE WHEN COUNT(*) > 2 THEN 'many' ELSE 'few' END as size
      FROM items GROUP BY cat ORDER BY cat
    `);
    assert.strictEqual(r.rows[0].size, 'many'); // x: 3 > 2
    assert.strictEqual(r.rows[1].size, 'few');  // y: 1 <= 2
  });

  it('arithmetic expression with aggregates in GROUP BY projection', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, a INT, b INT)');
    db.execute("INSERT INTO t VALUES ('g1', 10, 2)");
    db.execute("INSERT INTO t VALUES ('g1', 20, 3)");
    db.execute("INSERT INTO t VALUES ('g2', 5, 1)");
    
    const r = db.execute(`
      SELECT grp, SUM(a) as sum_a, SUM(b) as sum_b,
        SUM(a) * 100 / SUM(b) as ratio
      FROM t GROUP BY grp ORDER BY grp
    `);
    assert.strictEqual(r.rows[0].ratio, 600); // g1: 30*100/5 = 600
    assert.strictEqual(r.rows[1].ratio, 500); // g2: 5*100/1 = 500
  });

  it('nested CASE in GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (team TEXT, pts INT)');
    db.execute("INSERT INTO scores VALUES ('A', 90)");
    db.execute("INSERT INTO scores VALUES ('A', 80)");
    db.execute("INSERT INTO scores VALUES ('B', 40)");
    db.execute("INSERT INTO scores VALUES ('B', 45)");
    
    const r = db.execute(`
      SELECT team, AVG(pts) as avg_pts,
        CASE
          WHEN AVG(pts) >= 80 THEN 'excellent'
          WHEN AVG(pts) >= 60 THEN 'good'
          ELSE 'needs improvement'
        END as rating
      FROM scores GROUP BY team ORDER BY team
    `);
    assert.strictEqual(r.rows[0].rating, 'excellent'); // A: avg 85
    assert.strictEqual(r.rows[1].rating, 'needs improvement'); // B: avg 42.5
  });
});
