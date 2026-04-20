// group-by-having.test.js — GROUP BY and HAVING tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('GROUP BY and HAVING', () => {
  it('basic GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (product TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES ('A',100),('B',200),('A',150),('B',250),('A',50)");
    const r = db.execute('SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY product');
    assert.equal(r.rows[0].product, 'A');
    assert.equal(r.rows[0].total, 300);
    assert.equal(r.rows[1].total, 450);
  });

  it('GROUP BY with COUNT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('X',1),('X',2),('Y',3)");
    const r = db.execute('SELECT grp, COUNT(*) as cnt FROM t GROUP BY grp ORDER BY grp');
    assert.equal(r.rows[0].cnt, 2);
    assert.equal(r.rows[1].cnt, 1);
  });

  it('GROUP BY multiple columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a TEXT, b TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('X','1',10),('X','2',20),('Y','1',30),('X','1',40)");
    const r = db.execute('SELECT a, b, SUM(val) as total FROM t GROUP BY a, b ORDER BY a, b');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].total, 50); // X,1: 10+40
  });

  it('HAVING filters groups', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',10),('A',20),('B',5),('C',30),('C',40),('C',50)");
    const r = db.execute('SELECT grp, SUM(val) as total FROM t GROUP BY grp HAVING SUM(val) > 20 ORDER BY grp');
    assert.equal(r.rows.length, 2); // A(30) and C(120)
    assert.equal(r.rows[0].grp, 'A');
    assert.equal(r.rows[1].grp, 'C');
  });

  it('HAVING with COUNT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('B',3),('C',4),('C',5),('C',6)");
    const r = db.execute('SELECT grp, COUNT(*) as cnt FROM t GROUP BY grp HAVING COUNT(*) >= 2 ORDER BY grp');
    assert.equal(r.rows.length, 2); // A(2) and C(3)
  });

  it('GROUP BY with all aggregate functions', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',10),('A',20),('A',30),('B',5),('B',15)");
    const r = db.execute(`
      SELECT grp, COUNT(*) as cnt, SUM(val) as total, AVG(val) as avg_val,
             MIN(val) as min_val, MAX(val) as max_val
      FROM t GROUP BY grp ORDER BY grp
    `);
    assert.equal(r.rows[0].cnt, 3);
    assert.equal(r.rows[0].total, 60);
    assert.equal(r.rows[0].avg_val, 20);
    assert.equal(r.rows[0].min_val, 10);
    assert.equal(r.rows[0].max_val, 30);
  });
});
