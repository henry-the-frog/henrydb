import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Parser Bug Fixes (2026-04-19)', () => {
  
  describe('Double NOT expressions', () => {
    it('SELECT NOT NOT TRUE returns true', () => {
      const db = new Database();
      const r = db.execute('SELECT NOT NOT TRUE AS result');
      assert.equal(r.rows[0].result, true);
    });
    
    it('SELECT NOT NOT FALSE returns false', () => {
      const db = new Database();
      const r = db.execute('SELECT NOT NOT FALSE AS result');
      assert.equal(r.rows[0].result, false);
    });
    
    it('triple NOT works', () => {
      const db = new Database();
      const r = db.execute('SELECT NOT NOT NOT TRUE AS result');
      assert.equal(r.rows[0].result, false);
    });
    
    it('NOT with comparison in SELECT list', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT)');
      db.execute('INSERT INTO t VALUES (1)');
      const r = db.execute('SELECT NOT (id > 5) AS small FROM t');
      assert.equal(r.rows[0].small, true);
    });
    
    it('NOT NOT in WHERE clause still works', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT, active BOOLEAN)');
      db.execute("INSERT INTO t VALUES (1, TRUE)");
      db.execute("INSERT INTO t VALUES (2, FALSE)");
      const r = db.execute('SELECT id FROM t WHERE NOT NOT active');
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].id, 1);
    });
  });
  
  describe('Window functions in arithmetic expressions', () => {
    let db;
    
    it('val - LAG(val) OVER', () => {
      db = new Database();
      db.execute('CREATE TABLE prices (day INT, price INT)');
      db.execute('INSERT INTO prices VALUES (1, 100)');
      db.execute('INSERT INTO prices VALUES (2, 130)');
      db.execute('INSERT INTO prices VALUES (3, 120)');
      const r = db.execute('SELECT day, price, price - LAG(price) OVER (ORDER BY day) AS change FROM prices');
      assert.equal(r.rows[0].change, null);  // first row has no lag
      assert.equal(r.rows[1].change, 30);    // 130 - 100
      assert.equal(r.rows[2].change, -10);   // 120 - 130
    });
    
    it('LEAD in arithmetic', () => {
      db = new Database();
      db.execute('CREATE TABLE nums (id INT, val INT)');
      db.execute('INSERT INTO nums VALUES (1, 10)');
      db.execute('INSERT INTO nums VALUES (2, 20)');
      db.execute('INSERT INTO nums VALUES (3, 30)');
      const r = db.execute('SELECT id, LEAD(val) OVER (ORDER BY id) - val AS next_diff FROM nums');
      assert.equal(r.rows[0].next_diff, 10);   // 20 - 10
      assert.equal(r.rows[1].next_diff, 10);   // 30 - 20
      assert.equal(r.rows[2].next_diff, null);  // null - 30
    });
    
    it('ROW_NUMBER in arithmetic', () => {
      db = new Database();
      db.execute('CREATE TABLE items (name TEXT)');
      db.execute("INSERT INTO items VALUES ('a')");
      db.execute("INSERT INTO items VALUES ('b')");
      db.execute("INSERT INTO items VALUES ('c')");
      const r = db.execute('SELECT name, ROW_NUMBER() OVER (ORDER BY name) * 10 AS rank10 FROM items');
      assert.equal(r.rows[0].rank10, 10);
      assert.equal(r.rows[1].rank10, 20);
      assert.equal(r.rows[2].rank10, 30);
    });
    
    it('SUM() OVER in arithmetic', () => {
      db = new Database();
      db.execute('CREATE TABLE sales (id INT, amount INT)');
      db.execute('INSERT INTO sales VALUES (1, 100)');
      db.execute('INSERT INTO sales VALUES (2, 200)');
      db.execute('INSERT INTO sales VALUES (3, 150)');
      const r = db.execute('SELECT id, amount, SUM(amount) OVER (ORDER BY id) - amount AS others_sum FROM sales');
      // Running sum: 100, 300, 450
      assert.equal(r.rows[0].others_sum, 0);     // 100 - 100
      assert.equal(r.rows[1].others_sum, 100);    // 300 - 200
      assert.equal(r.rows[2].others_sum, 300);    // 450 - 150
    });
  });
  
  describe('SELECT * with additional columns', () => {
    let db;
    
    it('SELECT *, ROW_NUMBER() OVER', () => {
      db = new Database();
      db.execute('CREATE TABLE t (id INT, name TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'a')");
      db.execute("INSERT INTO t VALUES (2, 'b')");
      const r = db.execute('SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t');
      assert.equal(r.rows.length, 2);
      assert.equal(r.rows[0].id, 1);
      assert.equal(r.rows[0].name, 'a');
      assert.equal(r.rows[0].rn, 1);
      assert.equal(r.rows[1].rn, 2);
    });
    
    it('SELECT *, expression', () => {
      db = new Database();
      db.execute('CREATE TABLE t (id INT, val INT)');
      db.execute('INSERT INTO t VALUES (1, 10)');
      db.execute('INSERT INTO t VALUES (2, 20)');
      const r = db.execute('SELECT *, id * val AS product FROM t');
      assert.equal(r.rows[0].product, 10);
      assert.equal(r.rows[1].product, 40);
    });
    
    it('SELECT *, multiple extra columns', () => {
      db = new Database();
      db.execute('CREATE TABLE t (id INT)');
      db.execute('INSERT INTO t VALUES (1)');
      db.execute('INSERT INTO t VALUES (2)');
      const r = db.execute("SELECT *, id * 2 AS doubled, 'x' AS tag FROM t");
      assert.equal(r.rows[0].doubled, 2);
      assert.equal(r.rows[0].tag, 'x');
      assert.equal(r.rows[1].doubled, 4);
    });
    
    it('SELECT *, aggregate window', () => {
      db = new Database();
      db.execute('CREATE TABLE t (id INT, val INT)');
      db.execute('INSERT INTO t VALUES (1, 10)');
      db.execute('INSERT INTO t VALUES (2, 20)');
      db.execute('INSERT INTO t VALUES (3, 30)');
      const r = db.execute('SELECT *, SUM(val) OVER (ORDER BY id) AS running FROM t');
      assert.equal(r.rows[0].running, 10);
      assert.equal(r.rows[1].running, 30);
      assert.equal(r.rows[2].running, 60);
    });
  });
});
