// filter-clause.test.js — FILTER clause with aggregate functions
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('FILTER Clause on Aggregates', () => {
  it('COUNT with FILTER', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (status TEXT, amount INT)');
    db.execute("INSERT INTO t VALUES ('shipped',100),('pending',200),('shipped',300),('cancelled',50)");
    
    const r = db.execute(`
      SELECT COUNT(*) FILTER (WHERE status = 'shipped') as shipped_count,
             COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
             COUNT(*) as total
      FROM t
    `);
    assert.equal(r.rows[0].shipped_count, 2);
    assert.equal(r.rows[0].pending_count, 1);
    assert.equal(r.rows[0].total, 4);
  });

  it('SUM with FILTER', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (status TEXT, amount INT)');
    db.execute("INSERT INTO t VALUES ('shipped',100),('pending',200),('shipped',300),('cancelled',50)");
    
    const r = db.execute(`
      SELECT SUM(amount) FILTER (WHERE status = 'shipped') as shipped_total,
             SUM(amount) as total
      FROM t
    `);
    assert.equal(r.rows[0].shipped_total, 400); // 100+300
    assert.equal(r.rows[0].total, 650);
  });

  it('AVG with FILTER', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (dept TEXT, salary INT)');
    db.execute("INSERT INTO t VALUES ('eng',90),('eng',80),('sales',70),('sales',60),('sales',50)");
    
    const r = db.execute(`
      SELECT AVG(salary) FILTER (WHERE dept = 'eng') as eng_avg,
             AVG(salary) FILTER (WHERE dept = 'sales') as sales_avg
      FROM t
    `);
    assert.equal(r.rows[0].eng_avg, 85); // (90+80)/2
    assert.equal(r.rows[0].sales_avg, 60); // (70+60+50)/3
  });

  it('FILTER with GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (region TEXT, product TEXT, amount INT)');
    db.execute("INSERT INTO t VALUES ('north','A',100),('north','B',200),('south','A',150),('south','B',250)");
    
    const r = db.execute(`
      SELECT region,
             SUM(amount) as total,
             SUM(amount) FILTER (WHERE product = 'A') as product_a_total
      FROM t
      GROUP BY region
      ORDER BY region
    `);
    assert.equal(r.rows[0].product_a_total, 100); // north A
    assert.equal(r.rows[1].product_a_total, 150); // south A
  });

  it('MIN/MAX with FILTER', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (type TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('a',10),('b',20),('a',30),('b',40),('a',50)");
    
    const r = db.execute(`
      SELECT MIN(val) FILTER (WHERE type = 'a') as min_a,
             MAX(val) FILTER (WHERE type = 'b') as max_b
      FROM t
    `);
    assert.equal(r.rows[0].min_a, 10);
    assert.equal(r.rows[0].max_b, 40);
  });

  it('FILTER returns null when no rows match', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');
    
    const r = db.execute(`
      SELECT COUNT(*) FILTER (WHERE val > 100) as cnt,
             SUM(val) FILTER (WHERE val > 100) as total
      FROM t
    `);
    assert.equal(r.rows[0].cnt, 0);
    assert.equal(r.rows[0].total, null);
  });
});
