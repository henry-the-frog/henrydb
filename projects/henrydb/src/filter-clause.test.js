import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';

describe('FILTER clause for aggregates', () => {
  let db;
  
  it('COUNT(*) FILTER (WHERE ...)', () => {
    db = new Database();
    db.execute("CREATE TABLE t (id INTEGER, status TEXT, amount INTEGER)");
    db.execute("INSERT INTO t VALUES (1, 'active', 100)");
    db.execute("INSERT INTO t VALUES (2, 'inactive', 200)");
    db.execute("INSERT INTO t VALUES (3, 'active', 300)");
    db.execute("INSERT INTO t VALUES (4, 'active', 400)");
    
    const r = db.execute("SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE status = 'active') AS active_count FROM t");
    assert.equal(r.rows[0].total, 4);
    assert.equal(r.rows[0].active_count, 3);
  });

  it('SUM with FILTER', () => {
    const r = db.execute("SELECT SUM(amount) AS total, SUM(amount) FILTER (WHERE status = 'active') AS active_sum FROM t");
    assert.equal(r.rows[0].total, 1000);
    assert.equal(r.rows[0].active_sum, 800);
  });

  it('AVG with FILTER', () => {
    const r = db.execute("SELECT AVG(amount) FILTER (WHERE status = 'active') AS avg_active FROM t");
    // (100 + 300 + 400) / 3 ≈ 266.67
    assert.ok(Math.abs(r.rows[0].avg_active - 266.67) < 1);
  });

  it('MIN/MAX with FILTER', () => {
    const r = db.execute("SELECT MIN(amount) FILTER (WHERE status = 'active') AS min_active, MAX(amount) FILTER (WHERE status = 'inactive') AS max_inactive FROM t");
    assert.equal(r.rows[0].min_active, 100);
    assert.equal(r.rows[0].max_inactive, 200);
  });

  it('FILTER with GROUP BY', () => {
    const db2 = new Database();
    db2.execute("CREATE TABLE sales (dept TEXT, status TEXT, amount INTEGER)");
    db2.execute("INSERT INTO sales VALUES ('eng', 'closed', 100)");
    db2.execute("INSERT INTO sales VALUES ('eng', 'open', 200)");
    db2.execute("INSERT INTO sales VALUES ('eng', 'closed', 300)");
    db2.execute("INSERT INTO sales VALUES ('sales', 'open', 150)");
    db2.execute("INSERT INTO sales VALUES ('sales', 'closed', 250)");
    
    const r = db2.execute("SELECT dept, COUNT(*) AS total, COUNT(*) FILTER (WHERE status = 'closed') AS closed_count, SUM(amount) FILTER (WHERE status = 'closed') AS closed_sum FROM sales GROUP BY dept ORDER BY dept");
    assert.equal(r.rows[0].dept, 'eng');
    assert.equal(r.rows[0].total, 3);
    assert.equal(r.rows[0].closed_count, 2);
    assert.equal(r.rows[0].closed_sum, 400);
    assert.equal(r.rows[1].dept, 'sales');
    assert.equal(r.rows[1].closed_count, 1);
    assert.equal(r.rows[1].closed_sum, 250);
  });

  it('FILTER with comparison operators', () => {
    const r = db.execute("SELECT COUNT(*) FILTER (WHERE amount > 200) AS big_count, SUM(amount) FILTER (WHERE amount <= 200) AS small_sum FROM t");
    assert.equal(r.rows[0].big_count, 2); // 300, 400
    assert.equal(r.rows[0].small_sum, 300); // 100 + 200
  });

  it('FILTER with no matching rows returns 0/null', () => {
    const r = db.execute("SELECT COUNT(*) FILTER (WHERE status = 'deleted') AS cnt, SUM(amount) FILTER (WHERE status = 'deleted') AS total FROM t");
    assert.equal(r.rows[0].cnt, 0);
    assert.equal(r.rows[0].total, null);
  });
});
