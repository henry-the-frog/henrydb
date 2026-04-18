// window-mvcc.test.js — Window functions + MVCC snapshot isolation
// Tests that window function results are consistent within a transaction's
// snapshot, even when concurrent modifications happen.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-window-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Window Functions + MVCC', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('ROW_NUMBER is consistent within a transaction snapshot', () => {
    db.execute('CREATE TABLE employees (id INT, dept TEXT, salary INT)');
    db.execute("INSERT INTO employees VALUES (1, 'eng', 100)");
    db.execute("INSERT INTO employees VALUES (2, 'eng', 150)");
    db.execute("INSERT INTO employees VALUES (3, 'sales', 120)");
    db.execute("INSERT INTO employees VALUES (4, 'sales', 90)");
    
    // Session 1: read snapshot
    const s1 = db.session();
    s1.begin();
    
    // Insert new row outside s1's snapshot
    db.execute("INSERT INTO employees VALUES (5, 'eng', 200)");
    
    // s1 should see consistent ROW_NUMBER (only 4 rows)
    const r = rows(s1.execute(
      'SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM employees ORDER BY dept, rn'
    ));
    
    // Only 4 rows in s1's snapshot
    assert.equal(r.length, 4, 'Should see 4 rows (snapshot isolation)');
    
    // Check ROW_NUMBER is sequential
    const engRows = r.filter(x => x.dept === 'eng');
    assert.equal(engRows.length, 2, 'Should see 2 eng rows');
    assert.equal(engRows[0].rn, 1);
    assert.equal(engRows[1].rn, 2);
    
    s1.commit();
  });

  it('RANK/DENSE_RANK with concurrent deletes', () => {
    db.execute('CREATE TABLE scores (id INT, score INT)');
    for (let i = 1; i <= 5; i++) {
      db.execute(`INSERT INTO scores VALUES (${i}, ${i * 20})`);
    }
    
    const s1 = db.session();
    s1.begin();
    
    // Delete rows outside s1's snapshot
    db.execute('DELETE FROM scores WHERE id IN (2, 4)');
    
    // s1 should see all 5 rows with correct rankings
    const r = rows(s1.execute(
      'SELECT id, score, RANK() OVER (ORDER BY score DESC) as rnk FROM scores ORDER BY rnk'
    ));
    
    assert.equal(r.length, 5, 'Should see all 5 rows (before delete)');
    assert.equal(r[0].id, 5, 'Highest score should rank first');
    assert.equal(r[0].rnk, 1);
    
    s1.commit();
    
    // After commit, new query should see only 3 rows
    const r2 = rows(db.execute(
      'SELECT id, score, RANK() OVER (ORDER BY score DESC) as rnk FROM scores ORDER BY rnk'
    ));
    assert.equal(r2.length, 3, 'After delete, should see 3 rows');
  });

  it('SUM window with concurrent updates', () => {
    db.execute('CREATE TABLE sales (id INT, amount INT, region TEXT)');
    db.execute("INSERT INTO sales VALUES (1, 100, 'west')");
    db.execute("INSERT INTO sales VALUES (2, 200, 'west')");
    db.execute("INSERT INTO sales VALUES (3, 150, 'east')");
    db.execute("INSERT INTO sales VALUES (4, 250, 'east')");
    
    const s1 = db.session();
    s1.begin();
    
    // Update amounts outside s1
    db.execute('UPDATE sales SET amount = 999 WHERE id = 1');
    
    // s1 should see original amounts
    const r = rows(s1.execute(
      'SELECT id, amount, SUM(amount) OVER (PARTITION BY region) as region_total FROM sales ORDER BY id'
    ));
    
    assert.equal(r.length, 4);
    // West total should be 100+200=300 (original values)
    const westRow = r.find(x => x.id === 1);
    assert.equal(westRow.region_total, 300, 'Should use pre-update values in snapshot');
    
    s1.commit();
  });

  it('LAG/LEAD with concurrent inserts preserve order', () => {
    db.execute('CREATE TABLE events (id INT, ts INT, val TEXT)');
    db.execute("INSERT INTO events VALUES (1, 100, 'a')");
    db.execute("INSERT INTO events VALUES (2, 200, 'b')");
    db.execute("INSERT INTO events VALUES (3, 300, 'c')");
    
    const s1 = db.session();
    s1.begin();
    
    // Insert between timestamps outside s1
    db.execute("INSERT INTO events VALUES (4, 150, 'x')");
    
    // LAG should see only 3 rows in order
    const r = rows(s1.execute(
      'SELECT id, val, LAG(val) OVER (ORDER BY ts) as prev_val FROM events ORDER BY ts'
    ));
    
    assert.equal(r.length, 3, 'Should see 3 rows (snapshot)');
    assert.equal(r[0].prev_val, null, 'First row has no LAG');
    assert.equal(r[1].prev_val, 'a', 'Second row LAGs to first');
    assert.equal(r[2].prev_val, 'b', 'Third row LAGs to second');
    
    s1.commit();
  });

  it('window function with ROWS BETWEEN in snapshot', () => {
    db.execute('CREATE TABLE timeseries (id INT, val INT)');
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO timeseries VALUES (${i}, ${i * 10})`);
    }
    
    const s1 = db.session();
    s1.begin();
    
    // Delete some rows outside s1
    db.execute('DELETE FROM timeseries WHERE id BETWEEN 4 AND 6');
    
    // Moving average should use all 10 rows
    const r = rows(s1.execute(
      'SELECT id, val, AVG(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_avg FROM timeseries ORDER BY id'
    ));
    
    assert.equal(r.length, 10, 'Should see all 10 rows in snapshot');
    // Moving avg of rows 4,5,6 = (40+50+60)/3 = 50
    const row5 = r.find(x => x.id === 5);
    assert.ok(Math.abs(row5.moving_avg - 50) < 1, `Moving avg for id=5 should be ~50, got ${row5.moving_avg}`);
    
    s1.commit();
  });

  it('NTILE with snapshot isolation', () => {
    db.execute('CREATE TABLE items (id INT, score INT)');
    for (let i = 1; i <= 12; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, ${i})`);
    }
    
    const s1 = db.session();
    s1.begin();
    
    // Add more items outside s1
    for (let i = 13; i <= 20; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, ${i})`);
    }
    
    // NTILE(4) with 12 rows = 3 per bucket
    const r = rows(s1.execute(
      'SELECT id, NTILE(4) OVER (ORDER BY score) as quartile FROM items ORDER BY id'
    ));
    
    assert.equal(r.length, 12, 'Should see 12 rows (snapshot)');
    // First 3 in bucket 1, next 3 in bucket 2, etc.
    assert.equal(r[0].quartile, 1);
    assert.equal(r[3].quartile, 2);
    assert.equal(r[6].quartile, 3);
    assert.equal(r[9].quartile, 4);
    
    s1.commit();
  });

  it('multiple window functions in same query are snapshot-consistent', () => {
    db.execute('CREATE TABLE data (id INT, cat TEXT, val INT)');
    db.execute("INSERT INTO data VALUES (1, 'a', 10)");
    db.execute("INSERT INTO data VALUES (2, 'a', 20)");
    db.execute("INSERT INTO data VALUES (3, 'b', 30)");
    db.execute("INSERT INTO data VALUES (4, 'b', 40)");
    
    const s1 = db.session();
    s1.begin();
    
    // Modify outside s1
    db.execute("INSERT INTO data VALUES (5, 'a', 50)");
    db.execute('DELETE FROM data WHERE id = 1');
    
    // Multiple window functions should all see same 4-row snapshot
    const r = rows(s1.execute(`
      SELECT id, cat, val,
        ROW_NUMBER() OVER (ORDER BY id) as rn,
        SUM(val) OVER () as total,
        COUNT(*) OVER (PARTITION BY cat) as cat_count
      FROM data
      ORDER BY id
    `));
    
    assert.equal(r.length, 4, 'Should see 4 rows (snapshot)');
    // Total should be 10+20+30+40=100
    assert.equal(r[0].total, 100, 'SUM window should use snapshot values');
    // cat_count for 'a' should be 2
    const catA = r.filter(x => x.cat === 'a');
    assert.equal(catA[0].cat_count, 2, 'cat_count for a should be 2');
    
    s1.commit();
  });

  it('window function result survives close/reopen', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    // Compute window function
    const r1 = rows(db.execute(
      'SELECT id, val, SUM(val) OVER (ORDER BY id) as running_sum FROM t ORDER BY id'
    ));
    
    assert.equal(r1.length, 5);
    assert.equal(r1[0].running_sum, 10);
    assert.equal(r1[1].running_sum, 30);
    assert.equal(r1[4].running_sum, 150);
    
    // Close and reopen
    db.close();
    db = TransactionalDatabase.open(dbDir);
    
    // Same result after recovery
    const r2 = rows(db.execute(
      'SELECT id, val, SUM(val) OVER (ORDER BY id) as running_sum FROM t ORDER BY id'
    ));
    
    assert.equal(r2.length, 5);
    assert.equal(r2[0].running_sum, 10);
    assert.equal(r2[1].running_sum, 30);
    assert.equal(r2[4].running_sum, 150);
  });
});
