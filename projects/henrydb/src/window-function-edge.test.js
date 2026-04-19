// window-function-edge.test.js — Window function edge cases

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Window Function Edge Cases', () => {
  let db;
  
  beforeEach(() => {
    db = new Database();
    db.execute("CREATE TABLE sales (id INT, rep TEXT, region TEXT, amount INT, month INT)");
    db.execute("INSERT INTO sales VALUES (1, 'Alice', 'East', 100, 1)");
    db.execute("INSERT INTO sales VALUES (2, 'Alice', 'East', 150, 2)");
    db.execute("INSERT INTO sales VALUES (3, 'Bob', 'East', 200, 1)");
    db.execute("INSERT INTO sales VALUES (4, 'Bob', 'East', 250, 2)");
    db.execute("INSERT INTO sales VALUES (5, 'Carol', 'West', 300, 1)");
    db.execute("INSERT INTO sales VALUES (6, 'Carol', 'West', 175, 2)");
    db.execute("INSERT INTO sales VALUES (7, 'Dave', 'West', 125, 1)");
    db.execute("INSERT INTO sales VALUES (8, 'Dave', 'West', 225, 2)");
  });
  
  it('ROW_NUMBER() OVER (ORDER BY)', () => {
    const r = db.execute(`
      SELECT rep, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
      FROM sales
    `);
    assert.equal(r.rows.length, 8);
    assert.equal(r.rows[0].rn, 1);
    assert.ok(r.rows[0].amount >= r.rows[1].amount);
  });
  
  it('ROW_NUMBER() OVER (PARTITION BY ... ORDER BY)', () => {
    const r = db.execute(`
      SELECT rep, amount, ROW_NUMBER() OVER (PARTITION BY rep ORDER BY amount DESC) AS rn
      FROM sales
    `);
    // Each rep should have rn 1 and 2
    const alice = r.rows.filter(x => x.rep === 'Alice');
    assert.equal(alice.length, 2);
    assert.equal(alice.find(x => x.rn === 1).amount, 150);
    assert.equal(alice.find(x => x.rn === 2).amount, 100);
  });
  
  it('RANK() with ties', () => {
    db.execute("INSERT INTO sales VALUES (9, 'Eve', 'East', 200, 1)"); // Same as Bob
    const r = db.execute(`
      SELECT rep, amount, RANK() OVER (ORDER BY amount DESC) AS rnk
      FROM sales
      WHERE month = 1
    `);
    // Carol(300), Bob(200)=Eve(200), Dave(125), Alice(100)
    const bobs = r.rows.filter(x => x.amount === 200);
    assert.equal(bobs.length, 2);
    assert.equal(bobs[0].rnk, bobs[1].rnk); // Same rank for ties
  });
  
  it('DENSE_RANK() with gaps', () => {
    const r = db.execute(`
      SELECT rep, amount, DENSE_RANK() OVER (ORDER BY amount DESC) AS drnk
      FROM sales
    `);
    assert.equal(r.rows.length, 8);
    // Ranks should be consecutive (no gaps)
    const ranks = [...new Set(r.rows.map(x => x.drnk))].sort((a, b) => a - b);
    for (let i = 1; i < ranks.length; i++) {
      assert.equal(ranks[i] - ranks[i-1], 1, 'Ranks should be consecutive');
    }
  });
  
  it('SUM() OVER (PARTITION BY)', () => {
    const r = db.execute(`
      SELECT rep, region, amount, SUM(amount) OVER (PARTITION BY region) AS region_total
      FROM sales
      ORDER BY region, rep
    `);
    const east = r.rows.filter(x => x.region === 'East');
    const expectedEast = 100 + 150 + 200 + 250; // 700
    assert.equal(east[0].region_total, expectedEast);
    
    const west = r.rows.filter(x => x.region === 'West');
    const expectedWest = 300 + 175 + 125 + 225; // 825
    assert.equal(west[0].region_total, expectedWest);
  });
  
  it('AVG() OVER (PARTITION BY)', () => {
    const r = db.execute(`
      SELECT rep, region, AVG(amount) OVER (PARTITION BY region) AS avg_amount
      FROM sales
      ORDER BY region
    `);
    const eastAvg = (100 + 150 + 200 + 250) / 4; // 175
    const east = r.rows.find(x => x.region === 'East');
    assert.equal(east.avg_amount, eastAvg);
  });
  
  it('COUNT() OVER (PARTITION BY)', () => {
    const r = db.execute(`
      SELECT rep, COUNT(*) OVER (PARTITION BY rep) AS sale_count
      FROM sales
    `);
    // Each rep should have count 2
    for (const row of r.rows) {
      assert.equal(row.sale_count, 2);
    }
  });
  
  it('multiple window functions in same query', () => {
    const r = db.execute(`
      SELECT rep, amount,
             ROW_NUMBER() OVER (ORDER BY amount DESC) AS global_rank,
             ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS region_rank,
             SUM(amount) OVER (PARTITION BY region) AS region_total
      FROM sales
    `);
    assert.equal(r.rows.length, 8);
    assert.ok(r.rows[0].global_rank >= 1);
    assert.ok(r.rows[0].region_rank >= 1);
    assert.ok(r.rows[0].region_total > 0);
  });
  
  it('window function with WHERE clause', () => {
    const r = db.execute(`
      SELECT rep, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
      FROM sales
      WHERE month = 1
      ORDER BY rn
    `);
    assert.equal(r.rows.length, 4); // Only month 1
    assert.equal(r.rows[0].rn, 1);
    assert.ok(r.rows[0].amount >= r.rows[1].amount);
  });
  
  it('LAG/LEAD window functions', () => {
    const r = db.execute(`
      SELECT rep, month, amount,
             LAG(amount) OVER (PARTITION BY rep ORDER BY month) AS prev_amount,
             LEAD(amount) OVER (PARTITION BY rep ORDER BY month) AS next_amount
      FROM sales
      ORDER BY rep, month
    `);
    // Alice: month 1 (100) → prev null, next 150; month 2 (150) → prev 100, next null
    const alice = r.rows.filter(x => x.rep === 'Alice');
    assert.equal(alice[0].prev_amount, null);
    assert.equal(alice[0].next_amount, alice[1].amount);
    assert.equal(alice[1].prev_amount, alice[0].amount);
    assert.equal(alice[1].next_amount, null);
  });
});
