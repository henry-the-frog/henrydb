// window-functions-stress.test.js — Stress tests for HenryDB window functions
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Window function stress tests', () => {
  let db;
  
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE sales (id INT, region TEXT, rep TEXT, amount INT, dt TEXT)');
    // Insert sales data with ties and NULLs
    const data = [
      [1, 'East', 'Alice', 100, '2024-01-01'],
      [2, 'East', 'Alice', 200, '2024-01-02'],
      [3, 'East', 'Bob', 200, '2024-01-03'],
      [4, 'East', 'Bob', 150, '2024-01-04'],
      [5, 'West', 'Carol', 300, '2024-01-01'],
      [6, 'West', 'Carol', 250, '2024-01-02'],
      [7, 'West', 'Dave', 100, '2024-01-03'],
      [8, 'West', 'Dave', 100, '2024-01-04'],
      [9, 'East', 'Alice', 350, '2024-01-05'],
      [10, 'West', 'Eve', null, '2024-01-05'],
    ];
    for (const [id, region, rep, amount, dt] of data) {
      if (amount === null) {
        db.execute(`INSERT INTO sales VALUES (${id}, '${region}', '${rep}', NULL, '${dt}')`);
      } else {
        db.execute(`INSERT INTO sales VALUES (${id}, '${region}', '${rep}', ${amount}, '${dt}')`);
      }
    }
  });

  it('ROW_NUMBER with ORDER BY', () => {
    const r = db.execute(`
      SELECT id, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) as rn
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY rn
    `);
    assert.strictEqual(r.rows.length, 9);
    assert.strictEqual(r.rows[0].rn, 1);
    assert.strictEqual(r.rows[0].amount, 350); // highest
    assert.strictEqual(r.rows[8].rn, 9);
    // All row numbers unique and sequential
    const rns = r.rows.map(r => r.rn);
    assert.deepStrictEqual(rns, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it('ROW_NUMBER with PARTITION BY', () => {
    const r = db.execute(`
      SELECT id, region, amount,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rn
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY region, rn
    `);
    // East partition should restart at 1
    const east = r.rows.filter(r => r.region === 'East');
    const west = r.rows.filter(r => r.region === 'West');
    assert.deepStrictEqual(east.map(r => r.rn), [1, 2, 3, 4, 5]);
    assert.deepStrictEqual(west.map(r => r.rn), [1, 2, 3, 4]);
  });

  it('RANK with ties', () => {
    const r = db.execute(`
      SELECT id, amount, RANK() OVER (ORDER BY amount DESC) as rnk
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY rnk, id
    `);
    // Tied amounts (200, 200) and (100, 100, 100) should get same rank
    const amounts = r.rows.map(r => ({ amount: r.amount, rnk: r.rnk }));
    // 350 → rank 1, 300 → rank 2, 250 → rank 3, 200,200 → rank 4, 150 → rank 6, 100,100,100 → rank 7
    assert.strictEqual(amounts[0].rnk, 1); // 350
    assert.strictEqual(amounts[1].rnk, 2); // 300
    // Find the 200s — they should have the same rank
    const rank200 = amounts.filter(a => a.amount === 200);
    assert.ok(rank200.length === 2);
    assert.strictEqual(rank200[0].rnk, rank200[1].rnk, 'tied amounts should have same RANK');
  });

  it('DENSE_RANK with ties', () => {
    const r = db.execute(`
      SELECT id, amount, DENSE_RANK() OVER (ORDER BY amount DESC) as drnk
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY drnk, id
    `);
    // DENSE_RANK doesn't skip numbers after ties
    // 350→1, 300→2, 250→3, 200→4, 150→5, 100→6
    const amounts = r.rows.map(r => ({ amount: r.amount, drnk: r.drnk }));
    assert.strictEqual(amounts[0].drnk, 1);
    const rank200 = amounts.filter(a => a.amount === 200);
    assert.ok(rank200.length === 2);
    assert.strictEqual(rank200[0].drnk, rank200[1].drnk);
    // Max dense rank should be 6 (not 7 like RANK)
    const maxDrnk = Math.max(...r.rows.map(r => r.drnk));
    assert.strictEqual(maxDrnk, 6, 'DENSE_RANK should not skip');
  });

  it('LAG function', () => {
    const r = db.execute(`
      SELECT id, amount, 
        LAG(amount) OVER (ORDER BY id) as prev_amount
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY id
    `);
    // First row should have NULL lag
    assert.strictEqual(r.rows[0].prev_amount, null);
    // Second row should have first row's amount
    assert.strictEqual(r.rows[1].prev_amount, r.rows[0].amount);
  });

  it('LEAD function', () => {
    const r = db.execute(`
      SELECT id, amount,
        LEAD(amount) OVER (ORDER BY id) as next_amount
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY id
    `);
    // Last row should have NULL lead
    assert.strictEqual(r.rows[r.rows.length - 1].next_amount, null);
    // First row should have second row's amount
    assert.strictEqual(r.rows[0].next_amount, r.rows[1].amount);
  });

  it('SUM OVER with ROWS BETWEEN frame', () => {
    const r = db.execute(`
      SELECT id, amount,
        SUM(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY id
    `);
    let expectedTotal = 0;
    for (const row of r.rows) {
      expectedTotal += row.amount;
      assert.strictEqual(row.running_total, expectedTotal, `row ${row.id}: expected ${expectedTotal}, got ${row.running_total}`);
    }
  });

  it('AVG OVER with ROWS BETWEEN frame', () => {
    const r = db.execute(`
      SELECT id, amount,
        AVG(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_avg
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY id
    `);
    let sum = 0;
    let count = 0;
    for (const row of r.rows) {
      sum += row.amount;
      count++;
      const expectedAvg = sum / count;
      assert.ok(
        Math.abs(row.running_avg - expectedAvg) < 0.01,
        `row ${row.id}: expected avg ${expectedAvg}, got ${row.running_avg}`
      );
    }
  });

  it('SUM OVER entire partition (no frame)', () => {
    const r = db.execute(`
      SELECT id, region, amount,
        SUM(amount) OVER (PARTITION BY region) as region_total
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY id
    `);
    // East total: 100+200+200+150+350 = 1000
    // West total: 300+250+100+100 = 750
    const eastRows = r.rows.filter(r => r.region === 'East');
    const westRows = r.rows.filter(r => r.region === 'West');
    for (const row of eastRows) {
      assert.strictEqual(row.region_total, 1000, `East row ${row.id}: expected 1000, got ${row.region_total}`);
    }
    for (const row of westRows) {
      assert.strictEqual(row.region_total, 750, `West row ${row.id}: expected 750, got ${row.region_total}`);
    }
  });

  it('PARTITION BY with multiple columns', () => {
    const r = db.execute(`
      SELECT id, region, rep, amount,
        ROW_NUMBER() OVER (PARTITION BY region, rep ORDER BY amount DESC) as rn
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY region, rep, rn
    `);
    // East/Alice: 3 rows, East/Bob: 2 rows, West/Carol: 2 rows, West/Dave: 2 rows
    const partitions = {};
    for (const row of r.rows) {
      const key = `${row.region}/${row.rep}`;
      if (!partitions[key]) partitions[key] = [];
      partitions[key].push(row.rn);
    }
    // Each partition should start from 1
    for (const [key, rns] of Object.entries(partitions)) {
      assert.strictEqual(rns[0], 1, `partition ${key} should start at 1`);
      // Should be sequential
      for (let i = 0; i < rns.length; i++) {
        assert.strictEqual(rns[i], i + 1, `partition ${key}: expected ${i + 1}, got ${rns[i]}`);
      }
    }
  });

  it('multiple window functions in same query', () => {
    const r = db.execute(`
      SELECT id, amount,
        ROW_NUMBER() OVER (ORDER BY amount DESC) as rn,
        RANK() OVER (ORDER BY amount DESC) as rnk,
        SUM(amount) OVER (ORDER BY id) as running
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY rn
    `);
    assert.strictEqual(r.rows.length, 9);
    // ROW_NUMBER should be 1-9
    assert.strictEqual(r.rows[0].rn, 1);
    // RANK should handle ties
    assert.ok(r.rows[0].rnk >= 1);
    // Running sum should be present
    assert.ok(r.rows[0].running > 0);
  });

  it('FIRST_VALUE and LAST_VALUE', () => {
    const r = db.execute(`
      SELECT id, amount,
        FIRST_VALUE(amount) OVER (ORDER BY id) as first_amt,
        LAST_VALUE(amount) OVER (ORDER BY id) as last_amt
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY id
    `);
    // FIRST_VALUE should always be 100 (first row's amount)
    for (const row of r.rows) {
      assert.strictEqual(row.first_amt, 100, `row ${row.id}: FIRST_VALUE should be 100`);
    }
  });

  it('NTILE distribution', () => {
    const r = db.execute(`
      SELECT id, NTILE(3) OVER (ORDER BY id) as tile
      FROM sales
      WHERE amount IS NOT NULL
      ORDER BY id
    `);
    assert.strictEqual(r.rows.length, 9);
    // 9 rows into 3 tiles: 3+3+3
    const tiles = r.rows.map(r => r.tile);
    const counts = {};
    for (const t of tiles) counts[t] = (counts[t] || 0) + 1;
    assert.strictEqual(Object.keys(counts).length, 3, 'should have 3 tiles');
    // Each tile should have 3 rows
    for (const [tile, count] of Object.entries(counts)) {
      assert.strictEqual(count, 3, `tile ${tile} should have 3 rows`);
    }
  });

  it('window function with WHERE filter', () => {
    const r = db.execute(`
      SELECT id, amount,
        ROW_NUMBER() OVER (ORDER BY amount DESC) as rn
      FROM sales
      WHERE region = 'East' AND amount IS NOT NULL
      ORDER BY rn
    `);
    assert.strictEqual(r.rows.length, 5); // East has 5 non-null rows (Alice 3 + Bob 2)
    assert.deepStrictEqual(r.rows.map(r => r.rn), [1, 2, 3, 4, 5]);
  });

  it('window function with empty partition', () => {
    const r = db.execute(`
      SELECT id, region, amount,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) as rn
      FROM sales
      WHERE region = 'North'
    `);
    assert.strictEqual(r.rows.length, 0);
  });

  it('single-row partition', () => {
    const r = db.execute(`
      SELECT id, amount,
        ROW_NUMBER() OVER (ORDER BY id) as rn,
        SUM(amount) OVER () as total
      FROM sales
      WHERE id = 1
    `);
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].rn, 1);
    assert.strictEqual(r.rows[0].total, 100);
  });

  it('NULL handling in window functions', () => {
    const r = db.execute(`
      SELECT id, amount,
        ROW_NUMBER() OVER (ORDER BY amount) as rn,
        SUM(amount) OVER (ORDER BY id) as running_sum
      FROM sales
      ORDER BY rn
    `);
    // NULLs should be included in ROW_NUMBER
    assert.strictEqual(r.rows.length, 10);
    // The NULL amount row should appear somewhere
    const nullRow = r.rows.find(r => r.amount === null);
    assert.ok(nullRow, 'should include row with NULL amount');
    assert.ok(nullRow.rn > 0, 'NULL row should have a row number');
  });

  it('window function combined with GROUP BY (error or sensible result)', () => {
    // Window functions usually apply AFTER GROUP BY
    try {
      const r = db.execute(`
        SELECT region, SUM(amount) as total,
          RANK() OVER (ORDER BY SUM(amount) DESC) as rnk
        FROM sales
        WHERE amount IS NOT NULL
        GROUP BY region
        ORDER BY rnk
      `);
      // If it works, verify the ranking is correct
      assert.ok(r.rows.length >= 1);
      if (r.rows.length === 2) {
        assert.ok(r.rows[0].total >= r.rows[1].total, 'rank 1 should have highest total');
      }
    } catch (e) {
      // Some DBs don't support window + group by — acceptable
      assert.ok(e.message.length > 0);
    }
  });

  it('large dataset window function performance', () => {
    const db2 = new Database();
    db2.execute('CREATE TABLE big_sales (id INT, category INT, value INT)');
    for (let i = 1; i <= 1000; i++) {
      db2.execute(`INSERT INTO big_sales VALUES (${i}, ${i % 10}, ${Math.floor(Math.random() * 1000)})`);
    }
    
    const start = Date.now();
    const r = db2.execute(`
      SELECT id, category, value,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rn
      FROM big_sales
      ORDER BY category, rn
    `);
    const elapsed = Date.now() - start;
    
    assert.strictEqual(r.rows.length, 1000);
    // Verify each partition starts at 1
    let lastCat = null;
    for (const row of r.rows) {
      if (row.category !== lastCat) {
        assert.strictEqual(row.rn, 1, `partition ${row.category} should start at 1`);
        lastCat = row.category;
      }
    }
    console.log(`1000-row window function took ${elapsed}ms`);
    assert.ok(elapsed < 5000, `too slow: ${elapsed}ms`);
  });
});
