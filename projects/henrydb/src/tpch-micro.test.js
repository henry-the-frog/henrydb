// tpch-micro.test.js — TPC-H Micro-Benchmark for HenryDB
// Simplified versions of TPC-H Q1, Q6, Q14
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

// Simplified TPC-H schema
// lineitem: core fact table
// part: dimension table for promotion analysis

function seedRandom(seed) {
  let s = seed;
  return function() {
    s = (s * 1103515245 + 12345) & 0x7fffffff;
    return s / 0x7fffffff;
  };
}

function generateData(db, scale = 1) {
  const rand = seedRandom(42);

  // Create tables
  db.execute(`CREATE TABLE lineitem (
    l_orderkey INT,
    l_partkey INT,
    l_suppkey INT,
    l_linenumber INT,
    l_quantity REAL,
    l_extendedprice REAL,
    l_discount REAL,
    l_tax REAL,
    l_returnflag TEXT,
    l_linestatus TEXT,
    l_shipdate TEXT,
    l_commitdate TEXT,
    l_receiptdate TEXT
  )`);

  db.execute(`CREATE TABLE part (
    p_partkey INT,
    p_name TEXT,
    p_type TEXT,
    p_size INT,
    p_container TEXT
  )`);

  // Generate parts
  const types = ['PROMO BURNISHED', 'STANDARD POLISHED', 'ECONOMY ANODIZED', 'PROMO BRUSHED', 'STANDARD PLATED'];
  const containers = ['SM CASE', 'MED BAG', 'LG BOX', 'SM PACK', 'WRAP DRUM'];
  const numParts = 200 * scale;
  
  for (let i = 1; i <= numParts; i++) {
    const type = types[Math.floor(rand() * types.length)];
    const container = containers[Math.floor(rand() * containers.length)];
    db.execute(`INSERT INTO part VALUES (${i}, 'part_${i}', '${type}', ${Math.floor(rand() * 50) + 1}, '${container}')`);
  }

  // Generate lineitem rows
  const flags = ['A', 'N', 'R'];
  const statuses = ['F', 'O'];
  const numLines = 1000 * scale;
  
  // Batch inserts for performance
  for (let i = 1; i <= numLines; i++) {
    const orderkey = Math.floor(rand() * 1000) + 1;
    const partkey = Math.floor(rand() * numParts) + 1;
    const suppkey = Math.floor(rand() * 10) + 1;
    const qty = Math.floor(rand() * 50) + 1;
    const price = Math.round((rand() * 900 + 100) * 100) / 100;
    const discount = Math.round(rand() * 10) / 100; // 0.00 to 0.10
    const tax = Math.round(rand() * 8) / 100; // 0.00 to 0.08
    const flag = flags[Math.floor(rand() * flags.length)];
    const status = statuses[Math.floor(rand() * statuses.length)];
    // Dates: 1993-01-01 to 1998-12-31
    const year = 1993 + Math.floor(rand() * 6);
    const month = String(Math.floor(rand() * 12) + 1).padStart(2, '0');
    const day = String(Math.floor(rand() * 28) + 1).padStart(2, '0');
    const shipdate = `${year}-${month}-${day}`;
    
    db.execute(`INSERT INTO lineitem VALUES (${orderkey}, ${partkey}, ${suppkey}, ${i}, ${qty}, ${price}, ${discount}, ${tax}, '${flag}', '${status}', '${shipdate}', '${shipdate}', '${shipdate}')`);
  }

  return { numParts, numLines };
}

describe('TPC-H Micro-Benchmark', () => {
  let db;
  let dataSize;

  before(() => {
    db = new Database();
    const start = performance.now();
    dataSize = generateData(db, 1);
    const elapsed = performance.now() - start;
    console.log(`\n  Data generation: ${dataSize.numLines} lineitem rows, ${dataSize.numParts} parts in ${elapsed.toFixed(0)}ms`);
  });

  it('TPC-H Q1: Pricing Summary Report', () => {
    // Original Q1: GROUP BY returnflag, linestatus with aggregates
    const sql = `
      SELECT
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) AS sum_qty,
        SUM(l_extendedprice) AS sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        AVG(l_quantity) AS avg_qty,
        AVG(l_extendedprice) AS avg_price,
        AVG(l_discount) AS avg_disc,
        COUNT(*) AS count_order
      FROM lineitem
      WHERE l_shipdate <= '1998-09-01'
      GROUP BY l_returnflag, l_linestatus
      ORDER BY l_returnflag, l_linestatus
    `;

    const start = performance.now();
    const result = db.execute(sql);
    const elapsed = performance.now() - start;

    console.log(`  Q1: ${elapsed.toFixed(1)}ms, ${result.rows.length} groups`);
    for (const row of result.rows) {
      console.log(`    ${row.l_returnflag}|${row.l_linestatus}: qty=${row.sum_qty?.toFixed(0)}, orders=${row.count_order}`);
    }

    // Verify structure
    assert.ok(result.rows.length > 0, 'Should have at least one group');
    assert.ok(result.rows.length <= 6, 'At most 6 groups (3 flags × 2 statuses)');
    for (const row of result.rows) {
      assert.ok(row.sum_qty > 0, 'sum_qty should be positive');
      assert.ok(row.count_order > 0, 'count_order should be positive');
      assert.ok(row.avg_qty > 0 && row.avg_qty <= 50, 'avg_qty should be reasonable');
    }
  });

  it('TPC-H Q6: Forecasting Revenue Change', () => {
    // Simple filter + aggregate: sum of revenue for discounted items
    const sql = `
      SELECT
        SUM(l_extendedprice * l_discount) AS revenue
      FROM lineitem
      WHERE l_shipdate >= '1994-01-01'
        AND l_shipdate < '1995-01-01'
        AND l_discount >= 0.05
        AND l_discount <= 0.07
        AND l_quantity < 24
    `;

    const start = performance.now();
    const result = db.execute(sql);
    const elapsed = performance.now() - start;

    console.log(`  Q6: ${elapsed.toFixed(1)}ms, revenue=${result.rows[0]?.revenue?.toFixed(2)}`);

    assert.equal(result.rows.length, 1, 'Should return exactly one row');
    assert.ok(typeof result.rows[0].revenue === 'number' || result.rows[0].revenue === null, 'Revenue should be a number');
  });

  it('TPC-H Q14: Promotion Effect', () => {
    // Simplified Q14: compute promo and total revenue separately
    const sql1 = `
      SELECT
        SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) AS promo_revenue
      FROM lineitem
      JOIN part ON l_partkey = p_partkey
      WHERE l_shipdate >= '1995-09-01'
        AND l_shipdate < '1995-10-01'
    `;
    const sql2 = `
      SELECT
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
      FROM lineitem
      JOIN part ON l_partkey = p_partkey
      WHERE l_shipdate >= '1995-09-01'
        AND l_shipdate < '1995-10-01'
    `;

    const start = performance.now();
    const r1 = db.execute(sql1);
    const r2 = db.execute(sql2);
    const elapsed = performance.now() - start;
    
    const promo = r1.rows[0]?.promo_revenue || 0;
    const total = r2.rows[0]?.total_revenue || 1;
    const pct = (100 * promo / total);

    console.log(`  Q14: ${elapsed.toFixed(1)}ms, promo=${promo.toFixed(2)}, total=${total.toFixed(2)}, pct=${pct.toFixed(2)}%`);

    assert.equal(r1.rows.length, 1, 'Promo query should return one row');
    assert.equal(r2.rows.length, 1, 'Total query should return one row');
    if (total > 0) {
      assert.ok(pct >= 0 && pct <= 100, 'Promo percentage should be 0-100');
    }
  });

  it('Verify data integrity', () => {
    const lineCount = db.execute('SELECT COUNT(*) AS cnt FROM lineitem');
    assert.equal(lineCount.rows[0].cnt, dataSize.numLines);

    const partCount = db.execute('SELECT COUNT(*) AS cnt FROM part');
    assert.equal(partCount.rows[0].cnt, dataSize.numParts);

    // Verify JOIN works
    const joined = db.execute('SELECT COUNT(*) AS cnt FROM lineitem JOIN part ON l_partkey = p_partkey');
    assert.equal(joined.rows[0].cnt, dataSize.numLines);
  });
});
