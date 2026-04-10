// vectorized.test.js — Tests and benchmarks for vectorized execution engine
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  ColumnBatch, VecScanOperator, VecFilterOperator, VecProjectOperator,
  VecHashAggOperator, VecSortOperator, VecLimitOperator, VecHashJoinOperator,
  vecMulScalar, vecAddScalar, vecAddCols, vecMulCols,
  collectRows, countRows, BATCH_SIZE
} from './vectorized.js';

// Helper: generate N rows of data
function generateData(n, numCols = 3) {
  const rows = [];
  for (let i = 0; i < n; i++) {
    const row = [i];
    for (let c = 1; c < numCols; c++) {
      row.push(Math.random() * 1000 | 0);
    }
    rows.push(row);
  }
  return rows;
}

describe('ColumnBatch', () => {
  it('converts between row-major and column-major', () => {
    const rows = [[1, 'a', 10], [2, 'b', 20], [3, 'c', 30]];
    const batch = ColumnBatch.fromRows(rows, 3);
    assert.deepEqual(batch.columns[0], [1, 2, 3]);
    assert.deepEqual(batch.columns[1], ['a', 'b', 'c']);
    assert.deepEqual(batch.columns[2], [10, 20, 30]);
    assert.equal(batch.length, 3);
    assert.deepEqual(batch.toRows(), rows);
  });

  it('getRow returns correct row', () => {
    const batch = ColumnBatch.fromRows([[10, 20], [30, 40]], 2);
    assert.deepEqual(batch.getRow(0), [10, 20]);
    assert.deepEqual(batch.getRow(1), [30, 40]);
  });
});

describe('VecScanOperator', () => {
  it('produces batches of correct size', () => {
    const data = generateData(BATCH_SIZE + 100, 3);
    const scan = new VecScanOperator(data, 3);
    
    const batch1 = scan.next();
    assert.equal(batch1.length, BATCH_SIZE);
    
    const batch2 = scan.next();
    assert.equal(batch2.length, 100);
    
    assert.equal(scan.next(), null);
  });

  it('handles empty input', () => {
    const scan = new VecScanOperator([], 3);
    assert.equal(scan.next(), null);
  });
});

describe('VecFilterOperator', () => {
  it('filters rows by predicate', () => {
    const data = [[1, 10], [2, 20], [3, 30], [4, 40], [5, 50]];
    const scan = new VecScanOperator(data, 2);
    const filter = new VecFilterOperator(scan, 0, '>', 3);
    
    const rows = collectRows(filter);
    assert.equal(rows.length, 2);
    assert.deepEqual(rows[0], [4, 40]);
    assert.deepEqual(rows[1], [5, 50]);
  });

  it('handles all operators', () => {
    const data = [[1], [2], [3], [4], [5]];
    
    let rows;
    rows = collectRows(new VecFilterOperator(new VecScanOperator(data, 1), 0, '=', 3));
    assert.equal(rows.length, 1);
    
    rows = collectRows(new VecFilterOperator(new VecScanOperator(data, 1), 0, '!=', 3));
    assert.equal(rows.length, 4);
    
    rows = collectRows(new VecFilterOperator(new VecScanOperator(data, 1), 0, '<=', 3));
    assert.equal(rows.length, 3);
    
    rows = collectRows(new VecFilterOperator(new VecScanOperator(data, 1), 0, '>=', 3));
    assert.equal(rows.length, 3);
    
    rows = collectRows(new VecFilterOperator(new VecScanOperator(data, 1), 0, '<', 1));
    assert.equal(rows.length, 0);
  });

  it('filters large dataset across multiple batches', () => {
    const data = generateData(BATCH_SIZE * 3, 2);
    // Filter: col[0] < 500 (roughly half)
    const scan = new VecScanOperator(data, 2);
    const filter = new VecFilterOperator(scan, 0, '<', 500);
    const count = countRows(filter);
    assert.equal(count, 500);
  });
});

describe('VecProjectOperator', () => {
  it('projects columns', () => {
    const data = [[1, 10, 100], [2, 20, 200]];
    const scan = new VecScanOperator(data, 3);
    const proj = new VecProjectOperator(scan, [
      { type: 'col', idx: 2 },
      { type: 'col', idx: 0 },
    ]);
    
    const rows = collectRows(proj);
    assert.deepEqual(rows[0], [100, 1]);
    assert.deepEqual(rows[1], [200, 2]);
  });

  it('computes expressions', () => {
    const data = [[10, 5], [20, 3], [30, 7]];
    const scan = new VecScanOperator(data, 2);
    const proj = new VecProjectOperator(scan, [
      { type: 'col', idx: 0 },
      { type: 'expr', fn: vecMulScalar(0, 0.8) },      // price * 0.8
      { type: 'expr', fn: vecAddCols(0, 1) },           // col0 + col1
    ]);
    
    const rows = collectRows(proj);
    assert.equal(rows[0][1], 8);     // 10 * 0.8
    assert.equal(rows[0][2], 15);    // 10 + 5
    assert.equal(rows[1][1], 16);    // 20 * 0.8
    assert.equal(rows[2][2], 37);    // 30 + 7
  });
});

describe('VecHashAggOperator', () => {
  it('groups and aggregates', () => {
    const data = [
      ['A', 10], ['B', 20], ['A', 30], ['B', 40], ['A', 50],
    ];
    const scan = new VecScanOperator(data, 2);
    const agg = new VecHashAggOperator(scan, 0, [
      { colIdx: 1, fn: 'sum' },
      { colIdx: 1, fn: 'count' },
      { colIdx: 1, fn: 'min' },
      { colIdx: 1, fn: 'max' },
      { colIdx: 1, fn: 'avg' },
    ]);
    
    const rows = collectRows(agg);
    assert.equal(rows.length, 2);
    
    const groupA = rows.find(r => r[0] === 'A');
    const groupB = rows.find(r => r[0] === 'B');
    
    assert.equal(groupA[1], 90);   // sum
    assert.equal(groupA[2], 3);    // count
    assert.equal(groupA[3], 10);   // min
    assert.equal(groupA[4], 50);   // max
    assert.equal(groupA[5], 30);   // avg
    
    assert.equal(groupB[1], 60);   // sum
    assert.equal(groupB[2], 2);    // count
  });

  it('handles single group', () => {
    const data = [[1, 100], [1, 200], [1, 300]];
    const scan = new VecScanOperator(data, 2);
    const agg = new VecHashAggOperator(scan, 0, [{ colIdx: 1, fn: 'sum' }]);
    const rows = collectRows(agg);
    assert.equal(rows.length, 1);
    assert.deepEqual(rows[0], [1, 600]);
  });
});

describe('VecSortOperator', () => {
  it('sorts ascending', () => {
    const data = [[3, 'c'], [1, 'a'], [4, 'd'], [2, 'b']];
    const scan = new VecScanOperator(data, 2);
    const sort = new VecSortOperator(scan, 0);
    const rows = collectRows(sort);
    assert.deepEqual(rows.map(r => r[0]), [1, 2, 3, 4]);
  });

  it('sorts descending', () => {
    const data = [[3], [1], [4], [2]];
    const scan = new VecScanOperator(data, 1);
    const sort = new VecSortOperator(scan, 0, true);
    const rows = collectRows(sort);
    assert.deepEqual(rows.map(r => r[0]), [4, 3, 2, 1]);
  });
});

describe('VecLimitOperator', () => {
  it('limits output', () => {
    const data = generateData(1000, 2);
    const scan = new VecScanOperator(data, 2);
    const limit = new VecLimitOperator(scan, 10);
    const rows = collectRows(limit);
    assert.equal(rows.length, 10);
  });

  it('handles limit larger than input', () => {
    const data = [[1], [2], [3]];
    const scan = new VecScanOperator(data, 1);
    const limit = new VecLimitOperator(scan, 100);
    const rows = collectRows(limit);
    assert.equal(rows.length, 3);
  });
});

describe('VecHashJoinOperator', () => {
  it('joins two relations', () => {
    const orders = [[1, 'order_a'], [2, 'order_b'], [1, 'order_c']];
    const users = [[1, 'Alice'], [2, 'Bob'], [3, 'Charlie']];
    
    const left = new VecScanOperator(orders, 2);
    const right = new VecScanOperator(users, 2);
    const join = new VecHashJoinOperator(left, right, 0, 0);
    
    const rows = collectRows(join);
    assert.equal(rows.length, 3); // order_a→Alice, order_b→Bob, order_c→Alice
    
    // Check that join produces probe cols + build cols
    const aliceOrders = rows.filter(r => r[3] === 'Alice');
    assert.equal(aliceOrders.length, 2);
  });

  it('handles no matches', () => {
    const left = new VecScanOperator([[10, 'x']], 2);
    const right = new VecScanOperator([[20, 'y']], 2);
    const join = new VecHashJoinOperator(left, right, 0, 0);
    const rows = collectRows(join);
    assert.equal(rows.length, 0);
  });
});

describe('Complex Pipelines', () => {
  it('scan → filter → project → sort → limit', () => {
    // TPC-H-like: find top 5 expensive items with discount
    const inventory = [];
    for (let i = 0; i < 1000; i++) {
      inventory.push([i, `item_${i}`, (Math.random() * 100) | 0, Math.random()]);
    }
    
    const scan = new VecScanOperator(inventory, 4);
    const filter = new VecFilterOperator(scan, 2, '>', 50);       // price > 50
    const project = new VecProjectOperator(filter, [
      { type: 'col', idx: 1 },                                     // name
      { type: 'col', idx: 2 },                                     // price
      { type: 'expr', fn: vecMulScalar(2, 0.9) },                  // discounted
    ]);
    const sort = new VecSortOperator(project, 1, true);            // sort by price desc
    const limit = new VecLimitOperator(sort, 5);
    
    const rows = collectRows(limit);
    assert.equal(rows.length, 5);
    // Verify sorted descending by price
    for (let i = 1; i < rows.length; i++) {
      assert.ok(rows[i][1] <= rows[i-1][1], `${rows[i][1]} should be <= ${rows[i-1][1]}`);
    }
    // Verify discount calc
    for (const row of rows) {
      assert.ok(Math.abs(row[2] - row[1] * 0.9) < 0.001);
    }
  });

  it('scan → group by → sort', () => {
    // Sales by department
    const sales = [];
    const depts = ['Electronics', 'Clothing', 'Food', 'Books'];
    for (let i = 0; i < 10000; i++) {
      sales.push([depts[i % 4], (Math.random() * 100) | 0]);
    }
    
    const scan = new VecScanOperator(sales, 2);
    const agg = new VecHashAggOperator(scan, 0, [
      { colIdx: 1, fn: 'sum' },
      { colIdx: 1, fn: 'count' },
      { colIdx: 1, fn: 'avg' },
    ]);
    const sort = new VecSortOperator(agg, 1, true); // sort by total sales desc
    
    const rows = collectRows(sort);
    assert.equal(rows.length, 4);
    // Each dept has 2500 rows
    for (const row of rows) {
      assert.equal(row[2], 2500); // count
    }
  });
});

describe('Performance — Vectorized vs Row-at-a-time', () => {
  const N = 100_000;
  
  it('benchmark: vectorized filter + project on 100K rows', () => {
    const data = generateData(N, 3);
    const t0 = performance.now();
    
    const scan = new VecScanOperator(data, 3);
    const filter = new VecFilterOperator(scan, 1, '>', 500);
    const project = new VecProjectOperator(filter, [
      { type: 'col', idx: 0 },
      { type: 'expr', fn: vecMulScalar(1, 0.8) },
      { type: 'expr', fn: vecAddCols(1, 2) },
    ]);
    const count = countRows(project);
    
    const elapsed = performance.now() - t0;
    console.log(`    Vectorized: ${count} rows in ${elapsed.toFixed(1)}ms (${(N / elapsed * 1000) | 0} rows/sec)`);
    assert.ok(count > 0);
    assert.ok(elapsed < 5000, `Should complete in under 5s, took ${elapsed}ms`);
  });

  it('benchmark: row-at-a-time filter + project on 100K rows', () => {
    const data = generateData(N, 3);
    const t0 = performance.now();
    
    // Simulate Volcano-style row-at-a-time
    let count = 0;
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      // Filter
      if (row[1] <= 500) continue;
      // Project
      const _discounted = row[1] * 0.8;
      const _sum = row[1] + row[2];
      count++;
    }
    
    const elapsed = performance.now() - t0;
    console.log(`    Row-at-a-time: ${count} rows in ${elapsed.toFixed(1)}ms (${(N / elapsed * 1000) | 0} rows/sec)`);
    assert.ok(count > 0);
  });

  it('benchmark: vectorized aggregation on 100K rows', () => {
    // GROUP BY category (10 categories), SUM(amount)
    const data = [];
    for (let i = 0; i < N; i++) {
      data.push([i % 10, Math.random() * 1000 | 0]);
    }
    
    const t0 = performance.now();
    const scan = new VecScanOperator(data, 2);
    const agg = new VecHashAggOperator(scan, 0, [
      { colIdx: 1, fn: 'sum' },
      { colIdx: 1, fn: 'count' },
    ]);
    const rows = collectRows(agg);
    const elapsed = performance.now() - t0;
    
    console.log(`    Vectorized agg: ${rows.length} groups from ${N} rows in ${elapsed.toFixed(1)}ms`);
    assert.equal(rows.length, 10);
    for (const row of rows) {
      assert.equal(row[2], N / 10); // each group has N/10 rows
    }
  });

  it('benchmark: vectorized hash join on 100K × 1K', () => {
    const orders = [];
    for (let i = 0; i < N; i++) {
      orders.push([i % 1000, `order_${i}`, Math.random() * 100 | 0]);
    }
    const customers = [];
    for (let i = 0; i < 1000; i++) {
      customers.push([i, `customer_${i}`]);
    }
    
    const t0 = performance.now();
    const left = new VecScanOperator(orders, 3);
    const right = new VecScanOperator(customers, 2);
    const join = new VecHashJoinOperator(left, right, 0, 0);
    const count = countRows(join);
    const elapsed = performance.now() - t0;
    
    console.log(`    Vectorized join: ${count} output rows in ${elapsed.toFixed(1)}ms`);
    assert.equal(count, N); // Each order matches exactly one customer
  });
});
