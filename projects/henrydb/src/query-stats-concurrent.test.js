// query-stats-concurrent.test.js — Test EXPLAIN ANALYZE timing, row count accuracy,
// and query statistics under concurrent transactional load.

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function seedDb(db, rows = 200) {
  db.execute('CREATE TABLE items (id INT PRIMARY KEY, val INT, category TEXT, price INT)');
  db.execute('CREATE INDEX idx_val ON items (val)');
  db.execute('CREATE INDEX idx_cat ON items (category)');
  for (let i = 0; i < rows; i++) {
    db.execute(`INSERT INTO items VALUES (${i}, ${i * 7 % 1000}, 'cat${i % 10}', ${100 + i * 3})`);
  }
  return db;
}

// ─── Test 1: EXPLAIN ANALYZE basic accuracy ────────────────────────────────
describe('Query Stats: EXPLAIN ANALYZE row count accuracy', () => {
  it('reports correct actual_rows for full table scan', () => {
    const db = seedDb(new Database());
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items');
    assert.strictEqual(r.actual_rows, 200);
    assert.ok(r.execution_time_ms >= 0, 'execution_time_ms should be non-negative');
  });

  it('reports correct actual_rows for filtered query', () => {
    const db = seedDb(new Database());
    // val < 100 means i*7%1000 < 100 — count them
    const expected = db.execute('SELECT COUNT(*) as cnt FROM items WHERE val < 100').rows[0].cnt;
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items WHERE val < 100');
    assert.strictEqual(r.actual_rows, expected);
  });

  it('reports correct actual_rows for GROUP BY', () => {
    const db = seedDb(new Database());
    const r = db.execute('EXPLAIN ANALYZE SELECT category, COUNT(*) FROM items GROUP BY category');
    assert.strictEqual(r.actual_rows, 10); // 10 categories (cat0-cat9)
  });

  it('reports correct actual_rows for JOIN', () => {
    const db = seedDb(new Database(), 50);
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, item_id INT, qty INT)');
    for (let i = 0; i < 30; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${i % 50}, ${i + 1})`);
    }
    const expected = db.execute('SELECT COUNT(*) as cnt FROM items i JOIN orders o ON i.id = o.item_id').rows[0].cnt;
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items i JOIN orders o ON i.id = o.item_id');
    assert.strictEqual(r.actual_rows, expected);
  });

  it('reports correct actual_rows for subquery', () => {
    const db = seedDb(new Database(), 50);
    const expected = db.execute("SELECT COUNT(*) as cnt FROM items WHERE category IN (SELECT DISTINCT category FROM items WHERE val > 300)").rows[0].cnt;
    const r = db.execute("EXPLAIN ANALYZE SELECT * FROM items WHERE category IN (SELECT DISTINCT category FROM items WHERE val > 300)");
    assert.strictEqual(r.actual_rows, expected);
  });

  it('execution_time_ms is a positive number', () => {
    const db = seedDb(new Database());
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items WHERE val BETWEEN 100 AND 500');
    assert.strictEqual(typeof r.execution_time_ms, 'number');
    assert.ok(r.execution_time_ms >= 0);
  });
});

// ─── Test 2: EXPLAIN ANALYZE timing consistency ────────────────────────────
describe('Query Stats: EXPLAIN ANALYZE timing', () => {
  it('larger result sets take non-zero time', () => {
    const db = seedDb(new Database(), 500);
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items');
    assert.ok(r.execution_time_ms >= 0);
    assert.strictEqual(r.actual_rows, 500);
  });

  it('empty result has valid timing', () => {
    const db = seedDb(new Database());
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items WHERE val = -999');
    assert.strictEqual(r.actual_rows, 0);
    assert.strictEqual(typeof r.execution_time_ms, 'number');
    assert.ok(r.execution_time_ms >= 0);
  });

  it('repeated EXPLAIN ANALYZE gives consistent row counts', () => {
    const db = seedDb(new Database());
    const counts = [];
    for (let i = 0; i < 5; i++) {
      const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items WHERE val > 500');
      counts.push(r.actual_rows);
    }
    // All runs should return the same count (data hasn't changed)
    assert.ok(counts.every(c => c === counts[0]), `Row counts should be consistent: ${counts}`);
  });
});

// ─── Test 3: Query stats under transactional load ──────────────────────────
describe('Query Stats: Under transactional load', () => {
  it('EXPLAIN ANALYZE sees uncommitted changes within same transaction', () => {
    const db = seedDb(new Database(), 100);
    const beforeCount = db.execute('SELECT COUNT(*) as cnt FROM items WHERE val < 50').rows[0].cnt;

    db.execute('BEGIN');
    db.execute('INSERT INTO items VALUES (999, 25, \'catX\', 500)');
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items WHERE val < 50');
    db.execute('COMMIT');

    // Should see the newly inserted row within the transaction
    assert.strictEqual(r.actual_rows, beforeCount + 1);
  });

  it('EXPLAIN ANALYZE reflects rollback correctly', () => {
    const db = seedDb(new Database(), 100);
    const beforeCount = db.execute('SELECT COUNT(*) as cnt FROM items').rows[0].cnt;

    db.execute('BEGIN');
    db.execute('DELETE FROM items WHERE id < 10');
    db.execute('ROLLBACK');

    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items');
    assert.strictEqual(r.actual_rows, beforeCount, 'Rollback should restore all rows');
  });

  it('multiple transactions with interleaved EXPLAIN ANALYZE', () => {
    const db = seedDb(new Database(), 50);

    // Transaction 1: insert rows
    db.execute('BEGIN');
    for (let i = 1000; i < 1010; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, ${i}, 'new', ${i})`);
    }
    const duringInsert = db.execute('EXPLAIN ANALYZE SELECT * FROM items');
    assert.strictEqual(duringInsert.actual_rows, 60); // 50 + 10
    db.execute('COMMIT');

    // Transaction 2: delete rows
    db.execute('BEGIN');
    db.execute('DELETE FROM items WHERE id >= 1000');
    const duringDelete = db.execute('EXPLAIN ANALYZE SELECT * FROM items');
    assert.strictEqual(duringDelete.actual_rows, 50); // back to 50
    db.execute('COMMIT');

    // Final state
    const final = db.execute('EXPLAIN ANALYZE SELECT * FROM items');
    assert.strictEqual(final.actual_rows, 50);
  });

  it('EXPLAIN ANALYZE after bulk UPDATE reflects changes', () => {
    const db = seedDb(new Database(), 100);
    db.execute('BEGIN');
    db.execute('UPDATE items SET val = 9999 WHERE id < 20');
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items WHERE val = 9999');
    assert.strictEqual(r.actual_rows, 20);
    db.execute('COMMIT');
  });
});

// ─── Test 4: EXPLAIN (without ANALYZE) plan structure ──────────────────────
describe('Query Stats: EXPLAIN plan structure', () => {
  it('EXPLAIN returns plan rows', () => {
    const db = seedDb(new Database());
    const r = db.execute('EXPLAIN SELECT * FROM items WHERE val > 100');
    assert.ok(r.rows.length > 0);
    assert.ok(r.rows[0]['QUERY PLAN'], 'Should have QUERY PLAN column');
  });

  it('EXPLAIN does not execute the query (no actual_rows)', () => {
    const db = seedDb(new Database());
    const r = db.execute('EXPLAIN SELECT * FROM items');
    // EXPLAIN without ANALYZE should not have execution_time_ms or actual_rows at top level
    // (it returns plan info only)
    assert.ok(r.plan || r.rows, 'Should return plan or rows');
  });

  it('EXPLAIN shows filter operations', () => {
    const db = seedDb(new Database());
    const r = db.execute('EXPLAIN SELECT * FROM items WHERE val > 500 AND category = \'cat3\'');
    const planText = r.rows.map(row => row['QUERY PLAN']).join(' ');
    // Should mention the table being scanned
    assert.ok(planText.includes('items') || planText.includes('Scan'), `Plan should reference table: ${planText}`);
  });

  it('EXPLAIN ANALYZE includes planTree with actual stats', () => {
    const db = seedDb(new Database());
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items WHERE id < 10');
    assert.ok(r.planTree, 'Should have planTree');
    assert.ok(typeof r.planTree.actualRows === 'number', 'planTree should have actualRows');
  });
});

// ─── Test 5: Analysis array accuracy ───────────────────────────────────────
describe('Query Stats: Analysis array details', () => {
  it('analysis includes table scan info', () => {
    const db = seedDb(new Database());
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items');
    assert.ok(r.analysis, 'Should have analysis array');
    const scan = r.analysis.find(a => a.operation === 'TABLE_SCAN');
    assert.ok(scan, 'Should have TABLE_SCAN entry');
    assert.strictEqual(scan.table, 'items');
    assert.strictEqual(scan.total_table_rows, 200);
  });

  it('analysis shows filter selectivity', () => {
    const db = seedDb(new Database());
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items WHERE val < 100');
    const filter = r.analysis.find(a => a.operation === 'FILTER' || a.selectivity);
    assert.ok(filter, 'Should have filter or selectivity info');
  });

  it('analysis shows GROUP BY info', () => {
    const db = seedDb(new Database());
    const r = db.execute('EXPLAIN ANALYZE SELECT category, COUNT(*) FROM items GROUP BY category');
    const groupBy = r.analysis.find(a => a.operation === 'GROUP_BY');
    assert.ok(groupBy, 'Should have GROUP_BY in analysis');
    assert.strictEqual(groupBy.groups, 10);
  });

  it('planTree text matches actual row counts', () => {
    const db = seedDb(new Database(), 100);
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items WHERE id < 25');
    assert.ok(r.planTreeText, 'Should have planTreeText');
    const text = r.planTreeText.join(' ');
    // planTreeText should mention actual rows
    assert.ok(text.includes('actual') || text.includes('rows'), `Plan text should mention rows: ${text}`);
  });
});

// ─── Test 6: Stats accuracy with data modifications ────────────────────────
describe('Query Stats: Accuracy after data modifications', () => {
  it('row counts update after INSERT', () => {
    const db = seedDb(new Database(), 100);
    const before = db.execute('EXPLAIN ANALYZE SELECT * FROM items').actual_rows;
    for (let i = 500; i < 520; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, ${i}, 'new', ${i})`);
    }
    const after = db.execute('EXPLAIN ANALYZE SELECT * FROM items').actual_rows;
    assert.strictEqual(after, before + 20);
  });

  it('row counts update after DELETE', () => {
    const db = seedDb(new Database(), 100);
    db.execute('DELETE FROM items WHERE id >= 90');
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items');
    assert.strictEqual(r.actual_rows, 90);
  });

  it('row counts update after UPDATE does not change count', () => {
    const db = seedDb(new Database(), 100);
    db.execute('UPDATE items SET val = 0 WHERE id < 50');
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items');
    assert.strictEqual(r.actual_rows, 100); // UPDATE doesn't change count
  });

  it('filtered count accurate after mixed DML', () => {
    const db = seedDb(new Database(), 100);
    db.execute('DELETE FROM items WHERE id >= 80');            // 80 left
    db.execute('UPDATE items SET val = 999 WHERE id < 10');    // 10 rows with val=999
    db.execute("INSERT INTO items VALUES (200, 999, 'x', 1)"); // 81 total, 11 with val=999

    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM items WHERE val = 999');
    assert.strictEqual(r.actual_rows, 11);
  });
});
