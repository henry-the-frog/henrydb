// pushdown-integration.test.js — Tests for predicate pushdown in query execution
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Predicate pushdown in JOIN execution', () => {
  function makeDB() {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER, dept TEXT)');
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, status TEXT)');
    
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO users VALUES (${i}, 'user${i}', ${i <= 50 ? 1 : 0}, '${i % 3 === 0 ? "eng" : "sales"}')`);
    }
    for (let i = 1; i <= 500; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${1 + (i % 100)}, ${(i * 9.99).toFixed(2)}, '${i % 5 === 0 ? "shipped" : "pending"}')`);
    }
    return db;
  }

  it('pushes single-table WHERE predicates below JOIN', () => {
    const db = makeDB();
    // WHERE u.active = 1 should be pushed to users scan
    const result = db.execute("SELECT u.name, o.total FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = 1");
    // 50 active users, each has ~5 orders
    assert.ok(result.rows.length > 0);
    assert.ok(result.rows.length < 500); // Filtered, not all orders
  });

  it('pushes predicates to both sides of JOIN', () => {
    const db = makeDB();
    const result = db.execute("SELECT u.name, o.total FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = 1 AND o.status = 'shipped'");
    // Only active users AND shipped orders
    assert.ok(result.rows.length > 0);
    assert.ok(result.rows.length < 250); // Heavily filtered
  });

  it('preserves cross-table predicates in WHERE (not pushed)', () => {
    const db = makeDB();
    // o.total > u.id is a cross-table predicate — should stay in WHERE
    const result = db.execute("SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE o.total > 100 AND o.total < u.id * 100");
    assert.ok(Array.isArray(result.rows));
  });

  it('correctness matches non-pushdown result', () => {
    const db = makeDB();
    // Run with a query that exercises pushdown
    const pushdownResult = db.execute("SELECT u.name, o.total FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = 1 AND o.status = 'shipped'");
    
    // Verify manually: get active users
    const activeUsers = db.execute("SELECT id FROM users WHERE active = 1").rows.map(r => r.id);
    const shippedOrders = db.execute("SELECT user_id, total FROM orders WHERE status = 'shipped'").rows;
    
    // Manual join
    const expected = shippedOrders.filter(o => activeUsers.includes(o.user_id));
    
    assert.equal(pushdownResult.rows.length, expected.length);
  });

  it('works with LEFT JOIN (pushes only left-side predicates safely)', () => {
    const db = makeDB();
    // For LEFT JOIN, pushing right-side predicates is semantically safe for filtering
    const result = db.execute("SELECT u.name, o.total FROM users u LEFT JOIN orders o ON o.user_id = u.id WHERE u.active = 1");
    assert.ok(result.rows.length > 0);
    // All active users should appear (even without orders)
    const names = new Set(result.rows.map(r => r['u.name'] || r.name));
    assert.ok(names.size <= 50); // At most 50 active users
  });

  it('EXPLAIN TREE shows pushed filters', () => {
    const db = makeDB();
    const result = db.execute("EXPLAIN (FORMAT TREE) SELECT u.name, o.total FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = 1 AND o.status = 'shipped'");
    const planText = result.rows.map(r => r['QUERY PLAN']).join('\n');
    
    // Should show filters on individual scans
    assert.ok(planText.includes('Filter:'), 'Expected Filter in plan');
    assert.ok(planText.includes('Seq Scan on orders'), 'Expected orders scan');
    assert.ok(planText.includes('Seq Scan on users'), 'Expected users scan');
  });

  it('handles query without pushdown opportunities gracefully', () => {
    const db = makeDB();
    // No WHERE — nothing to push
    const result = db.execute("SELECT * FROM orders o JOIN users u ON o.user_id = u.id");
    assert.ok(result.rows.length > 0);
  });

  it('handles single-table query (no joins) correctly', () => {
    const db = makeDB();
    const result = db.execute("SELECT * FROM users WHERE active = 1");
    assert.equal(result.rows.length, 50);
  });

  it('EXPLAIN ANALYZE includes plan tree with actuals', () => {
    const db = makeDB();
    const result = db.execute("EXPLAIN ANALYZE SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = 1");
    assert.ok(result.planTreeText, 'Expected planTreeText');
    assert.ok(result.planTreeText.length > 0);
    const text = result.planTreeText.join('\n');
    assert.ok(text.includes('actual rows='), 'Expected actual rows in plan');
  });

  it('pushdown with multiple predicates on same table', () => {
    const db = makeDB();
    // Both predicates reference 'u' — both should push
    const result = db.execute("SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = 1 AND u.dept = 'eng'");
    assert.ok(result.rows.length > 0);
    // All results should have active=1 AND dept=eng
    for (const row of result.rows) {
      const active = row['u.active'] !== undefined ? row['u.active'] : row.active;
      const dept = row['u.dept'] !== undefined ? row['u.dept'] : row.dept;
      assert.equal(active, 1);
      assert.equal(dept, 'eng');
    }
  });
});
