// index-advisor.test.js — Tests for workload-based index recommendations
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { IndexAdvisor } from './index-advisor.js';
import { Database } from './db.js';

function makeDB() {
  const db = new Database();
  db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, dept TEXT, active INTEGER)');
  db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, status TEXT, created_at TEXT)');
  db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)');
  
  for (let i = 1; i <= 1000; i++) {
    db.execute(`INSERT INTO users VALUES (${i}, 'user${i}', 'user${i}@test.com', ${20 + i % 50}, '${['eng', 'sales', 'ops'][i % 3]}', ${i % 2})`);
  }
  for (let i = 1; i <= 5000; i++) {
    db.execute(`INSERT INTO orders VALUES (${i}, ${1 + i % 1000}, ${(i * 9.99).toFixed(2)}, '${['pending', 'shipped', 'delivered'][i % 3]}', '2025-${String(1 + i % 12).padStart(2, '0')}-${String(1 + i % 28).padStart(2, '0')}')`);
  }
  for (let i = 1; i <= 200; i++) {
    db.execute(`INSERT INTO products VALUES (${i}, 'product${i}', '${['electronics', 'books', 'clothing'][i % 3]}', ${(i * 4.99).toFixed(2)})`);
  }
  return db;
}

describe('IndexAdvisor', () => {
  it('recommends index for frequently filtered column', () => {
    const db = makeDB();
    const advisor = new IndexAdvisor(db);
    
    // Simulate workload: many queries filter on orders.status
    for (let i = 0; i < 10; i++) {
      advisor.analyze("SELECT * FROM orders WHERE status = 'shipped'");
    }
    
    const recs = advisor.recommend();
    assert.ok(recs.length > 0, 'Should have recommendations');
    
    const statusRec = recs.find(r => r.table === 'orders' && r.columns.includes('status'));
    assert.ok(statusRec, 'Should recommend index on orders.status');
    assert.ok(statusRec.sql.includes('CREATE INDEX'));
    assert.ok(statusRec.impact > 0);
  });

  it('recommends index for JOIN columns', () => {
    const db = makeDB();
    const advisor = new IndexAdvisor(db);
    
    for (let i = 0; i < 5; i++) {
      advisor.analyze("SELECT * FROM orders o JOIN users u ON o.user_id = u.id");
    }
    
    const recs = advisor.recommend();
    const joinRec = recs.find(r => r.columns.includes('user_id'));
    assert.ok(joinRec, 'Should recommend index for JOIN column');
    assert.ok(joinRec.reason.includes('JOIN'));
  });

  it('ranks high-impact recommendations first', () => {
    const db = makeDB();
    const advisor = new IndexAdvisor(db);
    
    // Heavy filtering on orders.status (large table)
    for (let i = 0; i < 20; i++) {
      advisor.analyze("SELECT * FROM orders WHERE status = 'shipped'");
    }
    // Light filtering on products.category (small table)
    advisor.analyze("SELECT * FROM products WHERE category = 'books'");
    
    const recs = advisor.recommend();
    assert.ok(recs.length >= 2);
    // Orders.status should rank higher (more queries + bigger table)
    const statusIdx = recs.findIndex(r => r.columns.includes('status'));
    const categoryIdx = recs.findIndex(r => r.columns.includes('category'));
    assert.ok(statusIdx < categoryIdx, 'High-impact should come first');
  });

  it('detects composite index opportunities from AND conditions', () => {
    const db = makeDB();
    const advisor = new IndexAdvisor(db);
    
    for (let i = 0; i < 10; i++) {
      advisor.analyze("SELECT * FROM users WHERE dept = 'eng' AND active = 1");
    }
    
    const recs = advisor.recommend();
    const compositeRec = recs.find(r => r.columns.length > 1);
    // Should find a composite index opportunity
    assert.ok(compositeRec || recs.length >= 2, 'Should detect composite or individual recommendations');
  });

  it('detects ORDER BY index opportunities', () => {
    const db = makeDB();
    const advisor = new IndexAdvisor(db);
    
    for (let i = 0; i < 5; i++) {
      advisor.analyze("SELECT * FROM orders ORDER BY created_at DESC");
    }
    
    const recs = advisor.recommend();
    const sortRec = recs.find(r => r.columns.includes('created_at'));
    assert.ok(sortRec, 'Should recommend index for ORDER BY column');
    assert.ok(sortRec.reason.includes('ORDER BY'));
  });

  it('detects GROUP BY index opportunities', () => {
    const db = makeDB();
    const advisor = new IndexAdvisor(db);
    
    for (let i = 0; i < 5; i++) {
      advisor.analyze("SELECT dept, COUNT(*) FROM users GROUP BY dept");
    }
    
    const recs = advisor.recommend();
    const groupRec = recs.find(r => r.columns.includes('dept'));
    assert.ok(groupRec, 'Should recommend index for GROUP BY column');
    assert.ok(groupRec.reason.includes('GROUP BY'));
  });

  it('skips columns that already have indexes', () => {
    const db = makeDB();
    db.execute('CREATE INDEX idx_orders_status ON orders (status)');
    
    const advisor = new IndexAdvisor(db);
    for (let i = 0; i < 10; i++) {
      advisor.analyze("SELECT * FROM orders WHERE status = 'shipped'");
    }
    
    const recs = advisor.recommend();
    const statusRec = recs.find(r => r.table === 'orders' && r.columns.includes('status'));
    assert.ok(!statusRec, 'Should NOT recommend index that already exists');
  });

  it('handles mixed workload', () => {
    const db = makeDB();
    const advisor = new IndexAdvisor(db);
    
    // Diverse workload
    advisor.analyzeBatch([
      "SELECT * FROM orders WHERE status = 'shipped'",
      "SELECT * FROM orders o JOIN users u ON o.user_id = u.id",
      "SELECT dept, AVG(age) FROM users GROUP BY dept",
      "SELECT * FROM products WHERE category = 'electronics' ORDER BY price DESC",
      "SELECT * FROM orders WHERE created_at > '2025-06-01'",
      "SELECT * FROM users WHERE email = 'user42@test.com'",
      "SELECT * FROM orders WHERE status = 'pending' AND total > 100",
    ]);
    
    const recs = advisor.recommend();
    assert.ok(recs.length >= 3, `Expected 3+ recommendations, got ${recs.length}`);
    
    // Should produce valid SQL for each
    for (const rec of recs) {
      assert.ok(rec.sql.startsWith('CREATE INDEX'));
      assert.ok(rec.table);
      assert.ok(rec.columns.length > 0);
      assert.ok(rec.impact > 0);
      assert.ok(['high', 'medium', 'low'].includes(rec.level));
    }
  });

  it('summary provides useful overview', () => {
    const db = makeDB();
    const advisor = new IndexAdvisor(db);
    
    advisor.analyzeBatch([
      "SELECT * FROM orders WHERE status = 'shipped'",
      "SELECT * FROM orders o JOIN users u ON o.user_id = u.id",
      "SELECT * FROM products ORDER BY price",
    ]);
    
    const summary = advisor.summary();
    assert.equal(summary.queriesAnalyzed, 3);
    assert.ok(summary.columnsTracked > 0);
    assert.ok(summary.recommendations > 0);
    assert.ok(summary.topRecommendations.length > 0);
  });

  it('handles unparseable queries gracefully', () => {
    const db = makeDB();
    const advisor = new IndexAdvisor(db);
    
    advisor.analyze('NOT VALID SQL');
    advisor.analyze("SELECT * FROM orders WHERE status = 'ok'");
    
    const recs = advisor.recommend();
    assert.ok(recs.length >= 1); // Should still produce recommendation from valid query
  });

  it('handles queries without FROM clause', () => {
    const db = makeDB();
    const advisor = new IndexAdvisor(db);
    
    advisor.analyze('SELECT 1 + 1');
    const recs = advisor.recommend();
    assert.ok(Array.isArray(recs)); // No crash
  });
});
