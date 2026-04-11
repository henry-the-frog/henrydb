// apply-indexes.test.js — Tests for APPLY RECOMMENDED INDEXES command
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('APPLY RECOMMENDED INDEXES', () => {
  it('creates recommended indexes', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, total REAL)');
    for (let i = 1; i <= 500; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, '${i % 3 === 0 ? "shipped" : "pending"}', ${i * 1.5})`);
    }
    
    // Build workload
    for (let i = 0; i < 10; i++) {
      db.execute("SELECT * FROM orders WHERE status = 'shipped'");
    }
    
    // Check recommendations exist
    const recs = db.execute('RECOMMEND INDEXES');
    assert.ok(recs.rows.some(r => r.columns === 'status'), 'Should have status recommendation');
    
    // Apply
    const result = db.execute('APPLY RECOMMENDED INDEXES');
    assert.ok(result.rows.length > 0);
    assert.ok(result.rows.some(r => r.status === 'created'), 'Should create at least one index');
    assert.ok(result.message.includes('Applied'));
    
    // After applying, recommendations should be gone (indexes now exist)
    const recsAfter = db.execute('RECOMMEND INDEXES');
    const statusRec = recsAfter.rows.find(r => r.columns === 'status');
    assert.ok(!statusRec, 'Status recommendation should be gone after applying');
  });

  it('reports no recommendations when none qualify', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    
    const result = db.execute('APPLY RECOMMENDED INDEXES');
    assert.ok(result.message.includes('No'));
    assert.equal(result.rows.length, 0);
  });

  it('handles already-existing index gracefully', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    
    // Build workload
    for (let i = 0; i < 10; i++) db.execute("SELECT * FROM t WHERE val = 'v50'");
    
    // Create the index manually first
    db.execute('CREATE INDEX idx_t_val ON t (val)');
    
    // Apply should find no recommendations (index already exists)
    const result = db.execute('APPLY RECOMMENDED INDEXES');
    assert.equal(result.rows.length, 0);
  });

  it('creates multiple indexes for mixed workload', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, dept TEXT)');
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, status TEXT)');
    for (let i = 1; i <= 200; i++) db.execute(`INSERT INTO users VALUES (${i}, 'u${i}@test.com', '${['eng', 'sales'][i % 2]}')`);
    for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO orders VALUES (${i}, ${1 + i % 200}, '${['pending', 'shipped'][i % 2]}')`);
    
    // Diverse workload
    for (let i = 0; i < 5; i++) {
      db.execute("SELECT * FROM orders WHERE status = 'shipped'");
      db.execute("SELECT * FROM users WHERE dept = 'eng'");
      db.execute("SELECT * FROM orders o JOIN users u ON o.user_id = u.id");
    }
    
    const result = db.execute('APPLY RECOMMENDED INDEXES');
    const created = result.rows.filter(r => r.status === 'created');
    assert.ok(created.length >= 1, `Expected at least 1 index created, got ${created.length}`);
  });

  it('case insensitive command', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    
    const r1 = db.execute('APPLY RECOMMENDED INDEXES');
    const r2 = db.execute('apply recommended indexes');
    assert.ok(r1.message);
    assert.ok(r2.message);
  });
});
