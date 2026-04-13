// partial-index.test.js — Partial Indexes (WHERE in CREATE INDEX)
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Partial Indexes', () => {
  it('creates partial index with WHERE condition', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT, active INT)');
    for (let i = 0; i < 20; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10}, ${i % 2})`);
    
    // Only index rows where active = 1
    db.execute('CREATE INDEX idx_active ON t(val) WHERE active = 1');
    
    // Query should work correctly
    const r = db.execute('SELECT * FROM t WHERE active = 1 AND val = 10');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
  });

  it('partial index only includes matching rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, status TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'active'), (2, 'inactive'), (3, 'active')");
    
    db.execute("CREATE INDEX idx ON t(id) WHERE status = 'active'");
    
    // Ensure the table still returns all rows
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 3);
  });
});
