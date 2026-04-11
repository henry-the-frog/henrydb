// upsert.test.js — Tests for INSERT ON CONFLICT (UPSERT)
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('UPSERT (INSERT ON CONFLICT)', () => {

  it('ON CONFLICT DO UPDATE SET with EXCLUDED', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    db.execute("INSERT INTO t VALUES (1, 10)");
    
    db.execute("INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
    
    const result = db.execute("SELECT * FROM t ORDER BY id");
    assert.equal(result.rows.length, 1, 'Should still have 1 row');
    assert.equal(result.rows[0].val, 99, 'Value should be updated to 99');
  });

  it('ON CONFLICT DO NOTHING', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    db.execute("INSERT INTO t VALUES (1, 10)");
    
    db.execute("INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO NOTHING");
    
    const result = db.execute("SELECT * FROM t ORDER BY id");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].val, 10, 'Value should remain 10');
  });

  it('ON CONFLICT with literal SET value', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    db.execute("INSERT INTO t VALUES (1, 10)");
    
    db.execute("INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET val = 42");
    
    const result = db.execute("SELECT * FROM t WHERE id = 1");
    assert.equal(result.rows[0].val, 42);
  });

  it('ON CONFLICT inserts when no conflict', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    db.execute("INSERT INTO t VALUES (1, 10)");
    
    db.execute("INSERT INTO t VALUES (2, 20) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
    
    const result = db.execute("SELECT * FROM t ORDER BY id");
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[1].val, 20);
  });

  it('multiple column UPSERT', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)");
    db.execute("INSERT INTO t VALUES (1, 'Alice', 80)");
    
    db.execute("INSERT INTO t VALUES (1, 'Updated', 95) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score");
    
    const result = db.execute("SELECT * FROM t WHERE id = 1");
    assert.equal(result.rows[0].name, 'Updated');
    assert.equal(result.rows[0].score, 95);
  });

  it('UPSERT with expression (val + EXCLUDED.val)', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    db.execute("INSERT INTO t VALUES (1, 10)");
    
    db.execute("INSERT INTO t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET val = val + EXCLUDED.val");
    
    const result = db.execute("SELECT * FROM t WHERE id = 1");
    assert.equal(result.rows[0].val, 15, '10 + 5 = 15');
  });

  it('UPSERT RETURNING', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    db.execute("INSERT INTO t VALUES (1, 10)");
    
    const result = db.execute("INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val RETURNING *");
    assert.equal(result.rows[0].val, 99);
  });
});
