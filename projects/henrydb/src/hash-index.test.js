// hash-index.test.js — Integration tests for HASH index type
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('HASH index integration', () => {
  it('CREATE INDEX ... USING HASH', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')");
    
    db.execute('CREATE INDEX idx_email ON users USING HASH (email)');
    
    const result = db.execute("SELECT * FROM users WHERE email = 'bob@example.com'");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Bob');
  });

  it('HASH index for equality lookup on non-PK column', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price INTEGER)');
    for (let i = 1; i <= 100; i++) {
      const cat = ['electronics', 'books', 'clothing'][i % 3];
      db.execute(`INSERT INTO products VALUES (${i}, '${cat}', ${i * 10})`);
    }
    
    db.execute('CREATE INDEX idx_cat ON products USING HASH (category)');
    
    const result = db.execute("SELECT COUNT(*) as cnt FROM products WHERE category = 'electronics'");
    assert.ok(result.rows[0].cnt > 0);
  });

  it('HASH index on integer column', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (id INTEGER PRIMARY KEY, user_id INTEGER, score INTEGER)');
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO scores VALUES (${i}, ${i % 10}, ${i * 5})`);
    }
    
    db.execute('CREATE INDEX idx_uid ON scores USING HASH (user_id)');
    
    const result = db.execute('SELECT * FROM scores WHERE user_id = 5');
    assert.ok(result.rows.length > 0);
    for (const row of result.rows) {
      assert.equal(row.user_id, 5);
    }
  });

  it('default CREATE INDEX uses BTREE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    
    db.execute('CREATE INDEX idx_val ON t (val)');
    
    const table = db.tables.get('t');
    const index = table.indexes.get('val');
    assert.ok(!index._isHash, 'Default should be B+tree, not hash');
  });

  it('HASH index with INSERT after index creation', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, tag TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'alpha')");
    
    db.execute('CREATE INDEX idx_tag ON t USING HASH (tag)');
    
    // Insert more rows after index creation
    db.execute("INSERT INTO t VALUES (2, 'beta')");
    db.execute("INSERT INTO t VALUES (3, 'alpha')");
    
    const result = db.execute("SELECT * FROM t WHERE tag = 'alpha'");
    assert.equal(result.rows.length, 2);
  });

  it('HASH and BTREE indexes coexist on same table', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price INTEGER)');
    
    for (let i = 1; i <= 20; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, 'item-${i}', 'cat-${i % 5}', ${i * 10})`);
    }
    
    db.execute('CREATE INDEX idx_name ON items (name)');           // B+tree
    db.execute('CREATE INDEX idx_cat ON items USING HASH (category)'); // Hash
    
    const r1 = db.execute("SELECT * FROM items WHERE name = 'item-5'");
    assert.equal(r1.rows.length, 1);
    
    const r2 = db.execute("SELECT * FROM items WHERE category = 'cat-0'");
    assert.ok(r2.rows.length > 0);
  });

  it('HASH index performance: 10K rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE perf (id INTEGER PRIMARY KEY, code TEXT)');
    
    for (let i = 1; i <= 10000; i++) {
      db.execute(`INSERT INTO perf VALUES (${i}, 'code-${i}')`);
    }
    
    db.execute('CREATE INDEX idx_code ON perf USING HASH (code)');
    
    // 1000 lookups
    const t0 = performance.now();
    for (let i = 1; i <= 1000; i++) {
      const code = `code-${Math.floor(Math.random() * 10000) + 1}`;
      db.execute(`SELECT * FROM perf WHERE code = '${code}'`);
    }
    const elapsed = performance.now() - t0;
    
    console.log(`  1K hash lookups in 10K rows: ${elapsed.toFixed(1)}ms (${(elapsed/1000).toFixed(3)}ms avg)`);
    assert.ok(elapsed < 5000);
  });

  it('HASH index with DELETE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'active')");
    db.execute("INSERT INTO t VALUES (2, 'inactive')");
    db.execute("INSERT INTO t VALUES (3, 'active')");
    
    db.execute('CREATE INDEX idx_status ON t USING HASH (status)');
    
    db.execute('DELETE FROM t WHERE id = 1');
    
    const result = db.execute("SELECT * FROM t WHERE status = 'active'");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 3);
  });

  it('EXPLAIN shows HASH index usage', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, tag TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'x')");
    db.execute('CREATE INDEX idx_tag ON t USING HASH (tag)');
    
    const result = db.execute("EXPLAIN SELECT * FROM t WHERE tag = 'x'");
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('Index Scan'), `Expected Index Scan in: ${plan}`);
  });
});
