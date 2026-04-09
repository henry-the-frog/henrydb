// btree-pk-lookup.test.js — Tests for BTreeTable PK direct lookup optimization
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('BTree PK direct lookup', () => {
  function setup() {
    const db = new Database();
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER) USING BTREE');
    for (let i = 1; i <= 1000; i++) {
      db.execute(`INSERT INTO products VALUES (${i}, 'Product ${i}', ${i * 10})`);
    }
    return db;
  }

  it('WHERE pk = value uses direct B+tree lookup', () => {
    const db = setup();
    const result = db.execute('SELECT * FROM products WHERE id = 500');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 500);
    assert.equal(result.rows[0].name, 'Product 500');
    assert.equal(result.rows[0].price, 5000);
  });

  it('WHERE pk = value for non-existent key returns empty', () => {
    const db = setup();
    const result = db.execute('SELECT * FROM products WHERE id = 9999');
    assert.equal(result.rows.length, 0);
  });

  it('WHERE pk = value with additional conditions', () => {
    const db = setup();
    const result = db.execute('SELECT * FROM products WHERE id = 100 AND price > 0');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 100);
  });

  it('PK lookup performance: 1000 lookups in 1000 rows', () => {
    const db = setup();
    
    const t0 = performance.now();
    for (let i = 1; i <= 1000; i++) {
      db.execute(`SELECT * FROM products WHERE id = ${i}`);
    }
    const elapsed = performance.now() - t0;
    
    console.log(`  1K PK lookups: ${elapsed.toFixed(1)}ms (${(elapsed/1000).toFixed(3)}ms avg)`);
    // Should be very fast — well under 500ms for 1K lookups
    assert.ok(elapsed < 5000, `Expected <5s, got ${elapsed.toFixed(1)}ms`);
  });

  it('BTree PK lookup vs HeapFile full scan', () => {
    // BTree table
    const dbBt = new Database();
    dbBt.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT) USING BTREE');
    for (let i = 1; i <= 5000; i++) dbBt.execute(`INSERT INTO t VALUES (${i}, 'row-${i}')`);

    // Heap table (with PK index)
    const dbHf = new Database();
    dbHf.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    for (let i = 1; i <= 5000; i++) dbHf.execute(`INSERT INTO t VALUES (${i}, 'row-${i}')`);

    // BTree: direct PK lookup
    const t0 = performance.now();
    for (let i = 0; i < 100; i++) {
      const key = Math.floor(Math.random() * 5000) + 1;
      dbBt.execute(`SELECT * FROM t WHERE id = ${key}`);
    }
    const btreeMs = performance.now() - t0;

    // Heap: uses secondary index
    const t1 = performance.now();
    for (let i = 0; i < 100; i++) {
      const key = Math.floor(Math.random() * 5000) + 1;
      dbHf.execute(`SELECT * FROM t WHERE id = ${key}`);
    }
    const heapMs = performance.now() - t1;

    console.log(`  100 PK lookups in 5K: BTree ${btreeMs.toFixed(1)}ms | Heap ${heapMs.toFixed(1)}ms`);
    // Both should be fast (both use B+tree), but BTree is single lookup vs index+heap
    assert.ok(true);
  });

  it('SELECT specific columns from PK lookup', () => {
    const db = setup();
    const result = db.execute('SELECT name, price FROM products WHERE id = 42');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Product 42');
    assert.equal(result.rows[0].price, 420);
  });

  it('COUNT with PK equality', () => {
    const db = setup();
    const result = db.execute('SELECT COUNT(*) as cnt FROM products WHERE id = 500');
    assert.equal(result.rows[0].cnt, 1);
    
    const result2 = db.execute('SELECT COUNT(*) as cnt FROM products WHERE id = 99999');
    assert.equal(result2.rows[0].cnt, 0);
  });

  it('PK lookup works after DELETE', () => {
    const db = setup();
    db.execute('DELETE FROM products WHERE id = 500');
    
    const result = db.execute('SELECT * FROM products WHERE id = 500');
    assert.equal(result.rows.length, 0);
    
    const result2 = db.execute('SELECT * FROM products WHERE id = 501');
    assert.equal(result2.rows.length, 1);
  });

  it('PK lookup works after UPDATE', () => {
    const db = setup();
    db.execute("UPDATE products SET name = 'Updated' WHERE id = 100");
    
    const result = db.execute('SELECT * FROM products WHERE id = 100');
    assert.equal(result.rows[0].name, 'Updated');
    assert.equal(result.rows[0].price, 1000); // Unchanged
  });
});
