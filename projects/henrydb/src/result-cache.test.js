// result-cache.test.js — Tests for query result caching with table-based invalidation
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Result Cache', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, 'item${i}', ${i * 1.5})`);
    }
    // Reset cache stats after setup
    db._resultCacheHits = 0;
    db._resultCacheMisses = 0;
    db._resultCache.clear();
  });

  it('second identical query is a cache hit', () => {
    db.execute('SELECT * FROM items WHERE id = 50');
    assert.equal(db._resultCacheHits, 0);
    assert.equal(db._resultCacheMisses, 1);

    db.execute('SELECT * FROM items WHERE id = 50');
    assert.equal(db._resultCacheHits, 1);
    assert.equal(db._resultCacheMisses, 1);
  });

  it('different queries are cache misses', () => {
    db.execute('SELECT * FROM items WHERE id = 50');
    db.execute('SELECT * FROM items WHERE id = 51');
    assert.equal(db._resultCacheHits, 0);
    assert.equal(db._resultCacheMisses, 2);
  });

  it('cached result matches fresh result', () => {
    const r1 = db.execute('SELECT * FROM items WHERE id = 50');
    const r2 = db.execute('SELECT * FROM items WHERE id = 50');
    assert.deepEqual(r1, r2);
  });

  it('INSERT invalidates cache', () => {
    db.execute('SELECT * FROM items WHERE id = 50');
    assert.equal(db._resultCache.size, 1);

    db.execute("INSERT INTO items VALUES (200, 'new', 99.99)");
    assert.equal(db._resultCache.size, 0);
  });

  it('UPDATE invalidates cache', () => {
    db.execute('SELECT * FROM items WHERE id = 50');
    assert.equal(db._resultCache.size, 1);

    db.execute("UPDATE items SET name = 'updated' WHERE id = 50");
    assert.equal(db._resultCache.size, 0);
  });

  it('DELETE invalidates cache', () => {
    db.execute('SELECT * FROM items WHERE id = 50');
    assert.equal(db._resultCache.size, 1);

    db.execute('DELETE FROM items WHERE id = 50');
    assert.equal(db._resultCache.size, 0);
  });

  it('write to different table does not invalidate', () => {
    db.execute('CREATE TABLE other (id INTEGER PRIMARY KEY)');
    db.execute('SELECT * FROM items WHERE id = 50');
    assert.equal(db._resultCache.size, 1);

    db.execute('INSERT INTO other VALUES (1)');
    // items cache should still be valid
    assert.equal(db._resultCache.size, 1);
  });

  it('EXPLAIN is not cached', () => {
    db.execute('EXPLAIN SELECT * FROM items WHERE id = 50');
    assert.equal(db._resultCache.size, 0);
  });

  it('cache respects max size', () => {
    db._resultCacheMaxSize = 5;
    for (let i = 1; i <= 10; i++) {
      db.execute(`SELECT * FROM items WHERE id = ${i}`);
    }
    assert.ok(db._resultCache.size <= 5, `Cache size should be <= 5: ${db._resultCache.size}`);
  });

  it('correctness after invalidation + re-query', () => {
    const r1 = db.execute("SELECT name FROM items WHERE id = 1");
    assert.equal(r1.rows[0].name, 'item1');

    db.execute("UPDATE items SET name = 'changed' WHERE id = 1");

    const r2 = db.execute("SELECT name FROM items WHERE id = 1");
    assert.equal(r2.rows[0].name, 'changed');
  });
});
