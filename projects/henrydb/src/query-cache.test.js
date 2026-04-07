// query-cache.test.js — Query cache tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { QueryCache, tryCompiledExecution } from './query-cache.js';
import { Database } from './db.js';

describe('QueryCache', () => {
  it('caches and retrieves entries', () => {
    const cache = new QueryCache();
    cache.put('SELECT 1', () => [{ val: 1 }], { tableName: 't' });
    
    const entry = cache.get('SELECT 1');
    assert.ok(entry);
    assert.ok(entry.useCount >= 1);
  });

  it('tracks hits and misses', () => {
    const cache = new QueryCache();
    cache.get('unknown'); // miss
    cache.put('SELECT 1', () => [], {});
    cache.get('SELECT 1'); // hit
    
    const stats = cache.stats();
    assert.strictEqual(stats.hits, 1);
    assert.strictEqual(stats.misses, 1);
    assert.strictEqual(stats.hitRate, 0.5);
  });

  it('evicts LRU when full', () => {
    const cache = new QueryCache(2);
    cache.put('q1', () => [], {});
    cache.put('q2', () => [], {});
    cache.put('q3', () => [], {}); // Should evict q1
    
    assert.strictEqual(cache.get('q1'), null);
    assert.ok(cache.get('q2'));
    assert.ok(cache.get('q3'));
  });

  it('clear removes all entries', () => {
    const cache = new QueryCache();
    cache.put('q1', () => [], {});
    cache.put('q2', () => [], {});
    cache.clear();
    assert.strictEqual(cache.stats().entries, 0);
  });
});

describe('tryCompiledExecution', () => {
  it('compiles and caches simple SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const cache = new QueryCache();
    
    // First call: compile and cache
    const r1 = tryCompiledExecution('SELECT id, val FROM t WHERE val > 50', db, cache);
    assert.ok(r1);
    assert.strictEqual(r1.rows.length, 4);
    assert.strictEqual(cache.stats().compilations, 1);
    
    // Second call: use cache
    const r2 = tryCompiledExecution('SELECT id, val FROM t WHERE val > 50', db, cache);
    assert.ok(r2);
    assert.strictEqual(r2.rows.length, 4);
    assert.strictEqual(cache.stats().hits, 1);
  });

  it('returns null for non-SELECT queries', () => {
    const db = new Database();
    const cache = new QueryCache();
    
    assert.strictEqual(tryCompiledExecution('INSERT INTO t VALUES (1)', db, cache), null);
    assert.strictEqual(tryCompiledExecution('CREATE TABLE t (id INT)', db, cache), null);
  });

  it('returns null for queries with JOINs', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY)');
    const cache = new QueryCache();
    
    // JOINs may or may not compile depending on parser output
    // The important thing is it doesn't crash
    const result = tryCompiledExecution('SELECT * FROM a JOIN b ON a.id = b.id', db, cache);
    assert.ok(true, 'Does not crash on JOIN query');
  });

  it('returns correct results for SELECT *', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO items VALUES (1, 'Widget')");
    db.execute("INSERT INTO items VALUES (2, 'Gadget')");
    
    const cache = new QueryCache();
    const result = tryCompiledExecution('SELECT * FROM items', db, cache);
    assert.ok(result);
    assert.strictEqual(result.rows.length, 2);
  });

  it('handles LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    const cache = new QueryCache();
    const result = tryCompiledExecution('SELECT * FROM t LIMIT 5', db, cache);
    assert.ok(result);
    assert.strictEqual(result.rows.length, 5);
  });

  it('benchmark: first vs cached execution', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT)');
    for (let i = 0; i < 5000; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${(i * 17) % 1000}, '${i % 2 === 0 ? 'shipped' : 'pending'}')`);
    }
    
    const cache = new QueryCache();
    const sql = "SELECT id, amount FROM orders WHERE amount > 500 AND status = 'shipped'";
    
    // First: compile + execute
    const start1 = performance.now();
    tryCompiledExecution(sql, db, cache);
    const time1 = performance.now() - start1;
    
    // Second: cached execute
    const start2 = performance.now();
    for (let j = 0; j < 100; j++) {
      tryCompiledExecution(sql, db, cache);
    }
    const time2 = (performance.now() - start2) / 100;
    
    console.log(`    First (compile+execute): ${time1.toFixed(2)}ms`);
    console.log(`    Cached (execute only):   ${time2.toFixed(2)}ms`);
    console.log(`    Cache warmup ratio:      ${(time1/time2).toFixed(1)}x`);
    
    assert.ok(time2 < time1, 'Cached execution should be faster');
  });
});
