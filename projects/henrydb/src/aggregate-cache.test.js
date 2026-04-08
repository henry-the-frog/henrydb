// aggregate-cache.test.js — Tests for server-side aggregate result cache
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { AggregateCache, CachingDatabase } from './aggregate-cache.js';

let db;

describe('AggregateCache', () => {
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount INTEGER, region TEXT)');
    db.execute("INSERT INTO sales VALUES (1, 'Widget', 100, 'East')");
    db.execute("INSERT INTO sales VALUES (2, 'Gadget', 200, 'West')");
    db.execute("INSERT INTO sales VALUES (3, 'Widget', 150, 'East')");
    db.execute("INSERT INTO sales VALUES (4, 'Gadget', 300, 'East')");
    db.execute("INSERT INTO sales VALUES (5, 'Doohickey', 50, 'West')");
  });

  test('basic cache hit/miss', () => {
    const cache = new AggregateCache();
    const result = { rows: [{ total: 800 }] };
    
    assert.equal(cache.get('SELECT SUM(amount) FROM sales').hit, false);
    cache.put('SELECT SUM(amount) FROM sales', result, ['sales']);
    
    const cached = cache.get('SELECT SUM(amount) FROM sales');
    assert.equal(cached.hit, true);
    assert.deepEqual(cached.result, result);
  });

  test('table-level invalidation', () => {
    const cache = new AggregateCache();
    cache.put('SELECT SUM(amount) FROM sales', { rows: [{ total: 800 }] }, ['sales']);
    cache.put('SELECT COUNT(*) FROM sales', { rows: [{ cnt: 5 }] }, ['sales']);
    
    assert.equal(cache.get('SELECT SUM(amount) FROM sales').hit, true);
    
    const invalidated = cache.invalidateTable('sales');
    assert.equal(invalidated, 2);
    
    assert.equal(cache.get('SELECT SUM(amount) FROM sales').hit, false);
    assert.equal(cache.get('SELECT COUNT(*) FROM sales').hit, false);
  });

  test('TTL expiration', async () => {
    const cache = new AggregateCache({ maxAgeMs: 50 });
    cache.put('SELECT 1', { rows: [{ x: 1 }] }, []);
    assert.equal(cache.get('SELECT 1').hit, true);
    
    await new Promise(r => setTimeout(r, 60));
    assert.equal(cache.get('SELECT 1').hit, false);
  });

  test('LRU eviction at capacity', async () => {
    const cache = new AggregateCache({ maxEntries: 3 });
    cache.put('q1', { rows: [] }, ['t1']);
    await new Promise(r => setTimeout(r, 5));
    cache.put('q2', { rows: [] }, ['t2']);
    await new Promise(r => setTimeout(r, 5));
    cache.put('q3', { rows: [] }, ['t3']);
    
    // Access q1 to make it recently used
    cache.get('q1');
    
    // Adding q4 should evict q2 (least recently used — q3 was put more recently than q2)
    cache.put('q4', { rows: [] }, ['t4']);
    
    assert.equal(cache.get('q1').hit, true);
    assert.equal(cache.get('q2').hit, false); // evicted
    assert.equal(cache.get('q4').hit, true);
  });

  test('isAggregateQuery detection', () => {
    const cache = new AggregateCache();
    assert.equal(cache.isAggregateQuery('SELECT product, SUM(amount) FROM sales GROUP BY product'), true);
    assert.equal(cache.isAggregateQuery('SELECT COUNT(*) FROM sales'), true);
    assert.equal(cache.isAggregateQuery('SELECT AVG(amount) FROM sales'), true);
    assert.equal(cache.isAggregateQuery('SELECT * FROM sales'), false);
    assert.equal(cache.isAggregateQuery('SELECT id, product FROM sales WHERE id = 1'), false);
  });

  test('extractTables from SQL', () => {
    const cache = new AggregateCache();
    assert.deepEqual(cache.extractTables('SELECT * FROM sales'), ['sales']);
    assert.deepEqual(cache.extractTables('SELECT * FROM sales JOIN products ON sales.pid = products.id'), ['sales', 'products']);
    assert.deepEqual(cache.extractTables('SELECT * FROM sales s LEFT JOIN orders o ON s.id = o.sid'), ['sales', 'orders']);
  });

  test('SQL normalization (whitespace, case)', () => {
    const cache = new AggregateCache();
    cache.put('SELECT  SUM(amount)  FROM  sales', { rows: [{ total: 800 }] }, ['sales']);
    assert.equal(cache.get('select sum(amount) from sales').hit, true);
    assert.equal(cache.get('SELECT SUM(amount) FROM sales').hit, true);
  });

  test('invalidateAll clears everything', () => {
    const cache = new AggregateCache();
    cache.put('q1', { rows: [] }, ['t1']);
    cache.put('q2', { rows: [] }, ['t2']);
    cache.put('q3', { rows: [] }, ['t3']);
    
    const count = cache.invalidateAll();
    assert.equal(count, 3);
    assert.equal(cache.getStats().entries, 0);
  });

  test('stats tracking', () => {
    const cache = new AggregateCache();
    cache.put('q1', { rows: [] }, ['t1']);
    cache.get('q1'); // hit
    cache.get('q1'); // hit
    cache.get('q2'); // miss
    cache.invalidateTable('t1');
    
    const stats = cache.getStats();
    assert.equal(stats.hits, 2);
    assert.equal(stats.misses, 1);
    assert.equal(stats.invalidations, 1);
    assert.equal(stats.hitRate, 66.7);
  });

  test('multi-table dependency: invalidating one table clears joint queries', () => {
    const cache = new AggregateCache();
    cache.put('SELECT COUNT(*) FROM sales JOIN products', { rows: [{ cnt: 10 }] }, ['sales', 'products']);
    
    // Invalidating either table should clear the cache
    assert.equal(cache.get('SELECT COUNT(*) FROM sales JOIN products').hit, true);
    cache.invalidateTable('products');
    assert.equal(cache.get('SELECT COUNT(*) FROM sales JOIN products').hit, false);
  });
});

describe('CachingDatabase', () => {
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)');
    db.execute("INSERT INTO items VALUES (1, 'A', 10)");
    db.execute("INSERT INTO items VALUES (2, 'B', 20)");
    db.execute("INSERT INTO items VALUES (3, 'C', 30)");
  });

  test('caches aggregate queries automatically', () => {
    const cdb = new CachingDatabase(db);
    
    // First call: miss
    const r1 = cdb.execute('SELECT SUM(price) as total FROM items');
    assert.equal(cdb.cache.getStats().misses, 1);
    
    // Second call: hit
    const r2 = cdb.execute('SELECT SUM(price) as total FROM items');
    assert.equal(cdb.cache.getStats().hits, 1);
    assert.deepEqual(r1, r2);
  });

  test('invalidates on INSERT', () => {
    const cdb = new CachingDatabase(db);
    cdb.execute('SELECT COUNT(*) as cnt FROM items');
    assert.equal(cdb.cache.getStats().misses, 1);
    
    cdb.execute("INSERT INTO items VALUES (4, 'D', 40)");
    
    // Cache should be invalidated
    const r = cdb.execute('SELECT COUNT(*) as cnt FROM items');
    assert.equal(cdb.cache.getStats().misses, 2);
    assert.equal(r.rows[0].cnt, 4);
  });

  test('invalidates on UPDATE', () => {
    const cdb = new CachingDatabase(db);
    cdb.execute('SELECT AVG(price) as avg FROM items');
    
    cdb.execute("UPDATE items SET price = 100 WHERE id = 1");
    
    const r = cdb.execute('SELECT AVG(price) as avg FROM items');
    assert.equal(cdb.cache.getStats().hits, 0); // should be a miss after invalidation
  });

  test('non-aggregate queries bypass cache', () => {
    const cdb = new CachingDatabase(db);
    cdb.execute('SELECT * FROM items');
    cdb.execute('SELECT * FROM items');
    
    assert.equal(cdb.cache.getStats().hits, 0);
    assert.equal(cdb.cache.getStats().misses, 0);
  });
});
