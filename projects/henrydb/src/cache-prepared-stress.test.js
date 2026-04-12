// cache-prepared-stress.test.js — Stress test query cache and prepared statements
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { QueryCache } from './query-cache.js';
import { Database } from './db.js';

describe('QueryCache stress', () => {
  it('invalidation: INSERT clears cached SELECT', () => {
    const cache = new QueryCache();
    const key = 'SELECT * FROM users';
    cache.set(key, [], { rows: [{ id: 1 }] });
    assert.ok(cache.get(key), 'Should be cached');
    
    cache.invalidate('users');
    assert.equal(cache.get(key), undefined, 'Should be invalidated after mutation');
  });

  it('invalidation: case-insensitive table matching', () => {
    const cache = new QueryCache();
    cache.set('SELECT * FROM Users', [], { rows: [] });
    cache.set('SELECT * FROM ORDERS', [], { rows: [] });
    
    cache.invalidate('users');
    assert.equal(cache.get('SELECT * FROM Users'), undefined, 'Should invalidate case-insensitively');
    assert.ok(cache.get('SELECT * FROM ORDERS'), 'Should not invalidate unrelated tables');
  });

  it('invalidation: JOIN queries cleared when either table mutated', () => {
    const cache = new QueryCache();
    const joinQuery = 'SELECT * FROM users JOIN orders ON users.id = orders.user_id';
    cache.set(joinQuery, [], { rows: [] });
    
    cache.invalidate('orders');
    assert.equal(cache.get(joinQuery), undefined, 'JOIN query should be invalidated when either table changes');
  });

  it('extractTables: covers FROM, JOIN, INSERT, UPDATE, DELETE', () => {
    assert.deepEqual(
      QueryCache.extractTables('SELECT * FROM users WHERE id = 1').sort(),
      ['users']
    );
    assert.deepEqual(
      QueryCache.extractTables('SELECT * FROM users JOIN orders ON users.id = orders.uid').sort(),
      ['orders', 'users']
    );
    assert.deepEqual(
      QueryCache.extractTables('INSERT INTO logs VALUES (1)'),
      ['logs']
    );
    assert.deepEqual(
      QueryCache.extractTables('UPDATE metrics SET val = 1'),
      ['metrics']
    );
    assert.deepEqual(
      QueryCache.extractTables('DELETE FROM sessions WHERE expired = 1'),
      ['sessions']
    );
  });

  it('eviction: respects maxSize', () => {
    const cache = new QueryCache(5);
    for (let i = 0; i < 10; i++) {
      cache.set(`SELECT ${i}`, [], { rows: [{ val: i }] });
    }
    // Only 5 should remain (FIFO eviction)
    let found = 0;
    for (let i = 0; i < 10; i++) {
      if (cache.get(`SELECT ${i}`)) found++;
    }
    assert.ok(found <= 5, `Should have at most 5 entries, found ${found}`);
  });

  it('stats: tracks hits and misses', () => {
    const cache = new QueryCache();
    cache.set('SELECT 1', [], { val: 1 });
    cache.get('SELECT 1');
    cache.get('SELECT 1');
    cache.get('SELECT 2'); // miss
    
    // Hits and misses are on the instance
    assert.equal(cache._hits, 2);
    assert.equal(cache._misses, 1);
  });

  it('invalidateAll: clears everything', () => {
    const cache = new QueryCache();
    for (let i = 0; i < 20; i++) cache.set(`SELECT ${i}`, [], { val: i });
    cache.invalidateAll();
    
    for (let i = 0; i < 20; i++) {
      assert.equal(cache.get(`SELECT ${i}`), undefined);
    }
  });

  it('stress: 1000 rapid set/get/invalidate cycles', () => {
    const cache = new QueryCache(100);
    const tables = ['users', 'orders', 'items', 'logs', 'metrics'];
    
    for (let i = 0; i < 1000; i++) {
      const table = tables[i % 5];
      if (i % 10 === 0) {
        // Invalidate every 10th
        cache.invalidate(table);
      } else if (i % 3 === 0) {
        // Get
        cache.get(`SELECT * FROM ${table} WHERE id = ${i}`);
      } else {
        // Set
        cache.set(`SELECT * FROM ${table} WHERE id = ${i}`, [], { rows: [{ id: i }] });
      }
    }
    
    // Should not crash
    assert.ok(true, 'Survived 1000 cycles');
  });
});

describe('Prepared statement correctness', () => {
  let db;

  before(() => {
    db = new Database();
    db.execute('CREATE TABLE prep_t (id INT PRIMARY KEY, val INT, name TEXT)');
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO prep_t VALUES (${i}, ${i * 7 % 100}, 'item_${i}')`);
    }
  });

  it('basic PREPARE/EXECUTE/DEALLOCATE', () => {
    db.execute("PREPARE q1 AS SELECT * FROM prep_t WHERE id = $1");
    const r = db.execute("EXECUTE q1(5)");
    assert.ok(r.rows, 'Should return rows');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 5);
    db.execute("DEALLOCATE q1");
  });

  it('EXECUTE with different params returns different results', () => {
    db.execute("PREPARE q2 AS SELECT * FROM prep_t WHERE val > $1");
    const r1 = db.execute("EXECUTE q2(90)");
    const r2 = db.execute("EXECUTE q2(10)");
    assert.ok(r2.rows.length > r1.rows.length, 'Lower threshold should return more rows');
    db.execute("DEALLOCATE q2");
  });

  it('prepared statement survives data mutation', () => {
    db.execute("PREPARE q3 AS SELECT COUNT(*) as cnt FROM prep_t WHERE val > $1");
    const r1 = db.execute("EXECUTE q3(0)");
    const count1 = r1.rows[0].cnt;
    
    // Mutate data
    db.execute("INSERT INTO prep_t VALUES (999, 50, 'new_item')");
    
    const r2 = db.execute("EXECUTE q3(0)");
    const count2 = r2.rows[0].cnt;
    
    assert.equal(count2, count1 + 1, 'Prepared statement should see new data');
    
    // Cleanup
    db.execute("DELETE FROM prep_t WHERE id = 999");
    db.execute("DEALLOCATE q3");
  });

  it('DEALLOCATE then re-PREPARE', () => {
    db.execute("PREPARE reuse AS SELECT * FROM prep_t LIMIT $1");
    const r1 = db.execute("EXECUTE reuse(3)");
    assert.equal(r1.rows.length, 3);
    db.execute("DEALLOCATE reuse");
    
    // Re-prepare with different query
    db.execute("PREPARE reuse AS SELECT id FROM prep_t WHERE val < $1");
    const r2 = db.execute("EXECUTE reuse(20)");
    assert.ok(r2.rows.length >= 0);
    db.execute("DEALLOCATE reuse");
  });

  it('EXECUTE non-existent prepared statement throws', () => {
    assert.throws(() => db.execute("EXECUTE nonexistent(1)"), /not found|does not exist|undefined|prepared/i);
  });

  it('DEALLOCATE non-existent is safe', () => {
    // Should either succeed silently or throw a gentle error
    try {
      db.execute("DEALLOCATE no_such_stmt");
    } catch (e) {
      assert.ok(e.message, 'Should have error message');
    }
  });

  it('stress: 100 prepare/execute/deallocate cycles', () => {
    for (let i = 0; i < 100; i++) {
      const name = `stress_${i}`;
      db.execute(`PREPARE ${name} AS SELECT * FROM prep_t WHERE id = $1`);
      const r = db.execute(`EXECUTE ${name}(${1 + i % 50})`);
      assert.ok(r.rows !== undefined);
      db.execute(`DEALLOCATE ${name}`);
    }
  });

  it('prepared statement with text parameter', () => {
    db.execute("PREPARE txt_q AS SELECT * FROM prep_t WHERE name = $1");
    const r = db.execute("EXECUTE txt_q('item_10')");
    assert.ok(r.rows.length >= 0);
    db.execute("DEALLOCATE txt_q");
  });
});

describe('Cache + Mutation integration', () => {
  it('SELECT result changes after INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE cache_test (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO cache_test VALUES (1, 100)');
    
    const r1 = db.execute('SELECT SUM(val) as total FROM cache_test');
    assert.equal(r1.rows[0].total, 100);
    
    db.execute('INSERT INTO cache_test VALUES (2, 200)');
    
    const r2 = db.execute('SELECT SUM(val) as total FROM cache_test');
    assert.equal(r2.rows[0].total, 300, 'Should reflect new data after INSERT');
  });

  it('SELECT result changes after UPDATE', () => {
    const db = new Database();
    db.execute('CREATE TABLE cache_test2 (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO cache_test2 VALUES (1, 100)');
    
    const r1 = db.execute('SELECT val FROM cache_test2 WHERE id = 1');
    assert.equal(r1.rows[0].val, 100);
    
    db.execute('UPDATE cache_test2 SET val = 999 WHERE id = 1');
    
    const r2 = db.execute('SELECT val FROM cache_test2 WHERE id = 1');
    assert.equal(r2.rows[0].val, 999, 'Should reflect updated data');
  });

  it('SELECT result changes after DELETE', () => {
    const db = new Database();
    db.execute('CREATE TABLE cache_test3 (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO cache_test3 VALUES (1, 100)');
    db.execute('INSERT INTO cache_test3 VALUES (2, 200)');
    
    const r1 = db.execute('SELECT COUNT(*) as cnt FROM cache_test3');
    assert.equal(r1.rows[0].cnt, 2);
    
    db.execute('DELETE FROM cache_test3 WHERE id = 1');
    
    const r2 = db.execute('SELECT COUNT(*) as cnt FROM cache_test3');
    assert.equal(r2.rows[0].cnt, 1, 'Should reflect deleted data');
  });

  it('DROP TABLE clears related cache', () => {
    const db = new Database();
    db.execute('CREATE TABLE temp_t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO temp_t VALUES (1)');
    db.execute('SELECT * FROM temp_t'); // warm cache
    
    db.execute('DROP TABLE temp_t');
    
    assert.throws(() => db.execute('SELECT * FROM temp_t'), /not found|does not exist/i);
  });
});
