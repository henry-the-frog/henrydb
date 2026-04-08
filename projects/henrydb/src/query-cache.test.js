// query-cache.test.js — Tests for query result cache
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { QueryCache } from './query-cache.js';

describe('QueryCache', () => {
  it('caches and retrieves results', () => {
    const cache = new QueryCache();
    const result = { type: 'ROWS', rows: [{ id: 1, name: 'Alice' }] };
    
    cache.set('SELECT * FROM users', result, ['users']);
    const cached = cache.get('SELECT * FROM users');
    
    assert.deepStrictEqual(cached, result);
  });

  it('returns null for cache miss', () => {
    const cache = new QueryCache();
    assert.strictEqual(cache.get('SELECT * FROM unknown'), null);
  });

  it('normalizes query whitespace', () => {
    const cache = new QueryCache();
    const result = { type: 'ROWS', rows: [] };
    
    cache.set('SELECT  *   FROM   users', result, ['users']);
    const cached = cache.get('select * from users');
    
    assert.deepStrictEqual(cached, result);
  });

  it('invalidates on table mutation', () => {
    const cache = new QueryCache();
    cache.set('SELECT * FROM users', { rows: [1] }, ['users']);
    cache.set('SELECT * FROM orders', { rows: [2] }, ['orders']);
    
    cache.invalidate('users');
    
    assert.strictEqual(cache.get('SELECT * FROM users'), null);
    assert.deepStrictEqual(cache.get('SELECT * FROM orders'), { rows: [2] });
  });

  it('invalidates cross-table queries', () => {
    const cache = new QueryCache();
    cache.set('SELECT * FROM users JOIN orders ON ...', { rows: [1] }, ['users', 'orders']);
    
    // Mutating either table should invalidate
    cache.invalidate('orders');
    assert.strictEqual(cache.get('SELECT * FROM users JOIN orders ON ...'), null);
  });

  it('evicts LRU entries when full', () => {
    const cache = new QueryCache({ maxSize: 3 });
    cache.set('SELECT 1', { v: 1 }, []);
    cache.set('SELECT 2', { v: 2 }, []);
    cache.set('SELECT 3', { v: 3 }, []);
    
    // Access 1 and 3 (making 2 the LRU)
    cache.get('SELECT 1');
    cache.get('SELECT 3');
    
    // Adding 4 should evict 2
    cache.set('SELECT 4', { v: 4 }, []);
    
    assert.strictEqual(cache.get('SELECT 2'), null);
    assert.deepStrictEqual(cache.get('SELECT 1'), { v: 1 });
    assert.deepStrictEqual(cache.get('SELECT 3'), { v: 3 });
    assert.deepStrictEqual(cache.get('SELECT 4'), { v: 4 });
  });

  it('respects TTL', async () => {
    const cache = new QueryCache({ maxAgeMs: 100 });
    cache.set('SELECT 1', { v: 1 }, []);
    
    assert.deepStrictEqual(cache.get('SELECT 1'), { v: 1 });
    
    await new Promise(r => setTimeout(r, 150));
    
    assert.strictEqual(cache.get('SELECT 1'), null);
  });

  it('tracks stats', () => {
    const cache = new QueryCache();
    cache.set('SELECT 1', { v: 1 }, ['t']);
    cache.get('SELECT 1'); // hit
    cache.get('SELECT 2'); // miss
    cache.invalidate('t');
    
    const stats = cache.getStats();
    assert.strictEqual(stats.hits, 1);
    assert.strictEqual(stats.misses, 1);
    assert.strictEqual(stats.sets, 1);
    assert.strictEqual(stats.invalidations, 1);
    assert.strictEqual(stats.hitRate, 50);
  });

  it('invalidateAll clears everything', () => {
    const cache = new QueryCache();
    cache.set('SELECT 1', { v: 1 }, []);
    cache.set('SELECT 2', { v: 2 }, []);
    cache.set('SELECT 3', { v: 3 }, []);
    
    cache.invalidateAll();
    
    assert.strictEqual(cache.get('SELECT 1'), null);
    assert.strictEqual(cache.get('SELECT 2'), null);
    assert.strictEqual(cache.get('SELECT 3'), null);
  });

  it('extractTables finds table names', () => {
    assert.deepStrictEqual(
      QueryCache.extractTables('SELECT * FROM users WHERE id = 1'),
      ['users']
    );
    assert.deepStrictEqual(
      QueryCache.extractTables('SELECT * FROM users JOIN orders ON users.id = orders.user_id').sort(),
      ['orders', 'users']
    );
  });

  it('disabled cache returns null', () => {
    const cache = new QueryCache({ enabled: false });
    cache.set('SELECT 1', { v: 1 }, []);
    assert.strictEqual(cache.get('SELECT 1'), null);
  });
});
