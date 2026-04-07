// lru-cache.test.js — Simple LRU cache test
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PlanCache } from './plan-cache.js';

describe('LRU Cache Extended', () => {
  it('accessing entry refreshes its position', () => {
    const cache = new PlanCache(3);
    cache.put('a', { t: 'a' });
    cache.put('b', { t: 'b' });
    cache.put('c', { t: 'c' });
    
    // Access 'a' to make it most recently used
    cache.get('a');
    
    // Adding 'd' should evict 'b' (now LRU), not 'a'
    cache.put('d', { t: 'd' });
    
    assert.ok(cache.get('a') !== null); // 'a' should survive
    assert.equal(cache.get('b'), null); // 'b' should be evicted
    assert.ok(cache.get('d') !== null);
  });

  it('overwriting existing key updates value', () => {
    const cache = new PlanCache(5);
    cache.put('key', { version: 1 });
    cache.put('key', { version: 2 });
    
    const result = cache.get('key');
    assert.equal(result.version, 2);
    assert.equal(cache.stats().size, 1);
  });
});
