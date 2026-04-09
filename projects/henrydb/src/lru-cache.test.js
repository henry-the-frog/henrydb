// lru-cache.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LRUCache } from './lru-cache.js';

describe('LRUCache', () => {
  it('evicts LRU entry', () => {
    const c = new LRUCache(2);
    c.put(1, 'a'); c.put(2, 'b'); c.put(3, 'c');
    assert.equal(c.get(1), undefined);
    assert.equal(c.get(2), 'b');
  });

  it('get refreshes entry', () => {
    const c = new LRUCache(2);
    c.put(1, 'a'); c.put(2, 'b');
    c.get(1); // Refresh 1
    c.put(3, 'c'); // Evicts 2, not 1
    assert.equal(c.get(1), 'a');
    assert.equal(c.get(2), undefined);
  });
});
