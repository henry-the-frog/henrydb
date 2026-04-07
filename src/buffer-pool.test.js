// buffer-pool.test.js — Buffer pool manager tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BufferPool } from './buffer-pool.js';

describe('BufferPool', () => {
  const loader = (pageId) => ({ pageId, content: `page_${pageId}` });
  const writer = (pageId, data) => {}; // no-op for tests

  it('fetches and caches pages', () => {
    const pool = new BufferPool(4);
    const frame = pool.fetchPage(1, loader);
    assert.equal(frame.pageId, 1);
    assert.deepEqual(frame.data, { pageId: 1, content: 'page_1' });
  });

  it('cache hit returns same frame', () => {
    const pool = new BufferPool(4);
    pool.fetchPage(1, loader);
    pool.unpinPage(1);
    
    const frame2 = pool.fetchPage(1, loader);
    assert.equal(frame2.pageId, 1);
    assert.equal(pool.stats().hits, 1);
    assert.equal(pool.stats().misses, 1); // First fetch was a miss
  });

  it('evicts LRU page when pool is full', () => {
    const pool = new BufferPool(3);
    pool.fetchPage(1, loader); pool.unpinPage(1);
    pool.fetchPage(2, loader); pool.unpinPage(2);
    pool.fetchPage(3, loader); pool.unpinPage(3);
    
    // Pool is full, fetching page 4 should evict page 1 (LRU)
    pool.fetchPage(4, loader);
    
    assert.equal(pool.stats().evictions, 1);
  });

  it('pinned pages are not evicted', () => {
    const pool = new BufferPool(2);
    pool.fetchPage(1, loader); // pinned
    pool.fetchPage(2, loader); pool.unpinPage(2); // unpinned
    
    // Fetch page 3: should evict page 2 (unpinned), not page 1 (pinned)
    pool.fetchPage(3, loader);
    assert.equal(pool.stats().evictions, 1);
    
    // Page 1 should still be accessible
    pool.unpinPage(1);
    const frame = pool.fetchPage(1, loader);
    assert.equal(frame.pageId, 1);
  });

  it('dirty page tracking', () => {
    const pool = new BufferPool(4);
    pool.fetchPage(1, loader);
    pool.unpinPage(1, true); // Mark as dirty
    
    assert.equal(pool.stats().dirty, 1);
  });

  it('flush writes dirty pages', () => {
    const written = [];
    const myWriter = (pageId, data) => written.push(pageId);
    
    const pool = new BufferPool(4);
    pool.fetchPage(1, loader); pool.unpinPage(1, true);
    pool.fetchPage(2, loader); pool.unpinPage(2, false);
    pool.fetchPage(3, loader); pool.unpinPage(3, true);
    
    const flushed = pool.flushAll(myWriter);
    assert.equal(flushed, 2); // Only dirty pages
    assert.ok(written.includes(1));
    assert.ok(written.includes(3));
  });

  it('throws when all frames are pinned', () => {
    const pool = new BufferPool(2);
    pool.fetchPage(1, loader);
    pool.fetchPage(2, loader);
    
    assert.throws(() => pool.fetchPage(3, loader), /all frames are pinned/);
  });

  it('stats reports hit rate', () => {
    const pool = new BufferPool(4);
    pool.fetchPage(1, loader); pool.unpinPage(1);
    pool.fetchPage(1, loader); pool.unpinPage(1); // hit
    pool.fetchPage(2, loader); pool.unpinPage(2);
    pool.fetchPage(1, loader); pool.unpinPage(1); // hit
    
    const stats = pool.stats();
    assert.equal(stats.hits, 2);
    assert.equal(stats.misses, 2);
    assert.equal(stats.hitRate, 0.5);
  });

  it('handles large number of pages', () => {
    const pool = new BufferPool(8);
    
    // Access 100 different pages through an 8-frame pool
    for (let i = 0; i < 100; i++) {
      pool.fetchPage(i, loader);
      pool.unpinPage(i);
    }
    
    const stats = pool.stats();
    assert.equal(stats.used, 8); // Pool is full
    assert.ok(stats.evictions > 0); // Had to evict many times
  });
});
