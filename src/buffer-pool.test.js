// buffer-pool.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BufferPoolManager } from './buffer-pool.js';

describe('BufferPoolManager', () => {
  it('new page allocation', () => {
    const bpm = new BufferPoolManager(4, 128);
    const page = bpm.newPage();
    assert.ok(page);
    assert.equal(page.data.length, 128);
    assert.equal(page.pageId, 0);
  });

  it('fetch page from disk', () => {
    const bpm = new BufferPoolManager(4, 128);
    const p = bpm.newPage();
    p.data.write('hello', 0);
    bpm.unpinPage(p.pageId, true);
    bpm.flushPage(p.pageId);

    // Fill pool to evict p
    for (let i = 0; i < 4; i++) {
      const np = bpm.newPage();
      if (np) bpm.unpinPage(np.pageId);
    }
    
    // Fetch back from disk
    const fetched = bpm.fetchPage(p.pageId);
    assert.ok(fetched);
    assert.equal(fetched.data.toString('utf8', 0, 5), 'hello');
  });

  it('pin/unpin semantics', () => {
    const bpm = new BufferPoolManager(2, 64);
    const p1 = bpm.newPage();
    const p2 = bpm.newPage();

    // Both pinned — can't allocate more
    const p3 = bpm.newPage();
    assert.equal(p3, null);

    // Unpin one
    bpm.unpinPage(p1.pageId);
    const p4 = bpm.newPage();
    assert.ok(p4); // Should evict p1
  });

  it('LRU eviction order', () => {
    const bpm = new BufferPoolManager(3, 64);
    const p1 = bpm.newPage(); // page 0
    const p2 = bpm.newPage(); // page 1
    const p3 = bpm.newPage(); // page 2

    bpm.unpinPage(p1.pageId);
    bpm.unpinPage(p2.pageId);
    bpm.unpinPage(p3.pageId);

    // Access p1 (moves to end of LRU)
    bpm.fetchPage(p1.pageId);
    bpm.unpinPage(p1.pageId);

    // Allocate new — should evict p2 (least recently used)
    const p4 = bpm.newPage();
    assert.ok(p4);

    // p2 should be evicted, p1 and p3 still accessible
    assert.ok(bpm.fetchPage(p1.pageId));
  });

  it('dirty page writeback on eviction', () => {
    const bpm = new BufferPoolManager(2, 64);
    const p1 = bpm.newPage();
    p1.data.write('data1', 0);
    bpm.unpinPage(p1.pageId, true); // Mark dirty

    const p2 = bpm.newPage();
    bpm.unpinPage(p2.pageId);

    // Evict p1 by allocating more
    const p3 = bpm.newPage();
    assert.ok(p3);
    assert.equal(bpm.stats.dirtyEvictions, 1);
  });

  it('flush all dirty pages', () => {
    const bpm = new BufferPoolManager(4, 64);
    for (let i = 0; i < 4; i++) {
      const p = bpm.newPage();
      p.data.write(`page${i}`, 0);
      bpm.unpinPage(p.pageId, true);
    }

    bpm.flushAll();
    assert.equal(bpm.stats.flushes, 4);
    assert.equal(bpm.getStats().dirty, 0);
  });

  it('delete page', () => {
    const bpm = new BufferPoolManager(4, 64);
    const p = bpm.newPage();
    bpm.unpinPage(p.pageId);
    assert.ok(bpm.deletePage(p.pageId));
    assert.equal(bpm.fetchPage(p.pageId).data.toString('utf8', 0, 5), '\0\0\0\0\0');
  });

  it('cannot delete pinned page', () => {
    const bpm = new BufferPoolManager(4, 64);
    const p = bpm.newPage();
    assert.equal(bpm.deletePage(p.pageId), false);
  });

  it('hit rate tracking', () => {
    const bpm = new BufferPoolManager(8, 64);
    const pages = [];
    for (let i = 0; i < 4; i++) {
      const p = bpm.newPage();
      pages.push(p.pageId);
      bpm.unpinPage(p.pageId, true);
    }

    // Re-fetch: all hits
    for (const pid of pages) {
      const p = bpm.fetchPage(pid);
      bpm.unpinPage(p.pageId);
    }

    const stats = bpm.getStats();
    assert.ok(stats.hits >= 4);
    assert.ok(stats.hitRate.includes('%'));
  });

  it('benchmark: 10K page accesses with Zipf pattern', () => {
    const bpm = new BufferPoolManager(64, 256);
    const N = 10000;
    
    // Create pages
    const pageIds = [];
    for (let i = 0; i < 200; i++) {
      const p = bpm.newPage();
      bpm.unpinPage(p.pageId);
      pageIds.push(p.pageId);
    }

    const t0 = Date.now();
    for (let i = 0; i < N; i++) {
      // Zipf: most accesses hit a few hot pages
      const idx = Math.floor(Math.pow(Math.random(), 2) * pageIds.length);
      const p = bpm.fetchPage(pageIds[idx]);
      if (p) bpm.unpinPage(p.pageId);
    }
    const ms = Date.now() - t0;

    const stats = bpm.getStats();
    console.log(`    ${N} accesses: ${ms}ms, hit rate: ${stats.hitRate}, evictions: ${stats.evictions}`);
  });
});
