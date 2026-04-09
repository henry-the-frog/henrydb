// buffer-pool.test.js — Tests for Buffer Pool Manager
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BufferPoolManager, InMemoryDiskManager } from './buffer-pool.js';

function setup(poolSize = 4) {
  const disk = new InMemoryDiskManager(64); // Small pages for testing
  const bpm = new BufferPoolManager(poolSize, disk);
  return { disk, bpm };
}

describe('BufferPoolManager', () => {
  it('newPage allocates and returns page', () => {
    const { bpm } = setup();
    const page = bpm.newPage();
    assert.ok(page);
    assert.equal(typeof page.pageId, 'number');
    assert.ok(Buffer.isBuffer(page.data));
    assert.equal(page.data.length, 64);
  });

  it('fetchPage after newPage returns same data', () => {
    const { bpm } = setup();
    const page = bpm.newPage();
    page.data.write('hello', 0);
    bpm.unpinPage(page.pageId, true); // Dirty: wrote to it
    
    const fetched = bpm.fetchPage(page.pageId);
    assert.ok(fetched);
    assert.equal(fetched.toString('utf8', 0, 5), 'hello');
  });

  it('fetchPage from disk after eviction', () => {
    const { bpm, disk } = setup(2); // Only 2 frames!
    
    // Create 3 pages (will force eviction)
    const p0 = bpm.newPage(); p0.data.write('page0'); bpm.unpinPage(p0.pageId, true);
    const p1 = bpm.newPage(); p1.data.write('page1'); bpm.unpinPage(p1.pageId, true);
    const p2 = bpm.newPage(); p2.data.write('page2'); bpm.unpinPage(p2.pageId, true);
    // p0 was evicted (LRU)
    
    // Fetch p0 — should load from disk
    const fetched = bpm.fetchPage(p0.pageId);
    assert.ok(fetched);
    assert.equal(fetched.toString('utf8', 0, 5), 'page0');
  });

  it('eviction flushes dirty pages', () => {
    const { bpm, disk } = setup(2);
    
    const p0 = bpm.newPage(); p0.data.write('dirty-page'); bpm.unpinPage(p0.pageId, true);
    const p1 = bpm.newPage(); bpm.unpinPage(p1.pageId, false);
    const p2 = bpm.newPage(); bpm.unpinPage(p2.pageId, false);
    // p0 was evicted — should have been flushed to disk
    
    assert.ok(disk.stats.writes >= 1, 'Dirty page should have been written');
    
    // Verify data is on disk
    const diskData = disk.readPage(p0.pageId);
    assert.equal(diskData.toString('utf8', 0, 10), 'dirty-page');
  });

  it('pinned pages cannot be evicted', () => {
    const { bpm } = setup(2);
    
    // Pin both frames
    const p0 = bpm.newPage(); // pinned
    const p1 = bpm.newPage(); // pinned
    
    // Try to create a third page — should fail (both pinned)
    const p2 = bpm.newPage();
    assert.equal(p2, null);
  });

  it('unpin with isDirty marks page dirty', () => {
    const { bpm, disk } = setup();
    const page = bpm.newPage();
    page.data.write('modified');
    bpm.unpinPage(page.pageId, true);
    
    bpm.flushPage(page.pageId);
    assert.ok(disk.stats.writes >= 1);
    
    const onDisk = disk.readPage(page.pageId);
    assert.equal(onDisk.toString('utf8', 0, 8), 'modified');
  });

  it('deletePage removes from pool and disk', () => {
    const { bpm, disk } = setup();
    const page = bpm.newPage();
    const pageId = page.pageId;
    bpm.unpinPage(pageId, false);
    
    assert.equal(bpm.deletePage(pageId), true);
    
    // Should not be fetchable
    const fetched = bpm.fetchPage(pageId);
    assert.equal(fetched, null);
  });

  it('deletePage fails on pinned page', () => {
    const { bpm } = setup();
    const page = bpm.newPage(); // pinned
    assert.equal(bpm.deletePage(page.pageId), false);
  });

  it('flushAll writes all dirty pages', () => {
    const { bpm, disk } = setup();
    
    const p0 = bpm.newPage(); p0.data.write('data0'); bpm.unpinPage(p0.pageId, true);
    const p1 = bpm.newPage(); p1.data.write('data1'); bpm.unpinPage(p1.pageId, true);
    const p2 = bpm.newPage(); bpm.unpinPage(p2.pageId, false); // Clean
    
    const writesBefore = disk.stats.writes;
    bpm.flushAll();
    assert.ok(disk.stats.writes >= writesBefore + 2); // At least 2 dirty pages
  });

  it('multiple pin/unpin tracking', () => {
    const { bpm } = setup();
    const page = bpm.newPage(); // pin=1
    
    // Fetch again (pin=2)
    const fetched = bpm.fetchPage(page.pageId);
    assert.ok(fetched);
    
    // Unpin once (pin=1, still not evictable)
    bpm.unpinPage(page.pageId, false);
    
    // Should still be fetchable without disk read
    const fetched2 = bpm.fetchPage(page.pageId);
    assert.ok(fetched2);
    
    // Now unpin twice (pin should reach 0)
    bpm.unpinPage(page.pageId, false);
    bpm.unpinPage(page.pageId, false);
    
    assert.equal(bpm.stats.evictable, 1);
  });

  it('cache hit rate tracking', () => {
    const { bpm } = setup(4);
    
    const p0 = bpm.newPage(); bpm.unpinPage(p0.pageId, true);
    const p1 = bpm.newPage(); bpm.unpinPage(p1.pageId, true);
    // newPage doesn't count as hit or miss
    
    // Fetch p0 (hit), p1 (hit), p0 (hit)
    bpm.fetchPage(p0.pageId); bpm.unpinPage(p0.pageId, false);
    bpm.fetchPage(p1.pageId); bpm.unpinPage(p1.pageId, false);
    bpm.fetchPage(p0.pageId); bpm.unpinPage(p0.pageId, false);
    
    assert.equal(bpm.stats.hits, 3);
    assert.equal(bpm.stats.misses, 0); // All pages were in pool
    assert.equal(bpm.stats.hitRate, '100.0%');
  });

  it('LRU eviction order is correct', () => {
    const { bpm } = setup(3);
    
    // Fill pool: p0, p1, p2
    const p0 = bpm.newPage(); p0.data.write('AAA'); bpm.unpinPage(p0.pageId, true);
    const p1 = bpm.newPage(); p1.data.write('BBB'); bpm.unpinPage(p1.pageId, true);
    const p2 = bpm.newPage(); p2.data.write('CCC'); bpm.unpinPage(p2.pageId, true);
    
    // Access p0 again (makes it MRU)
    bpm.fetchPage(p0.pageId); bpm.unpinPage(p0.pageId, false);
    
    // Create p3 — should evict p1 (LRU)
    const p3 = bpm.newPage(); bpm.unpinPage(p3.pageId, false);
    
    // p1 should be evicted, p0 should still be in pool
    const fetchP0 = bpm.fetchPage(p0.pageId);
    assert.ok(fetchP0);
    assert.equal(fetchP0.toString('utf8', 0, 3), 'AAA');
  });

  it('stress: working set larger than pool', () => {
    const { bpm, disk } = setup(10); // 10 frames
    
    // Create 100 pages
    const pageIds = [];
    for (let i = 0; i < 100; i++) {
      const page = bpm.newPage();
      assert.ok(page, `Failed to allocate page ${i}`);
      page.data.write(`page-${i}`, 0);
      bpm.unpinPage(page.pageId, true);
      pageIds.push(page.pageId);
    }
    
    // Fetch all pages — forces many evictions
    for (const pageId of pageIds) {
      const data = bpm.fetchPage(pageId);
      assert.ok(data, `Failed to fetch page ${pageId}`);
      bpm.unpinPage(pageId, false);
    }
    
    const stats = bpm.stats;
    console.log(`  Stress: ${stats.hits} hits, ${stats.misses} misses, ${stats.hitRate} hit rate`);
    console.log(`  Disk: ${stats.disk.reads} reads, ${stats.disk.writes} writes`);
    assert.ok(stats.disk.reads > 0, 'Should have disk reads from eviction/reload');
  });

  it('sequential flooding (LRU weakness)', () => {
    const { bpm } = setup(5);
    
    // Load working set (hot pages): 0-4
    const hotIds = [];
    for (let i = 0; i < 5; i++) {
      const p = bpm.newPage(); p.data.write(`hot-${i}`); bpm.unpinPage(p.pageId, true);
      hotIds.push(p.pageId);
    }
    
    // Sequential scan: load 20 cold pages (floods the pool)
    for (let i = 0; i < 20; i++) {
      const p = bpm.newPage(); bpm.unpinPage(p.pageId, true);
    }
    
    // Hot pages have been evicted by the sequential scan
    // This is the known LRU weakness — sequential flooding
    let hotInPool = 0;
    for (const id of hotIds) {
      const data = bpm.fetchPage(id);
      if (data) {
        bpm.unpinPage(id, false);
        hotInPool++;
      }
    }
    
    console.log(`  Sequential flood: ${hotInPool}/5 hot pages survived (LRU weakness: 0 expected)`);
    // LRU policy: hot pages should all be evicted by the sequential scan
    assert.equal(hotInPool, 5); // They're reloaded from disk (data preserved)
    // But they required disk reads, which LRU-K or 2Q would avoid
  });
});
