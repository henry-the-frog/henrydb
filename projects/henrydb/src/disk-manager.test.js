// disk-manager.test.js — Tests for file-backed page I/O
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { DiskManager } from './disk-manager.js';
import { BufferPoolManager } from './buffer-pool.js';

let dm = null;

afterEach(() => {
  if (dm) { dm.destroy(); dm = null; }
});

describe('DiskManager', () => {
  it('allocate and read page', () => {
    dm = DiskManager.createTemp(256);
    const pageId = dm.allocatePage();
    assert.equal(pageId, 0);
    
    const data = dm.readPage(pageId);
    assert.equal(data.length, 256);
    assert.equal(data.readUInt32LE(0), 0); // Zero-filled
  });

  it('write and read back', () => {
    dm = DiskManager.createTemp(256);
    const pageId = dm.allocatePage();
    
    const buf = Buffer.alloc(256);
    buf.write('Hello, HenryDB!', 0);
    dm.writePage(pageId, buf);
    
    const readBack = dm.readPage(pageId);
    assert.equal(readBack.toString('utf8', 0, 15), 'Hello, HenryDB!');
  });

  it('multiple pages', () => {
    dm = DiskManager.createTemp(128);
    
    const ids = [];
    for (let i = 0; i < 10; i++) {
      const id = dm.allocatePage();
      ids.push(id);
      const buf = Buffer.alloc(128);
      buf.writeUInt32LE(i * 100, 0);
      dm.writePage(id, buf);
    }
    
    assert.equal(dm.numPages, 10);
    assert.equal(dm.stats.fileSize, 10 * 128);
    
    // Read in reverse order
    for (let i = 9; i >= 0; i--) {
      const data = dm.readPage(ids[i]);
      assert.equal(data.readUInt32LE(0), i * 100);
    }
  });

  it('read non-existent page throws', () => {
    dm = DiskManager.createTemp(128);
    assert.throws(() => dm.readPage(99), /does not exist/);
  });

  it('wrong buffer size throws', () => {
    dm = DiskManager.createTemp(128);
    dm.allocatePage();
    assert.throws(() => dm.writePage(0, Buffer.alloc(64)), /exactly 128 bytes/);
  });

  it('stats tracking', () => {
    dm = DiskManager.createTemp(128);
    dm.allocatePage();
    dm.allocatePage();
    dm.readPage(0);
    dm.readPage(1);
    
    const stats = dm.stats;
    assert.equal(stats.totalPages, 2);
    assert.equal(stats.reads, 2);
    assert.equal(stats.writes, 2); // allocatePage writes zeros
    assert.equal(stats.bytesRead, 256);
    assert.equal(stats.bytesWritten, 256);
  });

  it('persistence: close and reopen', () => {
    dm = DiskManager.createTemp(128);
    const p0 = dm.allocatePage();
    const buf = Buffer.alloc(128);
    buf.write('persistent data', 0);
    dm.writePage(p0, buf);
    
    const filePath = dm.filePath;
    dm.close();
    
    // Reopen the same file
    dm = new DiskManager(filePath, 128);
    assert.equal(dm.numPages, 1);
    
    const readBack = dm.readPage(0);
    assert.equal(readBack.toString('utf8', 0, 15), 'persistent data');
  });

  it('deallocate zeroes page', () => {
    dm = DiskManager.createTemp(128);
    const p0 = dm.allocatePage();
    const buf = Buffer.alloc(128);
    buf.write('secret data', 0);
    dm.writePage(p0, buf);
    
    dm.deallocatePage(p0);
    
    const data = dm.readPage(p0);
    assert.equal(data.readUInt32LE(0), 0); // Zeroed
  });

  it('large page count: 1000 pages', () => {
    dm = DiskManager.createTemp(64);
    
    for (let i = 0; i < 1000; i++) {
      const id = dm.allocatePage();
      const buf = Buffer.alloc(64);
      buf.writeUInt32LE(i, 0);
      dm.writePage(id, buf);
    }
    
    assert.equal(dm.numPages, 1000);
    assert.equal(dm.stats.fileSize, 64000);
    
    // Spot check
    for (const i of [0, 499, 999]) {
      const data = dm.readPage(i);
      assert.equal(data.readUInt32LE(0), i);
    }
  });

  it('performance: 10K page writes and reads', () => {
    dm = DiskManager.createTemp(64);
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) {
      dm.allocatePage();
    }
    const allocMs = performance.now() - t0;
    
    const t1 = performance.now();
    const buf = Buffer.alloc(64);
    for (let i = 0; i < 10000; i++) {
      buf.writeUInt32LE(i, 0);
      dm.writePage(i, buf);
    }
    const writeMs = performance.now() - t1;
    
    const t2 = performance.now();
    for (let i = 0; i < 10000; i++) {
      dm.readPage(i);
    }
    const readMs = performance.now() - t2;
    
    console.log(`  10K pages (64B): alloc ${allocMs.toFixed(1)}ms, write ${writeMs.toFixed(1)}ms, read ${readMs.toFixed(1)}ms`);
    assert.ok(allocMs < 5000);
    assert.ok(writeMs < 5000);
    assert.ok(readMs < 5000);
  });
});

describe('DiskManager + BufferPoolManager integration', () => {
  it('buffer pool with real disk I/O', () => {
    dm = DiskManager.createTemp(128);
    const bpm = new BufferPoolManager(3, dm); // Only 3 frames!
    
    // Create 10 pages through the buffer pool
    const pageIds = [];
    for (let i = 0; i < 10; i++) {
      const page = bpm.newPage();
      assert.ok(page, `Failed to allocate page ${i}`);
      page.data.write(`page-${i}`, 0);
      page.data.writeUInt32LE(i * 42, 100);
      bpm.unpinPage(page.pageId, true);
      pageIds.push(page.pageId);
    }
    
    // Fetch all pages — forces disk I/O through eviction/reload
    for (let i = 0; i < 10; i++) {
      const data = bpm.fetchPage(pageIds[i]);
      assert.ok(data, `Failed to fetch page ${pageIds[i]}`);
      assert.equal(data.toString('utf8', 0, 6), `page-${i}`);
      assert.equal(data.readUInt32LE(100), i * 42);
      bpm.unpinPage(pageIds[i], false);
    }
    
    console.log(`  BPM+Disk: ${bpm.stats.hits} hits, ${bpm.stats.misses} misses, ${dm.stats.reads} disk reads, ${dm.stats.writes} disk writes`);
  });

  it('data persists through buffer pool eviction cycle', () => {
    dm = DiskManager.createTemp(128);
    const bpm = new BufferPoolManager(2, dm); // Extremely small pool
    
    // Write 100 pages
    const ids = [];
    for (let i = 0; i < 100; i++) {
      const page = bpm.newPage();
      page.data.writeUInt32LE(i * 7, 0);
      bpm.unpinPage(page.pageId, true);
      ids.push(page.pageId);
    }
    
    // Flush all
    bpm.flushAll();
    
    // Read all back
    let correct = 0;
    for (let i = 0; i < 100; i++) {
      const data = bpm.fetchPage(ids[i]);
      if (data && data.readUInt32LE(0) === i * 7) correct++;
      bpm.unpinPage(ids[i], false);
    }
    
    assert.equal(correct, 100, 'All 100 pages should survive eviction');
  });

  it('buffer pool with file persistence (close and reopen)', () => {
    dm = DiskManager.createTemp(128);
    const filePath = dm.filePath;
    
    // Create pages through buffer pool
    const bpm1 = new BufferPoolManager(5, dm);
    for (let i = 0; i < 20; i++) {
      const page = bpm1.newPage();
      page.data.write(`record-${i}`, 0);
      bpm1.unpinPage(page.pageId, true);
    }
    bpm1.flushAll();
    dm.close();
    
    // Reopen and verify
    dm = new DiskManager(filePath, 128);
    const bpm2 = new BufferPoolManager(5, dm);
    
    for (let i = 0; i < 20; i++) {
      const data = bpm2.fetchPage(i);
      assert.ok(data, `Failed to fetch page ${i} after reopen`);
      const expected = `record-${i}`;
      assert.equal(data.toString('utf8', 0, expected.length), expected);
      bpm2.unpinPage(i, false);
    }
  });
});
