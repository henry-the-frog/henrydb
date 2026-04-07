// disk-manager.test.js — Tests for file-backed page I/O
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { DiskManager, PAGE_SIZE } from './disk-manager.js';
import { unlinkSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const testFile = () => join(tmpdir(), `henrydb-test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);

describe('DiskManager', () => {
  const files = [];
  
  function createDM(opts) {
    const f = testFile();
    files.push(f);
    return new DiskManager(f, opts);
  }

  afterEach(() => {
    for (const f of files) {
      try { if (existsSync(f)) unlinkSync(f); } catch {}
    }
    files.length = 0;
  });

  it('creates a new database file', () => {
    const dm = createDM();
    assert.strictEqual(dm.pageCount, 0);
    dm.close();
  });

  it('allocates and reads pages', () => {
    const dm = createDM();
    
    const p0 = dm.allocatePage();
    assert.strictEqual(p0, 0);
    assert.strictEqual(dm.pageCount, 1);
    
    const p1 = dm.allocatePage();
    assert.strictEqual(p1, 1);
    assert.strictEqual(dm.pageCount, 2);
    
    // Write data to page 0
    const data = Buffer.alloc(PAGE_SIZE);
    data.write('Hello, HenryDB!', 0, 'utf8');
    dm.writePage(0, data);
    
    // Read it back
    const read = dm.readPage(0);
    assert.strictEqual(read.toString('utf8', 0, 15), 'Hello, HenryDB!');
    
    dm.close();
  });

  it('persists across close/reopen', () => {
    const f = testFile();
    files.push(f);
    
    // Write data
    const dm1 = new DiskManager(f);
    const p = dm1.allocatePage();
    const data = Buffer.alloc(PAGE_SIZE);
    data.writeInt32LE(42, 0);
    data.writeInt32LE(123456, 4);
    dm1.writePage(p, data);
    dm1.close();
    
    // Reopen and read
    const dm2 = new DiskManager(f, { create: false });
    assert.strictEqual(dm2.pageCount, 1);
    const read = dm2.readPage(0);
    assert.strictEqual(read.readInt32LE(0), 42);
    assert.strictEqual(read.readInt32LE(4), 123456);
    dm2.close();
  });

  it('free list recycles deallocated pages', () => {
    const dm = createDM();
    
    const p0 = dm.allocatePage();
    const p1 = dm.allocatePage();
    const p2 = dm.allocatePage();
    assert.strictEqual(dm.pageCount, 3);
    
    // Deallocate page 1
    dm.deallocatePage(p1);
    
    // Next allocation should reuse page 1
    const p3 = dm.allocatePage();
    assert.strictEqual(p3, 1);
    
    dm.close();
  });

  it('handles many pages', () => {
    const dm = createDM();
    
    for (let i = 0; i < 100; i++) {
      const p = dm.allocatePage();
      const data = Buffer.alloc(PAGE_SIZE);
      data.writeInt32LE(i * 7, 0);
      dm.writePage(p, data);
    }
    
    assert.strictEqual(dm.pageCount, 100);
    
    // Verify all pages
    for (let i = 0; i < 100; i++) {
      const read = dm.readPage(i);
      assert.strictEqual(read.readInt32LE(0), i * 7);
    }
    
    dm.close();
  });

  it('throws on invalid page ID', () => {
    const dm = createDM();
    assert.throws(() => dm.readPage(0), /Invalid page ID/);
    assert.throws(() => dm.readPage(-1), /Invalid page ID/);
    dm.close();
  });

  it('throws on wrong-size write', () => {
    const dm = createDM();
    dm.allocatePage();
    assert.throws(() => dm.writePage(0, Buffer.alloc(100)), /exactly.*4096/);
    dm.close();
  });

  it('free list persists across close/reopen', () => {
    const f = testFile();
    files.push(f);
    
    const dm1 = new DiskManager(f);
    dm1.allocatePage();
    dm1.allocatePage();
    dm1.allocatePage();
    dm1.deallocatePage(1);
    dm1.close();
    
    const dm2 = new DiskManager(f, { create: false });
    const p = dm2.allocatePage();
    assert.strictEqual(p, 1, 'Reused deallocated page after reopen');
    dm2.close();
  });

  it('sync mode forces fsync on every write', () => {
    const dm = createDM({ sync: true });
    const p = dm.allocatePage();
    const data = Buffer.alloc(PAGE_SIZE);
    data.write('sync test', 0, 'utf8');
    dm.writePage(p, data); // Should fsync
    
    const read = dm.readPage(p);
    assert.strictEqual(read.toString('utf8', 0, 9), 'sync test');
    dm.close();
  });
});
