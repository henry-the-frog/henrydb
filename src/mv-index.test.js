// mv-index.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MVIndex } from './mv-index.js';

describe('MVIndex (MVCC-aware index)', () => {
  it('insert and read at same txn', () => {
    const idx = new MVIndex();
    idx.insert('k1', 'v1', 1);
    assert.equal(idx.read('k1', 1), 'v1');
  });

  it('update creates new version', () => {
    const idx = new MVIndex();
    idx.insert('k1', 'old', 1);
    idx.insert('k1', 'new', 5);
    assert.equal(idx.read('k1', 3), 'old'); // Before update
    assert.equal(idx.read('k1', 5), 'new'); // At update
    assert.equal(idx.read('k1', 10), 'new'); // After update
  });

  it('snapshot isolation — reads past version', () => {
    const idx = new MVIndex();
    idx.insert('k1', 'v1', 1);
    idx.insert('k1', 'v2', 10);
    idx.insert('k1', 'v3', 20);
    assert.equal(idx.read('k1', 5), 'v1');
    assert.equal(idx.read('k1', 15), 'v2');
    assert.equal(idx.read('k1', 25), 'v3');
  });

  it('delete hides value', () => {
    const idx = new MVIndex();
    idx.insert('k1', 'v1', 1);
    idx.delete('k1', 5);
    assert.equal(idx.read('k1', 3), 'v1');
    assert.equal(idx.read('k1', 10), undefined);
  });

  it('scan returns all visible keys', () => {
    const idx = new MVIndex();
    idx.insert('a', 1, 1);
    idx.insert('b', 2, 2);
    idx.insert('c', 3, 3);
    idx.delete('b', 5);
    
    const at4 = idx.scan(4);
    assert.equal(at4.length, 3);
    
    const at6 = idx.scan(6);
    assert.equal(at6.length, 2); // b deleted
  });

  it('read missing key', () => {
    const idx = new MVIndex();
    assert.equal(idx.read('missing', 1), undefined);
  });

  it('GC removes old versions', () => {
    const idx = new MVIndex();
    idx.insert('k1', 'v1', 1);
    idx.insert('k1', 'v2', 5);
    idx.insert('k1', 'v3', 10);
    assert.ok(idx.versionCount >= 3);
    
    const collected = idx.gc(8);
    assert.ok(collected > 0);
  });

  it('version count tracking', () => {
    const idx = new MVIndex();
    idx.insert('k1', 'v1', 1);
    idx.insert('k1', 'v2', 2);
    idx.insert('k2', 'v3', 3);
    assert.equal(idx.keyCount, 2);
    assert.equal(idx.versionCount, 3);
  });
});
