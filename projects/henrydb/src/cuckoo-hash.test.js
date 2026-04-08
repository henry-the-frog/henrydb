// cuckoo-hash.test.js — Tests for cuckoo hash table
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CuckooHashTable } from './cuckoo-hash.js';

describe('CuckooHashTable', () => {

  it('basic set and get', () => {
    const ht = new CuckooHashTable();
    ht.set(1, 'hello');
    ht.set(2, 'world');
    assert.equal(ht.get(1), 'hello');
    assert.equal(ht.get(2), 'world');
    assert.equal(ht.get(3), undefined);
  });

  it('update existing key', () => {
    const ht = new CuckooHashTable();
    ht.set(5, 'old');
    ht.set(5, 'new');
    assert.equal(ht.get(5), 'new');
    assert.equal(ht.size, 1);
  });

  it('delete', () => {
    const ht = new CuckooHashTable();
    ht.set(1, 'a');
    assert.ok(ht.delete(1));
    assert.equal(ht.get(1), undefined);
    assert.equal(ht.size, 0);
  });

  it('has()', () => {
    const ht = new CuckooHashTable();
    ht.set(10, 'val');
    assert.ok(ht.has(10));
    assert.ok(!ht.has(20));
  });

  it('handles collisions via cuckoo eviction', () => {
    const ht = new CuckooHashTable(16); // Small capacity
    for (let i = 0; i < 12; i++) ht.set(i, i * 10);

    for (let i = 0; i < 12; i++) {
      assert.equal(ht.get(i), i * 10, `Key ${i} not found`);
    }
  });

  it('auto-resize on high load', () => {
    const ht = new CuckooHashTable(8);
    for (let i = 0; i < 20; i++) ht.set(i, i);

    assert.equal(ht.size, 20);
    assert.ok(ht.stats.resizes > 0);
    for (let i = 0; i < 20; i++) assert.equal(ht.get(i), i);
  });

  it('string keys', () => {
    const ht = new CuckooHashTable();
    ht.set('alice', 1);
    ht.set('bob', 2);
    ht.set('charlie', 3);

    assert.equal(ht.get('alice'), 1);
    assert.equal(ht.get('bob'), 2);
    assert.equal(ht.get('charlie'), 3);
  });

  it('1000 sequential inserts', () => {
    const ht = new CuckooHashTable(512);
    for (let i = 0; i < 1000; i++) ht.set(i, i * 7);

    assert.equal(ht.size, 1000);
    for (let i = 0; i < 1000; i++) {
      assert.equal(ht.get(i), i * 7);
    }
  });

  it('benchmark: cuckoo vs Map on 10K lookups', () => {
    const n = 10000;
    
    const ht = new CuckooHashTable(n * 2);
    const map = new Map();
    for (let i = 0; i < n; i++) {
      ht.set(i, i);
      map.set(i, i);
    }

    // Cuckoo lookup
    const t0 = Date.now();
    for (let i = 0; i < n; i++) ht.get(i);
    const cuckooMs = Date.now() - t0;

    // Map lookup
    const t1 = Date.now();
    for (let i = 0; i < n; i++) map.get(i);
    const mapMs = Date.now() - t1;

    console.log(`    Cuckoo: ${cuckooMs}ms vs Map: ${mapMs}ms | Load: ${ht.getStats().loadFactor}`);
  });

  it('stats tracked', () => {
    const ht = new CuckooHashTable(16);
    ht.set(1, 'a');
    ht.get(1);
    ht.get(999);

    const stats = ht.getStats();
    assert.equal(stats.inserts, 1);
    assert.equal(stats.lookups, 2);
    assert.ok(stats.loadFactor);
  });
});
