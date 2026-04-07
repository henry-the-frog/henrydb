// skip-list.test.js — Skip list tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SkipList } from './skip-list.js';

describe('SkipList', () => {
  it('insert and find', () => {
    const sl = new SkipList();
    sl.insert(5, 'five');
    sl.insert(3, 'three');
    sl.insert(7, 'seven');
    
    assert.equal(sl.find(5), 'five');
    assert.equal(sl.find(3), 'three');
    assert.equal(sl.find(7), 'seven');
    assert.equal(sl.find(99), null);
  });

  it('maintains sorted order', () => {
    const sl = new SkipList();
    const values = [50, 20, 80, 10, 40, 60, 30, 70, 90];
    for (const v of values) sl.insert(v, v);
    
    const sorted = [...sl].map(e => e.key);
    assert.deepEqual(sorted, [10, 20, 30, 40, 50, 60, 70, 80, 90]);
  });

  it('range scan', () => {
    const sl = new SkipList();
    for (let i = 1; i <= 20; i++) sl.insert(i, `val${i}`);
    
    const range = sl.range(5, 10);
    assert.equal(range.length, 6);
    assert.equal(range[0].key, 5);
    assert.equal(range[5].key, 10);
  });

  it('delete removes entries', () => {
    const sl = new SkipList();
    sl.insert(1, 'a');
    sl.insert(2, 'b');
    sl.insert(3, 'c');
    
    sl.delete(2);
    assert.equal(sl.find(2), null);
    assert.equal(sl.find(1), 'a');
    assert.equal(sl.find(3), 'c');
    assert.equal(sl.size, 2);
  });

  it('handles many entries', () => {
    const sl = new SkipList();
    for (let i = 0; i < 10000; i++) {
      sl.insert(i, i);
    }
    assert.equal(sl.size, 10000);
    assert.equal(sl.find(5000), 5000);
    assert.equal(sl.find(9999), 9999);
  });

  it('duplicate keys create multiple entries', () => {
    const sl = new SkipList();
    sl.insert(5, 'a');
    sl.insert(5, 'b');
    
    const results = sl.findAll(5);
    assert.equal(results.length, 2);
  });

  it('stats reports level distribution', () => {
    const sl = new SkipList();
    for (let i = 0; i < 100; i++) sl.insert(i, i);
    
    const stats = sl.stats();
    assert.equal(stats.size, 100);
    assert.ok(stats.maxLevel > 0);
    assert.ok(stats.levelCounts[0] === 100); // All nodes at level 0
  });
});
