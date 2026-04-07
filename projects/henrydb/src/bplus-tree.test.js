// bplus-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BPlusTree } from './bplus-tree.js';

describe('BPlusTree', () => {
  it('insert and find', () => {
    const tree = new BPlusTree(4);
    tree.insert(5, 'five');
    tree.insert(3, 'three');
    tree.insert(7, 'seven');
    
    assert.equal(tree.find(5), 'five');
    assert.equal(tree.find(3), 'three');
    assert.equal(tree.find(7), 'seven');
    assert.equal(tree.find(99), undefined);
  });

  it('ordered iteration', () => {
    const tree = new BPlusTree(4);
    [10, 5, 15, 3, 7, 12, 20, 1].forEach(k => tree.insert(k, `val_${k}`));
    
    const entries = tree.entries();
    const keys = entries.map(e => e.key);
    for (let i = 1; i < keys.length; i++) {
      assert.ok(keys[i] > keys[i - 1], `${keys[i]} should be > ${keys[i - 1]}`);
    }
  });

  it('range scan', () => {
    const tree = new BPlusTree(4);
    for (let i = 1; i <= 20; i++) tree.insert(i, i * 10);
    
    const result = tree.range(5, 10);
    assert.equal(result.length, 6); // 5,6,7,8,9,10
    assert.equal(result[0].key, 5);
    assert.equal(result[5].key, 10);
  });

  it('handles many insertions (forces splits)', () => {
    const tree = new BPlusTree(3); // Small order to force many splits
    for (let i = 0; i < 100; i++) tree.insert(i, `val_${i}`);
    
    assert.equal(tree.size, 100);
    // Verify all keys retrievable
    for (let i = 0; i < 100; i++) {
      assert.equal(tree.find(i), `val_${i}`);
    }
  });

  it('random insertion order', () => {
    const tree = new BPlusTree(4);
    const keys = Array.from({ length: 50 }, (_, i) => i);
    // Shuffle
    for (let i = keys.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [keys[i], keys[j]] = [keys[j], keys[i]];
    }
    
    for (const k of keys) tree.insert(k, k * 2);
    
    const entries = tree.entries();
    assert.equal(entries.length, 50);
    // Check ordering
    for (let i = 1; i < entries.length; i++) {
      assert.ok(entries[i].key > entries[i - 1].key);
    }
  });

  it('update existing key', () => {
    const tree = new BPlusTree(4);
    tree.insert(1, 'old');
    tree.insert(1, 'new');
    assert.equal(tree.find(1), 'new');
  });

  it('delete', () => {
    const tree = new BPlusTree(4);
    tree.insert(1, 'a');
    tree.insert(2, 'b');
    tree.insert(3, 'c');
    
    assert.ok(tree.delete(2));
    assert.equal(tree.find(2), undefined);
    assert.equal(tree.find(1), 'a');
    assert.equal(tree.find(3), 'c');
  });

  it('delete nonexistent key', () => {
    const tree = new BPlusTree(4);
    tree.insert(1, 'a');
    assert.ok(!tree.delete(99));
  });

  it('range scan empty range', () => {
    const tree = new BPlusTree(4);
    for (let i = 1; i <= 10; i++) tree.insert(i, i);
    
    const result = tree.range(50, 100);
    assert.equal(result.length, 0);
  });

  it('leaf linking for sequential scan', () => {
    const tree = new BPlusTree(3);
    for (let i = 1; i <= 20; i++) tree.insert(i, i);
    
    // Full scan should return all entries in order
    const all = tree.entries();
    assert.equal(all.length, 20);
    assert.equal(all[0].key, 1);
    assert.equal(all[19].key, 20);
  });

  it('1000 entries performance', () => {
    const tree = new BPlusTree(32);
    const start = performance.now();
    for (let i = 0; i < 1000; i++) tree.insert(i, i);
    const insertTime = performance.now() - start;
    
    const lookupStart = performance.now();
    for (let i = 0; i < 1000; i++) assert.equal(tree.find(i), i);
    const lookupTime = performance.now() - lookupStart;
    
    assert.ok(insertTime < 500, `Insert too slow: ${insertTime}ms`);
    assert.ok(lookupTime < 500, `Lookup too slow: ${lookupTime}ms`);
  });
});
