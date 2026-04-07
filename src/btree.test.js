import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BPlusTree } from './btree.js';

describe('B+ Tree', () => {
  describe('Basic operations', () => {
    it('insert and search', () => {
      const tree = new BPlusTree(4);
      tree.insert(5, 'five');
      tree.insert(3, 'three');
      tree.insert(7, 'seven');
      assert.equal(tree.search(5), 'five');
      assert.equal(tree.search(3), 'three');
      assert.equal(tree.search(7), 'seven');
    });

    it('returns undefined for missing key', () => {
      const tree = new BPlusTree(4);
      tree.insert(1, 'one');
      assert.equal(tree.search(99), undefined);
    });

    it('updates existing key', () => {
      const tree = new BPlusTree(4);
      tree.insert(1, 'old');
      tree.insert(1, 'new');
      assert.equal(tree.search(1), 'new');
      assert.equal(tree.size, 1);
    });

    it('delete key', () => {
      const tree = new BPlusTree(4);
      tree.insert(1, 'a');
      tree.insert(2, 'b');
      assert.ok(tree.delete(1));
      assert.equal(tree.search(1), undefined);
      assert.equal(tree.search(2), 'b');
    });

    it('delete nonexistent returns false', () => {
      const tree = new BPlusTree(4);
      assert.ok(!tree.delete(99));
    });
  });

  describe('Splitting', () => {
    it('handles leaf split', () => {
      const tree = new BPlusTree(4);
      for (let i = 0; i < 10; i++) tree.insert(i, `val${i}`);
      for (let i = 0; i < 10; i++) assert.equal(tree.search(i), `val${i}`);
    });

    it('handles internal node split', () => {
      const tree = new BPlusTree(4);
      for (let i = 0; i < 50; i++) tree.insert(i, i * 10);
      for (let i = 0; i < 50; i++) assert.equal(tree.search(i), i * 10);
      assert.ok(tree.height >= 2);
    });

    it('handles many inserts', () => {
      const tree = new BPlusTree(8);
      for (let i = 0; i < 1000; i++) tree.insert(i, i);
      assert.equal(tree.size, 1000);
      for (let i = 0; i < 1000; i++) assert.equal(tree.search(i), i);
    });

    it('random order inserts', () => {
      const tree = new BPlusTree(4);
      const keys = Array.from({ length: 100 }, (_, i) => i);
      // Shuffle
      for (let i = keys.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [keys[i], keys[j]] = [keys[j], keys[i]];
      }
      for (const k of keys) tree.insert(k, k * 2);
      for (let i = 0; i < 100; i++) assert.equal(tree.search(i), i * 2);
    });
  });

  describe('Range scan', () => {
    it('returns results in range', () => {
      const tree = new BPlusTree(4);
      for (let i = 0; i < 20; i++) tree.insert(i, `v${i}`);
      const results = tree.range(5, 10);
      assert.equal(results.length, 6);
      assert.equal(results[0].key, 5);
      assert.equal(results[5].key, 10);
    });

    it('empty range', () => {
      const tree = new BPlusTree(4);
      for (let i = 0; i < 10; i++) tree.insert(i, i);
      assert.deepStrictEqual(tree.range(20, 30), []);
    });

    it('single element range', () => {
      const tree = new BPlusTree(4);
      for (let i = 0; i < 10; i++) tree.insert(i, i);
      const results = tree.range(5, 5);
      assert.equal(results.length, 1);
      assert.equal(results[0].key, 5);
    });
  });

  describe('Scan', () => {
    it('scans all in order', () => {
      const tree = new BPlusTree(4);
      const vals = [5, 2, 8, 1, 9, 3, 7, 4, 6, 0];
      for (const v of vals) tree.insert(v, v);
      const scanned = [...tree.scan()];
      assert.equal(scanned.length, 10);
      for (let i = 0; i < 10; i++) assert.equal(scanned[i].key, i);
    });

    it('empty tree scan', () => {
      const tree = new BPlusTree(4);
      assert.deepStrictEqual([...tree.scan()], []);
    });
  });

  describe('Metadata', () => {
    it('size tracks correctly', () => {
      const tree = new BPlusTree(4);
      assert.equal(tree.size, 0);
      tree.insert(1, 'a');
      tree.insert(2, 'b');
      assert.equal(tree.size, 2);
    });

    it('height grows with data', () => {
      const tree = new BPlusTree(4);
      assert.equal(tree.height, 1);
      for (let i = 0; i < 100; i++) tree.insert(i, i);
      assert.ok(tree.height >= 3);
    });

    it('min and max', () => {
      const tree = new BPlusTree(4);
      for (let i = 10; i <= 50; i++) tree.insert(i, i);
      assert.equal(tree.min().key, 10);
      assert.equal(tree.max().key, 50);
    });

    it('min/max on empty tree', () => {
      const tree = new BPlusTree(4);
      assert.equal(tree.min(), null);
      assert.equal(tree.max(), null);
    });
  });

  describe('Bulk load', () => {
    it('loads sorted data', () => {
      const entries = Array.from({ length: 100 }, (_, i) => ({ key: i, value: `v${i}` }));
      const tree = BPlusTree.bulkLoad(entries, 8);
      assert.equal(tree.size, 100);
      assert.equal(tree.search(50), 'v50');
    });
  });

  describe('String keys', () => {
    it('works with string keys', () => {
      const tree = new BPlusTree(4);
      tree.insert('banana', 2);
      tree.insert('apple', 1);
      tree.insert('cherry', 3);
      assert.equal(tree.search('apple'), 1);
      assert.equal(tree.search('banana'), 2);
      const all = [...tree.scan()];
      assert.equal(all[0].key, 'apple');
      assert.equal(all[1].key, 'banana');
      assert.equal(all[2].key, 'cherry');
    });
  });
});
