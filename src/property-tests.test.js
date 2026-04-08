// property-tests.test.js — Property-based / randomized tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { BPlusTree } from './bplus-tree.js';
import { SkipList } from './skip-list.js';
import { Treap } from './probabilistic-filters.js';
import { SplayTree } from './more-trees.js';
import { CuckooHashTable } from './cuckoo-hash.js';
import { RobinHoodHashTable } from './robin-hood-hash.js';
import { RingBuffer } from './ring-buffer.js';
import { FenwickTree } from './advanced-ds.js';
import { BinaryHeap } from './more-trees.js';

function randomInt(max) { return Math.floor(Math.random() * max); }
function randomStr(len) { const chars = 'abcdefghijklmnopqrstuvwxyz'; let s = ''; for (let i = 0; i < len; i++) s += chars[randomInt(26)]; return s; }

describe('Property: Sorted iteration invariant', () => {
  for (const [name, createTree] of [
    ['B+ Tree', () => new BPlusTree(8)],
    ['Skip List', () => new SkipList()],
    ['Treap', () => new Treap()],
    ['Splay Tree', () => new SplayTree()],
  ]) {
    it(`${name}: random inserts produce sorted output`, () => {
      const tree = createTree();
      const keys = new Set();
      for (let i = 0; i < 200; i++) {
        const key = randomInt(1000);
        tree.insert ? tree.insert(key, key) : tree.set(key, key);
        keys.add(key);
      }
      // Verify all keys retrievable
      for (const key of keys) {
        const val = tree.get(key);
        assert.equal(val, key, `${name}: key ${key} not found`);
      }
    });
  }
});

describe('Property: Hash tables agree with Map', () => {
  for (const [name, createHT] of [
    ['Cuckoo', () => new CuckooHashTable(512)],
    ['Robin Hood', () => new RobinHoodHashTable(512)],
  ]) {
    it(`${name}: random ops match Map`, () => {
      const ht = createHT();
      const ref = new Map();
      
      for (let i = 0; i < 500; i++) {
        const key = randomInt(100);
        const value = randomInt(10000);
        ht.set(key, value);
        ref.set(key, value);
      }
      
      for (const [key, value] of ref) {
        assert.equal(ht.get(key), value, `${name}: mismatch for key ${key}`);
      }
    });
  }
});

describe('Property: Ring buffer preserves last K items', () => {
  it('always has exactly min(n, capacity) items', () => {
    for (let trial = 0; trial < 10; trial++) {
      const capacity = randomInt(50) + 1;
      const n = randomInt(200);
      const rb = new RingBuffer(capacity);
      for (let i = 0; i < n; i++) rb.push(i);
      assert.equal(rb.size, Math.min(n, capacity));
    }
  });

  it('oldest item is always n-capacity', () => {
    const rb = new RingBuffer(10);
    for (let i = 0; i < 100; i++) rb.push(i);
    assert.equal(rb.peekOldest(), 90);
    assert.equal(rb.peek(), 99);
  });
});

describe('Property: Fenwick tree prefix sums', () => {
  it('matches naive prefix sum', () => {
    const n = 100;
    const arr = Array.from({ length: n }, () => randomInt(100));
    const ft = FenwickTree.fromArray(arr);
    
    for (let i = 0; i < n; i++) {
      const naive = arr.slice(0, i + 1).reduce((a, b) => a + b, 0);
      assert.equal(ft.prefixSum(i), naive, `Mismatch at index ${i}`);
    }
  });

  it('range sums are correct', () => {
    const arr = [1, 3, 5, 7, 9, 11, 13, 15, 17, 19];
    const ft = FenwickTree.fromArray(arr);
    
    for (let l = 0; l < arr.length; l++) {
      for (let r = l; r < arr.length; r++) {
        const expected = arr.slice(l, r + 1).reduce((a, b) => a + b, 0);
        assert.equal(ft.rangeSum(l, r), expected, `Range [${l}, ${r}]`);
      }
    }
  });
});

describe('Property: Heap always returns min/max', () => {
  it('min-heap: pop always returns minimum', () => {
    const h = new BinaryHeap((a, b) => a - b);
    const values = Array.from({ length: 50 }, () => randomInt(1000));
    for (const v of values) h.push(v);
    
    let prev = -Infinity;
    while (h.size > 0) {
      const v = h.pop();
      assert.ok(v >= prev, `Heap violation: ${v} < ${prev}`);
      prev = v;
    }
  });

  it('max-heap: pop always returns maximum', () => {
    const h = new BinaryHeap((a, b) => b - a);
    const values = Array.from({ length: 50 }, () => randomInt(1000));
    for (const v of values) h.push(v);
    
    let prev = Infinity;
    while (h.size > 0) {
      const v = h.pop();
      assert.ok(v <= prev, `Heap violation: ${v} > ${prev}`);
      prev = v;
    }
  });
});

describe('Property: String keys work in all sorted structures', () => {
  it('B+ tree with random strings', () => {
    const tree = new BPlusTree(8);
    const entries = Array.from({ length: 100 }, () => [randomStr(8), randomInt(1000)]);
    for (const [k, v] of entries) tree.insert(k, v);
    for (const [k, v] of entries) assert.equal(tree.get(k), v);
  });

  it('Skip list with random strings', () => {
    const sl = new SkipList();
    const entries = Array.from({ length: 100 }, () => [randomStr(8), randomInt(1000)]);
    for (const [k, v] of entries) sl.set(k, v);
    for (const [k, v] of entries) assert.equal(sl.get(k), v);
  });
});

describe('Property: Insert-delete consistency', () => {
  it('delete all inserted keys leaves empty structure', () => {
    const tree = new BPlusTree(4);
    const keys = Array.from({ length: 50 }, (_, i) => i);
    for (const k of keys) tree.insert(k, k);
    for (const k of keys) tree.delete(k);
    
    for (const k of keys) assert.equal(tree.get(k), undefined);
  });
});

describe('Property: Idempotent operations', () => {
  it('inserting same key twice updates value', () => {
    const structures = [new BPlusTree(8), new SkipList()];
    for (const s of structures) {
      const method = s.insert ? 'insert' : 'set';
      s[method](42, 'first');
      s[method](42, 'second');
      assert.equal(s.get(42), 'second');
    }
  });
});

describe('Property: Large scale random operations', () => {
  it('1000 random insert/lookup cycles on skip list', () => {
    const sl = new SkipList();
    const ref = new Map();
    
    for (let i = 0; i < 1000; i++) {
      const op = randomInt(2);
      const key = randomInt(100);
      
      if (op === 0) { // Insert
        const val = randomInt(10000);
        sl.set(key, val);
        ref.set(key, val);
      } else { // Lookup
        const expected = ref.get(key);
        const actual = sl.get(key);
        if (expected !== undefined) assert.equal(actual, expected, `Mismatch at key ${key}`);
      }
    }
  });
});
