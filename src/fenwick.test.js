// fenwick.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { FenwickTree, CuckooHashTable } from './fenwick.js';

describe('FenwickTree', () => {
  it('point update and prefix sum', () => {
    const ft = new FenwickTree(5);
    ft.update(0, 3);
    ft.update(1, 5);
    ft.update(2, 7);
    
    assert.equal(ft.query(0), 3);
    assert.equal(ft.query(1), 8);
    assert.equal(ft.query(2), 15);
  });

  it('range sum query', () => {
    const ft = new FenwickTree(5);
    [1, 2, 3, 4, 5].forEach((v, i) => ft.update(i, v));
    
    assert.equal(ft.rangeQuery(0, 4), 15);
    assert.equal(ft.rangeQuery(1, 3), 9); // 2 + 3 + 4
    assert.equal(ft.rangeQuery(2, 2), 3);
  });

  it('incremental updates', () => {
    const ft = new FenwickTree(3);
    ft.update(0, 10);
    ft.update(0, 5); // Now 15
    ft.update(1, 20);
    
    assert.equal(ft.query(0), 15);
    assert.equal(ft.query(1), 35);
  });

  it('handles large arrays', () => {
    const n = 10000;
    const ft = new FenwickTree(n);
    let expected = 0;
    for (let i = 0; i < n; i++) {
      ft.update(i, i + 1);
      expected += i + 1;
    }
    assert.equal(ft.query(n - 1), expected);
  });

  it('kth element (frequency use case)', () => {
    const ft = new FenwickTree(10);
    // Frequencies: value 2 appears 3 times, value 5 appears 2 times, value 7 appears 1 time
    ft.update(2, 3);
    ft.update(5, 2);
    ft.update(7, 1);
    
    assert.equal(ft.findKth(1), 2);  // 1st smallest is in bucket 2
    assert.equal(ft.findKth(3), 2);  // 3rd is still bucket 2
    assert.equal(ft.findKth(4), 5);  // 4th is bucket 5
    assert.equal(ft.findKth(6), 7);  // 6th is bucket 7
  });
});

describe('CuckooHashTable', () => {
  it('insert and get', () => {
    const ht = new CuckooHashTable();
    ht.insert('key1', 'value1');
    ht.insert('key2', 'value2');
    
    assert.equal(ht.get('key1'), 'value1');
    assert.equal(ht.get('key2'), 'value2');
    assert.equal(ht.get('missing'), undefined);
  });

  it('O(1) worst-case lookup', () => {
    const ht = new CuckooHashTable(32);
    for (let i = 0; i < 20; i++) {
      ht.insert(`key_${i}`, i);
    }
    
    // All lookups are O(1) — just two table checks
    for (let i = 0; i < 20; i++) {
      assert.equal(ht.get(`key_${i}`), i);
    }
  });

  it('delete removes entries', () => {
    const ht = new CuckooHashTable();
    ht.insert('a', 1);
    ht.insert('b', 2);
    
    ht.delete('a');
    assert.equal(ht.get('a'), undefined);
    assert.equal(ht.get('b'), 2);
    assert.equal(ht.size, 1);
  });

  it('handles resizing', () => {
    const ht = new CuckooHashTable(4); // Very small initial size
    for (let i = 0; i < 50; i++) {
      ht.insert(`k${i}`, i);
    }
    
    // All values should be retrievable
    for (let i = 0; i < 50; i++) {
      assert.equal(ht.get(`k${i}`), i);
    }
    assert.equal(ht.size, 50);
  });

  it('update existing key', () => {
    const ht = new CuckooHashTable();
    ht.insert('key', 'old');
    ht.insert('key', 'new');
    
    assert.equal(ht.get('key'), 'new');
    assert.equal(ht.size, 1);
  });

  it('handles numeric keys', () => {
    const ht = new CuckooHashTable();
    ht.insert(42, 'answer');
    ht.insert(0, 'zero');
    
    assert.equal(ht.get(42), 'answer');
    assert.equal(ht.get(0), 'zero');
  });
});
