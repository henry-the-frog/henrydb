// probabilistic-filters.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Treap, CuckooFilter, XORFilter } from './probabilistic-filters.js';

describe('Treap', () => {
  it('insert/get', () => {
    const t = new Treap();
    t.insert(5, 'five'); t.insert(3, 'three'); t.insert(8, 'eight');
    assert.equal(t.get(5), 'five');
    assert.equal(t.get(3), 'three');
    assert.equal(t.get(99), undefined);
  });
  it('sorted order via inorder', () => {
    const t = new Treap();
    [5, 3, 8, 1, 4, 7, 9].forEach(k => t.insert(k, k));
    assert.deepEqual([...t.inorder()].map(e => e.key), [1, 3, 4, 5, 7, 8, 9]);
  });
  it('1000 inserts', () => {
    const t = new Treap();
    for (let i = 0; i < 1000; i++) t.insert(i, i);
    assert.equal(t.size, 1000);
    for (let i = 0; i < 1000; i++) assert.equal(t.get(i), i);
  });
  it('update existing', () => {
    const t = new Treap();
    t.insert(1, 'old'); t.insert(1, 'new');
    assert.equal(t.get(1), 'new');
    assert.equal(t.size, 1);
  });
});

describe('CuckooFilter', () => {
  it('insert and contains', () => {
    const cf = new CuckooFilter(256);
    cf.insert('hello'); cf.insert('world');
    assert.ok(cf.contains('hello'));
    assert.ok(cf.contains('world'));
  });
  it('delete support', () => {
    const cf = new CuckooFilter(256);
    cf.insert('temp');
    assert.ok(cf.contains('temp'));
    assert.ok(cf.delete('temp'));
    assert.ok(!cf.contains('temp'));
  });
  it('false positive rate low', () => {
    const cf = new CuckooFilter(4096, 4);
    for (let i = 0; i < 1000; i++) cf.insert(`key_${i}`);
    let fp = 0;
    for (let i = 1000; i < 2000; i++) { if (cf.contains(`key_${i}`)) fp++; }
    console.log(`    Cuckoo FP rate: ${(fp / 1000 * 100).toFixed(1)}%`);
    assert.ok(fp < 200); // < 20% FP rate
  });
  it('size tracking', () => {
    const cf = new CuckooFilter(256);
    cf.insert('a'); cf.insert('b');
    assert.equal(cf.size, 2);
  });
});

describe('XORFilter', () => {
  it('contains inserted keys', () => {
    const keys = ['a', 'b', 'c', 'd'];
    const xf = new XORFilter(keys);
    for (const k of keys) assert.ok(xf.contains(k));
  });
  it('empty filter', () => {
    const xf = new XORFilter([]);
    assert.ok(!xf.contains('anything'));
  });
});
