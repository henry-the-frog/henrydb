// more-joins.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { NestedHashJoin, HashIndex, FractionalCascading } from './more-joins.js';

describe('NestedHashJoin', () => {
  it('single-pass join', () => {
    const build = [{ id: 1, name: 'A' }, { id: 2, name: 'B' }];
    const probe = [{ oid: 1, id: 1, amt: 100 }, { oid: 2, id: 2, amt: 200 }];
    const nhj = new NestedHashJoin(100);
    const results = nhj.join(build, probe, 'id', 'id');
    assert.equal(results.length, 2);
  });

  it('multi-pass join', () => {
    const build = Array.from({ length: 100 }, (_, i) => ({ id: i, name: `n${i}` }));
    const probe = Array.from({ length: 50 }, (_, i) => ({ id: i * 2, val: i }));
    const nhj = new NestedHashJoin(20); // 5 passes
    const results = nhj.join(build, probe, 'id', 'id');
    assert.equal(results.length, 50);
  });

  it('no matches', () => {
    const nhj = new NestedHashJoin(100);
    const results = nhj.join([{ id: 1 }], [{ id: 2 }], 'id', 'id');
    assert.equal(results.length, 0);
  });

  it('many-to-many', () => {
    const build = [{ id: 1, tag: 'a' }, { id: 1, tag: 'b' }];
    const probe = [{ id: 1, val: 1 }, { id: 1, val: 2 }];
    const nhj = new NestedHashJoin(100);
    assert.equal(nhj.join(build, probe, 'id', 'id').length, 4); // 2×2
  });
});

describe('HashIndex', () => {
  it('insert and lookup', () => {
    const idx = new HashIndex();
    idx.insert('a', 0);
    idx.insert('a', 5);
    idx.insert('b', 1);
    assert.deepEqual(idx.lookup('a'), [0, 5]);
    assert.deepEqual(idx.lookup('b'), [1]);
    assert.deepEqual(idx.lookup('c'), []);
  });

  it('delete', () => {
    const idx = new HashIndex();
    idx.insert('a', 0);
    assert.equal(idx.delete('a'), true);
    assert.deepEqual(idx.lookup('a'), []);
    assert.equal(idx.delete('missing'), false);
  });

  it('buildFromRows', () => {
    const rows = [{ name: 'Alice' }, { name: 'Bob' }, { name: 'Alice' }];
    const idx = HashIndex.buildFromRows(rows, 'name');
    assert.deepEqual(idx.lookup('Alice'), [0, 2]);
  });

  it('load factor', () => {
    const idx = new HashIndex(10);
    for (let i = 0; i < 20; i++) idx.insert(`k${i}`, i);
    assert.equal(idx.size, 20);
    assert.equal(idx.loadFactor, 2);
  });

  it('benchmark: 50K inserts + lookups', () => {
    const idx = new HashIndex(1024);
    const t0 = Date.now();
    for (let i = 0; i < 50000; i++) idx.insert(`key_${i}`, i);
    const t1 = Date.now();
    for (let i = 0; i < 50000; i++) idx.lookup(`key_${i}`);
    console.log(`    HashIndex 50K: insert=${t1 - t0}ms, lookup=${Date.now() - t1}ms`);
    assert.equal(idx.size, 50000);
  });
});

describe('FractionalCascading', () => {
  it('search across multiple lists', () => {
    const lists = [[1, 3, 5, 7], [2, 3, 6, 8], [3, 4, 5, 9]];
    const fc = new FractionalCascading(lists);
    const results = fc.search(3);
    assert.equal(results.length, 3); // 3 appears in all lists
  });

  it('search missing value', () => {
    const fc = new FractionalCascading([[1, 2, 3], [4, 5, 6]]);
    assert.equal(fc.search(10).length, 0);
  });

  it('search in subset', () => {
    const fc = new FractionalCascading([[1, 5, 10], [2, 5, 8], [3, 7, 10]]);
    const results = fc.search(5);
    assert.equal(results.length, 2); // In lists 0 and 1
  });
});
