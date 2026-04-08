// skip-list.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SkipList } from './skip-list.js';

describe('SkipList', () => {
  it('basic set/get', () => {
    const sl = new SkipList();
    sl.set(5, 'five');
    sl.set(3, 'three');
    sl.set(8, 'eight');
    assert.equal(sl.get(5), 'five');
    assert.equal(sl.get(3), 'three');
    assert.equal(sl.get(8), 'eight');
    assert.equal(sl.get(999), undefined);
  });

  it('update existing key', () => {
    const sl = new SkipList();
    sl.set(1, 'old');
    sl.set(1, 'new');
    assert.equal(sl.get(1), 'new');
    assert.equal(sl.size, 1);
  });

  it('delete', () => {
    const sl = new SkipList();
    sl.set(1, 'a');
    sl.set(2, 'b');
    assert.ok(sl.delete(1));
    assert.equal(sl.get(1), undefined);
    assert.equal(sl.size, 1);
    assert.ok(!sl.delete(999));
  });

  it('range scan', () => {
    const sl = new SkipList();
    for (let i = 0; i < 100; i++) sl.set(i, i * 10);

    const range = sl.range(25, 30);
    assert.equal(range.length, 6); // 25,26,27,28,29,30
    assert.equal(range[0].key, 25);
    assert.equal(range[5].key, 30);
  });

  it('sorted iteration', () => {
    const sl = new SkipList();
    sl.set(50, 'c');
    sl.set(10, 'a');
    sl.set(30, 'b');

    const entries = [...sl];
    assert.deepEqual(entries.map(e => e.key), [10, 30, 50]);
  });

  it('first and last', () => {
    const sl = new SkipList();
    sl.set(50, 'c');
    sl.set(10, 'a');
    sl.set(30, 'b');
    assert.equal(sl.first().key, 10);
    assert.equal(sl.last().key, 50);
  });

  it('string keys', () => {
    const sl = new SkipList();
    sl.set('banana', 2);
    sl.set('apple', 1);
    sl.set('cherry', 3);
    assert.equal(sl.get('apple'), 1);
    assert.deepEqual([...sl].map(e => e.key), ['apple', 'banana', 'cherry']);
  });

  it('10K inserts + lookups', () => {
    const sl = new SkipList();
    for (let i = 0; i < 10000; i++) sl.set(i, i);

    assert.equal(sl.size, 10000);
    for (let i = 0; i < 10000; i++) assert.equal(sl.get(i), i);
  });

  it('benchmark vs Map on 50K entries', () => {
    const n = 50000;
    const sl = new SkipList();
    const map = new Map();

    const t0 = Date.now();
    for (let i = 0; i < n; i++) sl.set(i, i);
    const slBuild = Date.now() - t0;

    const t1 = Date.now();
    for (let i = 0; i < n; i++) map.set(i, i);
    const mapBuild = Date.now() - t1;

    const t2 = Date.now();
    for (let i = 0; i < n; i++) sl.get(i);
    const slLookup = Date.now() - t2;

    const t3 = Date.now();
    for (let i = 0; i < n; i++) map.get(i);
    const mapLookup = Date.now() - t3;

    console.log(`    Build: SL ${slBuild}ms vs Map ${mapBuild}ms | Lookup: SL ${slLookup}ms vs Map ${mapLookup}ms`);
    console.log(`    Skip list levels: ${sl.level}, avg comparisons/search: ${sl.getStats().avgComparisonsPerSearch}`);
  });
});
