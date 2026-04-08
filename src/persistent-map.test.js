// persistent-map.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PersistentSortedMap } from './persistent-map.js';

describe('PersistentSortedMap', () => {
  it('set and get', () => {
    const m = new PersistentSortedMap().set(5, 'five').set(3, 'three').set(7, 'seven');
    assert.equal(m.get(5), 'five');
    assert.equal(m.get(3), 'three');
    assert.equal(m.get(99), undefined);
  });

  it('immutability — old map unchanged', () => {
    const m1 = new PersistentSortedMap().set(1, 'one');
    const m2 = m1.set(2, 'two');
    assert.equal(m1.size, 1);
    assert.equal(m2.size, 2);
    assert.equal(m1.get(2), undefined);
    assert.equal(m2.get(2), 'two');
  });

  it('update value', () => {
    const m1 = new PersistentSortedMap().set(1, 'old');
    const m2 = m1.set(1, 'new');
    assert.equal(m1.get(1), 'old');
    assert.equal(m2.get(1), 'new');
  });

  it('sorted iteration', () => {
    let m = new PersistentSortedMap();
    [5, 3, 7, 1, 4].forEach(k => m = m.set(k, k * 10));
    const keys = [...m.keys()];
    assert.deepEqual(keys, [1, 3, 4, 5, 7]);
  });

  it('toObject', () => {
    const m = new PersistentSortedMap().set('a', 1).set('b', 2);
    assert.deepEqual(m.toObject(), { a: 1, b: 2 });
  });

  it('has', () => {
    const m = new PersistentSortedMap().set(1, 'x');
    assert.equal(m.has(1), true);
    assert.equal(m.has(2), false);
  });

  it('structural sharing — many versions', () => {
    let versions = [new PersistentSortedMap()];
    for (let i = 0; i < 100; i++) {
      versions.push(versions[i].set(i, i));
    }
    assert.equal(versions[0].size, 0);
    assert.equal(versions[50].size, 50);
    assert.equal(versions[100].size, 100);
    assert.equal(versions[100].get(50), 50);
    assert.equal(versions[50].get(99), undefined);
  });

  it('benchmark: 10K inserts', () => {
    let m = new PersistentSortedMap();
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) m = m.set(i, i);
    const insertMs = Date.now() - t0;
    
    const t1 = Date.now();
    for (let i = 0; i < 10000; i++) m.get(i);
    console.log(`    Persistent map 10K: insert=${insertMs}ms, get=${Date.now() - t1}ms, size=${m.size}`);
    assert.equal(m.size, 10000);
  });
});
