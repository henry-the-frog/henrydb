// more-ds.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { topologicalSort, LFUCache, kmpSearch, murmur3 } from './more-ds.js';

describe('TopologicalSort', () => {
  it('simple DAG', () => {
    const graph = new Map([['a', new Set()], ['b', new Set(['a'])], ['c', new Set(['a', 'b'])]]);
    const order = topologicalSort(graph);
    assert.ok(order.indexOf('a') < order.indexOf('b'));
    assert.ok(order.indexOf('b') < order.indexOf('c'));
  });
  it('detects cycle', () => {
    const graph = new Map([['a', new Set(['b'])], ['b', new Set(['a'])]]);
    assert.equal(topologicalSort(graph), null);
  });
  it('independent nodes', () => {
    const graph = new Map([['a', new Set()], ['b', new Set()], ['c', new Set()]]);
    assert.equal(topologicalSort(graph).length, 3);
  });
});

describe('LFUCache', () => {
  it('basic set/get', () => {
    const c = new LFUCache(3);
    c.set('a', 1); c.set('b', 2);
    assert.equal(c.get('a'), 1);
  });
  it('evicts least frequent', () => {
    const c = new LFUCache(2);
    c.set('a', 1); c.set('b', 2);
    c.get('a'); // freq: a=2, b=1
    c.set('c', 3); // Evict b (least frequent)
    assert.equal(c.get('b'), undefined);
    assert.equal(c.get('a'), 1);
  });
  it('capacity 0', () => {
    const c = new LFUCache(0);
    c.set('a', 1);
    assert.equal(c.get('a'), undefined);
  });
});

describe('KMP', () => {
  it('finds all occurrences', () => {
    assert.deepEqual(kmpSearch('abcabcabc', 'abc'), [0, 3, 6]);
  });
  it('overlapping matches', () => {
    assert.deepEqual(kmpSearch('aaaa', 'aa'), [0, 1, 2]);
  });
  it('no match', () => {
    assert.deepEqual(kmpSearch('hello', 'xyz'), []);
  });
  it('single char', () => {
    assert.deepEqual(kmpSearch('banana', 'a'), [1, 3, 5]);
  });
});

describe('Murmur3', () => {
  it('produces 32-bit hash', () => {
    const h = murmur3('hello');
    assert.ok(h >= 0 && h <= 0xFFFFFFFF);
  });
  it('different inputs produce different hashes', () => {
    assert.notEqual(murmur3('hello'), murmur3('world'));
  });
  it('deterministic', () => {
    assert.equal(murmur3('test'), murmur3('test'));
  });
  it('seed changes output', () => {
    assert.notEqual(murmur3('key', 0), murmur3('key', 42));
  });
  it('benchmark: 100K hashes', () => {
    const t0 = Date.now();
    for (let i = 0; i < 100000; i++) murmur3(`key_${i}`);
    console.log(`    100K murmur3: ${Date.now() - t0}ms`);
  });
});
