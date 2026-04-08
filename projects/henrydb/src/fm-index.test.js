// fm-index.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { FMIndex, SparseIndex } from './fm-index.js';

describe('FMIndex', () => {
  it('count occurrences', () => {
    const fm = new FMIndex('abracadabra');
    assert.equal(fm.count('abra'), 2);
    assert.equal(fm.count('bra'), 2);
    assert.equal(fm.count('a'), 5);
    assert.equal(fm.count('xyz'), 0);
  });

  it('locate positions', () => {
    const fm = new FMIndex('abracadabra');
    const positions = fm.locate('abra').sort((a, b) => a - b);
    assert.deepEqual(positions, [0, 7]);
  });

  it('single character', () => {
    const fm = new FMIndex('banana');
    assert.equal(fm.count('a'), 3);
    assert.equal(fm.count('n'), 2);
    assert.equal(fm.count('b'), 1);
  });

  it('full text match', () => {
    const fm = new FMIndex('hello');
    assert.equal(fm.count('hello'), 1);
    assert.deepEqual(fm.locate('hello'), [0]);
  });

  it('locate returns empty for no match', () => {
    const fm = new FMIndex('test');
    assert.deepEqual(fm.locate('xyz'), []);
  });

  it('benchmark: 1K text, 100 queries', () => {
    const text = 'the quick brown fox jumps over the lazy dog '.repeat(25);
    const fm = new FMIndex(text);
    const t0 = Date.now();
    for (let i = 0; i < 100; i++) fm.count('the');
    console.log(`    FM-index 1K text, 100 queries: ${Date.now() - t0}ms`);
    assert.ok(fm.count('the') > 0);
  });
});

describe('SparseIndex', () => {
  it('build from sorted data', () => {
    const data = Array.from({ length: 500 }, (_, i) => ({ key: i, value: `v${i}` }));
    const idx = SparseIndex.build(data, r => r.key, 100);
    assert.equal(idx.blockCount, 5);
  });

  it('lookup key', () => {
    const data = Array.from({ length: 500 }, (_, i) => ({ key: i * 10, value: `v${i}` }));
    const idx = SparseIndex.build(data, r => r.key, 100);
    const blocks = idx.lookup(2500);
    assert.ok(blocks.length > 0);
    assert.ok(blocks[0].minKey <= 2500);
    assert.ok(blocks[0].maxKey >= 2500);
  });

  it('range blocks', () => {
    const data = Array.from({ length: 1000 }, (_, i) => ({ key: i }));
    const idx = SparseIndex.build(data, r => r.key, 100);
    const blocks = idx.rangeBlocks(150, 350);
    assert.ok(blocks.length >= 2);
  });

  it('lookup misses when key not in range', () => {
    const data = Array.from({ length: 100 }, (_, i) => ({ key: i }));
    const idx = SparseIndex.build(data, r => r.key, 100);
    assert.equal(idx.lookup(500).length, 0);
  });
});
