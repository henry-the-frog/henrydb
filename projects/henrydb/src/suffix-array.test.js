// suffix-array.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SuffixArray } from './suffix-array.js';

describe('SuffixArray', () => {
  it('search finds all occurrences', () => {
    const sa = new SuffixArray('banana');
    const pos = sa.search('ana');
    assert.deepEqual(pos, [1, 3]);
  });

  it('no match returns empty', () => {
    const sa = new SuffixArray('hello world');
    assert.deepEqual(sa.search('xyz'), []);
  });

  it('getSuffix', () => {
    const sa = new SuffixArray('abc');
    // Sorted: abc, bc, c
    assert.equal(sa.getSuffix(0), 'abc');
  });

  it('LCP array', () => {
    const sa = new SuffixArray('banana');
    const lcp = sa.getLCPArray();
    assert.ok(lcp.some(v => v > 0)); // Some shared prefixes
  });

  it('use case: text search', () => {
    const text = 'the quick brown fox jumps over the lazy dog';
    const sa = new SuffixArray(text);
    
    assert.ok(sa.search('quick').length === 1);
    assert.ok(sa.search('the').length === 2);
    assert.ok(sa.search('cat').length === 0);
  });

  it('performance: search in 10K text', () => {
    const text = Array.from({ length: 10000 }, () => String.fromCharCode(97 + Math.floor(Math.random() * 26))).join('');
    const sa = new SuffixArray(text);
    
    const t0 = performance.now();
    for (let i = 0; i < 1000; i++) {
      sa.search(text.substring(i * 5, i * 5 + 3));
    }
    const elapsed = performance.now() - t0;
    console.log(`  1K searches in 10K text: ${elapsed.toFixed(1)}ms`);
  });
});
