// wavelet-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { WaveletTree } from './wavelet-tree.js';

describe('WaveletTree', () => {
  const seq = ['a', 'b', 'r', 'a', 'c', 'a', 'd', 'a', 'b', 'r', 'a'];

  it('access', () => {
    const wt = new WaveletTree(seq);
    assert.equal(wt.access(0), 'a');
    assert.equal(wt.access(1), 'b');
    assert.equal(wt.access(2), 'r');
    assert.equal(wt.access(4), 'c');
  });

  it('rank', () => {
    const wt = new WaveletTree(seq);
    assert.equal(wt.rank('a', 11), 5); // 5 'a's in whole sequence
    assert.equal(wt.rank('a', 6), 3);  // 3 'a's in first 6 chars
    assert.equal(wt.rank('b', 11), 2); // 2 'b's total
    assert.equal(wt.rank('z', 11), 0); // 'z' not present
  });

  it('select', () => {
    const wt = new WaveletTree(seq);
    assert.equal(wt.select('a', 1), 0); // 1st 'a' at position 0
    assert.equal(wt.select('a', 2), 3); // 2nd 'a' at position 3
    assert.equal(wt.select('b', 1), 1); // 1st 'b' at position 1
    assert.equal(wt.select('z', 1), -1); // Not found
  });

  it('numeric sequence', () => {
    const nums = [3, 1, 4, 1, 5, 9, 2, 6];
    const wt = new WaveletTree(nums);
    assert.equal(wt.access(0), 3);
    assert.equal(wt.access(2), 4);
    assert.equal(wt.rank(1, 8), 2);
  });

  it('count distinct', () => {
    const wt = new WaveletTree(seq);
    assert.equal(wt.countDistinct(0, 10), 5); // a, b, c, d, r
    assert.equal(wt.countDistinct(0, 1), 2);  // a, b
  });

  it('single character', () => {
    const wt = new WaveletTree(['x', 'x', 'x']);
    assert.equal(wt.access(0), 'x');
    assert.equal(wt.rank('x', 3), 3);
    assert.equal(wt.select('x', 2), 1);
  });

  it('benchmark: 10K sequence', () => {
    const alpha = 'abcdefghij'.split('');
    const big = Array.from({ length: 10000 }, () => alpha[Math.floor(Math.random() * alpha.length)]);
    const wt = new WaveletTree(big, alpha);
    
    const t0 = Date.now();
    for (let i = 0; i < 1000; i++) wt.rank('a', Math.floor(Math.random() * 10000));
    console.log(`    Wavelet tree 10K: 1K rank queries in ${Date.now() - t0}ms`);
    assert.equal(wt.n, 10000);
  });
});
