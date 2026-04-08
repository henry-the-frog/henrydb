// sampling.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ReservoirSampler, MinHash } from './sampling.js';

describe('ReservoirSampler', () => {
  it('exact sample when stream < k', () => {
    const rs = new ReservoirSampler(10);
    for (let i = 0; i < 5; i++) rs.add(i);
    assert.equal(rs.sample.length, 5);
  });

  it('samples k from larger stream', () => {
    const rs = new ReservoirSampler(100);
    for (let i = 0; i < 10000; i++) rs.add(i);
    assert.equal(rs.sample.length, 100);
    assert.equal(rs.count, 10000);
  });

  it('roughly uniform distribution', () => {
    const k = 1000, n = 10000;
    const counts = new Array(10).fill(0);
    for (let trial = 0; trial < 10; trial++) {
      const rs = new ReservoirSampler(k);
      for (let i = 0; i < n; i++) rs.add(i);
      for (const v of rs.sample) counts[Math.floor(v / (n / 10))]++;
    }
    // Each bucket should get roughly k/10 * 10 = 1000
    assert.ok(counts.every(c => c > 500 && c < 1500));
  });
});

describe('MinHash', () => {
  it('identical sets have similarity ~1', () => {
    const mh = new MinHash(256);
    const set = ['a', 'b', 'c', 'd', 'e'];
    const s1 = mh.signature(set);
    const s2 = mh.signature(set);
    assert.ok(mh.similarity(s1, s2) > 0.99);
  });

  it('disjoint sets have similarity ~0', () => {
    const mh = new MinHash(256);
    const s1 = mh.signature(['a', 'b', 'c']);
    const s2 = mh.signature(['x', 'y', 'z']);
    assert.ok(mh.similarity(s1, s2) < 0.1);
  });

  it('overlapping sets have intermediate similarity', () => {
    const mh = new MinHash(256);
    const a = ['a', 'b', 'c', 'd'];
    const b = ['c', 'd', 'e', 'f'];
    // Jaccard: |A∩B|/|A∪B| = 2/6 ≈ 0.33
    const sim = mh.similarity(mh.signature(a), mh.signature(b));
    assert.ok(sim > 0.15 && sim < 0.55, `Similarity ${sim} out of range`);
  });
});
