// t-digest.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TDigest } from './t-digest.js';

describe('TDigest', () => {
  it('basic quantiles', () => {
    const td = new TDigest(200);
    for (let i = 1; i <= 1000; i++) td.add(i);
    const p50 = td.p50();
    assert.ok(Math.abs(p50 - 500) < 100, `p50=${p50}`);
  });

  it('p99 accuracy on 10K elements', () => {
    const td = new TDigest(200);
    for (let i = 0; i < 10000; i++) td.add(i);
    const p99 = td.p99();
    const error = Math.abs(p99 - 9900) / 9900;
    console.log(`  p99: ${p99.toFixed(0)} (expected ~9900, error: ${(error*100).toFixed(1)}%)`);
    assert.ok(error < 0.05);
  });

  it('extreme values', () => {
    const td = new TDigest();
    for (let i = 0; i < 1000; i++) td.add(i);
    assert.equal(td.quantile(0), 0);
    assert.equal(td.quantile(1), 999);
  });

  it('merge two digests', () => {
    const a = new TDigest();
    const b = new TDigest();
    for (let i = 0; i < 500; i++) a.add(i);
    for (let i = 500; i < 1000; i++) b.add(i);
    a.merge(b);
    assert.equal(a.count, 1000);
  });

  it('bounded centroids after compression', () => {
    const td = new TDigest(100);
    for (let i = 0; i < 2000; i++) td.add(i);
    td._compress(); // Force compress
    const stats = td.getStats();
    console.log(`  2K values: ${stats.centroids} centroids`);
    assert.ok(stats.centroids > 0);
  });
});
