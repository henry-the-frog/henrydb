// tdigest.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TDigest } from './tdigest.js';

describe('TDigest', () => {
  it('basic percentiles on uniform distribution', () => {
    const td = new TDigest(200);
    for (let i = 0; i < 10000; i++) td.add(i);

    const p50 = td.percentile(50);
    const p95 = td.percentile(95);
    const p99 = td.percentile(99);

    console.log(`    P50: ${p50.toFixed(0)} (expected ~5000), P95: ${p95.toFixed(0)} (expected ~9500), P99: ${p99.toFixed(0)} (expected ~9900)`);
    assert.ok(Math.abs(p50 - 5000) < 500, `P50 too far: ${p50}`);
    assert.ok(Math.abs(p95 - 9500) < 500, `P95 too far: ${p95}`);
    assert.ok(Math.abs(p99 - 9900) < 300, `P99 too far: ${p99}`);
  });

  it('handles normal-like distribution', () => {
    const td = new TDigest(200);
    // Box-Muller transform for normal distribution
    for (let i = 0; i < 10000; i++) {
      const u = Math.random();
      const v = Math.random();
      const z = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
      td.add(z * 100 + 500); // mean=500, std=100
    }

    const p50 = td.percentile(50);
    assert.ok(Math.abs(p50 - 500) < 50, `P50 too far from mean: ${p50}`);
  });

  it('min and max', () => {
    const td = new TDigest();
    td.add(10);
    td.add(90);
    td.add(50);
    assert.equal(td.min, 10);
    assert.equal(td.max, 90);
  });

  it('empty returns null', () => {
    const td = new TDigest();
    assert.equal(td.quantile(0.5), null);
  });

  it('single element', () => {
    const td = new TDigest();
    td.add(42);
    assert.equal(td.percentile(50), 42);
  });

  it('merge two digests', () => {
    const a = new TDigest(100);
    const b = new TDigest(100);
    for (let i = 0; i < 5000; i++) a.add(i);
    for (let i = 5000; i < 10000; i++) b.add(i);

    a.merge(b);
    const p50 = a.percentile(50);
    assert.ok(Math.abs(p50 - 5000) < 1000, `Merged P50: ${p50}`);
    assert.equal(a.count, 10000);
  });

  it('centroids bounded by compression', () => {
    const td = new TDigest(50);
    for (let i = 0; i < 100000; i++) td.add(Math.random() * 1000);
    assert.ok(td.centroidCount < 500, `Too many centroids: ${td.centroidCount}`);
  });

  it('benchmark: 100K values', () => {
    const td = new TDigest(200);
    const t0 = Date.now();
    for (let i = 0; i < 100000; i++) td.add(Math.random() * 10000);
    const ms = Date.now() - t0;
    console.log(`    100K inserts: ${ms}ms, ${td.centroidCount} centroids`);
    assert.ok(ms < 10000);
  });
});
