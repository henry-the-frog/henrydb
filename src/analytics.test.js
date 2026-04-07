// analytics.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TDigest, SegmentTree } from './analytics.js';

describe('TDigest', () => {
  it('estimates median of uniform data', () => {
    const td = new TDigest();
    for (let i = 1; i <= 1000; i++) td.add(i);
    
    const p50 = td.p50();
    assert.ok(p50 > 400 && p50 < 600, `P50 = ${p50}, expected ~500`);
  });

  it('estimates P95 correctly', () => {
    const td = new TDigest();
    for (let i = 1; i <= 1000; i++) td.add(i);
    
    const p95 = td.p95();
    assert.ok(p95 > 900 && p95 < 1000, `P95 = ${p95}, expected ~950`);
  });

  it('handles single value', () => {
    const td = new TDigest();
    td.add(42);
    assert.equal(td.p50(), 42);
  });

  it('min and max tracked correctly', () => {
    const td = new TDigest();
    td.add(5);
    td.add(100);
    td.add(1);
    
    assert.equal(td.min, 1);
    assert.equal(td.max, 100);
    assert.equal(td.count, 3);
  });

  it('quantile edges', () => {
    const td = new TDigest();
    for (let i = 0; i < 100; i++) td.add(i);
    
    assert.equal(td.quantile(0), 0);
    assert.equal(td.quantile(1), 99);
  });

  it('latency percentiles use case', () => {
    const td = new TDigest();
    // Simulate API latencies: mostly fast, some slow
    for (let i = 0; i < 950; i++) td.add(5 + Math.random() * 20);  // 5-25ms
    for (let i = 0; i < 45; i++) td.add(100 + Math.random() * 200); // 100-300ms
    for (let i = 0; i < 5; i++) td.add(1000 + Math.random() * 2000); // 1-3s outliers
    
    assert.ok(td.p50() < 50, `P50 should be fast: ${td.p50()}`);
    assert.ok(td.p99() > 50, `P99 should be slow: ${td.p99()}`);
  });
});

describe('SegmentTree', () => {
  it('range sum query', () => {
    const st = new SegmentTree([1, 3, 5, 7, 9, 11], 'sum');
    assert.equal(st.query(0, 5), 36);  // Sum of all
    assert.equal(st.query(1, 3), 15);  // 3 + 5 + 7
    assert.equal(st.query(0, 0), 1);   // Single element
  });

  it('range min query', () => {
    const st = new SegmentTree([5, 2, 8, 1, 9, 3], 'min');
    assert.equal(st.query(0, 5), 1);
    assert.equal(st.query(0, 2), 2);
    assert.equal(st.query(3, 5), 1);
  });

  it('range max query', () => {
    const st = new SegmentTree([5, 2, 8, 1, 9, 3], 'max');
    assert.equal(st.query(0, 5), 9);
    assert.equal(st.query(0, 2), 8);
    assert.equal(st.query(4, 5), 9);
  });

  it('point update', () => {
    const st = new SegmentTree([1, 2, 3, 4, 5], 'sum');
    assert.equal(st.query(0, 4), 15);
    
    st.update(2, 10); // Change 3 → 10
    assert.equal(st.query(0, 4), 22); // 1 + 2 + 10 + 4 + 5
    assert.equal(st.query(2, 2), 10);
  });

  it('handles large arrays', () => {
    const data = Array.from({ length: 10000 }, (_, i) => i + 1);
    const st = new SegmentTree(data, 'sum');
    
    // Sum of 1..10000 = 10000 * 10001 / 2
    assert.equal(st.query(0, 9999), 50005000);
    
    // Partial range
    assert.equal(st.query(0, 99), 5050); // Sum of 1..100
  });

  it('single element', () => {
    const st = new SegmentTree([42], 'sum');
    assert.equal(st.query(0, 0), 42);
  });
});
