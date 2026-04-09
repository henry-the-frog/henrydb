// quotient-filter.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { QuotientFilter } from './quotient-filter.js';

describe('QuotientFilter', () => {
  it('insert and contains', () => {
    const qf = new QuotientFilter(10);
    qf.insert('hello');
    qf.insert('world');
    assert.equal(qf.contains('hello'), true);
    assert.equal(qf.contains('world'), true);
  });

  it('no false negatives', () => {
    const qf = new QuotientFilter(12);
    const items = [];
    for (let i = 0; i < 1000; i++) {
      items.push(`item-${i}`);
      qf.insert(`item-${i}`);
    }
    for (const item of items) {
      assert.equal(qf.contains(item), true, `False negative for ${item}`);
    }
  });

  it('low false positive rate', () => {
    const qf = new QuotientFilter(12); // 4096 slots
    for (let i = 0; i < 1000; i++) qf.insert(`key-${i}`);
    
    let fp = 0;
    for (let i = 1000; i < 2000; i++) {
      if (qf.contains(`key-${i}`)) fp++;
    }
    console.log(`  FPR: ${(fp/1000*100).toFixed(1)}% (${fp}/1000)`);
    assert.ok(fp < 200, `Too many false positives: ${fp}`);
  });

  it('merge two filters', () => {
    const a = new QuotientFilter(10);
    const b = new QuotientFilter(10);
    a.insert('a1'); a.insert('a2');
    b.insert('b1'); b.insert('b2');
    
    const merged = QuotientFilter.merge(a, b);
    assert.equal(merged.contains('a1'), true);
    assert.equal(merged.contains('b1'), true);
  });

  it('load factor', () => {
    const qf = new QuotientFilter(8); // 256 slots
    for (let i = 0; i < 100; i++) qf.insert(`k${i}`);
    const stats = qf.getStats();
    assert.ok(stats.loadFactor > 0.3);
    assert.ok(stats.loadFactor < 0.5);
  });
});
