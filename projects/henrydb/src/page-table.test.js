// page-table.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PageTable } from './page-table.js';

describe('PageTable', () => {
  it('translate virtual to physical', () => {
    const pt = new PageTable(4096);
    pt.map(0, 5); // Virtual page 0 → physical page 5
    
    const phys = pt.translate(100); // Offset 100 in page 0
    assert.equal(phys, 5 * 4096 + 100);
  });

  it('TLB hit on repeated access', () => {
    const pt = new PageTable(4096);
    pt.map(0, 0);
    
    pt.translate(0); // Miss
    pt.translate(100); // Hit (TLB cached)
    
    assert.equal(pt.getStats().hits, 1);
    assert.equal(pt.getStats().misses, 1);
  });

  it('page fault on unmapped page', () => {
    const pt = new PageTable(4096);
    assert.equal(pt.translate(99999), null);
    assert.equal(pt.getStats().faults, 1);
  });

  it('unmap invalidates TLB', () => {
    const pt = new PageTable(4096);
    pt.map(0, 0);
    pt.translate(0); // Populate TLB
    pt.unmap(0);
    assert.equal(pt.translate(0), null); // Page fault
  });

  it('10K translations with TLB', () => {
    const pt = new PageTable(4096);
    for (let i = 0; i < 100; i++) pt.map(i, i);
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) pt.translate((i % 100) * 4096);
    const elapsed = performance.now() - t0;
    
    const stats = pt.getStats();
    console.log(`  10K translations: ${elapsed.toFixed(1)}ms, TLB hit rate: ${(stats.hitRate*100).toFixed(1)}%`);
  });
});
