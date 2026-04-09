// bitmap-index.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BitmapIndex } from './bitmap-index.js';

describe('BitmapIndex', () => {
  it('set and lookup', () => {
    const bi = new BitmapIndex();
    bi.set(0, 'red'); bi.set(1, 'blue'); bi.set(2, 'red'); bi.set(3, 'green');
    
    assert.deepEqual(bi.lookup('red'), [0, 2]);
    assert.deepEqual(bi.lookup('blue'), [1]);
    assert.deepEqual(bi.lookup('green'), [3]);
    assert.deepEqual(bi.lookup('yellow'), []);
  });

  it('AND: rows matching all values', () => {
    const bi = new BitmapIndex();
    // Two different columns would need two indexes, but for testing:
    const idx1 = new BitmapIndex(); // color
    const idx2 = new BitmapIndex(); // size
    idx1.set(0, 'red'); idx1.set(1, 'blue'); idx1.set(2, 'red');
    idx2.set(0, 'large'); idx2.set(1, 'large'); idx2.set(2, 'small');
    
    // Rows where color=red: [0, 2]
    // Rows where size=large: [0, 1]
    // AND (manual): row 0 is in both
    const redRows = new Set(idx1.lookup('red'));
    const largeRows = new Set(idx2.lookup('large'));
    const both = [...redRows].filter(r => largeRows.has(r));
    assert.deepEqual(both, [0]);
  });

  it('OR: rows matching any value', () => {
    const bi = new BitmapIndex();
    bi.set(0, 'red'); bi.set(1, 'blue'); bi.set(2, 'green');
    
    const rows = bi.or(['red', 'green']);
    assert.deepEqual(rows.sort((a,b)=>a-b), [0, 2]);
  });

  it('count', () => {
    const bi = new BitmapIndex();
    for (let i = 0; i < 100; i++) bi.set(i, i % 3 === 0 ? 'A' : 'B');
    assert.equal(bi.count('A'), 34); // 0,3,6,...,99
  });

  it('large: 10K rows', () => {
    const bi = new BitmapIndex();
    for (let i = 0; i < 10000; i++) bi.set(i, ['cat', 'dog', 'fish'][i % 3]);
    
    const t0 = performance.now();
    for (let q = 0; q < 1000; q++) bi.lookup('cat');
    const elapsed = performance.now() - t0;
    
    assert.equal(bi.count('cat'), 3334);
    console.log(`  1K lookups on 10K rows: ${elapsed.toFixed(1)}ms`);
  });
});
