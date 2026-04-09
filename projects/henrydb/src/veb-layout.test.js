// veb-layout.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { VanEmdeBoasLayout, EytzingerLayout } from './veb-layout.js';

describe('VanEmdeBoasLayout', () => {
  it('search finds all elements', () => {
    const arr = [1, 2, 3, 4, 5, 6, 7];
    const veb = new VanEmdeBoasLayout(arr);
    for (const v of arr) assert.equal(veb.search(v), true);
    assert.equal(veb.search(99), false);
  });

  it('root is median', () => {
    const arr = [1, 2, 3, 4, 5, 6, 7];
    const veb = new VanEmdeBoasLayout(arr);
    assert.equal(veb.getLayout()[0], 4); // Root = median
  });
});

describe('EytzingerLayout', () => {
  it('search finds all elements', () => {
    const arr = [1, 2, 3, 4, 5, 6, 7];
    const eyt = new EytzingerLayout(arr);
    for (const v of arr) assert.equal(eyt.search(v), true);
    assert.equal(eyt.search(99), false);
  });

  it('performance: 100K search (vs standard binary search)', () => {
    const arr = Array.from({ length: 100000 }, (_, i) => i);
    const eyt = new EytzingerLayout(arr);
    
    const t0 = performance.now();
    for (let i = 0; i < 100000; i++) eyt.search(i);
    const eytMs = performance.now() - t0;
    
    // Compare with standard binary search
    const t1 = performance.now();
    for (let i = 0; i < 100000; i++) {
      let lo = 0, hi = arr.length - 1;
      while (lo <= hi) {
        const mid = (lo + hi) >>> 1;
        if (arr[mid] === i) break;
        if (arr[mid] < i) lo = mid + 1;
        else hi = mid - 1;
      }
    }
    const stdMs = performance.now() - t1;
    
    console.log(`  100K Eytzinger: ${eytMs.toFixed(1)}ms vs Standard: ${stdMs.toFixed(1)}ms (${(stdMs/eytMs).toFixed(2)}x)`);
  });
});
