// columnar-ops.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  simdFilterGT, simdFilterLT, simdFilterEQ, simdFilterBetween,
  selIntersect, selUnion,
  bitmapAnd, bitmapOr, bitmapNot, bitmapPopcount,
  columnarHashJoin, LateMaterializer,
} from './columnar-ops.js';

describe('SIMD Filter', () => {
  const col = new Float64Array([10, 20, 30, 40, 50]);

  it('filterGT', () => assert.deepEqual(simdFilterGT(col, 30), [3, 4]));
  it('filterLT', () => assert.deepEqual(simdFilterLT(col, 30), [0, 1]));
  it('filterEQ', () => assert.deepEqual(simdFilterEQ(col, 30), [2]));
  it('filterBetween', () => assert.deepEqual(simdFilterBetween(col, 20, 40), [1, 2, 3]));
  it('selIntersect', () => assert.deepEqual(selIntersect([1, 2, 3], [2, 3, 4]), [2, 3]));
  it('selUnion', () => assert.deepEqual(selUnion([1, 3], [2, 4]), [1, 2, 3, 4]));

  it('benchmark: 1M filter', () => {
    const big = new Float64Array(1000000);
    for (let i = 0; i < big.length; i++) big[i] = i;
    const t0 = Date.now();
    const sel = simdFilterGT(big, 500000);
    console.log(`    SIMD filter 1M: ${Date.now() - t0}ms, ${sel.length} selected`);
    assert.equal(sel.length, 499999);
  });
});

describe('Bitmap SIMD', () => {
  it('bitmapAnd', () => {
    const a = new Uint32Array([0xFF00FF00]);
    const b = new Uint32Array([0xF0F0F0F0]);
    assert.equal(bitmapAnd(a, b)[0], 0xF000F000);
  });

  it('bitmapOr', () => {
    const a = new Uint32Array([0xFF000000]);
    const b = new Uint32Array([0x00FF0000]);
    assert.equal(bitmapOr(a, b)[0], 0xFFFF0000);
  });

  it('bitmapPopcount', () => {
    const a = new Uint32Array([0xFF]); // 8 bits
    assert.equal(bitmapPopcount(a), 8);
  });
});

describe('Columnar Hash Join', () => {
  it('basic join', () => {
    const leftKeys = [1, 2, 3, 4, 5];
    const rightKeys = [2, 4, 6];
    const result = columnarHashJoin(leftKeys, rightKeys);
    assert.equal(result.count, 2); // 2 and 4 match
  });

  it('many-to-many', () => {
    const leftKeys = [1, 1, 2];
    const rightKeys = [1, 1];
    const result = columnarHashJoin(leftKeys, rightKeys);
    assert.equal(result.count, 4); // 2×2
  });

  it('benchmark: 10K × 10K join', () => {
    const left = Array.from({ length: 10000 }, (_, i) => i);
    const right = Array.from({ length: 10000 }, (_, i) => i * 2);
    const t0 = Date.now();
    const result = columnarHashJoin(left, right);
    console.log(`    Columnar join 10K×10K: ${Date.now() - t0}ms, ${result.count} matches`);
    assert.equal(result.count, 5000);
  });
});

describe('LateMaterializer', () => {
  it('filter then materialize', () => {
    const lm = new LateMaterializer({
      id: [1, 2, 3, 4, 5],
      name: ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve'],
      age: [25, 30, 35, 28, 22],
    });
    
    let rows = lm.allRows();
    rows = lm.filter(rows, 'age', v => v > 25);
    const result = lm.materialize(rows, ['name', 'age']);
    
    assert.equal(result.length, 3);
    assert.ok(result.every(r => r.age > 25));
  });

  it('chained filters', () => {
    const lm = new LateMaterializer({
      x: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      y: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    });
    
    let rows = lm.allRows();
    rows = lm.filter(rows, 'x', v => v > 3);
    rows = lm.filter(rows, 'y', v => v < 80);
    assert.equal(rows.length, 4); // x=4..7, y=40..70
  });
});
