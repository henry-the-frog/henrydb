// simd-numeric.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  vecAdd, vecSub, vecMul, vecDiv, vecScalarMul,
  vecGT, vecLT, vecEQ, vecBetween,
  vecSum, vecMin, vecMax, vecAvg, vecCount,
  vecSumSel, vecMinSel, vecMaxSel,
  vecGather, vecScatter, vecDot,
} from './simd-numeric.js';

const a = new Float64Array([1, 2, 3, 4, 5]);
const b = new Float64Array([5, 4, 3, 2, 1]);

describe('SIMD Numeric: Arithmetic', () => {
  it('vecAdd', () => assert.deepEqual([...vecAdd(a, b)], [6, 6, 6, 6, 6]));
  it('vecSub', () => assert.deepEqual([...vecSub(a, b)], [-4, -2, 0, 2, 4]));
  it('vecMul', () => assert.deepEqual([...vecMul(a, b)], [5, 8, 9, 8, 5]));
  it('vecDiv', () => assert.deepEqual([...vecDiv(a, b)], [0.2, 0.5, 1, 2, 5]));
  it('vecScalarMul', () => assert.deepEqual([...vecScalarMul(a, 10)], [10, 20, 30, 40, 50]));
  it('vecDiv by zero', () => assert.equal(vecDiv(new Float64Array([1]), new Float64Array([0]))[0], 0));
});

describe('SIMD Numeric: Comparisons', () => {
  it('vecGT', () => assert.deepEqual(vecGT(a, 3), [3, 4]));
  it('vecLT', () => assert.deepEqual(vecLT(a, 3), [0, 1]));
  it('vecEQ', () => assert.deepEqual(vecEQ(a, 3), [2]));
  it('vecBetween', () => assert.deepEqual(vecBetween(a, 2, 4), [1, 2, 3]));
});

describe('SIMD Numeric: Aggregations', () => {
  it('vecSum', () => assert.equal(vecSum(a), 15));
  it('vecMin', () => assert.equal(vecMin(a), 1));
  it('vecMax', () => assert.equal(vecMax(a), 5));
  it('vecAvg', () => assert.equal(vecAvg(a), 3));
  it('vecCount', () => assert.equal(vecCount(a), 5));
});

describe('SIMD Numeric: Selection vector ops', () => {
  it('vecSumSel', () => assert.equal(vecSumSel(a, [0, 2, 4]), 9)); // 1+3+5
  it('vecMinSel', () => assert.equal(vecMinSel(a, [1, 3]), 2));
  it('vecMaxSel', () => assert.equal(vecMaxSel(a, [0, 4]), 5));
});

describe('SIMD Numeric: Gather/Scatter', () => {
  it('vecGather', () => assert.deepEqual([...vecGather(a, [0, 4])], [1, 5]));
  it('vecScatter', () => {
    const t = new Float64Array(5);
    vecScatter(t, [1, 3], new Float64Array([10, 20]));
    assert.equal(t[1], 10);
    assert.equal(t[3], 20);
  });
});

describe('SIMD Numeric: Dot product', () => {
  it('vecDot', () => assert.equal(vecDot(a, b), 35)); // 5+8+9+8+5

  it('benchmark: 1M element vectorized sum', () => {
    const big = new Float64Array(1000000);
    for (let i = 0; i < big.length; i++) big[i] = i;
    const t0 = Date.now();
    const sum = vecSum(big);
    console.log(`    1M vecSum: ${Date.now() - t0}ms (sum=${sum})`);
    assert.ok(sum > 0);
  });

  it('benchmark: 1M element vectorized filter', () => {
    const big = new Float64Array(1000000);
    for (let i = 0; i < big.length; i++) big[i] = i;
    const t0 = Date.now();
    const sel = vecGT(big, 500000);
    console.log(`    1M vecGT: ${Date.now() - t0}ms (${sel.length} selected)`);
    assert.equal(sel.length, 499999);
  });
});
