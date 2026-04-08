// batch-ops.test.js — Tests for SIMD-like batch operations
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TypedColumn } from './typed-columns.js';
import { BatchOps } from './batch-ops.js';

function makeCol(type, values) {
  const col = new TypedColumn(type, values.length);
  for (const v of values) col.push(v);
  return col;
}

describe('BatchOps', () => {

  it('add: element-wise addition', () => {
    const a = makeCol('INT', [1, 2, 3, 4, 5]);
    const b = makeCol('INT', [10, 20, 30, 40, 50]);
    const result = BatchOps.add(a, b);
    assert.equal(result.get(0), 11);
    assert.equal(result.get(4), 55);
  });

  it('sub: element-wise subtraction', () => {
    const a = makeCol('INT', [100, 200, 300]);
    const b = makeCol('INT', [10, 20, 30]);
    const result = BatchOps.sub(a, b);
    assert.equal(result.get(0), 90);
    assert.equal(result.get(2), 270);
  });

  it('mul: element-wise multiplication', () => {
    const a = makeCol('INT', [2, 3, 4]);
    const b = makeCol('INT', [5, 6, 7]);
    const result = BatchOps.mul(a, b);
    assert.equal(result.get(0), 10);
    assert.equal(result.get(1), 18);
    assert.equal(result.get(2), 28);
  });

  it('mulScalar: scalar multiplication', () => {
    const col = makeCol('INT', [1, 2, 3, 4, 5]);
    const result = BatchOps.mulScalar(col, 10);
    assert.equal(result.get(0), 10);
    assert.equal(result.get(4), 50);
  });

  it('eqColumns: element-wise equality', () => {
    const a = makeCol('INT', [1, 2, 3, 4, 5]);
    const b = makeCol('INT', [1, 9, 3, 9, 5]);
    const sel = BatchOps.eqColumns(a, b);
    assert.deepEqual([...sel], [0, 2, 4]);
  });

  it('gtColumns: element-wise greater-than', () => {
    const a = makeCol('INT', [10, 2, 30, 4, 50]);
    const b = makeCol('INT', [5, 5, 5, 5, 5]);
    const sel = BatchOps.gtColumns(a, b);
    assert.deepEqual([...sel], [0, 2, 4]);
  });

  it('intersect: AND of selection vectors', () => {
    const a = new Uint32Array([0, 2, 4, 6, 8]);
    const b = new Uint32Array([1, 2, 3, 4, 5]);
    const result = BatchOps.intersect(a, b);
    assert.deepEqual([...result], [2, 4]);
  });

  it('union: OR of selection vectors', () => {
    const a = new Uint32Array([0, 2, 4]);
    const b = new Uint32Array([1, 2, 3]);
    const result = BatchOps.union(a, b);
    assert.deepEqual([...result], [0, 1, 2, 3, 4]);
  });

  it('negate: NOT of selection vector', () => {
    const sel = new Uint32Array([1, 3, 5]);
    const result = BatchOps.negate(sel, 7);
    assert.deepEqual([...result], [0, 2, 4, 6]);
  });

  it('gather: select elements by indices', () => {
    const col = makeCol('INT', [100, 200, 300, 400, 500]);
    const sel = new Uint32Array([0, 2, 4]);
    const result = BatchOps.gather(col, sel);
    assert.equal(result.length, 3);
    assert.equal(result.get(0), 100);
    assert.equal(result.get(1), 300);
    assert.equal(result.get(2), 500);
  });

  it('sumAt/avgAt/minAt/maxAt', () => {
    const col = makeCol('INT', [10, 20, 30, 40, 50]);
    const sel = new Uint32Array([1, 3]); // values: 20, 40

    assert.equal(BatchOps.sumAt(col, sel), 60);
    assert.equal(BatchOps.avgAt(col, sel), 30);
    assert.equal(BatchOps.minAt(col, sel), 20);
    assert.equal(BatchOps.maxAt(col, sel), 40);
    assert.equal(BatchOps.countAt(sel), 2);
  });

  it('buildHash + probeHash: hash join building blocks', () => {
    const rightKey = makeCol('INT', [1, 2, 3, 1, 2, 3]);
    const leftKey = makeCol('INT', [2, 3, 4]);

    const ht = BatchOps.buildHash(rightKey);
    const { left, right } = BatchOps.probeHash(leftKey, ht);

    // left[0]=2 matches right indices [1, 4], left[1]=3 matches [2, 5], left[2]=4 no match
    assert.equal(left.length, 4);
    assert.equal(right.length, 4);
  });

  it('end-to-end: vectorized filter + aggregate', () => {
    // Simulate: SELECT SUM(amount) FROM orders WHERE amount > 100
    const amount = makeCol('INT', [50, 150, 200, 80, 300, 10, 250]);
    
    // Filter
    const selection = amount.filterGT(100);
    
    // Aggregate
    const total = BatchOps.sumAt(amount, selection);
    assert.equal(total, 900); // 150 + 200 + 300 + 250
    assert.equal(BatchOps.countAt(selection), 4);
  });

  it('end-to-end: vectorized join', () => {
    // Simulate: SELECT a.val, b.data FROM a JOIN b ON a.id = b.a_id
    const aId = makeCol('INT', [0, 1, 2, 3, 4]);
    const aVal = makeCol('INT', [100, 200, 300, 400, 500]);
    const bAId = makeCol('INT', [2, 0, 4, 2, 1]);
    const bData = makeCol('INT', [10, 20, 30, 40, 50]);

    const ht = BatchOps.buildHash(bAId);
    const { left, right } = BatchOps.probeHash(aId, ht);

    // Gather results
    const resultVal = BatchOps.gather(aVal, left);
    const resultData = BatchOps.gather(bData, right);

    assert.equal(resultVal.length, 5); // 5 matches total
    assert.equal(resultData.length, 5);
  });

  it('benchmark: batch ops on 1M elements', () => {
    const n = 1000000;
    const a = new TypedColumn('INT', n);
    const b = new TypedColumn('INT', n);
    for (let i = 0; i < n; i++) {
      a.push(i);
      b.push(n - i);
    }

    const t0 = Date.now();
    const result = BatchOps.add(a, b);
    const addMs = Date.now() - t0;

    const t1 = Date.now();
    const sel = a.filterGT(500000);
    const sumVal = BatchOps.sumAt(b, sel);
    const filterAggMs = Date.now() - t1;

    console.log(`    BatchAdd 1M: ${addMs}ms | Filter+Agg: ${filterAggMs}ms`);
    assert.equal(result.length, n);
    assert.ok(sumVal > 0);
  });
});
