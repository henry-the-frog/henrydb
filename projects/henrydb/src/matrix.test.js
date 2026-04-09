// matrix.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Matrix } from './matrix.js';

describe('Matrix', () => {
  it('multiply', () => {
    const a = Matrix.fromArray([[1, 2], [3, 4]]);
    const b = Matrix.fromArray([[5, 6], [7, 8]]);
    const c = a.multiply(b);
    assert.deepEqual(c.toArray(), [[19, 22], [43, 50]]);
  });

  it('transpose', () => {
    const m = Matrix.fromArray([[1, 2, 3], [4, 5, 6]]);
    const t = m.transpose();
    assert.deepEqual(t.toArray(), [[1, 4], [2, 5], [3, 6]]);
  });

  it('identity', () => {
    const I = Matrix.identity(3);
    assert.equal(I.get(0, 0), 1);
    assert.equal(I.get(0, 1), 0);
    assert.equal(I.trace(), 3);
  });

  it('add and scale', () => {
    const a = Matrix.fromArray([[1, 2], [3, 4]]);
    const b = a.scale(2);
    assert.deepEqual(b.toArray(), [[2, 4], [6, 8]]);
    
    const c = a.add(b);
    assert.deepEqual(c.toArray(), [[3, 6], [9, 12]]);
  });

  it('Frobenius norm', () => {
    const m = Matrix.fromArray([[3, 4]]);
    assert.equal(m.frobenius(), 5); // sqrt(9 + 16)
  });

  it('performance: 100x100 multiply', () => {
    const a = Matrix.zeros(100, 100);
    const b = Matrix.zeros(100, 100);
    for (let i = 0; i < 100; i++) for (let j = 0; j < 100; j++) {
      a.set(i, j, Math.random());
      b.set(i, j, Math.random());
    }
    const t0 = performance.now();
    a.multiply(b);
    const elapsed = performance.now() - t0;
    console.log(`  100x100 multiply: ${elapsed.toFixed(1)}ms`);
  });
});
