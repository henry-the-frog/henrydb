// constant-folding.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ConstantFolder } from './constant-folding.js';

const lit = v => ({ type: 'literal', value: v });
const col = n => ({ type: 'column_ref', name: n });

describe('ConstantFolder', () => {
  it('folds arithmetic: 2 + 3 → 5', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'ARITHMETIC', op: '+', left: lit(2), right: lit(3) });
    assert.deepEqual(result, lit(5));
  });

  it('folds nested arithmetic: (2 + 3) * 4 → 20', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({
      type: 'ARITHMETIC', op: '*',
      left: { type: 'ARITHMETIC', op: '+', left: lit(2), right: lit(3) },
      right: lit(4),
    });
    assert.deepEqual(result, lit(20));
  });

  it('identity: x + 0 → x', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'ARITHMETIC', op: '+', left: col('x'), right: lit(0) });
    assert.deepEqual(result, col('x'));
  });

  it('identity: x * 1 → x', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'ARITHMETIC', op: '*', left: col('x'), right: lit(1) });
    assert.deepEqual(result, col('x'));
  });

  it('zero: x * 0 → 0', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'ARITHMETIC', op: '*', left: col('x'), right: lit(0) });
    assert.deepEqual(result, lit(0));
  });

  it('folds comparison: 1 = 1 → true', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'COMPARE', op: 'EQ', left: lit(1), right: lit(1) });
    assert.deepEqual(result, lit(true));
  });

  it('folds comparison: 1 > 2 → false', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'COMPARE', op: 'GT', left: lit(1), right: lit(2) });
    assert.deepEqual(result, lit(false));
  });

  it('folds AND: true AND x → x', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'AND', left: lit(true), right: col('x') });
    assert.deepEqual(result, col('x'));
  });

  it('folds AND: false AND x → false', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'AND', left: lit(false), right: col('x') });
    assert.deepEqual(result, lit(false));
  });

  it('folds OR: true OR x → true', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'OR', left: lit(true), right: col('x') });
    assert.deepEqual(result, lit(true));
  });

  it('folds OR: false OR x → x', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'OR', left: lit(false), right: col('x') });
    assert.deepEqual(result, col('x'));
  });

  it('folds NOT true → false', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'NOT', expr: lit(true) });
    assert.deepEqual(result, lit(false));
  });

  it('folds NOT NOT x → x', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'NOT', expr: { type: 'NOT', expr: col('x') } });
    assert.deepEqual(result, col('x'));
  });

  it('folds string concat: "hello" || " world" → "hello world"', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'ARITHMETIC', op: '||', left: lit('hello'), right: lit(' world') });
    assert.deepEqual(result, lit('hello world'));
  });

  it('folds BETWEEN: 5 BETWEEN 1 AND 10 → true', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'BETWEEN', value: lit(5), low: lit(1), high: lit(10) });
    assert.deepEqual(result, lit(true));
  });

  it('folds IN: 3 IN (1,2,3) → true', () => {
    const cf = new ConstantFolder();
    const result = cf.fold({ type: 'IN', value: lit(3), list: [lit(1), lit(2), lit(3)] });
    assert.deepEqual(result, lit(true));
  });

  it('leaves non-constant expressions unchanged', () => {
    const cf = new ConstantFolder();
    const expr = { type: 'ARITHMETIC', op: '+', left: col('x'), right: col('y') };
    const result = cf.fold(expr);
    assert.equal(result.type, 'ARITHMETIC');
  });

  it('tracks stats', () => {
    const cf = new ConstantFolder();
    cf.fold({ type: 'ARITHMETIC', op: '+', left: lit(2), right: lit(3) });
    cf.fold({ type: 'AND', left: lit(true), right: col('x') });
    assert.equal(cf.stats.folds, 1);
    assert.equal(cf.stats.eliminations, 1);
  });
});
