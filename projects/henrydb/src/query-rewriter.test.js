// query-rewriter.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { QueryRewriter } from './query-rewriter.js';

const col = n => ({ type: 'column', name: n });
const lit = v => ({ type: 'literal', value: v });

describe('QueryRewriter', () => {
  it('removes true AND x → x', () => {
    const rw = new QueryRewriter();
    const result = rw.simplifyPredicate({ type: 'AND', left: lit(true), right: col('x') });
    assert.deepEqual(result, col('x'));
  });

  it('removes false AND x → false', () => {
    const rw = new QueryRewriter();
    const result = rw.simplifyPredicate({ type: 'AND', left: lit(false), right: col('x') });
    assert.deepEqual(result, lit(false));
  });

  it('removes false OR x → x', () => {
    const rw = new QueryRewriter();
    const result = rw.simplifyPredicate({ type: 'OR', left: lit(false), right: col('x') });
    assert.deepEqual(result, col('x'));
  });

  it('NOT NOT x → x', () => {
    const rw = new QueryRewriter();
    const result = rw.simplifyPredicate({ type: 'NOT', expr: { type: 'NOT', expr: col('x') } });
    assert.deepEqual(result, col('x'));
  });

  it('merges range: x > 5 AND x > 3 → x > 5', () => {
    const rw = new QueryRewriter();
    const result = rw.simplifyPredicate({
      type: 'AND',
      left: { type: 'COMPARE', op: 'GT', left: col('x'), right: lit(5) },
      right: { type: 'COMPARE', op: 'GT', left: col('x'), right: lit(3) },
    });
    assert.equal(result.right.value, 5);
  });

  it('OR to IN: x=1 OR x=2 → x IN (1,2)', () => {
    const rw = new QueryRewriter();
    const result = rw.simplifyPredicate({
      type: 'OR',
      left: { type: 'COMPARE', op: 'EQ', left: col('x'), right: lit(1) },
      right: { type: 'COMPARE', op: 'EQ', left: col('x'), right: lit(2) },
    });
    assert.equal(result.type, 'IN');
    assert.equal(result.values.length, 2);
  });

  it('self-comparison: x = x → true', () => {
    const rw = new QueryRewriter();
    const result = rw.simplifyPredicate({
      type: 'COMPARE', op: 'EQ', left: col('x'), right: col('x'),
    });
    assert.deepEqual(result, lit(true));
  });

  it('suggestJoinOrder: smallest first', () => {
    const rw = new QueryRewriter();
    const order = rw.suggestJoinOrder([
      { name: 'big', estimatedRows: 1000000 },
      { name: 'small', estimatedRows: 100 },
      { name: 'medium', estimatedRows: 10000 },
    ]);
    assert.equal(order[0].name, 'small');
    assert.equal(order[2].name, 'big');
  });

  it('predicate pushdown', () => {
    const rw = new QueryRewriter();
    const pred = {
      type: 'AND',
      left: { type: 'COMPARE', op: 'GT', left: col('age'), right: lit(25) },
      right: { type: 'COMPARE', op: 'EQ', left: col('dept'), right: lit('eng') },
    };
    const tables = [
      { name: 'employees', columns: ['age', 'dept', 'name'] },
    ];
    const result = rw.pushdownPredicates(pred, tables);
    assert.equal(result.pushed.length, 2);
    assert.equal(result.remaining, null);
  });
});
