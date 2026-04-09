// expression-evaluator.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { evaluate } from './expression-evaluator.js';

describe('ExpressionEvaluator', () => {
  const row = { name: 'Alice', age: 30, salary: 50000 };

  it('column reference', () => {
    assert.equal(evaluate({ type: 'column', name: 'age' }, row), 30);
  });

  it('binary expression', () => {
    const expr = { type: 'binary', op: '+', left: { type: 'column', name: 'age' }, right: { type: 'literal', value: 5 } };
    assert.equal(evaluate(expr, row), 35);
  });

  it('comparison', () => {
    const expr = { type: 'binary', op: '>', left: { type: 'column', name: 'age' }, right: { type: 'literal', value: 25 } };
    assert.equal(evaluate(expr, row), true);
  });

  it('function call', () => {
    const expr = { type: 'function', name: 'UPPER', args: [{ type: 'column', name: 'name' }] };
    assert.equal(evaluate(expr, row), 'ALICE');
  });
});
