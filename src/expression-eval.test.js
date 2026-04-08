// expression-eval.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ExpressionEvaluator } from './expression-eval.js';

const ev = new ExpressionEvaluator();
const col = n => ({ type: 'column', name: n });
const lit = v => ({ type: 'literal', value: v });
const arith = (op, l, r) => ({ type: 'ARITHMETIC', op, left: l, right: r });
const cmp = (op, l, r) => ({ type: 'COMPARE', op, left: l, right: r });

const row = { name: 'Alice', age: 30, salary: 75000, dept: null };

describe('ExpressionEvaluator', () => {
  it('literal', () => assert.equal(ev.evaluate(lit(42)), 42));
  it('column', () => assert.equal(ev.evaluate(col('name'), row), 'Alice'));
  it('arithmetic +', () => assert.equal(ev.evaluate(arith('+', lit(10), lit(20))), 30));
  it('arithmetic -', () => assert.equal(ev.evaluate(arith('-', col('age'), lit(5)), row), 25));
  it('arithmetic * /', () => assert.equal(ev.evaluate(arith('/', col('salary'), lit(12)), row), 6250));
  it('compare =', () => assert.equal(ev.evaluate(cmp('=', col('name'), lit('Alice')), row), true));
  it('compare >', () => assert.equal(ev.evaluate(cmp('>', col('age'), lit(25)), row), true));
  it('AND', () => assert.equal(ev.evaluate({ type: 'AND', left: cmp('>', col('age'), lit(20)), right: cmp('<', col('age'), lit(40)) }, row), true));
  it('OR', () => assert.equal(ev.evaluate({ type: 'OR', left: cmp('=', col('age'), lit(25)), right: cmp('=', col('age'), lit(30)) }, row), true));
  it('NOT', () => assert.equal(ev.evaluate({ type: 'NOT', expr: cmp('=', col('name'), lit('Bob')) }, row), true));

  it('CASE expression', () => {
    const expr = {
      type: 'CASE',
      cases: [
        { when: cmp('>', col('salary'), lit(100000)), then: lit('high') },
        { when: cmp('>', col('salary'), lit(50000)), then: lit('medium') },
      ],
      else: lit('low'),
    };
    assert.equal(ev.evaluate(expr, row), 'medium');
  });

  it('COALESCE', () => {
    assert.equal(ev.evaluate({ type: 'COALESCE', args: [col('dept'), lit('unknown')] }, row), 'unknown');
    assert.equal(ev.evaluate({ type: 'COALESCE', args: [col('name'), lit('unknown')] }, row), 'Alice');
  });

  it('NULLIF', () => {
    assert.equal(ev.evaluate({ type: 'NULLIF', left: lit(10), right: lit(10) }), null);
    assert.equal(ev.evaluate({ type: 'NULLIF', left: lit(10), right: lit(20) }), 10);
  });

  it('CAST', () => {
    assert.equal(ev.evaluate({ type: 'CAST', expr: lit('42'), targetType: 'INT' }), 42);
    assert.equal(ev.evaluate({ type: 'CAST', expr: lit(42), targetType: 'VARCHAR' }), '42');
  });

  it('BETWEEN', () => {
    assert.equal(ev.evaluate({ type: 'BETWEEN', expr: col('age'), low: lit(20), high: lit(40) }, row), true);
    assert.equal(ev.evaluate({ type: 'BETWEEN', expr: col('age'), low: lit(31), high: lit(40) }, row), false);
  });

  it('IN', () => {
    assert.equal(ev.evaluate({ type: 'IN', expr: col('age'), values: [lit(25), lit(30), lit(35)] }, row), true);
  });

  it('IS_NULL', () => {
    assert.equal(ev.evaluate({ type: 'IS_NULL', expr: col('dept') }, row), true);
    assert.equal(ev.evaluate({ type: 'IS_NULL', expr: col('name') }, row), false);
  });

  it('LIKE', () => {
    assert.equal(ev.evaluate({ type: 'LIKE', expr: col('name'), pattern: 'Ali%' }, row), true);
    assert.equal(ev.evaluate({ type: 'LIKE', expr: col('name'), pattern: 'Bob%' }, row), false);
  });

  it('FUNCTION calls', () => {
    assert.equal(ev.evaluate({ type: 'FUNCTION', name: 'UPPER', args: [col('name')] }, row), 'ALICE');
    assert.equal(ev.evaluate({ type: 'FUNCTION', name: 'LENGTH', args: [col('name')] }, row), 5);
    assert.equal(ev.evaluate({ type: 'FUNCTION', name: 'ABS', args: [lit(-42)] }), 42);
  });

  it('null propagation in arithmetic', () => {
    assert.equal(ev.evaluate(arith('+', col('dept'), lit(10)), row), null);
  });

  it('division by zero', () => {
    assert.equal(ev.evaluate(arith('/', lit(10), lit(0))), null);
  });
});
