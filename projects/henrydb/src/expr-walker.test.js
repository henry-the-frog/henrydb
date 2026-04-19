import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { getExprChildren, exprContains, exprCollect } from './expr-walker.js';

describe('expr-walker', () => {
  it('getExprChildren handles arith', () => {
    const node = { type: 'arith', op: '+', left: { type: 'literal', value: 1 }, right: { type: 'literal', value: 2 } };
    const children = getExprChildren(node);
    assert.equal(children.length, 2);
  });

  it('getExprChildren handles function_call args', () => {
    const node = { type: 'function_call', func: 'COALESCE', args: [
      { type: 'aggregate_expr', func: 'SUM' },
      { type: 'literal', value: 0 }
    ]};
    const children = getExprChildren(node);
    assert.equal(children.length, 2);
  });

  it('getExprChildren handles case_expr', () => {
    const node = { type: 'case_expr', whens: [
      { condition: { type: 'literal', value: true }, result: { type: 'literal', value: 1 } }
    ], elseResult: { type: 'literal', value: 0 } };
    const children = getExprChildren(node);
    assert.equal(children.length, 3);  // condition + result + else
  });

  it('getExprChildren handles IS_NULL', () => {
    const node = { type: 'IS_NULL', left: { type: 'column_ref', name: 'x' } };
    const children = getExprChildren(node);
    assert.equal(children.length, 1);
  });

  it('exprContains finds window node', () => {
    const expr = { type: 'arith', op: '-', 
      left: { type: 'column_ref', name: 'val' },
      right: { type: 'window', func: 'LAG', arg: { type: 'column_ref', name: 'val' } }
    };
    assert.ok(exprContains(expr, n => n.type === 'window'));
    assert.ok(!exprContains(expr, n => n.type === 'aggregate_expr'));
  });

  it('exprContains finds aggregate inside COALESCE', () => {
    const expr = { type: 'function_call', func: 'COALESCE', args: [
      { type: 'aggregate_expr', func: 'SUM', arg: { type: 'column_ref', name: 'x' } },
      { type: 'literal', value: 0 }
    ]};
    assert.ok(exprContains(expr, n => n.type === 'aggregate_expr'));
  });

  it('exprContains finds window inside CASE', () => {
    const expr = { type: 'case_expr', whens: [
      { condition: { type: 'IS_NULL', left: { type: 'window', func: 'LAG' } },
        result: { type: 'literal', value: 'first' } }
    ], elseResult: { type: 'literal', value: 'rest' } };
    assert.ok(exprContains(expr, n => n.type === 'window'));
  });

  it('exprContains returns false for missing type', () => {
    const expr = { type: 'literal', value: 42 };
    assert.ok(!exprContains(expr, n => n.type === 'window'));
  });

  it('exprCollect gathers all window nodes', () => {
    const expr = { type: 'arith', op: '-',
      left: { type: 'window', func: 'LAG', _id: 1 },
      right: { type: 'window', func: 'LEAD', _id: 2 }
    };
    const windows = exprCollect(expr, n => n.type === 'window');
    assert.equal(windows.length, 2);
  });

  it('handles null/undefined gracefully', () => {
    assert.deepEqual(getExprChildren(null), []);
    assert.deepEqual(getExprChildren(undefined), []);
    assert.ok(!exprContains(null, () => true));
    assert.deepEqual(exprCollect(null, () => true), []);
  });
});
