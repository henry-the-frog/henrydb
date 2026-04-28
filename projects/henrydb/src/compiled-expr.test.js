// compiled-expr.test.js — Tests for compiled expression evaluator
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { compileExpr, getCompiledExpr } from './compiled-expr.js';

describe('Compiled Expression Evaluator', () => {
  describe('literals', () => {
    it('number literal', () => {
      const fn = compileExpr({ type: 'literal', value: 42 });
      assert.strictEqual(fn({ x: 1 }), 42);
    });

    it('string literal', () => {
      const fn = compileExpr({ type: 'literal', value: 'hello' });
      assert.strictEqual(fn({}), 'hello');
    });

    it('null literal', () => {
      const fn = compileExpr({ type: 'literal', value: null });
      assert.strictEqual(fn({}), null);
    });
  });

  describe('column references', () => {
    it('simple column', () => {
      const fn = compileExpr({ type: 'column_ref', name: 'id' });
      assert.strictEqual(fn({ id: 42 }), 42);
    });

    it('qualified column (table.col) returns null (may be outer scope ref)', () => {
      const fn = compileExpr({ type: 'column_ref', name: 't.id' });
      assert.strictEqual(fn, null);
    });
  });

  describe('comparisons', () => {
    it('EQ', () => {
      const fn = compileExpr({
        type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 'id' },
        right: { type: 'literal', value: 5 }
      });
      assert.strictEqual(fn({ id: 5 }), true);
      assert.strictEqual(fn({ id: 6 }), false);
    });

    it('NE', () => {
      const fn = compileExpr({
        type: 'COMPARE', op: 'NE',
        left: { type: 'column_ref', name: 'id' },
        right: { type: 'literal', value: 5 }
      });
      assert.strictEqual(fn({ id: 5 }), false);
      assert.strictEqual(fn({ id: 6 }), true);
    });

    it('LT', () => {
      const fn = compileExpr({
        type: 'COMPARE', op: 'LT',
        left: { type: 'column_ref', name: 'val' },
        right: { type: 'literal', value: 10 }
      });
      assert.strictEqual(fn({ val: 5 }), true);
      assert.strictEqual(fn({ val: 10 }), false);
      assert.strictEqual(fn({ val: 15 }), false);
    });

    it('GT', () => {
      const fn = compileExpr({
        type: 'COMPARE', op: 'GT',
        left: { type: 'column_ref', name: 'val' },
        right: { type: 'literal', value: 10 }
      });
      assert.strictEqual(fn({ val: 5 }), false);
      assert.strictEqual(fn({ val: 15 }), true);
    });

    it('LE', () => {
      const fn = compileExpr({
        type: 'COMPARE', op: 'LE',
        left: { type: 'column_ref', name: 'val' },
        right: { type: 'literal', value: 10 }
      });
      assert.strictEqual(fn({ val: 10 }), true);
      assert.strictEqual(fn({ val: 11 }), false);
    });

    it('GE', () => {
      const fn = compileExpr({
        type: 'COMPARE', op: 'GE',
        left: { type: 'column_ref', name: 'val' },
        right: { type: 'literal', value: 10 }
      });
      assert.strictEqual(fn({ val: 10 }), true);
      assert.strictEqual(fn({ val: 9 }), false);
    });
  });

  describe('logical operators', () => {
    it('AND', () => {
      const fn = compileExpr({
        type: 'AND',
        left: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'a' }, right: { type: 'literal', value: 5 } },
        right: { type: 'COMPARE', op: 'LT', left: { type: 'column_ref', name: 'b' }, right: { type: 'literal', value: 10 } }
      });
      assert.strictEqual(fn({ a: 6, b: 9 }), true);
      assert.strictEqual(fn({ a: 4, b: 9 }), false);
      assert.strictEqual(fn({ a: 6, b: 11 }), false);
    });

    it('OR', () => {
      const fn = compileExpr({
        type: 'OR',
        left: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'a' }, right: { type: 'literal', value: 1 } },
        right: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'a' }, right: { type: 'literal', value: 2 } }
      });
      assert.strictEqual(fn({ a: 1 }), true);
      assert.strictEqual(fn({ a: 2 }), true);
      assert.strictEqual(fn({ a: 3 }), false);
    });

    it('NOT', () => {
      const fn = compileExpr({
        type: 'NOT',
        operand: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'a' }, right: { type: 'literal', value: 5 } }
      });
      assert.strictEqual(fn({ a: 5 }), false);
      assert.strictEqual(fn({ a: 6 }), true);
    });
  });

  describe('null checks', () => {
    it('IS_NULL', () => {
      const fn = compileExpr({ type: 'IS_NULL', operand: { type: 'column_ref', name: 'x' } });
      assert.strictEqual(fn({ x: null }), true);
      assert.strictEqual(fn({ x: undefined }), true);
      assert.strictEqual(fn({ x: 5 }), false);
    });

    it('IS_NOT_NULL', () => {
      const fn = compileExpr({ type: 'IS_NOT_NULL', operand: { type: 'column_ref', name: 'x' } });
      assert.strictEqual(fn({ x: null }), false);
      assert.strictEqual(fn({ x: 5 }), true);
    });
  });

  describe('complex expressions', () => {
    it('BETWEEN', () => {
      const fn = compileExpr({
        type: 'BETWEEN',
        expr: { type: 'column_ref', name: 'val' },
        low: { type: 'literal', value: 5 },
        high: { type: 'literal', value: 10 }
      });
      assert.strictEqual(fn({ val: 7 }), true);
      assert.strictEqual(fn({ val: 5 }), true);
      assert.strictEqual(fn({ val: 10 }), true);
      assert.strictEqual(fn({ val: 4 }), false);
      assert.strictEqual(fn({ val: 11 }), false);
    });

    it('IN', () => {
      const fn = compileExpr({
        type: 'IN',
        expr: { type: 'column_ref', name: 'id' },
        values: [
          { type: 'literal', value: 1 },
          { type: 'literal', value: 3 },
          { type: 'literal', value: 5 }
        ]
      });
      assert.strictEqual(fn({ id: 1 }), true);
      assert.strictEqual(fn({ id: 3 }), true);
      assert.strictEqual(fn({ id: 2 }), false);
    });
  });

  describe('fallback for uncompilable', () => {
    it('PARAM returns null', () => {
      const fn = compileExpr({ type: 'PARAM', index: 1 });
      assert.strictEqual(fn, null);
    });

    it('unknown type returns null', () => {
      const fn = compileExpr({ type: 'SUBQUERY', query: {} });
      assert.strictEqual(fn, null);
    });
  });

  describe('getCompiledExpr', () => {
    it('returns a working function for simple expr', () => {
      const ast = { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'literal', value: 5 } };
      const fn = getCompiledExpr(ast);
      assert.strictEqual(fn({ id: 5 }), true);
      assert.strictEqual(fn({ id: 6 }), false);
    });

    it('null expr returns always-true', () => {
      const fn = getCompiledExpr(null);
      assert.strictEqual(fn({}), true);
    });
  });
});
