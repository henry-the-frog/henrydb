// query-rewriter.test.js — Tests for AST-to-AST query rewriting engine
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { QueryRewriter } from './query-rewriter.js';

let rw;

describe('QueryRewriter', () => {
  beforeEach(() => {
    rw = new QueryRewriter();
  });

  describe('View Expansion', () => {
    test('expands view in FROM clause', () => {
      rw.views.set('active_users', {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'users' },
        where: { type: 'EQUALS', left: { type: 'column_ref', column: 'active' }, right: { type: 'literal', value: true } },
      });

      const ast = {
        type: 'SELECT',
        columns: [{ name: 'name' }],
        from: { table: 'active_users' },
      };

      const rewritten = rw.rewrite(ast);
      assert.equal(rewritten.from.type, 'subquery');
      assert.equal(rewritten.from.query.from.table, 'users');
      assert.equal(rw.getStats().viewExpansions, 1);
    });

    test('expands view in JOIN clause', () => {
      rw.views.set('recent_orders', {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'orders' },
        where: { type: 'GT', left: { type: 'column_ref', column: 'date' }, right: { type: 'literal', value: '2024-01-01' } },
      });

      const ast = {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'users' },
        joins: [{
          type: 'INNER',
          table: { table: 'recent_orders', alias: 'ro' },
          condition: { type: 'EQUALS' },
        }],
      };

      const rewritten = rw.rewrite(ast);
      assert.equal(rewritten.joins[0].table.type, 'subquery');
      assert.equal(rewritten.joins[0].table.query.from.table, 'orders');
    });

    test('does not expand non-view tables', () => {
      const ast = {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'users' },
      };

      const rewritten = rw.rewrite(ast);
      assert.equal(rewritten.from.table, 'users');
      assert.equal(rw.getStats().viewExpansions, 0);
    });
  });

  describe('Predicate Pushdown', () => {
    test('pushes single-table predicate to FROM', () => {
      const ast = {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'users', alias: 'u' },
        joins: [{
          type: 'INNER',
          table: { table: 'orders', alias: 'o' },
          condition: { type: 'EQUALS' },
        }],
        where: {
          type: 'EQUALS',
          left: { type: 'column_ref', table: 'u', column: 'active' },
          right: { type: 'literal', value: true },
        },
      };

      const rewritten = rw.rewrite(ast);
      assert.ok(rewritten.from.filter);
      assert.equal(rewritten.from.filter.length, 1);
      assert.equal(rw.getStats().predicatePushdowns, 1);
    });

    test('pushes predicate to JOIN table', () => {
      const ast = {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'users', alias: 'u' },
        joins: [{
          type: 'INNER',
          table: { table: 'orders', alias: 'o' },
          condition: { type: 'EQUALS' },
        }],
        where: {
          type: 'GT',
          left: { type: 'column_ref', table: 'o', column: 'amount' },
          right: { type: 'literal', value: 100 },
        },
      };

      const rewritten = rw.rewrite(ast);
      assert.ok(rewritten.joins[0].additionalConditions);
      assert.equal(rewritten.joins[0].additionalConditions.length, 1);
    });

    test('keeps multi-table predicates in WHERE', () => {
      const ast = {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'users', alias: 'u' },
        joins: [{
          type: 'INNER',
          table: { table: 'orders', alias: 'o' },
          condition: { type: 'EQUALS' },
        }],
        where: {
          type: 'EQUALS',
          left: { type: 'column_ref', table: 'u', column: 'id' },
          right: { type: 'column_ref', table: 'o', column: 'user_id' },
        },
      };

      const rewritten = rw.rewrite(ast);
      // Multi-table predicate should remain in WHERE
      assert.ok(rewritten.where);
      assert.equal(rw.getStats().predicatePushdowns, 0);
    });
  });

  describe('Subquery Flattening', () => {
    test('flattens IN (SELECT ...) to JOIN', () => {
      const ast = {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'users' },
        where: {
          type: 'IN',
          left: { type: 'column_ref', column: 'id' },
          right: {
            type: 'SELECT',
            columns: [{ name: 'user_id' }],
            from: { table: 'orders' },
          },
        },
      };

      const rewritten = rw.rewrite(ast);
      assert.ok(rewritten.joins);
      assert.equal(rewritten.joins.length, 1);
      assert.equal(rewritten.where, null);
      assert.equal(rewritten.distinct, true);
      assert.equal(rw.getStats().subqueryFlattenings, 1);
    });

    test('preserves complex subqueries', () => {
      const ast = {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'users' },
        where: {
          type: 'IN',
          left: { type: 'column_ref', column: 'id' },
          right: {
            type: 'SELECT',
            columns: [{ name: 'user_id' }, { name: 'total' }], // Multi-column = can't flatten
            from: { table: 'orders' },
            joins: [{ type: 'INNER', table: 'items' }], // Has joins = complex
          },
        },
      };

      const rewritten = rw.rewrite(ast);
      assert.ok(rewritten.where); // Should remain as subquery
      assert.equal(rw.getStats().subqueryFlattenings, 0);
    });
  });

  describe('Constant Folding', () => {
    test('folds arithmetic constants', () => {
      const ast = {
        type: 'SELECT',
        columns: [{
          name: 'result',
          expression: {
            type: 'BINARY_OP',
            op: '+',
            left: { type: 'literal', value: 3 },
            right: { type: 'literal', value: 7 },
          },
        }],
        from: { table: 'dual' },
      };

      const rewritten = rw.rewrite(ast);
      assert.equal(rewritten.columns[0].expression.value, 10);
      assert.equal(rw.getStats().constantFolds, 1);
    });

    test('folds multiplication', () => {
      const ast = {
        type: 'SELECT',
        columns: [{
          expression: {
            type: 'BINARY_OP', op: '*',
            left: { type: 'literal', value: 6 },
            right: { type: 'literal', value: 7 },
          },
        }],
        from: { table: 'dual' },
      };

      const rewritten = rw.rewrite(ast);
      assert.equal(rewritten.columns[0].expression.value, 42);
    });

    test('folds string concatenation', () => {
      const ast = {
        type: 'SELECT',
        columns: [{
          expression: {
            type: 'CONCAT',
            left: { type: 'literal', value: 'Hello' },
            right: { type: 'literal', value: ' World' },
          },
        }],
        from: { table: 'dual' },
      };

      const rewritten = rw.rewrite(ast);
      assert.equal(rewritten.columns[0].expression.value, 'Hello World');
    });

    test('does not fold non-constant expressions', () => {
      const ast = {
        type: 'SELECT',
        columns: [{
          expression: {
            type: 'BINARY_OP', op: '+',
            left: { type: 'column_ref', column: 'x' },
            right: { type: 'literal', value: 1 },
          },
        }],
        from: { table: 'nums' },
      };

      const rewritten = rw.rewrite(ast);
      assert.equal(rewritten.columns[0].expression.type, 'BINARY_OP');
    });
  });

  describe('Redundant Predicate Elimination', () => {
    test('eliminates x = x tautology', () => {
      const colRef = { type: 'column_ref', table: 'u', column: 'id' };
      const ast = {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'users' },
        where: { type: 'EQUALS', left: colRef, right: { ...colRef } },
      };

      const rewritten = rw.rewrite(ast);
      assert.equal(rewritten.where.type, 'literal');
      assert.equal(rewritten.where.value, true);
      assert.equal(rw.getStats().redundantEliminations, 1);
    });

    test('simplifies AND with TRUE', () => {
      const ast = {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'users' },
        where: {
          type: 'AND',
          left: { type: 'literal', value: true },
          right: { type: 'GT', left: { type: 'column_ref', column: 'age' }, right: { type: 'literal', value: 18 } },
        },
      };

      const rewritten = rw.rewrite(ast);
      assert.equal(rewritten.where.type, 'GT');
    });

    test('simplifies OR with TRUE', () => {
      const ast = {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'users' },
        where: {
          type: 'OR',
          left: { type: 'literal', value: true },
          right: { type: 'GT', left: { type: 'column_ref', column: 'age' }, right: { type: 'literal', value: 18 } },
        },
      };

      const rewritten = rw.rewrite(ast);
      assert.equal(rewritten.where.type, 'literal');
      assert.equal(rewritten.where.value, true);
    });
  });

  describe('Combined rewrites', () => {
    test('applies multiple rules together', () => {
      rw.views.set('active_users', {
        type: 'SELECT',
        columns: [{ name: '*' }],
        from: { table: 'users' },
        where: { type: 'EQUALS', left: { type: 'column_ref', column: 'active' }, right: { type: 'literal', value: true } },
      });

      const ast = {
        type: 'SELECT',
        columns: [{
          expression: {
            type: 'BINARY_OP', op: '+',
            left: { type: 'literal', value: 1 },
            right: { type: 'literal', value: 2 },
          },
        }],
        from: { table: 'active_users' },
      };

      const rewritten = rw.rewrite(ast);
      // View should be expanded AND constant should be folded
      assert.equal(rewritten.from.type, 'subquery');
      assert.equal(rewritten.columns[0].expression.value, 3);

      const stats = rw.getStats();
      assert.equal(stats.viewExpansions, 1);
      assert.equal(stats.constantFolds, 1);
    });

    test('original AST is not modified', () => {
      const ast = {
        type: 'SELECT',
        columns: [{
          expression: {
            type: 'BINARY_OP', op: '+',
            left: { type: 'literal', value: 1 },
            right: { type: 'literal', value: 2 },
          },
        }],
        from: { table: 'nums' },
      };

      const original = JSON.stringify(ast);
      rw.rewrite(ast);
      assert.equal(JSON.stringify(ast), original);
    });
  });
});
