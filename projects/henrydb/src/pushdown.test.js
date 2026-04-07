// pushdown.test.js — Predicate pushdown tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { pushdownPredicates } from './pushdown.js';

describe('Predicate Pushdown', () => {
  it('pushes single-table predicate to FROM table', () => {
    const ast = {
      type: 'SELECT',
      from: { table: 'users', alias: 'u' },
      joins: [{ joinType: 'INNER', table: 'orders', alias: 'o', on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'u.id' }, right: { type: 'column_ref', name: 'o.user_id' } } }],
      where: {
        type: 'AND',
        left: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'u.status' }, right: { type: 'literal', value: 'active' } },
        right: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'o.amount' }, right: { type: 'literal', value: 100 } },
      },
    };

    const { ast: result, pushed } = pushdownPredicates(ast);
    assert.equal(pushed, 2);
    
    // u.status = 'active' pushed to FROM
    assert.ok(result.from.filter, 'FROM table should have filter');
    assert.equal(result.from.filter.type, 'COMPARE');
    assert.equal(result.from.filter.left.name, 'u.status');
    
    // o.amount > 100 pushed to JOIN
    assert.ok(result.joins[0].filter, 'JOIN table should have filter');
    assert.equal(result.joins[0].filter.left.name, 'o.amount');
    
    // WHERE should be empty
    assert.equal(result.where, null);
  });

  it('keeps cross-table predicates in WHERE', () => {
    const ast = {
      type: 'SELECT',
      from: { table: 'users', alias: 'u' },
      joins: [{ joinType: 'INNER', table: 'orders', alias: 'o', on: {} }],
      where: {
        type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 'u.id' },
        right: { type: 'column_ref', name: 'o.user_id' },
      },
    };

    const { ast: result, pushed } = pushdownPredicates(ast);
    assert.equal(pushed, 0);
    assert.ok(result.where, 'Cross-table predicate should stay in WHERE');
  });

  it('handles mixed pushable and non-pushable predicates', () => {
    const ast = {
      type: 'SELECT',
      from: { table: 'a', alias: 'a' },
      joins: [{ joinType: 'INNER', table: 'b', alias: 'b', on: {} }],
      where: {
        type: 'AND',
        left: {
          type: 'AND',
          left: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'a.x' }, right: { type: 'literal', value: 1 } },
          right: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'a.id' }, right: { type: 'column_ref', name: 'b.a_id' } },
        },
        right: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'b.y' }, right: { type: 'literal', value: 5 } },
      },
    };

    const { ast: result, pushed } = pushdownPredicates(ast);
    assert.equal(pushed, 2); // a.x = 1 and b.y > 5 are pushable
    assert.ok(result.from.filter, 'a.x = 1 pushed to FROM');
    assert.ok(result.joins[0].filter, 'b.y > 5 pushed to JOIN');
    assert.ok(result.where, 'Cross-table predicate stays in WHERE');
    assert.equal(result.where.type, 'COMPARE'); // Only the cross-table one
  });

  it('does nothing without WHERE', () => {
    const ast = {
      type: 'SELECT',
      from: { table: 'a' },
      joins: [{ joinType: 'INNER', table: 'b' }],
      where: null,
    };

    const { ast: result, pushed } = pushdownPredicates(ast);
    assert.equal(pushed, 0);
  });

  it('does nothing without JOINs', () => {
    const ast = {
      type: 'SELECT',
      from: { table: 'a' },
      joins: [],
      where: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'x' }, right: { type: 'literal', value: 1 } },
    };

    const { ast: result, pushed } = pushdownPredicates(ast);
    assert.equal(pushed, 0);
    assert.ok(result.where);
  });

  it('handles multiple JOINs', () => {
    const ast = {
      type: 'SELECT',
      from: { table: 'a', alias: 'a' },
      joins: [
        { joinType: 'INNER', table: 'b', alias: 'b', on: {} },
        { joinType: 'INNER', table: 'c', alias: 'c', on: {} },
      ],
      where: {
        type: 'AND',
        left: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'a.x' }, right: { type: 'literal', value: 1 } },
        right: {
          type: 'AND',
          left: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'b.y' }, right: { type: 'literal', value: 2 } },
          right: { type: 'COMPARE', op: 'LT', left: { type: 'column_ref', name: 'c.z' }, right: { type: 'literal', value: 3 } },
        },
      },
    };

    const { ast: result, pushed } = pushdownPredicates(ast);
    assert.equal(pushed, 3);
    assert.ok(result.from.filter);
    assert.ok(result.joins[0].filter);
    assert.ok(result.joins[1].filter);
    assert.equal(result.where, null);
  });
});
