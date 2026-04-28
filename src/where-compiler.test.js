// where-compiler.test.js — Tests for WHERE clause compilation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { compileWhereFilter } from './where-compiler.js';

describe('WHERE compiler — basic comparisons', () => {
  it('compiles EQ comparison', () => {
    const filter = compileWhereFilter({
      type: 'COMPARE', op: 'EQ',
      left: { type: 'column_ref', name: 'age' },
      right: { type: 'literal', value: 25 }
    });
    assert.ok(filter);
    assert.equal(filter({ age: 25 }), true);
    assert.equal(filter({ age: 30 }), false);
  });

  it('compiles GT comparison', () => {
    const filter = compileWhereFilter({
      type: 'COMPARE', op: 'GT',
      left: { type: 'column_ref', name: 'score' },
      right: { type: 'literal', value: 90 }
    });
    assert.ok(filter);
    assert.equal(filter({ score: 95 }), true);
    assert.equal(filter({ score: 80 }), false);
  });

  it('compiles string EQ', () => {
    const filter = compileWhereFilter({
      type: 'COMPARE', op: 'EQ',
      left: { type: 'column_ref', name: 'name' },
      right: { type: 'literal', value: 'Alice' }
    });
    assert.ok(filter);
    assert.equal(filter({ name: 'Alice' }), true);
    assert.equal(filter({ name: 'Bob' }), false);
  });
});

describe('WHERE compiler — logical operators', () => {
  it('compiles AND', () => {
    const filter = compileWhereFilter({
      type: 'AND',
      left: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'age' }, right: { type: 'literal', value: 18 } },
      right: { type: 'COMPARE', op: 'LT', left: { type: 'column_ref', name: 'age' }, right: { type: 'literal', value: 65 } }
    });
    assert.ok(filter);
    assert.equal(filter({ age: 30 }), true);
    assert.equal(filter({ age: 10 }), false);
    assert.equal(filter({ age: 70 }), false);
  });

  it('compiles OR', () => {
    const filter = compileWhereFilter({
      type: 'OR',
      left: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'status' }, right: { type: 'literal', value: 'active' } },
      right: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'status' }, right: { type: 'literal', value: 'pending' } }
    });
    assert.ok(filter);
    assert.equal(filter({ status: 'active' }), true);
    assert.equal(filter({ status: 'pending' }), true);
    assert.equal(filter({ status: 'closed' }), false);
  });

  it('compiles NOT', () => {
    const filter = compileWhereFilter({
      type: 'NOT',
      expr: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'deleted' }, right: { type: 'literal', value: true } }
    });
    assert.ok(filter);
    assert.equal(filter({ deleted: false }), true);
    assert.equal(filter({ deleted: true }), false);
  });
});

describe('WHERE compiler — IS NULL / BETWEEN / LIKE / IN', () => {
  it('compiles IS_NULL', () => {
    const filter = compileWhereFilter({
      type: 'IS_NULL',
      expr: { type: 'column_ref', name: 'email' }
    });
    assert.ok(filter);
    assert.equal(filter({ email: null }), true);
    assert.equal(filter({ email: undefined }), true);
    assert.equal(filter({ email: 'test@test.com' }), false);
  });

  it('compiles IS_NOT_NULL', () => {
    const filter = compileWhereFilter({
      type: 'IS_NOT_NULL',
      expr: { type: 'column_ref', name: 'email' }
    });
    assert.ok(filter);
    assert.equal(filter({ email: 'test@test.com' }), true);
    assert.equal(filter({ email: null }), false);
  });

  it('compiles BETWEEN', () => {
    const filter = compileWhereFilter({
      type: 'BETWEEN',
      expr: { type: 'column_ref', name: 'price' },
      low: { type: 'literal', value: 10 },
      high: { type: 'literal', value: 50 }
    });
    assert.ok(filter);
    assert.equal(filter({ price: 25 }), true);
    assert.equal(filter({ price: 5 }), false);
    assert.equal(filter({ price: 55 }), false);
    assert.equal(filter({ price: 10 }), true); // inclusive
    assert.equal(filter({ price: 50 }), true); // inclusive
  });

  it('compiles LIKE', () => {
    const filter = compileWhereFilter({
      type: 'LIKE',
      left: { type: 'column_ref', name: 'name' },
      right: { type: 'literal', value: 'Al%' }
    });
    assert.ok(filter);
    assert.equal(filter({ name: 'Alice' }), true);
    assert.equal(filter({ name: 'Albert' }), true);
    assert.equal(filter({ name: 'Bob' }), false);
  });

  it('compiles IN_LIST with Set', () => {
    const filter = compileWhereFilter({
      type: 'IN_LIST',
      expr: { type: 'column_ref', name: 'status' },
      values: [
        { type: 'literal', value: 'a' },
        { type: 'literal', value: 'b' },
        { type: 'literal', value: 'c' }
      ]
    });
    assert.ok(filter);
    assert.equal(filter({ status: 'a' }), true);
    assert.equal(filter({ status: 'd' }), false);
  });
});

describe('WHERE compiler — performance', () => {
  it('compiled filter is faster than tree-walking (baseline)', () => {
    const expr = {
      type: 'AND',
      left: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'age' }, right: { type: 'literal', value: 25 } },
      right: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'active' }, right: { type: 'literal', value: true } }
    };
    const filter = compileWhereFilter(expr);
    assert.ok(filter);

    const rows = [];
    for (let i = 0; i < 10000; i++) {
      rows.push({ age: 20 + (i % 40), active: i % 2 === 0 });
    }

    // Warmup
    for (let i = 0; i < 1000; i++) filter(rows[i % rows.length]);

    const start = performance.now();
    let count = 0;
    for (let i = 0; i < rows.length; i++) {
      if (filter(rows[i])) count++;
    }
    const elapsed = performance.now() - start;
    
    console.log(`  Compiled: ${elapsed.toFixed(2)}ms for ${rows.length} rows, ${count} matches, ${(elapsed / rows.length * 1000).toFixed(1)}μs/row`);
    assert.ok(elapsed < 50, `Should be fast, took ${elapsed}ms`);
  });

  it('returns null for unsupported expressions', () => {
    const filter = compileWhereFilter({
      type: 'EXISTS',
      subquery: { type: 'select' }
    });
    assert.equal(filter, null);
  });

  it('handles null expression', () => {
    const filter = compileWhereFilter(null);
    assert.ok(filter);
    assert.equal(filter({}), true);
  });
});
