// expression-compiler.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ExpressionCompiler } from './expression-compiler.js';

const col = name => ({ type: 'column', name });
const lit = value => ({ type: 'literal', value });

const rows = [
  { id: 1, name: 'Alice', age: 30, dept: 'eng', salary: 120000 },
  { id: 2, name: 'Bob', age: 25, dept: 'eng', salary: 110000 },
  { id: 3, name: 'Charlie', age: 35, dept: 'sales', salary: 95000 },
  { id: 4, name: 'Dana', age: 28, dept: 'hr', salary: 85000 },
  { id: 5, name: 'Eve', age: 32, dept: 'eng', salary: 130000 },
];

describe('ExpressionCompiler', () => {
  it('simple equality', () => {
    const ec = new ExpressionCompiler();
    const result = ec.filter(rows, { type: 'COMPARE', op: 'EQ', left: col('dept'), right: lit('eng') });
    assert.equal(result.length, 3);
  });

  it('comparison: age > 30', () => {
    const ec = new ExpressionCompiler();
    const result = ec.filter(rows, { type: 'COMPARE', op: 'GT', left: col('age'), right: lit(30) });
    assert.equal(result.length, 2); // Charlie=35, Eve=32
  });

  it('AND: dept=eng AND salary > 115000', () => {
    const ec = new ExpressionCompiler();
    const expr = {
      type: 'AND',
      left: { type: 'COMPARE', op: 'EQ', left: col('dept'), right: lit('eng') },
      right: { type: 'COMPARE', op: 'GT', left: col('salary'), right: lit(115000) },
    };
    const result = ec.filter(rows, expr);
    assert.equal(result.length, 2); // Alice=120K, Eve=130K
  });

  it('OR: dept=hr OR dept=sales', () => {
    const ec = new ExpressionCompiler();
    const expr = {
      type: 'OR',
      left: { type: 'COMPARE', op: 'EQ', left: col('dept'), right: lit('hr') },
      right: { type: 'COMPARE', op: 'EQ', left: col('dept'), right: lit('sales') },
    };
    const result = ec.filter(rows, expr);
    assert.equal(result.length, 2);
  });

  it('NOT', () => {
    const ec = new ExpressionCompiler();
    const result = ec.filter(rows, {
      type: 'NOT',
      expr: { type: 'COMPARE', op: 'EQ', left: col('dept'), right: lit('eng') },
    });
    assert.equal(result.length, 2);
  });

  it('BETWEEN', () => {
    const ec = new ExpressionCompiler();
    const result = ec.filter(rows, {
      type: 'BETWEEN', value: col('age'), low: lit(28), high: lit(32),
    });
    assert.equal(result.length, 3); // Alice=30, Dana=28, Eve=32
  });

  it('IN', () => {
    const ec = new ExpressionCompiler();
    const result = ec.filter(rows, {
      type: 'IN', value: col('dept'), list: [lit('eng'), lit('hr')],
    });
    assert.equal(result.length, 4); // 3 eng + 1 hr
  });

  it('LIKE', () => {
    const ec = new ExpressionCompiler();
    const result = ec.filter(rows, {
      type: 'LIKE', column: col('name'), pattern: 'A%',
    });
    assert.equal(result.length, 1); // Alice
  });

  it('IS_NULL / IS_NOT_NULL', () => {
    const data = [{ a: 1 }, { a: null }, { a: 3 }, { a: undefined }];
    const ec = new ExpressionCompiler();
    
    const nulls = ec.filter(data, { type: 'IS_NULL', expr: col('a') });
    assert.equal(nulls.length, 2);
    
    const notNulls = ec.filter(data, { type: 'IS_NOT_NULL', expr: col('a') });
    assert.equal(notNulls.length, 2);
  });

  it('function: UPPER', () => {
    const ec = new ExpressionCompiler();
    const { fn } = ec.compile({ type: 'FUNC', name: 'UPPER', args: [col('name')] });
    assert.equal(fn({ name: 'Alice' }), 'ALICE');
  });

  it('compilation cache', () => {
    const ec = new ExpressionCompiler();
    const expr = { type: 'COMPARE', op: 'EQ', left: col('dept'), right: lit('eng') };
    ec.compile(expr);
    ec.compile(expr);
    assert.equal(ec.stats.compilations, 1);
    assert.equal(ec.stats.cacheHits, 1);
  });

  it('generated code is readable', () => {
    const ec = new ExpressionCompiler();
    const { code } = ec.compile({
      type: 'AND',
      left: { type: 'COMPARE', op: 'EQ', left: col('dept'), right: lit('eng') },
      right: { type: 'COMPARE', op: 'GT', left: col('salary'), right: lit(100000) },
    });
    assert.ok(code.includes('row["dept"]'));
    assert.ok(code.includes('==='));
    assert.ok(code.includes('&&'));
  });

  it('benchmark: compiled vs interpreted on 100K rows', () => {
    const n = 100000;
    const data = Array.from({ length: n }, (_, i) => ({ a: i, b: i % 10, c: `str_${i}` }));
    
    const ec = new ExpressionCompiler();
    const expr = {
      type: 'AND',
      left: { type: 'COMPARE', op: 'GT', left: col('a'), right: lit(50000) },
      right: { type: 'COMPARE', op: 'EQ', left: col('b'), right: lit(3) },
    };

    // Compiled
    const { fn } = ec.compile(expr);
    const t0 = Date.now();
    const r1 = data.filter(fn);
    const compiledMs = Date.now() - t0;

    // Interpreted
    const t1 = Date.now();
    const r2 = data.filter(row => row.a > 50000 && row.b === 3);
    const nativeMs = Date.now() - t1;

    console.log(`    Compiled: ${compiledMs}ms, Native: ${nativeMs}ms, ratio: ${(compiledMs / nativeMs).toFixed(1)}x`);
    assert.equal(r1.length, r2.length);
  });
});
