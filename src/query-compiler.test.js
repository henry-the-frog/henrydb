// query-compiler.test.js — Tests for query compilation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { compileFilter, compileProjection, compileScanFilterProject } from './query-compiler.js';
import { parse } from './sql.js';

const schema = [
  { name: 'id' },
  { name: 'name' },
  { name: 'age' },
  { name: 'city' },
];

function getWhere(sql) {
  return parse(sql).where;
}

describe('Expression Compiler', () => {
  it('simple equality', () => {
    const fn = compileFilter(getWhere("SELECT * FROM t WHERE age = 30"), schema);
    assert.strictEqual(fn([1, 'Alice', 30, 'NYC']), true);
    assert.strictEqual(fn([2, 'Bob', 25, 'LA']), false);
  });

  it('comparison operators', () => {
    assert.strictEqual(compileFilter(getWhere("SELECT * FROM t WHERE age > 25"), schema)([1, '', 30, '']), true);
    assert.strictEqual(compileFilter(getWhere("SELECT * FROM t WHERE age > 25"), schema)([1, '', 20, '']), false);
    assert.strictEqual(compileFilter(getWhere("SELECT * FROM t WHERE age <= 25"), schema)([1, '', 25, '']), true);
    assert.strictEqual(compileFilter(getWhere("SELECT * FROM t WHERE age < 25"), schema)([1, '', 25, '']), false);
  });

  it('AND', () => {
    const fn = compileFilter(getWhere("SELECT * FROM t WHERE age > 25 AND city = 'NYC'"), schema);
    assert.strictEqual(fn([1, 'Alice', 30, 'NYC']), true);
    assert.strictEqual(fn([2, 'Bob', 30, 'LA']), false);
    assert.strictEqual(fn([3, 'Carol', 20, 'NYC']), false);
  });

  it('OR', () => {
    const fn = compileFilter(getWhere("SELECT * FROM t WHERE age > 50 OR city = 'NYC'"), schema);
    assert.strictEqual(fn([1, 'Alice', 30, 'NYC']), true);
    assert.strictEqual(fn([2, 'Bob', 60, 'LA']), true);
    assert.strictEqual(fn([3, 'Carol', 20, 'LA']), false);
  });

  it('IN list', () => {
    const fn = compileFilter(getWhere("SELECT * FROM t WHERE id IN (1, 3, 5)"), schema);
    assert.strictEqual(fn([1, '', 0, '']), true);
    assert.strictEqual(fn([2, '', 0, '']), false);
    assert.strictEqual(fn([5, '', 0, '']), true);
  });

  it('string equality', () => {
    const fn = compileFilter(getWhere("SELECT * FROM t WHERE name = 'Alice'"), schema);
    assert.strictEqual(fn([1, 'Alice', 30, 'NYC']), true);
    assert.strictEqual(fn([2, 'Bob', 25, 'LA']), false);
  });

  it('arithmetic in WHERE', () => {
    const fn = compileFilter(getWhere("SELECT * FROM t WHERE age * 2 > 50"), schema);
    assert.strictEqual(fn([1, '', 30, '']), true);
    assert.strictEqual(fn([2, '', 20, '']), false);
  });

  it('no WHERE returns true', () => {
    const fn = compileFilter(null, schema);
    assert.strictEqual(fn([1, 'Alice', 30, 'NYC']), true);
  });
});

describe('Projection Compiler', () => {
  it('select specific columns', () => {
    const ast = parse('SELECT name, age FROM t');
    const fn = compileProjection(ast.columns, schema);
    const result = fn([1, 'Alice', 30, 'NYC']);
    assert.deepStrictEqual(result, { name: 'Alice', age: 30 });
  });

  it('select with expression', () => {
    const ast = parse('SELECT name, age * 2 as double_age FROM t');
    const fn = compileProjection(ast.columns, schema);
    const result = fn([1, 'Alice', 30, 'NYC']);
    assert.strictEqual(result.name, 'Alice');
    assert.strictEqual(result.double_age, 60);
  });

  it('select star', () => {
    const ast = parse('SELECT * FROM t');
    const fn = compileProjection(ast.columns, schema);
    const result = fn([1, 'Alice', 30, 'NYC']);
    assert.strictEqual(result.id, 1);
    assert.strictEqual(result.name, 'Alice');
    assert.strictEqual(result.city, 'NYC');
  });
});

describe('Scan-Filter-Project Compiler', () => {
  const makeHeap = (rows) => rows.map(r => ({ values: r }));
  
  it('compiled pipeline returns correct results', () => {
    const ast = parse("SELECT name, age FROM t WHERE age > 25");
    const fn = compileScanFilterProject(ast.where, ast.columns, schema);
    
    const heap = makeHeap([
      [1, 'Alice', 30, 'NYC'],
      [2, 'Bob', 20, 'LA'],
      [3, 'Carol', 35, 'SF'],
    ]);
    
    const results = fn(heap);
    assert.strictEqual(results.length, 2);
    assert.strictEqual(results[0].name, 'Alice');
    assert.strictEqual(results[1].name, 'Carol');
  });

  it('compiled pipeline with LIMIT', () => {
    const ast = parse("SELECT name FROM t WHERE age > 0");
    const fn = compileScanFilterProject(ast.where, ast.columns, schema, { limit: 2 });
    
    const heap = makeHeap([
      [1, 'Alice', 30, 'NYC'],
      [2, 'Bob', 20, 'LA'],
      [3, 'Carol', 35, 'SF'],
    ]);
    
    const results = fn(heap);
    assert.strictEqual(results.length, 2);
  });

  it('compiled pipeline with OFFSET', () => {
    const ast = parse("SELECT name FROM t WHERE age > 0");
    const fn = compileScanFilterProject(ast.where, ast.columns, schema, { offset: 1 });
    
    const heap = makeHeap([
      [1, 'Alice', 30, 'NYC'],
      [2, 'Bob', 20, 'LA'],
      [3, 'Carol', 35, 'SF'],
    ]);
    
    const results = fn(heap);
    assert.strictEqual(results.length, 2);
    assert.strictEqual(results[0].name, 'Bob');
  });

  it('benchmark: compiled vs interpreted on 10K rows', () => {
    const rows = [];
    for (let i = 0; i < 10000; i++) {
      rows.push([i, `name_${i}`, i % 100, `city_${i % 50}`]);
    }
    const heap = makeHeap(rows);
    
    const ast = parse("SELECT name, age FROM t WHERE age > 50 AND age < 80");
    
    // Compiled
    const compiled = compileScanFilterProject(ast.where, ast.columns, schema);
    const startC = performance.now();
    for (let j = 0; j < 100; j++) compiled(heap);
    const timeC = performance.now() - startC;
    
    // Interpreted (manual simulation)
    const startI = performance.now();
    for (let j = 0; j < 100; j++) {
      const results = [];
      for (const entry of heap) {
        const v = entry.values;
        if (v[2] > 50 && v[2] < 80) {
          results.push({ name: v[1], age: v[2] });
        }
      }
    }
    const timeI = performance.now() - startI;
    
    const compiledResults = compiled(heap);
    assert.strictEqual(compiledResults.length, 2900); // 29 values (51-79) * 100 rows each
    
    console.log(`    Compiled: ${timeC.toFixed(1)}ms, Interpreted: ${timeI.toFixed(1)}ms, Ratio: ${(timeI/timeC).toFixed(2)}x`);
    // The compiled version should be close to the hand-written interpreted version
    // since both compile to similar JS. The real win is vs the AST-walking interpreter.
  });
});
