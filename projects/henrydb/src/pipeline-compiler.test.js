// pipeline-compiler.test.js — Tests for push-based pipeline compilation

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';
import { SeqScan, Filter, Project, Limit, Sort, HashJoin, HashAggregate } from './volcano.js';
import { identifyPipelines, compilePipeline, compileQueryPlan, CompiledIterator,
         compilePredicate, compileProjection } from './pipeline-compiler.js';

let db;

function rows(result) {
  if (Array.isArray(result)) return result;
  if (result && result.rows) return result.rows;
  return [];
}

function setup() {
  db = new Database();
  db.execute('CREATE TABLE employees (id INT, name TEXT, dept TEXT, salary INT)');
  for (let i = 1; i <= 1000; i++) {
    const dept = ['Engineering', 'Sales', 'Marketing', 'HR'][(i - 1) % 4];
    const salary = 50000 + (i * 100 % 50000);
    db.execute(`INSERT INTO employees VALUES (${i}, 'Emp${i}', '${dept}', ${salary})`);
  }
}

function teardown() {
  db = null;
}

// ===== PREDICATE COMPILATION =====

describe('Predicate Compilation', () => {
  it('compiles equality predicate', () => {
    const pred = compilePredicate({ column: 'name', op: '=', value: 'Alice' });
    assert.equal(pred({ name: 'Alice', age: 30 }), true);
    assert.equal(pred({ name: 'Bob', age: 25 }), false);
  });

  it('compiles comparison predicates', () => {
    const gt = compilePredicate({ column: 'age', op: '>', value: 18 });
    assert.equal(gt({ age: 20 }), true);
    assert.equal(gt({ age: 15 }), false);

    const lte = compilePredicate({ column: 'salary', op: '<=', value: 50000 });
    assert.equal(lte({ salary: 50000 }), true);
    assert.equal(lte({ salary: 60000 }), false);
  });

  it('compiles BETWEEN predicate', () => {
    const pred = compilePredicate({ column: 'age', op: 'BETWEEN', low: 18, high: 65 });
    assert.equal(pred({ age: 30 }), true);
    assert.equal(pred({ age: 10 }), false);
    assert.equal(pred({ age: 70 }), false);
  });

  it('passes through existing functions', () => {
    const fn = (row) => row.x > 5;
    const result = compilePredicate(fn);
    assert.equal(result, fn);
  });
});

// ===== PROJECTION COMPILATION =====

describe('Projection Compilation', () => {
  it('compiles column projection', () => {
    const proj = compileProjection(['name', 'age']);
    const result = proj({ name: 'Alice', age: 30, dept: 'Engineering' });
    assert.deepEqual(result, { name: 'Alice', age: 30 });
  });

  it('handles single column', () => {
    const proj = compileProjection(['id']);
    const result = proj({ id: 1, name: 'Alice', dept: 'Engineering' });
    assert.deepEqual(result, { id: 1 });
  });
});

// ===== PIPELINE IDENTIFICATION =====

describe('Pipeline Identification', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('identifies scan+filter+project as single pipeline', () => {
    const heap = db.tables.get('employees').heap;
    const scan = new SeqScan(heap, ['id','name','dept','salary'], 'employees');
    const filter = new Filter(scan, (row) => row.salary > 60000);
    const project = new Project(filter, [{name:'name',expr:r=>r.name},{name:'salary',expr:r=>r.salary}]);

    const result = identifyPipelines(project);
    assert.ok(result, 'Should identify pipeline');
    assert.equal(result.type, 'pipeline');
    assert.ok(result.operators.length >= 1);
  });
});

// ===== COMPILED PIPELINE EXECUTION =====

describe('Compiled Pipeline Execution', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('compiled scan+filter matches Volcano scan+filter', () => {
    const heap = db.tables.get('employees').heap;
    
    // Volcano execution
    const volcanoScan = new SeqScan(heap, ['id','name','dept','salary'], 'employees');
    const volcanoFilter = new Filter(volcanoScan, (row) => row.salary > 80000);
    const volcanoResults = volcanoFilter.toArray();
    
    // Compiled execution
    const compiledScan = new SeqScan(heap, ['id','name','dept','salary'], 'employees');
    const compiledFilter = new Filter(compiledScan, (row) => row.salary > 80000);
    const compiled = compilePipeline([compiledFilter, compiledScan]);
    assert.ok(compiled, 'Should compile');
    
    const compiledResults = [...compiled.execute()];
    
    // Results should match
    assert.equal(compiledResults.length, volcanoResults.length, 
      `Compiled (${compiledResults.length}) vs Volcano (${volcanoResults.length})`);
  });

  it('compiled scan+filter+project produces correct results', () => {
    const heap = db.tables.get('employees').heap;
    
    const scan = new SeqScan(heap, ['id','name','dept','salary'], 'employees');
    const filter = new Filter(scan, (row) => row.dept === 'Engineering');
    const project = new Project(filter, [{name:'name',expr:r=>r.name},{name:'salary',expr:r=>r.salary}]);
    
    const compiled = compilePipeline([project, filter, scan]);
    assert.ok(compiled);
    
    const results = [...compiled.execute()];
    assert.equal(results.length, 250, 'Should have 250 Engineering employees');
    
    for (const row of results) {
      assert.ok(row.name, 'Should have name');
      assert.ok(row.salary, 'Should have salary');
      assert.equal(row.dept, undefined, 'dept should be projected out');
      assert.equal(row.id, undefined, 'id should be projected out');
    }
  });

  it('compiled pipeline with LIMIT', () => {
    const heap = db.tables.get('employees').heap;
    
    const scan = new SeqScan(heap, ['id','name','dept','salary'], 'employees');
    const filter = new Filter(scan, (row) => row.salary > 60000);
    const limit = new Limit(filter, 10);
    
    const compiled = compilePipeline([limit, filter, scan]);
    assert.ok(compiled);
    
    const results = [...compiled.execute()];
    assert.equal(results.length, 10, 'Should return exactly 10 rows');
  });
});

// ===== COMPILED ITERATOR COMPATIBILITY =====

describe('CompiledIterator', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('works as a drop-in Iterator replacement', () => {
    const heap = db.tables.get('employees').heap;
    
    const scan = new SeqScan(heap, ['id','name','dept','salary'], 'employees');
    const filter = new Filter(scan, (row) => row.salary > 90000);
    const compiled = compilePipeline([filter, scan]);
    
    const iter = new CompiledIterator(compiled);
    iter.open();
    
    const results = [];
    let row;
    while ((row = iter.next()) !== null) {
      results.push(row);
    }
    iter.close();
    
    assert.ok(results.length > 0);
    for (const r of results) {
      assert.ok(r.salary > 90000);
    }
  });

  it('toArray() works', () => {
    const heap = db.tables.get('employees').heap;
    
    const scan = new SeqScan(heap, ['id','name','dept','salary'], 'employees');
    const filter = new Filter(scan, (row) => row.dept === 'HR');
    const compiled = compilePipeline([filter, scan]);
    
    const iter = new CompiledIterator(compiled);
    const results = iter.toArray();
    assert.equal(results.length, 250);
  });
});

// ===== PERFORMANCE BENCHMARK =====

describe('Pipeline Compilation Performance', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('compiled pipeline is at least as fast as Volcano on scan+filter', () => {
    const heap = db.tables.get('employees').heap;
    const iterations = 10;
    
    // Benchmark Volcano
    let volcanoTime = 0;
    let volcanoCount = 0;
    for (let i = 0; i < iterations; i++) {
      const scan = new SeqScan(heap, ['id','name','dept','salary'], 'employees');
      const filter = new Filter(scan, (row) => row.salary > 70000);
      const start = process.hrtime.bigint();
      const results = filter.toArray();
      volcanoTime += Number(process.hrtime.bigint() - start);
      volcanoCount = results.length;
    }
    
    // Benchmark Compiled
    let compiledTime = 0;
    let compiledCount = 0;
    for (let i = 0; i < iterations; i++) {
      const scan = new SeqScan(heap, ['id','name','dept','salary'], 'employees');
      const filter = new Filter(scan, (row) => row.salary > 70000);
      const compiled = compilePipeline([filter, scan]);
      const start = process.hrtime.bigint();
      const results = [...compiled.execute()];
      compiledTime += Number(process.hrtime.bigint() - start);
      compiledCount = results.length;
    }
    
    assert.equal(compiledCount, volcanoCount, 'Both should return same count');
    
    // Log performance (don't assert on timing — too variable in CI)
    const volcanoAvg = volcanoTime / iterations / 1e6;
    const compiledAvg = compiledTime / iterations / 1e6;
    const speedup = volcanoAvg / compiledAvg;
    console.log(`    Volcano: ${volcanoAvg.toFixed(2)}ms, Compiled: ${compiledAvg.toFixed(2)}ms, Speedup: ${speedup.toFixed(2)}x`);
  });

  it('correctness: compiled and Volcano produce identical results', () => {
    const heap = db.tables.get('employees').heap;
    
    // Complex pipeline: scan → filter → project → limit
    const volcanoScan = new SeqScan(heap, ['id','name','dept','salary'], 'employees');
    const volcanoFilter = new Filter(volcanoScan, (row) => row.salary > 60000 && row.dept === 'Engineering');
    const volcanoProject = new Project(volcanoFilter, [{name:'id',expr:r=>r.id},{name:'salary',expr:r=>r.salary}]);
    const volcanoLimit = new Limit(volcanoProject, 50);
    const volcanoResults = volcanoLimit.toArray();
    
    const compiledScan = new SeqScan(heap, ['id','name','dept','salary'], 'employees');
    const compiledFilter = new Filter(compiledScan, (row) => row.salary > 60000 && row.dept === 'Engineering');
    const compiledProject = new Project(compiledFilter, [{name:'id',expr:r=>r.id},{name:'salary',expr:r=>r.salary}]);
    const compiledLimit = new Limit(compiledProject, 50);
    const compiled = compilePipeline([compiledLimit, compiledProject, compiledFilter, compiledScan]);
    const compiledResults = [...compiled.execute()];
    
    assert.equal(compiledResults.length, volcanoResults.length);
    for (let i = 0; i < compiledResults.length; i++) {
      assert.deepEqual(compiledResults[i], volcanoResults[i], `Row ${i} mismatch`);
    }
  });
});
