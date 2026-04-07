// pipeline-benchmark.test.js — Benchmark push-based compilation vs Volcano on larger data

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';
import { SeqScan, Filter, Project, Limit } from './volcano.js';
import { compilePipeline, compilePipelineJIT } from './pipeline-compiler.js';

let db;

function rows(result) {
  if (Array.isArray(result)) return result;
  if (result && result.rows) return result.rows;
  return [];
}

function setup() {
  db = new Database();
  db.execute('CREATE TABLE big (id INT, a INT, b INT, c TEXT, d INT)');
  
  // 10,000 rows
  for (let i = 1; i <= 10000; i++) {
    const a = i * 7 % 1000;
    const b = i * 13 % 500;
    const c = ['alpha', 'beta', 'gamma', 'delta'][(i - 1) % 4];
    const d = i * 3 % 100;
    db.execute(`INSERT INTO big VALUES (${i}, ${a}, ${b}, '${c}', ${d})`);
  }
}

function teardown() { db = null; }

function bench(label, fn, iterations) {
  // Warm up
  fn();
  fn();
  
  const start = process.hrtime.bigint();
  let result;
  for (let i = 0; i < iterations; i++) {
    result = fn();
  }
  const elapsed = Number(process.hrtime.bigint() - start) / 1e6;
  return { label, totalMs: elapsed, avgMs: elapsed / iterations, result };
}

describe('Pipeline Benchmark: 10K rows', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('selective filter (10% selectivity)', () => {
    const heap = db.tables.get('big').heap;
    const cols = ['id', 'a', 'b', 'c', 'd'];
    const predicate = (row) => row.a > 900;
    const iterations = 20;
    
    const volcano = bench('Volcano', () => {
      const scan = new SeqScan(heap, cols);
      const filter = new Filter(scan, predicate);
      return filter.toArray();
    }, iterations);
    
    const jit = bench('JIT', () => {
      const scan = new SeqScan(heap, cols);
      const filter = new Filter(scan, predicate);
      const compiled = compilePipelineJIT([filter, scan]);
      return compiled.execute();
    }, iterations);
    
    // Amortized JIT: compile once, run many times
    const jitScan = new SeqScan(heap, cols);
    const jitFilter = new Filter(jitScan, predicate);
    const compiledOnce = compilePipelineJIT([jitFilter, jitScan]);
    const amortized = bench('JIT-amortized', () => {
      return compiledOnce.execute();
    }, iterations);
    
    assert.equal(volcano.result.length, jit.result.length, 'Same result count');
    
    const speedupJIT = volcano.avgMs / jit.avgMs;
    const speedupAmort = volcano.avgMs / amortized.avgMs;
    console.log(`    10K rows, 10% selectivity (${iterations} iterations):`);
    console.log(`      Volcano:       ${volcano.avgMs.toFixed(2)}ms`);
    console.log(`      JIT (fresh):   ${jit.avgMs.toFixed(2)}ms (${speedupJIT.toFixed(2)}x)`);
    console.log(`      JIT (amort):   ${amortized.avgMs.toFixed(2)}ms (${speedupAmort.toFixed(2)}x)`);
  });

  it('filter + project (select specific columns)', () => {
    const heap = db.tables.get('big').heap;
    const cols = ['id', 'a', 'b', 'c', 'd'];
    const predicate = (row) => row.c === 'alpha' && row.b > 250;
    const projection = [{name:'id',expr:r=>r.id},{name:'a',expr:r=>r.a}];
    const iterations = 20;
    
    const volcano = bench('Volcano', () => {
      const scan = new SeqScan(heap, cols);
      const filter = new Filter(scan, predicate);
      const project = new Project(filter, projection);
      return project.toArray();
    }, iterations);
    
    const scan = new SeqScan(heap, cols);
    const filter = new Filter(scan, predicate);
    const project = new Project(filter, projection);
    const compiledOnce = compilePipelineJIT([project, filter, scan]);
    const amortized = bench('JIT-amortized', () => {
      return compiledOnce.execute();
    }, iterations);
    
    assert.equal(volcano.result.length, amortized.result.length);
    
    const speedup = volcano.avgMs / amortized.avgMs;
    console.log(`    Filter+Project (${iterations} iterations):`);
    console.log(`      Volcano:       ${volcano.avgMs.toFixed(2)}ms`);
    console.log(`      JIT (amort):   ${amortized.avgMs.toFixed(2)}ms (${speedup.toFixed(2)}x)`);
  });

  it('full scan with LIMIT 100', () => {
    const heap = db.tables.get('big').heap;
    const cols = ['id', 'a', 'b', 'c', 'd'];
    const iterations = 50;
    
    const volcano = bench('Volcano', () => {
      const scan = new SeqScan(heap, cols);
      const limit = new Limit(scan, 100);
      return limit.toArray();
    }, iterations);
    
    const scan = new SeqScan(heap, cols);
    const limit = new Limit(scan, 100);
    const compiledOnce = compilePipelineJIT([limit, scan]);
    const amortized = bench('JIT-amortized', () => {
      return compiledOnce.execute();
    }, iterations);
    
    assert.equal(volcano.result.length, 100);
    assert.equal(amortized.result.length, 100);
    
    const speedup = volcano.avgMs / amortized.avgMs;
    console.log(`    Full scan + LIMIT 100 (${iterations} iterations):`);
    console.log(`      Volcano:       ${volcano.avgMs.toFixed(2)}ms`);
    console.log(`      JIT (amort):   ${amortized.avgMs.toFixed(2)}ms (${speedup.toFixed(2)}x)`);
  });

  it('wide filter (50% selectivity)', () => {
    const heap = db.tables.get('big').heap;
    const cols = ['id', 'a', 'b', 'c', 'd'];
    const predicate = (row) => row.d > 50;
    const iterations = 20;
    
    const volcano = bench('Volcano', () => {
      const scan = new SeqScan(heap, cols);
      const filter = new Filter(scan, predicate);
      return filter.toArray();
    }, iterations);
    
    const scan = new SeqScan(heap, cols);
    const filter = new Filter(scan, predicate);
    const compiledOnce = compilePipelineJIT([filter, scan]);
    const amortized = bench('JIT-amortized', () => {
      return compiledOnce.execute();
    }, iterations);
    
    assert.equal(volcano.result.length, amortized.result.length);
    
    const speedup = volcano.avgMs / amortized.avgMs;
    console.log(`    Wide filter 50% (${iterations} iterations):`);
    console.log(`      Volcano:       ${volcano.avgMs.toFixed(2)}ms`);
    console.log(`      JIT (amort):   ${amortized.avgMs.toFixed(2)}ms (${speedup.toFixed(2)}x)`);
  });
});
