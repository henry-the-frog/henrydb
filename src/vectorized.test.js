// vectorized.test.js — Vectorized execution engine tests + benchmark
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  ColumnBatch, VectorizedScan, VectorizedFilter, VectorizedProject,
  VectorizedAggregate, buildFilterPredicate, andPredicate,
} from './vectorized.js';
import { Database } from './db.js';
import { compileScanFilterProject } from './query-compiler.js';
import { parse } from './sql.js';

describe('Vectorized Execution', () => {
  describe('ColumnBatch', () => {
    it('creates batch with columns', () => {
      const cols = new Map([
        ['id', [1, 2, 3]],
        ['name', ['Alice', 'Bob', 'Carol']],
      ]);
      const batch = new ColumnBatch(cols, 3);
      assert.strictEqual(batch.length, 3);
      assert.strictEqual(batch.activeCount, 3);
      assert.deepStrictEqual(batch.getColumn('id'), [1, 2, 3]);
    });

    it('rows() iterates all rows', () => {
      const cols = new Map([['id', [1, 2]], ['val', [10, 20]]]);
      const batch = new ColumnBatch(cols, 2);
      const rows = [...batch.rows()];
      assert.strictEqual(rows.length, 2);
      assert.deepStrictEqual(rows[0], { id: 1, val: 10 });
    });

    it('filter creates selection vector', () => {
      const cols = new Map([['id', [1, 2, 3, 4, 5]]]);
      const batch = new ColumnBatch(cols, 5);
      const filtered = batch.filter(new Uint32Array([0, 2, 4]));
      assert.strictEqual(filtered.activeCount, 3);
      const rows = [...filtered.rows()];
      assert.strictEqual(rows.length, 3);
      assert.deepStrictEqual(rows.map(r => r.id), [1, 3, 5]);
    });
  });

  describe('VectorizedScan', () => {
    it('scans heap into batches', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
      
      const schema = [{ name: 'id' }, { name: 'val' }];
      const scan = new VectorizedScan(db.tables.get('t').heap, schema, 30);
      
      const batches = [...scan.execute()];
      assert.strictEqual(batches.length, 4); // 30 + 30 + 30 + 10
      assert.strictEqual(batches[0].length, 30);
      assert.strictEqual(batches[3].length, 10);
    });
  });

  describe('VectorizedFilter', () => {
    it('filters batch with predicate', () => {
      const cols = new Map([['id', [1, 2, 3, 4, 5]], ['val', [10, 20, 30, 40, 50]]]);
      const batch = new ColumnBatch(cols, 5);
      
      const pred = buildFilterPredicate('val', '>', 25);
      const filter = new VectorizedFilter(pred);
      const result = filter.execute(batch);
      
      assert.strictEqual(result.activeCount, 3);
      const rows = [...result.rows()];
      assert.deepStrictEqual(rows.map(r => r.val), [30, 40, 50]);
    });
  });

  describe('VectorizedProject', () => {
    it('extracts columns', () => {
      const cols = new Map([['id', [1, 2]], ['name', ['A', 'B']], ['extra', [true, false]]]);
      const batch = new ColumnBatch(cols, 2);
      
      const project = new VectorizedProject(['id', 'name']);
      const result = project.execute(batch);
      
      assert.ok(result.getColumn('id'));
      assert.ok(result.getColumn('name'));
      assert.strictEqual(result.getColumn('extra'), undefined);
    });
  });

  describe('VectorizedAggregate', () => {
    it('computes aggregates', () => {
      const cols = new Map([['val', [10, 20, 30, 40, 50]]]);
      const batch = new ColumnBatch(cols, 5);
      
      const agg = new VectorizedAggregate([
        { fn: 'COUNT', column: 'val', alias: 'cnt' },
        { fn: 'SUM', column: 'val', alias: 'total' },
        { fn: 'AVG', column: 'val', alias: 'avg' },
        { fn: 'MIN', column: 'val', alias: 'min' },
        { fn: 'MAX', column: 'val', alias: 'max' },
      ]);
      
      const result = agg.execute(batch);
      assert.strictEqual(result.cnt, 5);
      assert.strictEqual(result.total, 150);
      assert.strictEqual(result.avg, 30);
      assert.strictEqual(result.min, 10);
      assert.strictEqual(result.max, 50);
    });

    it('aggregates with selection vector', () => {
      const cols = new Map([['val', [10, 20, 30, 40, 50]]]);
      const batch = new ColumnBatch(cols, 5, new Uint32Array([1, 3])); // rows 20, 40
      
      const agg = new VectorizedAggregate([
        { fn: 'SUM', column: 'val', alias: 'total' },
      ]);
      
      assert.strictEqual(agg.execute(batch).total, 60);
    });
  });

  describe('Benchmark: row-at-a-time vs compiled vs vectorized', () => {
    it('10K rows with complex filter', () => {
      const db = new Database();
      db.execute('CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT, region TEXT)');
      for (let i = 0; i < 10000; i++) {
        const status = ['pending', 'shipped', 'delivered', 'cancelled'][i % 4];
        const region = ['US', 'EU', 'APAC', 'LATAM'][i % 4];
        db.execute(`INSERT INTO orders VALUES (${i}, ${(i * 17) % 1000}, '${status}', '${region}')`);
      }
      
      const runs = 10;
      
      // 1. Row-at-a-time (interpreted)
      const startI = performance.now();
      for (let j = 0; j < runs; j++) {
        db.execute("SELECT id, amount FROM orders WHERE amount > 500 AND status = 'shipped'");
      }
      const timeI = (performance.now() - startI) / runs;
      
      // 2. Compiled
      const schema = [{ name: 'id' }, { name: 'amount' }, { name: 'status' }, { name: 'region' }];
      const ast = parse("SELECT id, amount FROM orders WHERE amount > 500 AND status = 'shipped'");
      const compiled = compileScanFilterProject(ast.where, ast.columns, schema);
      const heap = [...db.tables.get('orders').heap.scan()];
      
      const startC = performance.now();
      for (let j = 0; j < runs; j++) {
        compiled(heap);
      }
      const timeC = (performance.now() - startC) / runs;
      
      // 3. Vectorized
      const scan = new VectorizedScan(db.tables.get('orders').heap, schema, 1024);
      const pred1 = buildFilterPredicate('amount', '>', 500);
      const pred2 = buildFilterPredicate('status', '=', 'shipped');
      const filter = new VectorizedFilter(andPredicate(pred1, pred2));
      const project = new VectorizedProject(['id', 'amount']);
      
      const startV = performance.now();
      for (let j = 0; j < runs; j++) {
        const results = [];
        for (const batch of scan.execute()) {
          const filtered = filter.execute(batch);
          const projected = project.execute(filtered);
          for (const row of projected.rows()) results.push(row);
        }
      }
      const timeV = (performance.now() - startV) / runs;
      
      console.log(`    Interpreted: ${timeI.toFixed(1)}ms`);
      console.log(`    Compiled:    ${timeC.toFixed(1)}ms (${(timeI/timeC).toFixed(1)}x)`);
      console.log(`    Vectorized:  ${timeV.toFixed(1)}ms (${(timeI/timeV).toFixed(1)}x)`);
      
      assert.ok(timeC < timeI, 'Compiled should be faster than interpreted');
      // Vectorized may or may not be faster than compiled for this scale
    });
  });
});
