// vectorized-codegen.test.js — Tests for vectorized compiled execution
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { VectorizedCodeGen } from './vectorized-codegen.js';
import { QueryCodeGen } from './query-codegen.js';
import { CompiledQueryEngine } from './compiled-query.js';
import { Database } from './db.js';

function setupDB(n = 200) {
  const db = new Database();
  db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region TEXT, tier INT)');
  db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT)');

  for (let i = 0; i < n; i++) {
    const region = ['US', 'EU', 'APAC'][i % 3];
    const tier = (i % 4) + 1;
    db.execute(`INSERT INTO customers VALUES (${i}, 'Customer ${i}', '${region}', ${tier})`);
  }
  for (let i = 0; i < n * 3; i++) {
    const custId = i % n;
    const amount = (i * 17 + 13) % 1000;
    const status = ['pending', 'shipped', 'delivered'][i % 3];
    db.execute(`INSERT INTO orders VALUES (${i}, ${custId}, ${amount}, '${status}')`);
  }

  return db;
}

describe('VectorizedCodeGen', () => {

  it('vectorized scan returns all rows', () => {
    const db = setupDB(100);
    const vec = new VectorizedCodeGen(db);

    const result = vec.execute({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers' },
    });

    assert.ok(result);
    assert.equal(result.rows.length, 100);
  });

  it('vectorized scan with equality filter', () => {
    const db = setupDB(300);
    const vec = new VectorizedCodeGen(db);

    const result = vec.execute({
      type: 'SELECT',
      columns: [{ name: 'id' }, { name: 'name' }, { name: 'region' }],
      from: { table: 'customers' },
      where: {
        type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 'region' },
        right: { type: 'literal', value: 'US' }
      }
    });

    assert.ok(result);
    assert.equal(result.rows.length, 100); // 300/3
    assert.ok(result.rows.every(r => r.region === 'US'));
  });

  it('vectorized scan with range filter', () => {
    const db = setupDB(200);
    const vec = new VectorizedCodeGen(db);

    const result = vec.execute({
      type: 'SELECT',
      columns: [{ name: 'id' }, { name: 'tier' }],
      from: { table: 'customers' },
      where: {
        type: 'COMPARE', op: 'GT',
        left: { type: 'column_ref', name: 'tier' },
        right: { type: 'literal', value: 3 }
      }
    });

    assert.ok(result);
    assert.ok(result.rows.every(r => r.tier > 3));
    assert.equal(result.rows.length, 50); // 200/4
  });

  it('vectorized scan with LIMIT', () => {
    const db = setupDB(200);
    const vec = new VectorizedCodeGen(db);

    const result = vec.execute({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers' },
      limit: { value: 25 }
    });

    assert.ok(result);
    assert.equal(result.rows.length, 25);
  });

  it('vectorized AND filter', () => {
    const db = setupDB(300);
    const vec = new VectorizedCodeGen(db);

    const result = vec.execute({
      type: 'SELECT',
      columns: [{ name: 'id' }],
      from: { table: 'customers' },
      where: {
        type: 'AND',
        left: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', name: 'region' },
          right: { type: 'literal', value: 'EU' }
        },
        right: {
          type: 'COMPARE', op: 'GE',
          left: { type: 'column_ref', name: 'tier' },
          right: { type: 'literal', value: 3 }
        }
      }
    });

    assert.ok(result);
    assert.ok(result.rows.length > 0);
  });

  it('vectorized hash join', () => {
    const db = setupDB(100);
    const vec = new VectorizedCodeGen(db);

    const result = vec.execute({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers', alias: 'c' },
      joins: [{
        table: 'orders',
        alias: 'o',
        joinType: 'INNER',
        on: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', table: 'c', name: 'id' },
          right: { type: 'column_ref', table: 'o', name: 'customer_id' }
        }
      }],
    });

    assert.ok(result);
    assert.equal(result.rows.length, 300); // 100 * 3
  });

  it('vectorized join with LIMIT', () => {
    const db = setupDB(100);
    const vec = new VectorizedCodeGen(db);

    const result = vec.execute({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers', alias: 'c' },
      joins: [{
        table: 'orders',
        alias: 'o',
        joinType: 'INNER',
        on: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', table: 'c', name: 'id' },
          right: { type: 'column_ref', table: 'o', name: 'customer_id' }
        }
      }],
      limit: { value: 50 }
    });

    assert.ok(result);
    assert.equal(result.rows.length, 50);
  });

  it('correctness: matches standard execution', () => {
    const db = setupDB(100);
    const vec = new VectorizedCodeGen(db);

    const vectorized = vec.execute({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers', alias: 'c' },
      joins: [{
        table: 'orders',
        alias: 'o',
        joinType: 'INNER',
        on: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', table: 'c', name: 'id' },
          right: { type: 'column_ref', table: 'o', name: 'customer_id' }
        }
      }],
    });

    const standard = db.execute('SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id');
    assert.equal(vectorized.rows.length, standard.rows.length);
  });

  it('benchmark: vectorized vs row-compiled vs closure vs Volcano (500 customers)', () => {
    const db = setupDB(500);
    const vec = new VectorizedCodeGen(db);
    const codegen = new QueryCodeGen(db);
    const closure = new CompiledQueryEngine(db);

    const ast = {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers', alias: 'c' },
      joins: [{
        table: 'orders',
        alias: 'o',
        joinType: 'INNER',
        on: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', table: 'c', name: 'id' },
          right: { type: 'column_ref', table: 'o', name: 'customer_id' }
        }
      }],
      limit: { value: 1000 }
    };

    // Vectorized
    const t0 = Date.now();
    const vecResult = vec.execute(ast);
    const vecMs = Date.now() - t0;

    // Row-compiled (codegen)
    const t1 = Date.now();
    const codegenResult = codegen.execute(ast);
    const codegenMs = Date.now() - t1;

    // Closure-compiled
    const t2 = Date.now();
    const closureResult = closure.executeSelect(ast);
    const closureMs = Date.now() - t2;

    // Volcano
    const t3 = Date.now();
    const volcanoResult = db.execute('SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id LIMIT 1000');
    const volcanoMs = Date.now() - t3;

    console.log(`    Vectorized: ${vecMs}ms | Codegen: ${codegenMs}ms | Closure: ${closureMs}ms | Volcano: ${volcanoMs}ms`);
    console.log(`    Vec vs Volcano: ${(volcanoMs / Math.max(vecMs, 1)).toFixed(1)}x | Codegen vs Volcano: ${(volcanoMs / Math.max(codegenMs, 1)).toFixed(1)}x`);

    assert.ok(vecResult);
    assert.equal(vecResult.rows.length, 1000);
  });

  it('tracks batch processing stats', () => {
    const db = setupDB(2000);
    const vec = new VectorizedCodeGen(db);

    vec.execute({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers' },
    });

    assert.ok(vec.stats.batchesProcessed >= 2); // 2000 rows / 1024 batch = 2 batches
    assert.equal(vec.stats.queriesCompiled, 1);
  });

  it('large dataset: 10K rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE big (id INT PRIMARY KEY, val INT, grp TEXT)');
    for (let i = 0; i < 10000; i++) {
      db.execute(`INSERT INTO big VALUES (${i}, ${i * 7 % 1000}, 'g${i % 100}')`);
    }

    const vec = new VectorizedCodeGen(db);
    const result = vec.execute({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'big' },
      where: {
        type: 'COMPARE', op: 'LT',
        left: { type: 'column_ref', name: 'val' },
        right: { type: 'literal', value: 100 }
      }
    });

    assert.ok(result);
    assert.ok(result.rows.length > 500); // ~10% of rows
    assert.ok(result.rows.every(r => r.val < 100));
    assert.ok(vec.stats.batchesProcessed >= 10); // 10K/1024 = ~10 batches
  });
});
