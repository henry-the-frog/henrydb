// query-codegen.test.js — Tests for batch Function() compilation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { QueryCodeGen } from './query-codegen.js';
import { Database } from './db.js';

import { CompiledQueryEngine } from './compiled-query.js';

function setupDB(n = 200) {
  const db = new Database();
  db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region TEXT, tier INT)');
  db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT)');
  db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT, category TEXT)');

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
  for (let i = 0; i < 50; i++) {
    const cat = ['electronics', 'books', 'clothing', 'food'][i % 4];
    db.execute(`INSERT INTO products VALUES (${i}, 'Product ${i}', ${(i + 1) * 10}, '${cat}')`);
  }

  return db;
}

describe('QueryCodeGen', () => {

  it('compiles single-table scan', () => {
    const db = setupDB(100);
    const codegen = new QueryCodeGen(db);

    const result = codegen.execute({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers' },
    });

    assert.ok(result);
    assert.equal(result.rows.length, 100);
    assert.ok(result.rows[0].id !== undefined);
    assert.ok(result.rows[0].name !== undefined);
  });

  it('single-table with equality filter', () => {
    const db = setupDB(200);
    const codegen = new QueryCodeGen(db);

    const result = codegen.execute({
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
    assert.ok(result.rows.length > 50 && result.rows.length < 100);
    assert.ok(result.rows.every(r => r.region === 'US'));
  });

  it('single-table with range filter', () => {
    const db = setupDB(200);
    const codegen = new QueryCodeGen(db);

    const result = codegen.execute({
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
  });

  it('AND filter', () => {
    const db = setupDB(200);
    const codegen = new QueryCodeGen(db);

    const result = codegen.execute({
      type: 'SELECT',
      columns: [{ name: 'id' }, { name: 'name' }],
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
    assert.ok(result.rows.length > 20);
  });

  it('LIMIT works', () => {
    const db = setupDB(200);
    const codegen = new QueryCodeGen(db);

    const result = codegen.execute({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers' },
      limit: { value: 10 }
    });

    assert.ok(result);
    assert.equal(result.rows.length, 10);
  });

  it('hash join between two tables', () => {
    const db = setupDB(100);
    const codegen = new QueryCodeGen(db);

    const result = codegen.execute({
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
    assert.equal(result.rows.length, 300); // 100 customers × 3 orders each
  });

  it('join with LIMIT', () => {
    const db = setupDB(100);
    const codegen = new QueryCodeGen(db);

    const result = codegen.execute({
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

  it('join with column collision disambiguated', () => {
    const db = setupDB(100);
    const codegen = new QueryCodeGen(db);

    const result = codegen.execute({
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
      limit: { value: 1 }
    });

    assert.ok(result);
    const row = result.rows[0];
    // Should have both id and o.id or similar disambiguation
    assert.ok('id' in row);
    assert.ok('name' in row);
    assert.ok('customer_id' in row);
    assert.ok('amount' in row);
  });

  it('specific column projection in join', () => {
    const db = setupDB(100);
    const codegen = new QueryCodeGen(db);

    const result = codegen.execute({
      type: 'SELECT',
      columns: [
        { name: 'name', table: 'c' },
        { name: 'amount', table: 'o' },
      ],
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
      limit: { value: 5 }
    });

    assert.ok(result);
    assert.equal(result.rows.length, 5);
    assert.ok('name' in result.rows[0]);
    assert.ok('amount' in result.rows[0]);
    assert.ok(!('id' in result.rows[0])); // Not selected
  });

  it('correctness: matches standard execution for filter', () => {
    const db = setupDB(200);
    const codegen = new QueryCodeGen(db);

    const compiled = codegen.execute({
      type: 'SELECT',
      columns: [{ name: 'id' }],
      from: { table: 'customers' },
      where: {
        type: 'COMPARE', op: 'GT',
        left: { type: 'column_ref', name: 'tier' },
        right: { type: 'literal', value: 2 }
      }
    });

    const standard = db.execute('SELECT id FROM customers WHERE tier > 2');
    assert.equal(compiled.rows.length, standard.rows.length);
  });

  it('correctness: matches standard execution for join', () => {
    const db = setupDB(100);
    const codegen = new QueryCodeGen(db);

    const compiled = codegen.execute({
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
    assert.equal(compiled.rows.length, standard.rows.length,
      `Codegen: ${compiled.rows.length} vs Standard: ${standard.rows.length}`);
  });

  it('explain shows generated source', () => {
    const db = setupDB(100);
    const codegen = new QueryCodeGen(db);

    const source = codegen.explain({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers' },
    });

    assert.ok(source.includes('customers_table'));
    assert.ok(source.includes('results'));
    assert.ok(source.includes('heap.scan'));
  });

  it('benchmark: codegen vs closure compilation', () => {
    const db = setupDB(500);
    const codegen = new QueryCodeGen(db);

    // Import the closure-based engine for comparison
    const closureEngine = new CompiledQueryEngine(db);

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

    // Codegen (batch compiled)
    const t0 = Date.now();
    const codegenResult = codegen.execute(ast);
    const codegenMs = Date.now() - t0;

    // Closure-based
    const t1 = Date.now();
    const closureResult = closureEngine.executeSelect(ast);
    const closureMs = Date.now() - t1;

    // Standard Volcano
    const t2 = Date.now();
    const standardResult = db.execute('SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id LIMIT 1000');
    const standardMs = Date.now() - t2;

    console.log(`    Codegen: ${codegenMs}ms | Closure: ${closureMs}ms | Volcano: ${standardMs}ms`);
    console.log(`    Codegen vs Volcano: ${(standardMs / Math.max(codegenMs, 1)).toFixed(1)}x`);
    console.log(`    Codegen vs Closure: ${(closureMs / Math.max(codegenMs, 1)).toFixed(1)}x`);

    assert.ok(codegenResult);
    assert.equal(codegenResult.rows.length, 1000);
  });

  it('tracks compilation stats', () => {
    const db = setupDB(100);
    const codegen = new QueryCodeGen(db);

    codegen.execute({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers' },
    });

    assert.equal(codegen.stats.compiled, 1);
    assert.ok(codegen.stats.totalCompileMs >= 0);
  });
});
