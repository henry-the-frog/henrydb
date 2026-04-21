// compiled-query.test.js — Tests for planner ↔ pipeline compiler integration
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CompiledQueryEngine } from './compiled-query.js';
import { Database } from './db.js';

function setupDB(rowCount = 200) {
  const db = new Database();
  db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region TEXT, tier INT)');
  db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT)');
  db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT, category TEXT)');
  db.execute('CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT, product_id INT, qty INT)');

  for (let i = 0; i < rowCount; i++) {
    const region = ['US', 'EU', 'APAC'][i % 3];
    const tier = (i % 4) + 1;
    db.execute(`INSERT INTO customers VALUES (${i}, 'Customer ${i}', '${region}', ${tier})`);
  }
  for (let i = 0; i < rowCount * 3; i++) {
    const custId = i % rowCount;
    const amount = (i * 17 + 13) % 1000;
    const status = ['pending', 'shipped', 'delivered'][i % 3];
    db.execute(`INSERT INTO orders VALUES (${i}, ${custId}, ${amount}, '${status}')`);
  }
  for (let i = 0; i < 50; i++) {
    const cat = ['electronics', 'books', 'clothing', 'food'][i % 4];
    db.execute(`INSERT INTO products VALUES (${i}, 'Product ${i}', ${(i + 1) * 10}, '${cat}')`);
  }
  for (let i = 0; i < rowCount * 5; i++) {
    const orderId = i % (rowCount * 3);
    const prodId = i % 50;
    const qty = (i % 10) + 1;
    db.execute(`INSERT INTO order_items VALUES (${i}, ${orderId}, ${prodId}, ${qty})`);
  }

  return db;
}

describe('CompiledQueryEngine', () => {
  it('constructs and has planner', () => {
    const db = new Database();
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });
    assert.ok(engine.planner);
    assert.equal(engine.stats.queriesCompiled, 0);
  });

  it('compiles a single-table scan with filter', () => {
    const db = setupDB(200);
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    // Parse a simple query AST
    const ast = {
      type: 'SELECT',
      columns: [{ name: 'id' }, { name: 'name' }, { name: 'region' }],
      from: { table: 'customers' },
      where: {
        type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 'region' },
        right: { type: 'literal', value: 'US' }
      }
    };

    const result = engine.executeSelect(ast);
    assert.ok(result, 'Should compile (200 rows > threshold)');
    // ~67 US customers out of 200
    assert.ok(result.rows.length > 50 && result.rows.length < 100, `Got ${result.rows.length} rows`);
    assert.ok(result.rows.every(r => r.region === 'US'), 'All should be US');
  });

  it('returns null for tiny tables (not worth compiling)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);

    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });
    const ast = {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 't' },
    };
    const result = engine.executeSelect(ast);
    assert.equal(result, null, 'Should not compile tiny table');
    assert.equal(engine.stats.queriesInterpreted, 1);
  });

  it('compiles hash join between two tables', () => {
    const db = setupDB(100);
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    const ast = {
      type: 'SELECT',
      columns: [{ name: 'name', table: 'c' }, { name: 'amount', table: 'o' }],
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
    };

    const result = engine.executeSelect(ast);
    assert.ok(result, 'Should compile join');
    // 100 customers × 3 orders each = 300 rows
    assert.equal(result.rows.length, 300, `Expected 300, got ${result.rows.length}`);
  });

  it('compiled hash join produces correct results', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT, data TEXT)');

    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO a VALUES (${i}, 'v${i}')`);
    for (let i = 0; i < 200; i++) db.execute(`INSERT INTO b VALUES (${i}, ${i % 100}, 'd${i}')`);

    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });
    const ast = {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'a' },
      joins: [{
        table: 'b',
        joinType: 'INNER',
        on: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', name: 'id' },
          right: { type: 'column_ref', name: 'a_id' }
        }
      }],
    };

    const result = engine.executeSelect(ast);
    assert.ok(result);
    assert.equal(result.rows.length, 200); // Each a row matches 2 b rows
  });

  it('compiled merge join produces correct results', () => {
    const db = setupDB(100);
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    // Force merge join by directly testing the method
    const leftRows = [];
    for (let i = 0; i < 100; i++) leftRows.push({ id: i, name: `c${i}` });
    const rightRows = [];
    for (let i = 0; i < 300; i++) rightRows.push({ customer_id: i % 100, amount: i * 10 });

    const result = engine._compiledMergeJoin(leftRows, rightRows, ['id', 'customer_id'], 'INNER');
    assert.equal(result.length, 300);
  });

  it('compiled nested loop join produces correct results', () => {
    const db = new Database();
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    const leftRows = [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }];
    const rightRows = [{ a_id: 1, val: 'x' }, { a_id: 1, val: 'y' }, { a_id: 2, val: 'z' }];
    const onExpr = {
      type: 'COMPARE', op: 'EQ',
      left: { type: 'column_ref', name: 'id' },
      right: { type: 'column_ref', name: 'a_id' }
    };

    const result = engine._compiledNestedLoopJoin(leftRows, rightRows, onExpr, 'INNER');
    assert.equal(result.length, 3);
    assert.equal(result[0].name, 'Alice');
    assert.equal(result[2].name, 'Bob');
  });

  it('LEFT JOIN preserves unmatched rows', () => {
    const db = new Database();
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    const leftRows = [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }, { id: 3, name: 'Charlie' }];
    const rightRows = [{ a_id: 1, val: 'x' }];

    const result = engine._compiledHashJoin(leftRows, rightRows, ['id', 'a_id'], 'LEFT');
    assert.equal(result.length, 3);
    assert.equal(result[0].val, 'x');
    assert.equal(result[1].val, undefined); // Bob has no match
    assert.equal(result[2].val, undefined); // Charlie has no match
  });

  it('LEFT JOIN via merge join preserves unmatched', () => {
    const db = new Database();
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    const leftRows = [{ id: 1, name: 'A' }, { id: 2, name: 'B' }, { id: 3, name: 'C' }];
    const rightRows = [{ a_id: 2, val: 'matched' }];

    const result = engine._compiledMergeJoin(leftRows, rightRows, ['id', 'a_id'], 'LEFT');
    assert.equal(result.length, 3);
    assert.ok(result.find(r => r.name === 'B' && r.val === 'matched'));
    assert.ok(result.find(r => r.name === 'A' && r.val === undefined));
    assert.ok(result.find(r => r.name === 'C' && r.val === undefined));
  });

  it('EXPLAIN COMPILED shows plan', () => {
    const db = setupDB(100);
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    const ast = {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers' },
      where: {
        type: 'COMPARE', op: 'GT',
        left: { type: 'column_ref', name: 'tier' },
        right: { type: 'literal', value: 2 }
      }
    };

    const explain = engine.explainCompiled(ast);
    assert.ok(explain.includes('Compiled Query Plan'));
    assert.ok(explain.includes('customers'));
  });

  it('tracks compilation stats', () => {
    const db = setupDB(200);
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    // Compile a query
    engine.executeSelect({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers' },
    });

    // Skip a small query
    const small = new Database();
    small.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    for (let i = 0; i < 5; i++) small.execute(`INSERT INTO t VALUES (${i})`);
    const engine2 = new CompiledQueryEngine(small);
    engine2.executeSelect({
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 't' },
    });

    assert.equal(engine.stats.queriesCompiled, 1);
    assert.equal(engine2.stats.queriesInterpreted, 1);
  });

  it('three-table join works', () => {
    const db = setupDB(100);
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    const ast = {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customers', alias: 'c' },
      joins: [
        {
          table: 'orders',
          alias: 'o',
          joinType: 'INNER',
          on: {
            type: 'COMPARE', op: 'EQ',
            left: { type: 'column_ref', table: 'c', name: 'id' },
            right: { type: 'column_ref', table: 'o', name: 'customer_id' }
          }
        },
        {
          table: 'order_items',
          alias: 'oi',
          joinType: 'INNER',
          on: {
            type: 'COMPARE', op: 'EQ',
            left: { type: 'column_ref', table: 'o', name: 'id' },
            right: { type: 'column_ref', table: 'oi', name: 'order_id' }
          }
        }
      ],
      limit: { value: 50 }
    };

    const result = engine.executeSelect(ast);
    assert.ok(result, 'Should compile 3-table join');
    assert.equal(result.rows.length, 50); // Limited to 50
  });

  it('compiled query matches Volcano results for filter', () => {
    const db = setupDB(200);
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    // Compiled execution
    const ast = {
      type: 'SELECT',
      columns: [{ name: 'id' }, { name: 'name' }],
      from: { table: 'customers' },
      where: {
        type: 'COMPARE', op: 'GT',
        left: { type: 'column_ref', name: 'tier' },
        right: { type: 'literal', value: 3 }
      }
    };

    const compiled = engine.executeSelect(ast);

    // Standard execution for comparison
    const standard = db.execute('SELECT id, name FROM customers WHERE tier > 3');

    assert.equal(compiled.rows.length, standard.rows.length,
      `Compiled ${compiled.rows.length} vs standard ${standard.rows.length}`);
  });

  it('compiled query matches Volcano for join', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT, data INT)');

    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO a VALUES (${i}, ${i * 10})`);
    for (let i = 0; i < 200; i++) db.execute(`INSERT INTO b VALUES (${i}, ${i % 100}, ${i})`);

    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    // Standard execution
    const standard = db.execute('SELECT a.val, b.data FROM a JOIN b ON a.id = b.a_id');

    // Compiled execution
    const ast = {
      type: 'SELECT',
      columns: [{ name: 'val', table: 'a' }, { name: 'data', table: 'b' }],
      from: { table: 'a' },
      joins: [{
        table: 'b',
        joinType: 'INNER',
        on: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', table: 'a', name: 'id' },
          right: { type: 'column_ref', table: 'b', name: 'a_id' }
        }
      }],
    };

    const compiled = engine.executeSelect(ast);
    assert.equal(compiled.rows.length, standard.rows.length,
      `Compiled ${compiled.rows.length} vs standard ${standard.rows.length}`);
  });

  it('AND filter compiles correctly', () => {
    const db = setupDB(200);
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    const ast = {
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
    };

    const compiled = engine.executeSelect(ast);
    assert.ok(compiled);
    // EU ~67 rows, tier >= 3 ~50% → ~33 rows
    assert.ok(compiled.rows.length > 20 && compiled.rows.length < 50);
  });

  it('cross join works', () => {
    const db = new Database();
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    const leftRows = [{ id: 1 }, { id: 2 }];
    const rightRows = [{ val: 'a' }, { val: 'b' }, { val: 'c' }];

    const result = engine._compiledCrossJoin(leftRows, rightRows);
    assert.equal(result.length, 6);
  });

  it('benchmark: compiled vs standard on 1000-row join', () => {
    const db = setupDB(500);
    const engine = new CompiledQueryEngine(db, { compileThreshold: 50 });

    // Compile
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

    const t0 = Date.now();
    const compiled = engine.executeSelect(ast);
    const compiledMs = Date.now() - t0;

    const t1 = Date.now();
    const standard = db.execute('SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id LIMIT 1000');
    const standardMs = Date.now() - t1;

    assert.ok(compiled);
    assert.equal(compiled.rows.length, 1000);
    // Both should produce results — timing is informational
    console.log(`    Compiled: ${compiledMs}ms, Standard: ${standardMs}ms, Rows: ${compiled.rows.length}`);
  });
});
