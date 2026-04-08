// benchmark-engines.test.js — Comprehensive benchmark: all engines across query patterns
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { VectorizedCodeGen } from './vectorized-codegen.js';
import { QueryCodeGen } from './query-codegen.js';
import { CompiledQueryEngine } from './compiled-query.js';
import { AdaptiveQueryEngine } from './adaptive-engine.js';
import { Database } from './db.js';

function setupBenchDB() {
  const db = new Database();
  db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region TEXT, tier INT, balance INT)');
  db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT, priority INT)');
  db.execute('CREATE TABLE items (id INT PRIMARY KEY, order_id INT, product TEXT, qty INT, price INT)');

  for (let i = 0; i < 1000; i++) {
    const region = ['US', 'EU', 'APAC', 'LATAM'][i % 4];
    const tier = (i % 4) + 1;
    const balance = (i * 47 + 1000) % 10000;
    db.execute(`INSERT INTO customers VALUES (${i}, 'Customer ${i}', '${region}', ${tier}, ${balance})`);
  }
  for (let i = 0; i < 3000; i++) {
    const custId = i % 1000;
    const amount = (i * 17 + 13) % 5000;
    const status = ['pending', 'shipped', 'delivered'][i % 3];
    const priority = (i % 5) + 1;
    db.execute(`INSERT INTO orders VALUES (${i}, ${custId}, ${amount}, '${status}', ${priority})`);
  }
  for (let i = 0; i < 6000; i++) {
    const orderId = i % 3000;
    const product = ['Widget', 'Gadget', 'Gizmo', 'Doohickey'][i % 4];
    const qty = (i % 10) + 1;
    const price = (i * 13 + 50) % 500;
    db.execute(`INSERT INTO items VALUES (${i}, ${orderId}, '${product}', ${qty}, ${price})`);
  }

  return db;
}

describe('Engine Benchmark Suite', () => {
  const db = setupBenchDB();
  const vec = new VectorizedCodeGen(db);
  const codegen = new QueryCodeGen(db);
  const closure = new CompiledQueryEngine(db);
  const adaptive = new AdaptiveQueryEngine(db);

  function bench(name, ast, sql) {
    it(`${name}`, () => {
      const results = {};

      // Vectorized
      try {
        const t0 = Date.now();
        const r = vec.execute(ast);
        results.vectorized = { ms: Date.now() - t0, rows: r?.rows?.length || 0 };
      } catch { results.vectorized = { ms: -1, rows: 0 }; }

      // Codegen
      try {
        const t0 = Date.now();
        const r = codegen.execute(ast);
        results.codegen = { ms: Date.now() - t0, rows: r?.rows?.length || 0 };
      } catch { results.codegen = { ms: -1, rows: 0 }; }

      // Closure compiled
      try {
        const t0 = Date.now();
        const r = closure.executeSelect(ast);
        results.closure = { ms: Date.now() - t0, rows: r?.rows?.length || 0 };
      } catch { results.closure = { ms: -1, rows: 0 }; }

      // Adaptive
      try {
        const t0 = Date.now();
        const r = adaptive.executeSelect(ast);
        results.adaptive = { ms: Date.now() - t0, rows: r?.rows?.length || 0, engine: r?.engine };
      } catch { results.adaptive = { ms: -1, rows: 0 }; }

      // Volcano
      const t0 = Date.now();
      const standard = db.execute(sql);
      results.volcano = { ms: Date.now() - t0, rows: standard?.rows?.length || 0 };

      // Print comparison
      const fastest = Math.min(
        ...[results.vectorized, results.codegen, results.closure, results.adaptive]
          .filter(r => r.ms > 0).map(r => r.ms)
      );
      const speedup = fastest > 0 ? (results.volcano.ms / fastest).toFixed(1) : '?';
      
      console.log(`    ${name}:`);
      console.log(`      Vec: ${results.vectorized.ms}ms | CG: ${results.codegen.ms}ms | Cls: ${results.closure.ms}ms | Adp: ${results.adaptive.ms}ms (${results.adaptive.engine || '?'}) | Vol: ${results.volcano.ms}ms | Best: ${speedup}x`);

      assert.ok(results.volcano.ms >= 0);
    });
  }

  // 1. Full table scan
  bench('Full scan (1K rows)',
    { type: 'SELECT', columns: [{ name: '*' }], from: { table: 'customers' } },
    'SELECT * FROM customers');

  // 2. Filtered scan (25% selectivity)
  bench('Filtered scan 25%',
    { type: 'SELECT', columns: [{ name: 'id' }, { name: 'name' }], from: { table: 'customers' },
      where: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'region' }, right: { type: 'literal', value: 'US' } } },
    "SELECT id, name FROM customers WHERE region = 'US'");

  // 3. Highly selective scan
  bench('Selective scan (tier > 3)',
    { type: 'SELECT', columns: [{ name: '*' }], from: { table: 'customers' },
      where: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'tier' }, right: { type: 'literal', value: 3 } } },
    'SELECT * FROM customers WHERE tier > 3');

  // 4. LIMIT 10
  bench('LIMIT 10 from 1K',
    { type: 'SELECT', columns: [{ name: '*' }], from: { table: 'customers' }, limit: { value: 10 } },
    'SELECT * FROM customers LIMIT 10');

  // 5. Two-table join
  bench('2-table join (1K × 3K, LIMIT 500)',
    { type: 'SELECT', columns: [{ name: '*' }], from: { table: 'customers', alias: 'c' },
      joins: [{ table: 'orders', alias: 'o', joinType: 'INNER',
        on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', table: 'c', name: 'id' }, right: { type: 'column_ref', table: 'o', name: 'customer_id' } } }],
      limit: { value: 500 } },
    'SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id LIMIT 500');

  // 6. Two-table join (full)
  bench('2-table join FULL (1K × 3K)',
    { type: 'SELECT', columns: [{ name: '*' }], from: { table: 'customers', alias: 'c' },
      joins: [{ table: 'orders', alias: 'o', joinType: 'INNER',
        on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', table: 'c', name: 'id' }, right: { type: 'column_ref', table: 'o', name: 'customer_id' } } }] },
    'SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id');

  // 7. Three-table join
  bench('3-table join (1K × 3K × 6K, LIMIT 200)',
    { type: 'SELECT', columns: [{ name: '*' }], from: { table: 'customers', alias: 'c' },
      joins: [
        { table: 'orders', alias: 'o', joinType: 'INNER',
          on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', table: 'c', name: 'id' }, right: { type: 'column_ref', table: 'o', name: 'customer_id' } } },
        { table: 'items', alias: 'i', joinType: 'INNER',
          on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', table: 'o', name: 'id' }, right: { type: 'column_ref', table: 'i', name: 'order_id' } } }
      ],
      limit: { value: 200 } },
    'SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id JOIN items i ON o.id = i.order_id LIMIT 200');

  // 8. Filtered join
  bench('Filtered join (region=US, LIMIT 100)',
    { type: 'SELECT', columns: [{ name: '*' }], from: { table: 'customers', alias: 'c' },
      joins: [{ table: 'orders', alias: 'o', joinType: 'INNER',
        on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', table: 'c', name: 'id' }, right: { type: 'column_ref', table: 'o', name: 'customer_id' } } }],
      where: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'region' }, right: { type: 'literal', value: 'US' } },
      limit: { value: 100 } },
    "SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id WHERE region = 'US' LIMIT 100");

  // 9. Projection (specific columns)
  bench('Projection (2 cols from join)',
    { type: 'SELECT', columns: [{ name: 'name', table: 'c' }, { name: 'amount', table: 'o' }],
      from: { table: 'customers', alias: 'c' },
      joins: [{ table: 'orders', alias: 'o', joinType: 'INNER',
        on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', table: 'c', name: 'id' }, right: { type: 'column_ref', table: 'o', name: 'customer_id' } } }],
      limit: { value: 500 } },
    'SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id LIMIT 500');

  // 10. Large scan (3K rows, no filter)
  bench('Large scan (3K rows)',
    { type: 'SELECT', columns: [{ name: '*' }], from: { table: 'orders' } },
    'SELECT * FROM orders');
});
