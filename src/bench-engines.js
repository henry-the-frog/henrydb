#!/usr/bin/env node
// bench-engines.js — Microbenchmark: Volcano vs Compiled vs Vectorized query engines
// Usage: node src/bench-engines.js [--rows=N] [--iterations=N]

import { Database } from './db.js';
import { buildPlan } from './volcano-planner.js';
import { parse } from './sql.js';
import { CompiledQueryEngine } from './compiled-query.js';
import { AdaptiveQueryEngine } from './adaptive-engine.js';
import { VectorizedCodeGen } from './vectorized-codegen.js';

const args = process.argv.slice(2);
const rowsArg = args.find(a => a.startsWith('--rows='));
const iterArg = args.find(a => a.startsWith('--iterations='));
const ROWS = rowsArg ? parseInt(rowsArg.split('=')[1]) : 10000;
const ITERATIONS = iterArg ? parseInt(iterArg.split('=')[1]) : 5;

console.log(`HenryDB Engine Benchmark — ${ROWS} rows, ${ITERATIONS} iterations\n`);

// Setup
const db = new Database();
db.execute('CREATE TABLE lineitem (id INT, order_id INT, product TEXT, quantity INT, price INT, discount INT, tax INT, status TEXT)');
db.execute('CREATE TABLE orders (id INT, customer_id INT, total INT, status TEXT)');
db.execute('CREATE TABLE customers (id INT, name TEXT, region TEXT)');

const products = ['Widget', 'Gadget', 'Doohickey', 'Thingamajig', 'Whatchamacallit'];
const statuses = ['shipped', 'pending', 'returned', 'cancelled'];
const regions = ['North', 'South', 'East', 'West', 'Central'];

console.log('Loading data...');
const loadStart = performance.now();
for (let i = 0; i < ROWS; i++) {
  db.execute(`INSERT INTO lineitem VALUES (${i}, ${i%1000}, '${products[i%5]}', ${1+i%20}, ${10+i%100}, ${i%10}, ${i%8}, '${statuses[i%4]}')`);
  if (i < 1000) {
    db.execute(`INSERT INTO orders VALUES (${i}, ${i%200}, ${i*100}, '${statuses[i%4]}')`);
  }
  if (i < 200) {
    db.execute(`INSERT INTO customers VALUES (${i}, 'customer_${i}', '${regions[i%5]}')`);
  }
}
console.log(`Loaded in ${Math.round(performance.now() - loadStart)}ms\n`);

// Queries
const queries = [
  {
    name: 'Full scan + filter',
    sql: "SELECT * FROM lineitem WHERE status = 'shipped'"
  },
  {
    name: 'Aggregate (SUM)',
    sql: "SELECT SUM(quantity * price) as revenue FROM lineitem WHERE status = 'shipped'"
  },
  {
    name: 'GROUP BY',
    sql: 'SELECT product, SUM(quantity) as total_qty, AVG(price) as avg_price FROM lineitem GROUP BY product'
  },
  {
    name: 'GROUP BY + HAVING',
    sql: 'SELECT product, SUM(quantity * price) as revenue FROM lineitem GROUP BY product HAVING SUM(quantity * price) > 100000'
  },
  {
    name: 'ORDER BY + LIMIT (Top-K)',
    sql: 'SELECT product, quantity, price FROM lineitem ORDER BY price DESC LIMIT 10'
  },
  {
    name: 'COUNT DISTINCT',
    sql: 'SELECT COUNT(DISTINCT product) as num_products FROM lineitem'
  },
  {
    name: 'JOIN (small × large)',
    sql: 'SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id WHERE c.region = \'North\''
  },
];

function bench(name, fn, iterations) {
  // Warmup
  try { fn(); } catch { return { name, ms: NaN, rows: 0, error: true }; }
  
  const times = [];
  let rowCount = 0;
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    const result = fn();
    times.push(performance.now() - start);
    rowCount = Array.isArray(result) ? result.length : (result?.length || 0);
  }
  const avg = times.reduce((a, b) => a + b, 0) / times.length;
  const min = Math.min(...times);
  return { name, ms: avg, min, rows: rowCount };
}

// Run benchmarks
const compiled = new CompiledQueryEngine(db);
const vectorized = new VectorizedCodeGen(db);

const header = `${'Query'.padEnd(30)} | ${'Volcano'.padStart(10)} | ${'Compiled'.padStart(10)} | ${'Vectorized'.padStart(10)} | ${'Speedup'.padStart(8)}`;
console.log(header);
console.log('-'.repeat(header.length));

for (const q of queries) {
  const ast = parse(q.sql);
  
  const volcanoResult = bench('volcano', () => {
    const plan = buildPlan(ast, db.tables);
    return plan.toArray();
  }, ITERATIONS);
  
  const compiledResult = bench('compiled', () => {
    try { return compiled.executeSelect(ast); }
    catch { return []; }
  }, ITERATIONS);
  
  const vectorizedResult = bench('vectorized', () => {
    try { return vectorized.execute(q.sql); }
    catch { return []; }
  }, ITERATIONS);
  
  const best = Math.min(
    volcanoResult.ms || Infinity,
    compiledResult.ms || Infinity,
    vectorizedResult.ms || Infinity
  );
  const speedup = volcanoResult.ms / best;
  
  const fmtMs = (r) => r.error ? 'N/A'.padStart(8) + 'ms' :
    (r.ms < 1 ? r.ms.toFixed(2) : Math.round(r.ms).toString()).padStart(8) + 'ms';
  
  console.log(
    `${q.name.padEnd(30)} | ${fmtMs(volcanoResult)} | ${fmtMs(compiledResult)} | ${fmtMs(vectorizedResult)} | ${speedup.toFixed(1).padStart(6)}x`
  );
}

console.log(`\nTotal rows: ${ROWS} lineitem, 1000 orders, 200 customers`);
