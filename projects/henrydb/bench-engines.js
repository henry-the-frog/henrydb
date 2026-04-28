// bench-engines.js — Benchmark all 4 execution engines on representative queries
// Usage: node bench-engines.js [--rows 1000] [--iterations 100]

import { Database } from './src/db.js';

const ROWS = parseInt(process.argv.find(a => a.startsWith('--rows='))?.split('=')[1] || '1000');
const ITERS = parseInt(process.argv.find(a => a.startsWith('--iters='))?.split('=')[1] || '50');

function benchmark(db, sql, iterations = ITERS) {
  // Warmup
  for (let i = 0; i < 3; i++) db.execute(sql);
  
  const times = [];
  for (let i = 0; i < iterations; i++) {
    const t = performance.now();
    db.execute(sql);
    times.push(performance.now() - t);
  }
  
  times.sort((a, b) => a - b);
  const median = times[Math.floor(times.length / 2)];
  const p95 = times[Math.floor(times.length * 0.95)];
  const mean = times.reduce((s, t) => s + t, 0) / times.length;
  
  return { median: +median.toFixed(3), p95: +p95.toFixed(3), mean: +mean.toFixed(3) };
}

// Setup
const db = new Database();
db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT)');
db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, city TEXT)');
db.execute('CREATE INDEX idx_orders_cid ON orders(customer_id)');
db.execute('CREATE INDEX idx_customers_city ON customers(city)');

const cities = ['NYC', 'LA', 'CHI', 'HOU', 'PHX', 'PHI', 'SAN', 'DAL', 'AUS', 'JAX'];
const statuses = ['pending', 'shipped', 'delivered', 'cancelled'];

console.log(`Inserting ${ROWS} rows...`);
const insertStart = performance.now();

for (let i = 0; i < Math.min(ROWS, 200); i++) {
  db.execute(`INSERT INTO customers VALUES (${i}, 'Customer ${i}', '${cities[i % cities.length]}')`);
}

for (let i = 0; i < ROWS; i++) {
  db.execute(`INSERT INTO orders VALUES (${i}, ${i % 200}, ${Math.floor(Math.random() * 1000)}, '${statuses[i % statuses.length]}')`);
}
console.log(`Insert time: ${(performance.now() - insertStart).toFixed(0)}ms\n`);

// Benchmark queries
const queries = [
  { name: 'Point query (PK)', sql: `SELECT * FROM orders WHERE id = ${Math.floor(ROWS / 2)}` },
  { name: 'Range scan', sql: `SELECT * FROM orders WHERE amount > 500` },
  { name: 'Full scan', sql: `SELECT * FROM orders` },
  { name: 'Aggregation (COUNT)', sql: `SELECT status, COUNT(*) FROM orders GROUP BY status` },
  { name: 'Aggregation (SUM)', sql: `SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id` },
  { name: 'JOIN', sql: `SELECT c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.amount > 800` },
  { name: 'ORDER BY + LIMIT', sql: `SELECT * FROM orders ORDER BY amount DESC LIMIT 10` },
];

console.log(`Benchmarking with ${ROWS} rows, ${ITERS} iterations each:\n`);
console.log('Query'.padEnd(30), 'Median(ms)'.padStart(12), 'P95(ms)'.padStart(12), 'Mean(ms)'.padStart(12));
console.log('-'.repeat(70));

for (const { name, sql } of queries) {
  try {
    const stats = benchmark(db, sql);
    console.log(name.padEnd(30), String(stats.median).padStart(12), String(stats.p95).padStart(12), String(stats.mean).padStart(12));
  } catch (e) {
    console.log(name.padEnd(30), `ERROR: ${e.message}`.padStart(36));
  }
}

console.log('\nDone.');
