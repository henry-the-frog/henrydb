#!/usr/bin/env node
// inl-benchmark.js — Benchmark IndexNestedLoopJoin vs HashJoin
import { Database } from './src/db.js';
import { buildPlan, explainPlan } from './src/volcano-planner.js';
import { parse } from './src/sql.js';

const ORDERS = 2000;
const USERS = 100;
const ITERS = 30;

function bench(label, fn) {
  for (let i = 0; i < 3; i++) fn(); // warmup
  const start = performance.now();
  for (let i = 0; i < ITERS; i++) fn();
  const elapsed = performance.now() - start;
  return { label, opsPerSec: Math.round(ITERS / (elapsed / 1000)), avgMs: (elapsed / ITERS).toFixed(2) };
}

console.log(`Setup: ${USERS} users (with PK index), ${ORDERS} orders\n`);

const db = new Database();
db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
db.execute('CREATE TABLE orders (id INT, user_id INT, amount INT)');

for (let i = 0; i < USERS; i++) {
  db.execute(`INSERT INTO users VALUES (${i}, 'user_${i}')`);
}
for (let i = 0; i < ORDERS; i++) {
  db.execute(`INSERT INTO orders VALUES (${i}, ${i % USERS}, ${i * 10})`);
}

const sql = 'SELECT o.amount, u.name FROM orders o JOIN users u ON o.user_id = u.id';
const ast = parse(sql);

// INL join (with indexCatalog)
console.log('EXPLAIN (with indexCatalog → INL join):');
console.log(explainPlan(ast, db.tables, db.indexCatalog));
console.log();

// HashJoin (without indexCatalog)
console.log('EXPLAIN (without indexCatalog → HashJoin):');
console.log(explainPlan(ast, db.tables));
console.log();

// Benchmark
const inl = bench('INL Join', () => {
  buildPlan(ast, db.tables, db.indexCatalog).toArray();
});

const hash = bench('HashJoin', () => {
  buildPlan(ast, db.tables).toArray();
});

// Standard engine
const std = bench('Standard', () => {
  db.execute(sql);
});

// Row count verification
const inlRows = buildPlan(ast, db.tables, db.indexCatalog).toArray().length;
const hashRows = buildPlan(ast, db.tables).toArray().length;
const stdRows = db.execute(sql).rows.length;

console.log('═'.repeat(60));
console.log(`${'Engine'.padEnd(20)} ${'ops/s'.padStart(10)} ${'avg ms'.padStart(10)} ${'rows'.padStart(8)}`);
console.log('─'.repeat(60));
console.log(`${'INL Join'.padEnd(20)} ${String(inl.opsPerSec).padStart(10)} ${inl.avgMs.padStart(10)} ${String(inlRows).padStart(8)}`);
console.log(`${'HashJoin'.padEnd(20)} ${String(hash.opsPerSec).padStart(10)} ${hash.avgMs.padStart(10)} ${String(hashRows).padStart(8)}`);
console.log(`${'Standard Engine'.padEnd(20)} ${String(std.opsPerSec).padStart(10)} ${std.avgMs.padStart(10)} ${String(stdRows).padStart(8)}`);
console.log('═'.repeat(60));

const speedup = (hash.opsPerSec > 0 ? inl.opsPerSec / hash.opsPerSec : 0).toFixed(2);
console.log(`\nINL vs HashJoin speedup: ${speedup}x`);
console.log(`Row count match: ${inlRows === hashRows && hashRows === stdRows ? '✓ All match' : '✗ MISMATCH'}`);
