#!/usr/bin/env node
// benchmark.js — HenryDB Performance Benchmark Suite
// Run: node benchmark.js
import { Database } from './src/db.js';

function bench(name, fn, iterations = 1000) {
  // Warmup
  for (let i = 0; i < 10; i++) fn();
  
  const start = Date.now();
  for (let i = 0; i < iterations; i++) fn();
  const elapsed = Date.now() - start;
  const opsPerSec = (iterations / elapsed * 1000).toFixed(0);
  const usPerOp = (elapsed / iterations * 1000).toFixed(1);
  console.log(`  ${name.padEnd(40)} ${opsPerSec.padStart(8)} ops/s  ${usPerOp.padStart(8)} µs/op  (${iterations} ops in ${elapsed}ms)`);
}

console.log('═══════════════════════════════════════════════════════════════════');
console.log('                    HenryDB Performance Benchmarks');
console.log('═══════════════════════════════════════════════════════════════════\n');

// ===== Setup =====
const db = new Database();

// ===== INSERT Benchmarks =====
console.log('▸ INSERT');
console.log('─────────────────────────────────────────────────────────────────');

db.execute('CREATE TABLE bench_insert (id INT PRIMARY KEY, name TEXT, val INT)');
let insertId = 1;
bench('Single INSERT', () => {
  db.execute(`INSERT INTO bench_insert VALUES (${insertId++}, 'name', ${insertId})`);
}, 5000);

db.execute('CREATE TABLE bench_insert_noindex (name TEXT, val INT)');
let insertId2 = 1;
bench('INSERT (no PK index)', () => {
  db.execute(`INSERT INTO bench_insert_noindex VALUES ('name-${insertId2++}', ${insertId2})`);
}, 5000);

// ===== SELECT Benchmarks =====
console.log('\n▸ SELECT');
console.log('─────────────────────────────────────────────────────────────────');

db.execute('CREATE TABLE bench_select (id INT PRIMARY KEY, name TEXT, val INT, cat TEXT)');
for (let i = 1; i <= 1000; i++) {
  db.execute(`INSERT INTO bench_select VALUES (${i}, 'name-${i}', ${i * 10}, 'cat-${i % 10}')`);
}

bench('SELECT * (1000 rows)', () => {
  db.execute('SELECT * FROM bench_select');
}, 500);

bench('SELECT with WHERE (point lookup)', () => {
  db.execute('SELECT * FROM bench_select WHERE id = 500');
}, 2000);

bench('SELECT with range WHERE', () => {
  db.execute('SELECT * FROM bench_select WHERE val > 5000 AND val < 6000');
}, 1000);

bench('SELECT COUNT(*)', () => {
  db.execute('SELECT COUNT(*) FROM bench_select');
}, 2000);

bench('SELECT with ORDER BY', () => {
  db.execute('SELECT * FROM bench_select ORDER BY val DESC LIMIT 10');
}, 500);

// ===== Aggregation Benchmarks =====
console.log('\n▸ Aggregation');
console.log('─────────────────────────────────────────────────────────────────');

bench('GROUP BY (10 groups)', () => {
  db.execute('SELECT cat, COUNT(*), SUM(val), AVG(val) FROM bench_select GROUP BY cat');
}, 500);

bench('GROUP BY with HAVING', () => {
  db.execute('SELECT cat, SUM(val) as total FROM bench_select GROUP BY cat HAVING SUM(val) > 50000');
}, 500);

bench('DISTINCT', () => {
  db.execute('SELECT DISTINCT cat FROM bench_select');
}, 1000);

// ===== JOIN Benchmarks =====
console.log('\n▸ JOIN');
console.log('─────────────────────────────────────────────────────────────────');

db.execute('CREATE TABLE bench_left (id INT PRIMARY KEY, left_val TEXT)');
db.execute('CREATE TABLE bench_right (id INT PRIMARY KEY, left_id INT, right_val TEXT)');
for (let i = 1; i <= 500; i++) {
  db.execute(`INSERT INTO bench_left VALUES (${i}, 'L-${i}')`);
}
for (let i = 1; i <= 1000; i++) {
  db.execute(`INSERT INTO bench_right VALUES (${i}, ${1 + (i % 500)}, 'R-${i}')`);
}

bench('INNER JOIN (500 × 1000)', () => {
  db.execute('SELECT l.left_val, r.right_val FROM bench_left l JOIN bench_right r ON l.id = r.left_id LIMIT 100');
}, 200);

bench('LEFT JOIN (500 × 1000)', () => {
  db.execute('SELECT l.left_val, r.right_val FROM bench_left l LEFT JOIN bench_right r ON l.id = r.left_id LIMIT 100');
}, 200);

// ===== UPDATE/DELETE Benchmarks =====
console.log('\n▸ UPDATE/DELETE');
console.log('─────────────────────────────────────────────────────────────────');

db.execute('CREATE TABLE bench_upd (id INT PRIMARY KEY, val INT)');
for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO bench_upd VALUES (${i}, 0)`);

bench('UPDATE single row by PK', () => {
  const id = 1 + Math.floor(Math.random() * 1000);
  db.execute(`UPDATE bench_upd SET val = val + 1 WHERE id = ${id}`);
}, 2000);

bench('UPDATE bulk (100 rows)', () => {
  db.execute('UPDATE bench_upd SET val = val + 1 WHERE id <= 100');
}, 200);

// ===== Subquery Benchmarks =====
console.log('\n▸ Subqueries');
console.log('─────────────────────────────────────────────────────────────────');

bench('Scalar subquery', () => {
  db.execute('SELECT * FROM bench_select WHERE val > (SELECT AVG(val) FROM bench_select) LIMIT 10');
}, 200);

bench('IN subquery', () => {
  db.execute('SELECT * FROM bench_select WHERE cat IN (SELECT DISTINCT cat FROM bench_select WHERE val > 5000) LIMIT 10');
}, 200);

// ===== Parser Benchmarks =====
console.log('\n▸ Parser');
console.log('─────────────────────────────────────────────────────────────────');

import { parse } from './src/sql.js';

bench('Parse simple SELECT', () => {
  parse('SELECT id, name, val FROM bench WHERE id = 42');
}, 10000);

bench('Parse complex SELECT', () => {
  parse('SELECT a.id, b.val, SUM(c.amount) FROM orders a JOIN users b ON a.user_id = b.id LEFT JOIN items c ON a.id = c.order_id WHERE a.status = 1 AND b.active = 1 GROUP BY a.id, b.val HAVING SUM(c.amount) > 100 ORDER BY a.id DESC LIMIT 50');
}, 5000);

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Benchmark complete. Numbers reflect single-threaded performance.');
console.log('═══════════════════════════════════════════════════════════════════');
