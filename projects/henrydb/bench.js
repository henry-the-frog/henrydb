#!/usr/bin/env node
// bench.js — Simple HenryDB benchmark
import { Database } from './src/db.js';

function bench(name, fn, iterations = 1000) {
  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn(i);
  const elapsed = performance.now() - start;
  const opsPerSec = Math.round(iterations / (elapsed / 1000));
  console.log(`  ${name}: ${opsPerSec.toLocaleString()} ops/sec (${elapsed.toFixed(1)}ms for ${iterations})`);
  return { name, opsPerSec, elapsed };
}

console.log('🐸 HenryDB Benchmark\n');

// Setup
const db = new Database();
db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, dept TEXT)');
db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT, status TEXT)');

// 1. INSERT benchmark
console.log('INSERT:');
bench('Single inserts', (i) => {
  db.execute(`INSERT INTO users VALUES (${i}, 'user_${i}', ${20 + i % 50}, '${['eng', 'sales', 'hr', 'ops'][i % 4]}')`);
}, 5000);

// 2. SELECT benchmarks
console.log('\nSELECT:');
bench('Full table scan (5000 rows)', () => {
  db.execute('SELECT * FROM users');
}, 100);

bench('WHERE clause (selective)', () => {
  db.execute("SELECT * FROM users WHERE dept = 'eng' AND age > 40");
}, 100);

bench('PK lookup', () => {
  db.execute('SELECT * FROM users WHERE id = 2500');
}, 5000);

bench('COUNT aggregate', () => {
  db.execute('SELECT COUNT(*) FROM users');
}, 100);

bench('GROUP BY', () => {
  db.execute('SELECT dept, COUNT(*), AVG(age) FROM users GROUP BY dept');
}, 100);

bench('ORDER BY + LIMIT', () => {
  db.execute('SELECT * FROM users ORDER BY age DESC LIMIT 10');
}, 100);

// 3. INDEX benchmark
console.log('\nINDEX:');
db.execute('CREATE INDEX idx_age ON users (age)');
db.execute('CREATE INDEX idx_dept ON users (dept)');

bench('Index lookup (age > 50)', () => {
  db.execute('SELECT * FROM users WHERE age > 50');
}, 100);

// 4. JOIN benchmark
console.log('\nJOIN:');
for (let i = 0; i < 1000; i++) {
  db.execute(`INSERT INTO orders VALUES (${i}, ${i % 5000}, ${(i * 17) % 1000}, '${['pending', 'shipped', 'delivered'][i % 3]}')`);
}

bench('Hash join (users × orders)', () => {
  db.execute('SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id');
}, 100);

bench('Join + WHERE + ORDER', () => {
  db.execute("SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'shipped' ORDER BY o.amount DESC LIMIT 10");
}, 10);

// 5. UPDATE/DELETE
console.log('\nUPDATE/DELETE:');
bench('UPDATE with WHERE', (i) => {
  db.execute(`UPDATE users SET age = ${30 + i % 20} WHERE id = ${i % 5000}`);
}, 100);

bench('DELETE + INSERT cycle', (i) => {
  const id = 10000 + i;
  db.execute(`INSERT INTO users VALUES (${id}, 'temp_${i}', 25, 'temp')`);
  db.execute(`DELETE FROM users WHERE id = ${id}`);
}, 100);

// 6. Advanced features
console.log('\nADVANCED:');
bench('EXPLAIN', () => {
  db.execute('EXPLAIN SELECT * FROM users WHERE age > 30');
}, 100);

console.log('\n✅ Benchmark complete');
