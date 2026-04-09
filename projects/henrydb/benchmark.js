// HenryDB Benchmark Suite
// Tests raw database performance (no network overhead)
import { Database } from './src/db.js';

function formatNum(n) { return n.toLocaleString(); }
function formatRate(count, ms) { return formatNum(Math.round(count / (ms / 1000))); }

console.log('=== HenryDB Benchmark Suite ===\n');

const db = new Database();

// 1. Schema creation
let t0 = performance.now();
db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)');
db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL, status TEXT)');
let t1 = performance.now();
console.log(`Schema creation: ${(t1 - t0).toFixed(1)}ms`);

// 2. Single-row INSERT
const N = 1000;
t0 = performance.now();
for (let i = 1; i <= N; i++) {
  db.execute(`INSERT INTO users VALUES (${i}, 'User_${i}', 'user${i}@example.com', ${20 + (i % 50)})`);
}
t1 = performance.now();
const insertMs = t1 - t0;
console.log(`INSERT ${formatNum(N)} rows: ${insertMs.toFixed(0)}ms (${formatRate(N, insertMs)} rows/sec)`);

// 3. Bulk INSERT for orders
t0 = performance.now();
for (let i = 1; i <= N; i++) {
  const userId = (i % N) + 1;
  const amount = Math.round(Math.random() * 10000) / 100;
  const status = ['pending', 'shipped', 'delivered'][i % 3];
  db.execute(`INSERT INTO orders VALUES (${i}, ${userId}, ${amount}, '${status}')`);
}
t1 = performance.now();
const orderInsertMs = t1 - t0;
console.log(`INSERT ${formatNum(N)} orders: ${orderInsertMs.toFixed(0)}ms (${formatRate(N, orderInsertMs)} rows/sec)`);

// 4. Full table scan
t0 = performance.now();
const allUsers = db.execute('SELECT * FROM users');
t1 = performance.now();
console.log(`SELECT * FROM users (${formatNum(allUsers.rows.length)} rows): ${(t1 - t0).toFixed(1)}ms`);

// 5. Point query (PK lookup)
t0 = performance.now();
for (let i = 0; i < 1000; i++) {
  db.execute(`SELECT * FROM users WHERE id = ${(i % N) + 1}`);
}
t1 = performance.now();
const pointMs = t1 - t0;
console.log(`Point queries (1000x): ${pointMs.toFixed(0)}ms (${formatRate(1000, pointMs)} queries/sec)`);

// 6. Range scan with filter
t0 = performance.now();
const filtered = db.execute('SELECT name, age FROM users WHERE age > 40');
t1 = performance.now();
console.log(`Range scan WHERE age > 40 (${formatNum(filtered.rows.length)} rows): ${(t1 - t0).toFixed(1)}ms`);

// 7. Aggregation
t0 = performance.now();
const agg = db.execute('SELECT COUNT(*) AS cnt, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age FROM users');
t1 = performance.now();
console.log(`Aggregation (COUNT, AVG, MIN, MAX): ${(t1 - t0).toFixed(1)}ms → ${JSON.stringify(agg.rows[0])}`);

// 8. GROUP BY
t0 = performance.now();
const grouped = db.execute('SELECT age, COUNT(*) AS cnt FROM users GROUP BY age ORDER BY cnt DESC');
t1 = performance.now();
console.log(`GROUP BY age (${grouped.rows.length} groups): ${(t1 - t0).toFixed(1)}ms`);

// 9. JOIN (users × orders)
t0 = performance.now();
const joined = db.execute('SELECT u.name, o.amount, o.status FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = \'delivered\' LIMIT 100');
t1 = performance.now();
console.log(`JOIN users×orders WHERE status='delivered' LIMIT 100: ${(t1 - t0).toFixed(1)}ms (${joined.rows.length} rows)`);

// 10. UPDATE
t0 = performance.now();
db.execute("UPDATE users SET age = age + 1 WHERE age < 30");
t1 = performance.now();
const updatedCount = db.execute('SELECT COUNT(*) AS cnt FROM users WHERE age <= 30').rows[0].cnt;
console.log(`UPDATE WHERE age < 30: ${(t1 - t0).toFixed(1)}ms`);

// 11. DELETE
t0 = performance.now();
db.execute("DELETE FROM orders WHERE status = 'pending'");
t1 = performance.now();
const remaining = db.execute('SELECT COUNT(*) AS cnt FROM orders').rows[0].cnt;
console.log(`DELETE WHERE status='pending': ${(t1 - t0).toFixed(1)}ms (${formatNum(remaining)} remaining)`);

// 12. ORDER BY
t0 = performance.now();
const sorted = db.execute('SELECT name, age FROM users ORDER BY age DESC, name ASC LIMIT 20');
t1 = performance.now();
console.log(`ORDER BY age DESC, name ASC LIMIT 20: ${(t1 - t0).toFixed(1)}ms`);

// Summary
console.log('\n=== Summary ===');
console.log(`Total rows inserted: ${formatNum(N * 2)}`);
console.log(`Insert rate: ${formatRate(N * 2, insertMs + orderInsertMs)} rows/sec`);
console.log(`Point query rate: ${formatRate(1000, pointMs)} queries/sec`);
