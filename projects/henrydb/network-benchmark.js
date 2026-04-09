// HenryDB Network Benchmark
// Tests performance through the pg wire protocol vs in-process
import { HenryDBServer } from './src/server.js';
import { Database } from './src/db.js';
import pg from 'pg';

const N = 1000;
const PORT = 15500;

function formatNum(n) { return n.toLocaleString(); }
function formatRate(count, ms) { return formatNum(Math.round(count / (ms / 1000))); }

// ===== In-Process Benchmark =====
console.log('=== In-Process Benchmark ===\n');

const db = new Database();
let t0 = performance.now();
db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
for (let i = 1; i <= N; i++) {
  db.execute(`INSERT INTO users VALUES (${i}, 'User_${i}', ${20 + (i % 50)})`);
}
const inProcessInsert = performance.now() - t0;
console.log(`INSERT ${N} rows: ${inProcessInsert.toFixed(0)}ms (${formatRate(N, inProcessInsert)}/sec)`);

t0 = performance.now();
for (let i = 0; i < 100; i++) {
  db.execute(`SELECT * FROM users WHERE id = ${(i % N) + 1}`);
}
const inProcessPoint = performance.now() - t0;
console.log(`Point queries (100x): ${inProcessPoint.toFixed(1)}ms (${formatRate(100, inProcessPoint)}/sec)`);

t0 = performance.now();
const allRows = db.execute('SELECT * FROM users');
const inProcessScan = performance.now() - t0;
console.log(`Full scan (${allRows.rows.length} rows): ${inProcessScan.toFixed(1)}ms`);

// ===== Network Benchmark =====
console.log('\n=== Network Benchmark (pg wire protocol) ===\n');

const server = new HenryDBServer({ port: PORT });
await server.start();

const client = new pg.Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
await client.connect();

t0 = performance.now();
await client.query('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
for (let i = 1; i <= N; i++) {
  await client.query(`INSERT INTO users VALUES ($1, $2, $3)`, [i, `User_${i}`, 20 + (i % 50)]);
}
const networkInsert = performance.now() - t0;
console.log(`INSERT ${N} rows: ${networkInsert.toFixed(0)}ms (${formatRate(N, networkInsert)}/sec)`);

t0 = performance.now();
for (let i = 0; i < 100; i++) {
  await client.query('SELECT * FROM users WHERE id = $1', [(i % N) + 1]);
}
const networkPoint = performance.now() - t0;
console.log(`Point queries (100x): ${networkPoint.toFixed(1)}ms (${formatRate(100, networkPoint)}/sec)`);

t0 = performance.now();
const netAllRows = await client.query('SELECT * FROM users');
const networkScan = performance.now() - t0;
console.log(`Full scan (${netAllRows.rows.length} rows): ${networkScan.toFixed(1)}ms`);

t0 = performance.now();
const joinResult = await client.query('SELECT u.name, u.age FROM users u WHERE u.age > 40 ORDER BY u.age DESC LIMIT 10');
const networkJoin = performance.now() - t0;
console.log(`Filtered+Sort+Limit (${joinResult.rows.length} rows): ${networkJoin.toFixed(1)}ms`);

t0 = performance.now();
const aggResult = await client.query('SELECT COUNT(*) AS cnt, AVG(age) AS avg_age FROM users');
const networkAgg = performance.now() - t0;
console.log(`Aggregation: ${networkAgg.toFixed(1)}ms → ${JSON.stringify(aggResult.rows[0])}`);

await client.end();
await server.stop();

// ===== Comparison =====
console.log('\n=== Network Overhead ===');
console.log(`INSERT: ${(networkInsert / inProcessInsert).toFixed(1)}x slower`);
console.log(`Point query: ${(networkPoint / inProcessPoint).toFixed(1)}x slower`);
console.log(`Full scan: ${(networkScan / inProcessScan).toFixed(1)}x slower`);

process.exit(0);
