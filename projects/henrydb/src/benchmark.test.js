// benchmark.test.js — Performance benchmarks for HenryDB
// Measures queries-per-second for common SQL patterns

import { describe, it, before, after, beforeEach } from 'node:test';
import { Database } from './db.js';
import { HenryDBServer } from './server.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';

const { Client } = pg;

function bench(name, fn, iterations = 1000) {
  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn(i);
  const elapsed = performance.now() - start;
  const qps = (iterations / elapsed * 1000).toFixed(0);
  console.log(`  ${name}: ${qps} ops/sec (${elapsed.toFixed(1)}ms for ${iterations} ops)`);
  return { name, qps: Number(qps), elapsed, iterations };
}

async function benchAsync(name, fn, iterations = 100) {
  const start = performance.now();
  for (let i = 0; i < iterations; i++) await fn(i);
  const elapsed = performance.now() - start;
  const qps = (iterations / elapsed * 1000).toFixed(0);
  console.log(`  ${name}: ${qps} ops/sec (${elapsed.toFixed(1)}ms for ${iterations} ops)`);
  return { name, qps: Number(qps), elapsed, iterations };
}

describe('In-Memory Performance', () => {
  let db;
  
  before(() => {
    db = new Database();
    db.execute("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, val INT, category TEXT)");
    db.execute("CREATE INDEX idx_cat ON bench (category)");
    for (let i = 1; i <= 10000; i++) {
      db.execute(`INSERT INTO bench VALUES (${i}, 'item${i}', ${i % 100}, 'cat${i % 10}')`);
    }
  });
  
  it('point query by PK', () => {
    const r = bench('PK lookup', (i) => {
      db.execute(`SELECT * FROM bench WHERE id = ${1 + (i % 10000)}`);
    }, 5000);
    console.log(`  → ${r.qps} point queries/sec on 10K rows`);
  });
  
  it('full table scan', () => {
    const r = bench('Full scan', () => {
      db.execute("SELECT COUNT(*) FROM bench");
    }, 1000);
    console.log(`  → ${r.qps} full scans/sec on 10K rows`);
  });
  
  it('filtered scan', () => {
    const r = bench('Filtered scan', (i) => {
      db.execute(`SELECT * FROM bench WHERE val = ${i % 100}`);
    }, 2000);
    console.log(`  → ${r.qps} filtered scans/sec on 10K rows`);
  });
  
  it('indexed lookup', () => {
    const r = bench('Index lookup', (i) => {
      db.execute(`SELECT * FROM bench WHERE category = 'cat${i % 10}'`);
    }, 2000);
    console.log(`  → ${r.qps} index lookups/sec on 10K rows`);
  });
  
  it('aggregation', () => {
    const r = bench('Aggregation', () => {
      db.execute("SELECT category, COUNT(*), AVG(val), SUM(val) FROM bench GROUP BY category");
    }, 500);
    console.log(`  → ${r.qps} GROUP BY/sec on 10K rows`);
  });
  
  it('INSERT throughput', () => {
    db.execute("CREATE TABLE bench_insert (id INT, val INT)");
    const r = bench('INSERT', (i) => {
      db.execute(`INSERT INTO bench_insert VALUES (${i}, ${i * 2})`);
    }, 10000);
    console.log(`  → ${r.qps} inserts/sec`);
  });
  
  it('UPDATE throughput', () => {
    const r = bench('UPDATE', (i) => {
      db.execute(`UPDATE bench SET val = ${i} WHERE id = ${1 + (i % 10000)}`);
    }, 2000);
    console.log(`  → ${r.qps} updates/sec on 10K rows`);
  });
  
  it('JOIN throughput', () => {
    db.execute("CREATE TABLE bench_join (id INT PRIMARY KEY, bench_id INT, extra TEXT)");
    for (let i = 1; i <= 1000; i++) {
      db.execute(`INSERT INTO bench_join VALUES (${i}, ${i}, 'extra${i}')`);
    }
    const r = bench('JOIN', (i) => {
      db.execute(`SELECT b.name, j.extra FROM bench b INNER JOIN bench_join j ON j.bench_id = b.id WHERE b.id = ${1 + (i % 1000)}`);
    }, 1000);
    console.log(`  → ${r.qps} joins/sec (10K × 1K rows)`);
  });
});

describe('Wire Protocol Performance', () => {
  let server, port, dir, client;
  
  before(async () => {
    port = 34600 + Math.floor(Math.random() * 100);
    dir = mkdtempSync(join(tmpdir(), 'henrydb-bench-'));
    server = new HenryDBServer({ port, dataDir: dir });
    await server.start();
    
    client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await client.connect();
    
    await client.query("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, val INT, category TEXT)");
    for (let i = 1; i <= 1000; i++) {
      await client.query('INSERT INTO bench VALUES ($1, $2, $3, $4)', [i, `item${i}`, i % 100, `cat${i % 10}`]);
    }
  });
  
  after(async () => {
    await client.end();
    await server.stop();
    rmSync(dir, { recursive: true });
  });
  
  it('wire protocol: point query', async () => {
    const r = await benchAsync('Wire PK lookup', async (i) => {
      await client.query('SELECT * FROM bench WHERE id = $1', [1 + (i % 1000)]);
    }, 500);
    console.log(`  → ${r.qps} wire queries/sec on 1K rows`);
  });
  
  it('wire protocol: parameterized INSERT', async () => {
    await client.query("CREATE TABLE bench_wire (id INT, val TEXT)");
    const r = await benchAsync('Wire INSERT', async (i) => {
      await client.query('INSERT INTO bench_wire VALUES ($1, $2)', [i, `val${i}`]);
    }, 500);
    console.log(`  → ${r.qps} wire inserts/sec`);
  });
  
  it('wire protocol: aggregation', async () => {
    const r = await benchAsync('Wire aggregation', async () => {
      await client.query("SELECT category, COUNT(*), SUM(val) FROM bench GROUP BY category");
    }, 200);
    console.log(`  → ${r.qps} wire aggregations/sec on 1K rows`);
  });
});
