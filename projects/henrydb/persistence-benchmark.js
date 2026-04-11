// persistence-benchmark.js — Comprehensive persistence benchmarks for HenryDB
// Usage: node persistence-benchmark.js

import { PersistentDatabase } from './src/persistent-db.js';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { rmSync, mkdirSync, existsSync, statSync, readdirSync } from 'node:fs';

function testDir(label) {
  const d = join(tmpdir(), `henrydb-bench-${label}-${Date.now()}`);
  return d;
}

function dirSize(dir) {
  let total = 0;
  try {
    for (const f of readdirSync(dir)) {
      total += statSync(join(dir, f)).size;
    }
  } catch {}
  return total;
}

function bench(name, fn) {
  const start = performance.now();
  const result = fn();
  const elapsed = performance.now() - start;
  return { name, elapsed, result };
}

function formatMs(ms) {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}µs`;
  if (ms < 1000) return `${ms.toFixed(1)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

function formatRate(count, ms) {
  const perSec = (count / ms) * 1000;
  if (perSec > 1000) return `${(perSec / 1000).toFixed(1)}K/s`;
  return `${perSec.toFixed(0)}/s`;
}

console.log('=== HenryDB Persistence Benchmark ===\n');

// ---------- Insert Throughput ----------
const N = 1000;
console.log(`--- Insert Throughput (${N} rows) ---`);

const d1 = testDir('insert');
{
  const { elapsed } = bench('Sequential INSERT', () => {
    const db = PersistentDatabase.open(d1, { poolSize: 32 });
    db.execute('CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT, data TEXT)');
    for (let i = 0; i < N; i++) {
      db.execute(`INSERT INTO bench VALUES (${i}, 'user_${i}', ${i * 7}, '${'x'.repeat(40)}')`);
    }
    db.close();
  });
  const size = dirSize(d1);
  console.log(`  ${N} inserts: ${formatMs(elapsed)} (${formatRate(N, elapsed)})`);
  console.log(`  Disk usage: ${(size / 1024).toFixed(0)}KB`);
}

// ---------- Close/Reopen Speed ----------
console.log(`\n--- Close/Reopen Speed ---`);
{
  const { elapsed: reopenElapsed } = bench('Reopen', () => {
    const db = PersistentDatabase.open(d1, { poolSize: 32 });
    const r = db.execute('SELECT COUNT(*) as cnt FROM bench');
    db.close();
    return r.rows[0].cnt;
  });
  console.log(`  Reopen + COUNT: ${formatMs(reopenElapsed)}`);
}

// ---------- WAL Replay Speed ----------
console.log(`\n--- WAL Replay Speed ---`);
const d2 = testDir('wal-replay');
{
  const db = PersistentDatabase.open(d2, { poolSize: 16 });
  db.execute('CREATE TABLE wal_bench (id INT PRIMARY KEY, val TEXT)');
  for (let i = 0; i < 1000; i++) {
    db.execute(`INSERT INTO wal_bench VALUES (${i}, 'data_${i}')`);
  }
  // Flush WAL but don't close cleanly — simulate crash
  db._wal.flush();
  db._saveCatalog();
  // Don't call close() — force WAL replay on next open
  
  const { elapsed: replayElapsed } = bench('WAL Recovery', () => {
    const db2 = PersistentDatabase.open(d2, { poolSize: 16 });
    const r = db2.execute('SELECT COUNT(*) as cnt FROM wal_bench');
    db2.close();
    return r.rows[0].cnt;
  });
  console.log(`  1000-row WAL recovery: ${formatMs(replayElapsed)}`);
}

// ---------- Checkpoint + Truncation ----------
console.log(`\n--- Checkpoint + Truncation ---`);
const d3 = testDir('checkpoint');
{
  const db = PersistentDatabase.open(d3, { poolSize: 32 });
  db.execute('CREATE TABLE cp_bench (id INT PRIMARY KEY, val INT)');
  for (let i = 0; i < 2000; i++) {
    db.execute(`INSERT INTO cp_bench VALUES (${i}, ${i})`);
  }
  
  const walSizeBefore = dirSize(d3);
  
  const { elapsed: cpElapsed } = bench('Checkpoint', () => {
    db.flush();
    db._wal.checkpoint();
    db._wal.truncate();
  });
  
  const walSizeAfter = dirSize(d3);
  console.log(`  Checkpoint + truncate: ${formatMs(cpElapsed)}`);
  console.log(`  Disk before: ${(walSizeBefore / 1024).toFixed(0)}KB → after: ${(walSizeAfter / 1024).toFixed(0)}KB`);
  
  // Verify data survives
  db.close();
  const db2 = PersistentDatabase.open(d3, { poolSize: 32 });
  const cnt = db2.execute('SELECT COUNT(*) as cnt FROM cp_bench').rows[0].cnt;
  console.log(`  Data survived: ${cnt === 2000 ? '✓' : '✗'} (${cnt} rows)`);
  db2.close();
}

// ---------- Multi-Cycle Persistence ----------
console.log(`\n--- Multi-Cycle Persistence (20 cycles) ---`);
const d4 = testDir('multi-cycle');
{
  let db = PersistentDatabase.open(d4, { poolSize: 16 });
  db.execute('CREATE TABLE cycle_bench (id INT PRIMARY KEY, cycle INT, val INT)');
  db.close();
  
  const { elapsed: cycleElapsed } = bench('20 open/insert/close cycles', () => {
    let totalInserted = 0;
    for (let cycle = 0; cycle < 20; cycle++) {
      db = PersistentDatabase.open(d4, { poolSize: 16 });
      for (let i = 0; i < 50; i++) {
        const id = cycle * 50 + i;
        db.execute(`INSERT INTO cycle_bench VALUES (${id}, ${cycle}, ${id * 3})`);
      }
      db.close();
      totalInserted += 50;
    }
    return totalInserted;
  });
  
  db = PersistentDatabase.open(d4, { poolSize: 16 });
  const cnt = db.execute('SELECT COUNT(*) as cnt FROM cycle_bench').rows[0].cnt;
  db.close();
  
  console.log(`  20 cycles × 50 rows: ${formatMs(cycleElapsed)} (${formatRate(1000, cycleElapsed)} rows)`);
  console.log(`  Data integrity: ${cnt === 1000 ? '✓' : '✗'} (${cnt} rows)`);
}

// ---------- Query Performance on Persisted Data ----------
console.log(`\n--- Query Performance (persisted ${N} rows) ---`);
{
  const db = PersistentDatabase.open(d1, { poolSize: 32 });
  
  const { elapsed: scanElapsed } = bench('Full scan', () => {
    return db.execute('SELECT COUNT(*) as cnt FROM bench').rows[0].cnt;
  });
  console.log(`  Full scan COUNT: ${formatMs(scanElapsed)}`);
  
  const { elapsed: filterElapsed } = bench('Filtered scan', () => {
    return db.execute('SELECT COUNT(*) as cnt FROM bench WHERE value > 20000').rows[0].cnt;
  });
  console.log(`  Filtered COUNT (value > 20000): ${formatMs(filterElapsed)}`);
  
  const { elapsed: aggElapsed } = bench('Aggregates', () => {
    return db.execute('SELECT SUM(value) as total, AVG(value) as avg, MIN(value) as mn, MAX(value) as mx FROM bench');
  });
  console.log(`  SUM/AVG/MIN/MAX: ${formatMs(aggElapsed)}`);
  
  const { elapsed: pointElapsed } = bench('Point query', () => {
    return db.execute('SELECT * FROM bench WHERE id = 500').rows[0].name;
  });
  console.log(`  Point query (id=500): ${formatMs(pointElapsed)}`);
  
  db.close();
}

// ---------- Tiny Buffer Pool Stress ----------
console.log(`\n--- Tiny Buffer Pool (4 pages, ${N} rows) ---`);
const d5 = testDir('tiny-pool');
{
  const { elapsed: tinyElapsed } = bench('Tiny pool insert', () => {
    const db = PersistentDatabase.open(d5, { poolSize: 4 });
    db.execute('CREATE TABLE tiny (id INT PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO tiny VALUES (${i}, 'data_${i}_${'y'.repeat(50)}')`);
    }
    db.close();
  });
  console.log(`  500 inserts (poolSize=4): ${formatMs(tinyElapsed)} (${formatRate(500, tinyElapsed)})`);
  
  const { elapsed: tinyReopen } = bench('Tiny pool reopen', () => {
    const db = PersistentDatabase.open(d5, { poolSize: 4 });
    const r = db.execute('SELECT COUNT(*) as cnt FROM tiny');
    db.close();
    return r.rows[0].cnt;
  });
  console.log(`  Reopen + COUNT (poolSize=4): ${formatMs(tinyReopen)}`);
}

// ---------- Cleanup ----------
console.log(`\n--- Cleanup ---`);
for (const d of [d1, d2, d3, d4, d5]) {
  try { rmSync(d, { recursive: true }); } catch {}
}
console.log('  Temp dirs cleaned.');
console.log('\nDone.');
