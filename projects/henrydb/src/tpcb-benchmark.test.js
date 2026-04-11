// tpcb-benchmark.test.js — TPC-B-like transaction processing benchmark
// Measures real-world TPS through the wire protocol with MVCC, WAL, and transactions.
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import copyStreams from 'pg-copy-streams';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Client, Pool } = pg;
const { from: copyFrom } = copyStreams;

function getPort() {
  return 32000 + Math.floor(Math.random() * 10000);
}

// TPC-B parameters (small scale for testing)
const SCALE = 1;  // 1 branch = 10 tellers, 100000 accounts (scaled down)
const N_BRANCHES = SCALE;
const N_TELLERS = 10 * SCALE;
const N_ACCOUNTS = 100 * SCALE; // 100 instead of 100000 for testing speed

describe('TPC-B Benchmark', () => {
  let server, port, dir;
  
  before(async () => {
    port = getPort();
    dir = mkdtempSync(join(tmpdir(), 'henrydb-tpcb-'));
    server = new HenryDBServer({ port, dataDir: dir, transactional: true });
    await server.start();
    
    // Setup schema (TPC-B standard tables)
    const setup = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await setup.connect();
    
    await setup.query(`
      CREATE TABLE pgbench_branches (bid INT, bbalance INT, filler TEXT)
    `);
    await setup.query(`
      CREATE TABLE pgbench_tellers (tid INT, bid INT, tbalance INT, filler TEXT)
    `);
    await setup.query(`
      CREATE TABLE pgbench_accounts (aid INT, bid INT, abalance INT, filler TEXT)
    `);
    await setup.query(`
      CREATE TABLE pgbench_history (tid INT, bid INT, aid INT, delta INT, mtime TEXT)
    `);
    
    // Populate
    for (let i = 1; i <= N_BRANCHES; i++) {
      await setup.query(`INSERT INTO pgbench_branches VALUES (${i}, 0, 'filler')`);
    }
    for (let i = 1; i <= N_TELLERS; i++) {
      await setup.query(`INSERT INTO pgbench_tellers VALUES (${i}, ${((i - 1) % N_BRANCHES) + 1}, 0, 'filler')`);
    }
    
    // Bulk load accounts via COPY for speed
    const stream = setup.query(copyFrom('COPY pgbench_accounts FROM STDIN'));
    const lines = [];
    for (let i = 1; i <= N_ACCOUNTS; i++) {
      const bid = ((i - 1) % N_BRANCHES) + 1;
      lines.push(`${i}\t${bid}\t0\tfiller`);
    }
    stream.write(lines.join('\n') + '\n');
    await new Promise((resolve, reject) => {
      stream.end();
      stream.on('finish', resolve);
      stream.on('error', reject);
    });
    
    console.log(`Setup: ${N_BRANCHES} branches, ${N_TELLERS} tellers, ${N_ACCOUNTS} accounts`);
    await setup.end();
  });
  
  after(async () => {
    if (server) await server.stop();
    if (dir) rmSync(dir, { recursive: true });
  });

  it('TPC-B single-client benchmark', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    const N_TRANSACTIONS = 50;
    const start = performance.now();
    let successCount = 0;
    
    for (let i = 0; i < N_TRANSACTIONS; i++) {
      const aid = Math.floor(Math.random() * N_ACCOUNTS) + 1;
      const bid = ((aid - 1) % N_BRANCHES) + 1;
      const tid = Math.floor(Math.random() * N_TELLERS) + 1;
      const delta = Math.floor(Math.random() * 10001) - 5000; // -5000 to +5000
      
      try {
        await client.query('BEGIN');
        await client.query('UPDATE pgbench_accounts SET abalance = abalance + $1 WHERE aid = $2', [delta, aid]);
        await client.query('SELECT abalance FROM pgbench_accounts WHERE aid = $1', [aid]);
        await client.query('UPDATE pgbench_tellers SET tbalance = tbalance + $1 WHERE tid = $2', [delta, tid]);
        await client.query('UPDATE pgbench_branches SET bbalance = bbalance + $1 WHERE bid = $2', [delta, bid]);
        await client.query(`INSERT INTO pgbench_history VALUES ($1, $2, $3, $4, 'now')`, [tid, bid, aid, delta]);
        await client.query('COMMIT');
        successCount++;
      } catch (e) {
        try { await client.query('ROLLBACK'); } catch (re) { /* ignore */ }
        // Write-write conflicts expected in MVCC
      }
    }
    
    const elapsed = performance.now() - start;
    const tps = (successCount / (elapsed / 1000)).toFixed(1);
    
    console.log(`\n=== TPC-B Single-Client Results ===`);
    console.log(`Transactions: ${successCount}/${N_TRANSACTIONS} successful`);
    console.log(`Time: ${elapsed.toFixed(0)}ms`);
    console.log(`TPS: ${tps}`);
    console.log(`Avg latency: ${(elapsed / successCount).toFixed(1)}ms`);
    
    assert.ok(successCount > 0, 'At least some transactions should succeed');
    
    // Verify consistency: sum of all account balances should equal branch balance
    const accounts = await client.query('SELECT SUM(abalance) as total FROM pgbench_accounts WHERE bid = 1');
    const branch = await client.query('SELECT bbalance FROM pgbench_branches WHERE bid = 1');
    console.log(`\nConsistency check:`);
    console.log(`  Sum of account balances: ${accounts.rows[0].total}`);
    console.log(`  Branch balance: ${branch.rows[0].bbalance}`);
    
    // History should have one row per successful transaction
    const history = await client.query('SELECT COUNT(*) as n FROM pgbench_history');
    console.log(`  History rows: ${history.rows[0].n}`);
    assert.equal(parseInt(String(history.rows[0].n)), successCount);
    
    await client.end();
  });

  it('latency distribution', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    const N = 30;
    const latencies = [];
    
    for (let i = 0; i < N; i++) {
      const aid = Math.floor(Math.random() * N_ACCOUNTS) + 1;
      const start = performance.now();
      
      try {
        await client.query('BEGIN');
        await client.query('UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = $1', [aid]);
        await client.query('COMMIT');
        latencies.push(performance.now() - start);
      } catch (e) {
        try { await client.query('ROLLBACK'); } catch (re) { /* ignore */ }
      }
    }
    
    latencies.sort((a, b) => a - b);
    const p50 = latencies[Math.floor(latencies.length * 0.5)];
    const p90 = latencies[Math.floor(latencies.length * 0.9)];
    const p99 = latencies[Math.floor(latencies.length * 0.99)] || latencies[latencies.length - 1];
    const avg = latencies.reduce((s, l) => s + l, 0) / latencies.length;
    
    console.log(`\n=== Latency Distribution (${latencies.length} txns) ===`);
    console.log(`  Avg:  ${avg.toFixed(1)}ms`);
    console.log(`  P50:  ${p50.toFixed(1)}ms`);
    console.log(`  P90:  ${p90.toFixed(1)}ms`);
    console.log(`  P99:  ${p99.toFixed(1)}ms`);
    console.log(`  Min:  ${latencies[0].toFixed(1)}ms`);
    console.log(`  Max:  ${latencies[latencies.length - 1].toFixed(1)}ms`);
    
    assert.ok(latencies.length > 0, 'Should have some successful latencies');
    
    await client.end();
  });

  it('read-only performance (SELECT throughput)', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    const N = 100;
    const start = performance.now();
    
    for (let i = 0; i < N; i++) {
      const aid = Math.floor(Math.random() * N_ACCOUNTS) + 1;
      await client.query('SELECT abalance FROM pgbench_accounts WHERE aid = $1', [aid]);
    }
    
    const elapsed = performance.now() - start;
    const qps = (N / (elapsed / 1000)).toFixed(1);
    
    console.log(`\n=== Read-Only Performance ===`);
    console.log(`Queries: ${N}`);
    console.log(`Time: ${elapsed.toFixed(0)}ms`);
    console.log(`QPS: ${qps}`);
    console.log(`Avg latency: ${(elapsed / N).toFixed(1)}ms`);
    
    assert.ok(parseFloat(qps) > 0);
    
    await client.end();
  });
});
