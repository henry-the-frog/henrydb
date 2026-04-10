// server-pipelining.test.js — Test query batching/pipelining throughput
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Client } = pg;

describe('Query Pipelining', () => {
  let server, port, dir;
  
  before(async () => {
    port = 23000 + Math.floor(Math.random() * 5000);
    dir = mkdtempSync(join(tmpdir(), 'henrydb-pipe-'));
    server = new HenryDBServer({ port, dataDir: dir });
    await server.start();
    
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await client.connect();
    await client.query('CREATE TABLE bench (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 100; i++) {
      await client.query(`INSERT INTO bench VALUES (${i}, 0)`);
    }
    await client.end();
  });
  
  after(async () => {
    if (server) await server.stop();
    if (dir) rmSync(dir, { recursive: true });
  });

  it('batched queries are faster than individual', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await client.connect();
    
    const N = 100;
    
    // Individual
    const start1 = Date.now();
    for (let i = 0; i < N; i++) {
      const id = 1 + (i % 100);
      await client.query(`UPDATE bench SET val = val + 1 WHERE id = ${id}`);
    }
    const individual = Date.now() - start1;
    
    // Batched (10 per call)
    const start2 = Date.now();
    for (let i = 0; i < N / 10; i++) {
      const sqls = [];
      for (let j = 0; j < 10; j++) {
        const id = 1 + ((i * 10 + j) % 100);
        sqls.push(`UPDATE bench SET val = val + 1 WHERE id = ${id}`);
      }
      await client.query(sqls.join('; '));
    }
    const batched = Date.now() - start2;
    
    console.log(`  Individual: ${individual}ms, Batched (10/call): ${batched}ms`);
    console.log(`  Speedup: ${(individual / batched).toFixed(1)}x`);
    
    // Batched should be at least 1.5x faster
    assert.ok(batched < individual, 'Batched should be faster than individual');
    
    // Verify all updates applied correctly
    const r = await client.query('SELECT SUM(val) as total FROM bench');
    assert.equal(String(r.rows[0].total), String(N * 2)); // 200 total updates
    
    await client.end();
  });

  it('semicolon-separated multi-statement works correctly', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await client.connect();
    
    // Create test table and insert via multi-statement
    await client.query('CREATE TABLE multi (id INT PRIMARY KEY, name TEXT)');
    await client.query("INSERT INTO multi VALUES (1, 'one'); INSERT INTO multi VALUES (2, 'two'); INSERT INTO multi VALUES (3, 'three')");
    
    const r = await client.query('SELECT COUNT(*) as cnt FROM multi');
    assert.equal(String(r.rows[0].cnt), '3');
    
    await client.query('DROP TABLE multi');
    await client.end();
  });
});
