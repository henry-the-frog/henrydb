// server-prepared-perf.test.js — Prepared statement performance through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15521;

describe('Prepared Statement Performance', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    await client.query('CREATE TABLE perf_data (id INTEGER, name TEXT, score INTEGER)');
    for (let i = 0; i < 200; i++) {
      await client.query(`INSERT INTO perf_data VALUES (${i}, 'item_${i}', ${i * 3})`);
    }
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('parameterized queries work', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT * FROM perf_data WHERE id = $1', [42]);
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(result.rows[0].name, 'item_42');

    await client.end();
  });

  it('repeated parameterized queries', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const start = Date.now();
    for (let i = 0; i < 100; i++) {
      const result = await client.query('SELECT * FROM perf_data WHERE id = $1', [i]);
      assert.strictEqual(result.rows.length, 1);
    }
    const elapsed = Date.now() - start;
    console.log(`  100 parameterized queries in ${elapsed}ms`);

    await client.end();
  });

  it('batch insert with parameters', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE batch_insert (id INTEGER, val TEXT)');
    
    const start = Date.now();
    for (let i = 0; i < 200; i++) {
      await client.query('INSERT INTO batch_insert VALUES ($1, $2)', [i, `batch_${i}`]);
    }
    const elapsed = Date.now() - start;

    const result = await client.query('SELECT COUNT(*) AS cnt FROM batch_insert');
    assert.strictEqual(parseInt(result.rows[0].cnt), 200);
    console.log(`  200 parameterized inserts in ${elapsed}ms`);

    await client.end();
  });

  it('mixed read/write workload', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE mixed_workload (id INTEGER, counter INTEGER)');
    
    // Interleaved reads and writes
    for (let i = 0; i < 50; i++) {
      await client.query(`INSERT INTO mixed_workload VALUES (${i}, 0)`);
    }

    // Then reads
    const result = await client.query('SELECT COUNT(*) AS cnt FROM mixed_workload');
    assert.strictEqual(parseInt(result.rows[0].cnt), 50);

    await client.end();
  });

  it('aggregate performance on 200 rows', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const start = Date.now();
    for (let i = 0; i < 50; i++) {
      await client.query('SELECT AVG(score) AS avg, MIN(score) AS mn, MAX(score) AS mx FROM perf_data');
    }
    const elapsed = Date.now() - start;
    console.log(`  50 aggregate queries on 200 rows in ${elapsed}ms`);

    // Verify correctness
    const result = await client.query('SELECT AVG(score) AS avg FROM perf_data');
    assert.ok(parseFloat(result.rows[0].avg) > 0);

    await client.end();
  });
});
