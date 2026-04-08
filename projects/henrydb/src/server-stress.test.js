// server-stress.test.js — Concurrent stress tests for HenryDB server
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client, Pool } = pg;
const PORT = 15511;

describe('Stress Tests', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
    
    // Setup
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    await client.query('CREATE TABLE stress (id INTEGER, val TEXT, counter INTEGER)');
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('handles 10 concurrent connections', async () => {
    const clients = [];
    for (let i = 0; i < 10; i++) {
      const c = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
      await c.connect();
      clients.push(c);
    }

    // Each client does a query
    const results = await Promise.all(
      clients.map((c, i) => c.query(`SELECT ${i} AS num`))
    );

    for (let i = 0; i < 10; i++) {
      assert.strictEqual(parseInt(results[i].rows[0].num), i);
    }

    await Promise.all(clients.map(c => c.end()));
  });

  it('concurrent inserts', async () => {
    const clients = [];
    for (let i = 0; i < 5; i++) {
      const c = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
      await c.connect();
      clients.push(c);
    }

    // Each client inserts 20 rows
    await Promise.all(
      clients.map(async (c, i) => {
        for (let j = 0; j < 20; j++) {
          const id = i * 20 + j;
          await c.query(`INSERT INTO stress VALUES (${id}, 'client_${i}', ${j})`);
        }
      })
    );

    // Verify total
    const check = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await check.connect();
    const result = await check.query('SELECT COUNT(*) AS cnt FROM stress');
    assert.strictEqual(parseInt(result.rows[0].cnt), 100);
    await check.end();

    await Promise.all(clients.map(c => c.end()));
  });

  it('concurrent reads and writes', async () => {
    const writer = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const readers = [];
    await writer.connect();

    for (let i = 0; i < 3; i++) {
      const r = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
      await r.connect();
      readers.push(r);
    }

    // Writer inserts while readers query
    const writeP = (async () => {
      for (let i = 200; i < 220; i++) {
        await writer.query(`INSERT INTO stress VALUES (${i}, 'concurrent', ${i})`);
      }
    })();

    const readP = Promise.all(
      readers.map(async (r) => {
        const results = [];
        for (let i = 0; i < 10; i++) {
          const res = await r.query('SELECT COUNT(*) AS cnt FROM stress');
          results.push(parseInt(res.rows[0].cnt));
        }
        return results;
      })
    );

    await Promise.all([writeP, readP]);

    // Final count should include new rows
    const finalR = await writer.query('SELECT COUNT(*) AS cnt FROM stress');
    assert.ok(parseInt(finalR.rows[0].cnt) >= 120);

    await writer.end();
    await Promise.all(readers.map(r => r.end()));
  });

  it('connection pool integration', async () => {
    const pool = new Pool({
      host: '127.0.0.1',
      port: PORT,
      user: 'test',
      database: 'test',
      max: 5,
    });

    // Run 20 queries through the pool
    const promises = [];
    for (let i = 0; i < 20; i++) {
      promises.push(pool.query(`SELECT ${i} AS num`));
    }

    const results = await Promise.all(promises);
    for (let i = 0; i < 20; i++) {
      assert.strictEqual(parseInt(results[i].rows[0].num), i);
    }

    await pool.end();
  });

  it('rapid connect/disconnect cycles', async () => {
    for (let i = 0; i < 20; i++) {
      const c = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
      await c.connect();
      await c.query(`SELECT ${i}`);
      await c.end();
    }
    // If we got here, all 20 cycles completed
  });
});
