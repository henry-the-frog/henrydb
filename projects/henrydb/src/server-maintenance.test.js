// server-maintenance.test.js — Tests for VACUUM, ANALYZE, TRUNCATE
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15505;

describe('Database Maintenance Commands', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('VACUUM completes without error', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE vacuum_test (id INTEGER, val TEXT)');
    await client.query("INSERT INTO vacuum_test VALUES (1, 'a')");
    await client.query("DELETE FROM vacuum_test WHERE id = 1");

    // VACUUM should succeed
    await client.query('VACUUM vacuum_test');
    await client.query('VACUUM'); // VACUUM all tables

    await client.end();
  });

  it('VACUUM ANALYZE completes', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('VACUUM ANALYZE vacuum_test');

    await client.end();
  });

  it('ANALYZE updates table stats', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE analyze_test (id INTEGER, name TEXT, score INTEGER)');
    for (let i = 0; i < 10; i++) {
      await client.query(`INSERT INTO analyze_test VALUES (${i}, 'name_${i}', ${i * 10})`);
    }

    await client.query('ANALYZE analyze_test');

    // Query should still work after ANALYZE
    const result = await client.query('SELECT COUNT(*) AS cnt FROM analyze_test');
    assert.strictEqual(parseInt(result.rows[0].cnt), 10);

    await client.end();
  });

  it('TRUNCATE empties a table', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE truncate_test (id INTEGER, val TEXT)');
    await client.query("INSERT INTO truncate_test VALUES (1, 'a')");
    await client.query("INSERT INTO truncate_test VALUES (2, 'b')");
    await client.query("INSERT INTO truncate_test VALUES (3, 'c')");

    const before = await client.query('SELECT COUNT(*) AS cnt FROM truncate_test');
    assert.strictEqual(parseInt(before.rows[0].cnt), 3);

    await client.query('TRUNCATE TABLE truncate_test');

    const after = await client.query('SELECT COUNT(*) AS cnt FROM truncate_test');
    assert.strictEqual(parseInt(after.rows[0].cnt), 0);

    await client.end();
  });

  it('TRUNCATE invalidates query cache', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE cache_trunc (id INTEGER)');
    await client.query('INSERT INTO cache_trunc VALUES (1)');

    // Run query to cache it
    await client.query('SELECT * FROM cache_trunc');

    // Truncate
    await client.query('TRUNCATE cache_trunc');

    // Should return empty (cache should be invalidated)
    const result = await client.query('SELECT * FROM cache_trunc');
    assert.strictEqual(result.rows.length, 0);

    await client.end();
  });
});
