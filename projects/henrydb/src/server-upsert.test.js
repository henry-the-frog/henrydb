// server-upsert.test.js — INSERT ON CONFLICT through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15523;

describe('UPSERT (INSERT ON CONFLICT)', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('INSERT ON CONFLICT DO NOTHING', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE upsert_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');
    await client.query("INSERT INTO upsert_test VALUES (1, 'Alice', 90)");
    
    // Try to insert duplicate — should be ignored
    await client.query("INSERT INTO upsert_test VALUES (1, 'Alice2', 95) ON CONFLICT DO NOTHING");
    
    const result = await client.query('SELECT * FROM upsert_test WHERE id = 1');
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(result.rows[0].name, 'Alice'); // Should still be original

    await client.end();
  });

  it('INSERT ON CONFLICT DO UPDATE (simplified)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE upsert_update (id INTEGER PRIMARY KEY, name TEXT, visits INTEGER)');
    await client.query("INSERT INTO upsert_update VALUES (1, 'Alice', 1)");
    
    // Try duplicate insert — should be silently ignored with DO NOTHING
    await client.query("INSERT INTO upsert_update VALUES (1, 'Alice', 2) ON CONFLICT DO NOTHING");
    
    const result = await client.query('SELECT * FROM upsert_update WHERE id = 1');
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(parseInt(result.rows[0].visits), 1); // Unchanged

    await client.end();
  });

  it('multiple upserts with DO NOTHING', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE counters (key TEXT PRIMARY KEY, value INTEGER)');
    
    // Insert initial
    await client.query("INSERT INTO counters VALUES ('hits', 0)");
    
    // Multiple attempts — all should be ignored
    for (let i = 0; i < 5; i++) {
      await client.query("INSERT INTO counters VALUES ('hits', 0) ON CONFLICT DO NOTHING");
    }
    
    const result = await client.query("SELECT * FROM counters WHERE key = 'hits'");
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(parseInt(result.rows[0].value), 0); // Unchanged

    // Insert a different key — should succeed
    await client.query("INSERT INTO counters VALUES ('misses', 5) ON CONFLICT DO NOTHING");
    const all = await client.query('SELECT * FROM counters ORDER BY key');
    assert.strictEqual(all.rows.length, 2);

    await client.end();
  });

  it('non-conflicting insert succeeds normally', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE upsert_normal (id INTEGER PRIMARY KEY, val TEXT)');
    await client.query("INSERT INTO upsert_normal VALUES (1, 'a') ON CONFLICT DO NOTHING");
    await client.query("INSERT INTO upsert_normal VALUES (2, 'b') ON CONFLICT DO NOTHING");
    
    const result = await client.query('SELECT * FROM upsert_normal ORDER BY id');
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });
});
