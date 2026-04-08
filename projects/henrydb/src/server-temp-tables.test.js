// server-temp-tables.test.js — Tests for TEMP TABLE support
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15507;

describe('Temporary Tables', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('CREATE TEMP TABLE works', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TEMP TABLE temp_data (id INTEGER, val TEXT)');
    await client.query("INSERT INTO temp_data VALUES (1, 'hello')");
    
    const result = await client.query('SELECT * FROM temp_data');
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(result.rows[0].val, 'hello');

    await client.end();
  });

  it('temp table is dropped on disconnect', async () => {
    // Create temp table
    const c1 = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await c1.connect();
    await c1.query('CREATE TEMP TABLE session_data (x INTEGER)');
    await c1.query('INSERT INTO session_data VALUES (42)');
    await c1.end();
    
    await new Promise(r => setTimeout(r, 200));

    // New connection should not see it
    const c2 = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await c2.connect();
    
    try {
      await c2.query('SELECT * FROM session_data');
      assert.fail('Should fail — temp table should be dropped');
    } catch (e) {
      assert.ok(e.message.includes('not found') || e.message.includes('session_data'));
    }

    await c2.end();
  });

  it('CREATE TEMPORARY TABLE syntax also works', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TEMPORARY TABLE tmp2 (a INTEGER, b TEXT)');
    await client.query("INSERT INTO tmp2 VALUES (1, 'test')");
    
    const result = await client.query('SELECT * FROM tmp2');
    assert.strictEqual(result.rows.length, 1);

    await client.end();
  });

  it('temp tables work alongside regular tables', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE permanent (id INTEGER, name TEXT)');
    await client.query("INSERT INTO permanent VALUES (1, 'stable')");
    
    await client.query('CREATE TEMP TABLE ephemeral (id INTEGER, note TEXT)');
    await client.query("INSERT INTO ephemeral VALUES (1, 'temporary')");

    const r1 = await client.query('SELECT * FROM permanent');
    const r2 = await client.query('SELECT * FROM ephemeral');
    assert.strictEqual(r1.rows.length, 1);
    assert.strictEqual(r2.rows.length, 1);

    await client.end();
  });
});
