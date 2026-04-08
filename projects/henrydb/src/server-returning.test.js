// server-returning.test.js — RETURNING clause through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15522;

describe('RETURNING Clause', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('INSERT ... RETURNING *', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE ret_test (id INTEGER, name TEXT, score INTEGER)');
    const result = await client.query("INSERT INTO ret_test VALUES (1, 'Alice', 95) RETURNING *");
    
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(parseInt(result.rows[0].id), 1);
    assert.strictEqual(result.rows[0].name, 'Alice');
    assert.strictEqual(parseInt(result.rows[0].score), 95);

    await client.end();
  });

  it('INSERT ... RETURNING specific columns', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("INSERT INTO ret_test VALUES (2, 'Bob', 88) RETURNING id, name");
    
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(parseInt(result.rows[0].id), 2);
    assert.strictEqual(result.rows[0].name, 'Bob');

    await client.end();
  });

  it('UPDATE ... RETURNING *', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("UPDATE ret_test SET score = 100 WHERE id = 1 RETURNING *");
    
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(parseInt(result.rows[0].score), 100);

    await client.end();
  });

  it('DELETE ... RETURNING *', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query("INSERT INTO ret_test VALUES (3, 'Charlie', 70)");
    const result = await client.query("DELETE FROM ret_test WHERE id = 3 RETURNING *");
    
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(result.rows[0].name, 'Charlie');

    // Verify it's actually deleted
    const check = await client.query('SELECT COUNT(*) AS cnt FROM ret_test WHERE id = 3');
    assert.strictEqual(parseInt(check.rows[0].cnt), 0);

    await client.end();
  });

  it('multi-row INSERT ... RETURNING', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query("INSERT INTO ret_test VALUES (10, 'X', 50) RETURNING id");
    await client.query("INSERT INTO ret_test VALUES (11, 'Y', 60) RETURNING id");
    
    // Verify both exist
    const result = await client.query('SELECT * FROM ret_test WHERE id >= 10 ORDER BY id');
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });
});
