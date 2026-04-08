// server-auth.test.js — Tests for MD5 password authentication
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15500;

describe('MD5 Authentication', () => {
  let server;

  before(async () => {
    const users = new Map([
      ['admin', { password: 'secret123' }],
      ['readonly', { password: 'readpass' }],
    ]);
    server = new HenryDBServer({ port: PORT, users });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('accepts valid credentials', async () => {
    const client = new Client({
      host: '127.0.0.1', port: PORT,
      user: 'admin', password: 'secret123',
      database: 'test',
    });
    await client.connect();

    const result = await client.query('SELECT 1 AS num');
    assert.strictEqual(result.rows[0].num, 1);

    await client.end();
  });

  it('accepts second valid user', async () => {
    const client = new Client({
      host: '127.0.0.1', port: PORT,
      user: 'readonly', password: 'readpass',
      database: 'test',
    });
    await client.connect();

    const result = await client.query('SELECT 1 AS alive');
    assert.strictEqual(result.rows[0].alive, 1);

    await client.end();
  });

  it('rejects wrong password', async () => {
    const client = new Client({
      host: '127.0.0.1', port: PORT,
      user: 'admin', password: 'wrong',
      database: 'test',
    });

    try {
      await client.connect();
      assert.fail('Should have rejected wrong password');
    } catch (e) {
      assert.ok(e.message.includes('authentication failed') || e.message.includes('password'), 
        `Expected auth error, got: ${e.message}`);
    }
  });

  it('rejects unknown user', async () => {
    const client = new Client({
      host: '127.0.0.1', port: PORT,
      user: 'hacker', password: 'anything',
      database: 'test',
    });

    try {
      await client.connect();
      assert.fail('Should have rejected unknown user');
    } catch (e) {
      assert.ok(e.message.includes('authentication failed') || e.message.includes('password'),
        `Expected auth error, got: ${e.message}`);
    }
  });

  it('authenticated connection works for full SQL', async () => {
    const client = new Client({
      host: '127.0.0.1', port: PORT,
      user: 'admin', password: 'secret123',
      database: 'test',
    });
    await client.connect();

    await client.query('CREATE TABLE auth_test (id INTEGER, val TEXT)');
    await client.query("INSERT INTO auth_test VALUES (1, 'secure')");
    const result = await client.query('SELECT * FROM auth_test');
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(result.rows[0].val, 'secure');

    await client.end();
  });

  it('multiple authenticated connections work', async () => {
    const c1 = new Client({ host: '127.0.0.1', port: PORT, user: 'admin', password: 'secret123', database: 'test' });
    const c2 = new Client({ host: '127.0.0.1', port: PORT, user: 'readonly', password: 'readpass', database: 'test' });
    await c1.connect();
    await c2.connect();

    const r1 = await c1.query('SELECT 1 AS a');
    const r2 = await c2.query('SELECT 2 AS b');
    assert.strictEqual(r1.rows[0].a, 1);
    assert.strictEqual(r2.rows[0].b, 2);

    await c1.end();
    await c2.end();
  });
});

describe('No Authentication (default)', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT + 1 });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('connects without password when no users configured', async () => {
    const client = new Client({
      host: '127.0.0.1', port: PORT + 1,
      user: 'anyone', database: 'test',
    });
    await client.connect();

    const result = await client.query('SELECT 1 AS ok');
    assert.strictEqual(result.rows[0].ok, 1);

    await client.end();
  });
});
