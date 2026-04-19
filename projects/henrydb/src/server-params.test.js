// server-params.test.js — Tests for SET/SHOW runtime parameters
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15497;

describe('Runtime Parameters (SET/SHOW)', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('SHOW server_version', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SHOW server_version');
    assert.ok(result.rows[0].server_version.startsWith('15.0'));

    await client.end();
  });

  it('SET and SHOW custom parameter', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query("SET work_mem = '16MB'");
    const result = await client.query('SHOW work_mem');
    assert.strictEqual(result.rows[0].work_mem, '16MB');

    await client.end();
  });

  it('SET statement_timeout', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('SET statement_timeout = 5000');
    const result = await client.query('SHOW statement_timeout');
    assert.strictEqual(result.rows[0].statement_timeout, '5000');

    await client.end();
  });

  it('SET with TO syntax', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query("SET search_path TO 'myschema, public'");
    const result = await client.query('SHOW search_path');
    assert.ok(result.rows[0].search_path.includes('myschema'));

    await client.end();
  });

  it('parameters are connection-scoped', async () => {
    const c1 = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const c2 = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await c1.connect();
    await c2.connect();

    // Set different values on different connections
    await c1.query("SET work_mem = '8MB'");
    await c2.query("SET work_mem = '32MB'");

    const r1 = await c1.query('SHOW work_mem');
    const r2 = await c2.query('SHOW work_mem');

    assert.strictEqual(r1.rows[0].work_mem, '8MB');
    assert.strictEqual(r2.rows[0].work_mem, '32MB');

    await c1.end();
    await c2.end();
  });

  it('SHOW ALL returns all parameters', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SHOW ALL');
    assert.ok(result.rows.length >= 10, `Expected at least 10 params, got ${result.rows.length}`);

    const paramNames = result.rows.map(r => r.name);
    assert.ok(paramNames.includes('server_version'));
    assert.ok(paramNames.includes('work_mem'));
    assert.ok(paramNames.includes('statement_timeout'));

    await client.end();
  });

  it('SET application_name', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query("SET application_name = 'my_app'");
    const result = await client.query('SHOW application_name');
    assert.strictEqual(result.rows[0].application_name, 'my_app');

    await client.end();
  });

  it('RESET is accepted', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Should not throw
    await client.query('RESET work_mem');
    await client.query('RESET ALL');

    await client.end();
  });
});
