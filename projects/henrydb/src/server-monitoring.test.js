// server-monitoring.test.js — Tests for pg_stat_activity and monitoring
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15496;

describe('Server Monitoring', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('pg_stat_activity shows current connection', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT * FROM pg_stat_activity');
    assert.ok(result.rows.length >= 1, 'Expected at least 1 active connection');

    // Current connection should be querying pg_stat_activity
    const myConn = result.rows.find(r => r.query?.includes('pg_stat_activity'));
    assert.ok(myConn, 'Should see own query in pg_stat_activity');
    assert.ok(myConn.pid > 0, 'Should have a PID');
    assert.strictEqual(myConn.datname, 'henrydb');

    await client.end();
  });

  it('pg_stat_activity shows multiple connections', async () => {
    const c1 = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const c2 = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await c1.connect();
    await c2.connect();

    const result = await c1.query('SELECT * FROM pg_stat_activity');
    assert.ok(result.rows.length >= 2, `Expected at least 2 connections, got ${result.rows.length}`);

    await c1.end();
    await c2.end();
  });

  it('pg_stat_activity tracks query count', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Run several queries
    await client.query('SELECT 1');
    await client.query('SELECT 2');
    await client.query('SELECT 3');

    const result = await client.query('SELECT query_count FROM pg_stat_activity ORDER BY pid DESC LIMIT 1');
    const queryCount = parseInt(result.rows[0]?.query_count);
    assert.ok(queryCount >= 4, `Expected query_count >= 4, got ${queryCount}`);

    await client.end();
  });

  it('pg_stat_activity shows backend_start', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT backend_start FROM pg_stat_activity LIMIT 1');
    assert.ok(result.rows[0].backend_start, 'Expected backend_start timestamp');

    await client.end();
  });

  it('pg_stat_user_tables shows table stats', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE mon_test (id INTEGER, val TEXT)');
    await client.query("INSERT INTO mon_test VALUES (1, 'a')");
    await client.query("INSERT INTO mon_test VALUES (2, 'b')");

    const result = await client.query('SELECT * FROM pg_stat_user_tables');
    assert.ok(result.rows.length >= 1);

    const monTable = result.rows.find(r => r.relname === 'mon_test');
    assert.ok(monTable, 'Expected mon_test in pg_stat_user_tables');
    assert.strictEqual(monTable.schemaname, 'public');

    await client.end();
  });

});
