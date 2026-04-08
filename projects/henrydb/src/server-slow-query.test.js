// server-slow-query.test.js — Tests for slow query logging
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15502;

describe('Slow Query Log', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT, slowQueryThresholdMs: 0 }); // Log ALL queries
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('logs executed queries', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE slow_test (id INTEGER, data TEXT)');
    await client.query("INSERT INTO slow_test VALUES (1, 'test')");
    await client.query('SELECT * FROM slow_test');

    const result = await client.query('SELECT * FROM pg_stat_slow_queries');
    assert.ok(result.rows.length >= 3, `Expected at least 3 slow query entries, got ${result.rows.length}`);

    // Most recent queries first
    assert.ok(result.rows[0].timestamp);
    assert.ok(result.rows[0].duration_ms >= 0);
    assert.ok(result.rows[0].query);

    await client.end();
  });

  it('includes query text and timing', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('SELECT 1 AS test_marker_12345');

    const result = await client.query('SELECT * FROM pg_stat_slow_queries');
    const markerQuery = result.rows.find(r => r.query?.includes('test_marker_12345'));
    assert.ok(markerQuery, 'Should find the marker query');
    assert.ok(markerQuery.duration_ms >= 0);
    assert.ok(markerQuery.pid > 0);

    await client.end();
  });

  it('pg_stat_server shows aggregate metrics', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT * FROM pg_stat_server');
    assert.strictEqual(result.rows.length, 1);
    assert.ok(result.rows[0].total_queries > 0, 'Should have total queries');
    assert.ok(result.rows[0].uptime_seconds >= 0);
    assert.ok(result.rows[0].active_connections >= 1);

    await client.end();
  });
});
