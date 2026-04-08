// server-health.test.js — Tests for HTTP health check endpoint
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { HenryDBServer } from './server.js';

const PORT = 15504;

describe('HTTP Health Check', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
    await new Promise(r => setTimeout(r, 100)); // Let health server start
  });

  after(async () => {
    await server.stop();
  });

  it('/health returns JSON status', async () => {
    const res = await fetch(`http://127.0.0.1:${PORT + 1}/health`);
    assert.strictEqual(res.status, 200);
    
    const data = await res.json();
    assert.strictEqual(data.status, 'healthy');
    assert.ok(data.version.includes('HenryDB'));
    assert.ok(typeof data.uptime_seconds === 'number');
    assert.ok(typeof data.connections.active === 'number');
    assert.ok(typeof data.queries.total === 'number');
    assert.ok(typeof data.cache.entries === 'number');
    assert.ok(typeof data.tables === 'number');
  });

  it('/metrics returns Prometheus format', async () => {
    const res = await fetch(`http://127.0.0.1:${PORT + 1}/metrics`);
    assert.strictEqual(res.status, 200);
    
    const text = await res.text();
    assert.ok(text.includes('henrydb_uptime_seconds'));
    assert.ok(text.includes('henrydb_connections_active'));
    assert.ok(text.includes('henrydb_queries_total'));
    assert.ok(text.includes('henrydb_cache_hits'));
  });

  it('health endpoint updates after queries', async () => {
    // Connect and run some queries
    const pg = await import('pg');
    const client = new pg.default.Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    await client.query('SELECT 1');
    await client.query('SELECT 2');

    const res = await fetch(`http://127.0.0.1:${PORT + 1}/health`);
    const data = await res.json();
    assert.ok(data.queries.total >= 2, `Expected at least 2 queries, got ${data.queries.total}`);
    assert.ok(data.connections.active >= 1);

    await client.end();
  });
});
