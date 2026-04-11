// server-dashboard.test.js — Tests for the performance dashboard endpoint
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { HenryDBServer } from './server.js';

describe('HTTP /dashboard endpoint', () => {
  let server;
  const PORT = 15434;
  const HTTP_PORT = PORT + 1;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
    server.db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)');
    server.db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, status TEXT)');
    for (let i = 1; i <= 50; i++) {
      server.db.execute(`INSERT INTO users VALUES (${i}, 'user${i}', ${i % 2})`);
    }
    for (let i = 1; i <= 200; i++) {
      server.db.execute(`INSERT INTO orders VALUES (${i}, ${1 + i % 50}, ${(i * 9.99).toFixed(2)}, '${i % 3 === 0 ? "shipped" : "pending"}')`);
    }
    // Run some queries to build workload
    for (let i = 0; i < 5; i++) {
      server.db.execute("SELECT * FROM orders WHERE status = 'shipped'");
    }
  });

  after(async () => {
    if (server) await server.stop();
  });

  it('returns HTML dashboard', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/dashboard`);
    assert.equal(res.status, 200);
    assert.ok(res.headers.get('content-type').includes('text/html'));
    const html = await res.text();
    assert.ok(html.includes('HenryDB'));
    assert.ok(html.includes('Performance Dashboard'));
  });

  it('shows table information', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/dashboard`);
    const html = await res.text();
    assert.ok(html.includes('users'), 'Should list users table');
    assert.ok(html.includes('orders'), 'Should list orders table');
  });

  it('shows plan cache stats', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/dashboard`);
    const html = await res.text();
    assert.ok(html.includes('Plan Cache'));
    assert.ok(html.includes('Hit rate'));
  });

  it('shows index recommendations', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/dashboard`);
    const html = await res.text();
    assert.ok(html.includes('Index Recommendations'));
    // After running queries, should have some recommendations
    assert.ok(html.includes('CREATE INDEX') || html.includes('No recommendations'));
  });

  it('shows slow queries section', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/dashboard`);
    const html = await res.text();
    assert.ok(html.includes('Slowest Queries'));
    // After running queries in setup, should have query stats
    assert.ok(html.includes('total calls') || html.includes('No query statistics'));
  });

  it('has navigation links', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/dashboard`);
    const html = await res.text();
    assert.ok(html.includes('/explain'), 'Should link to EXPLAIN visualizer');
    assert.ok(html.includes('/health'), 'Should link to health check');
    assert.ok(html.includes('/metrics'), 'Should link to metrics');
  });
});
