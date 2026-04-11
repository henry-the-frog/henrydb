// server-explain.test.js — Tests for HTTP EXPLAIN endpoint
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { HenryDBServer } from './server.js';

describe('HTTP EXPLAIN endpoint', () => {
  let server;
  const PORT = 15433;
  const HTTP_PORT = PORT + 1;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
    // Create test tables
    server.db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)');
    server.db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, status TEXT)');
    for (let i = 1; i <= 50; i++) {
      server.db.execute(`INSERT INTO users VALUES (${i}, 'user${i}', ${i <= 25 ? 1 : 0})`);
    }
    for (let i = 1; i <= 200; i++) {
      server.db.execute(`INSERT INTO orders VALUES (${i}, ${1 + i % 50}, ${(i * 9.99).toFixed(2)}, '${i % 4 === 0 ? "shipped" : "pending"}')`);
    }
  });

  after(async () => {
    if (server) await server.stop();
  });

  it('GET /explain returns the EXPLAIN UI page', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/explain`);
    assert.equal(res.status, 200);
    const html = await res.text();
    assert.ok(html.includes('HenryDB'));
    assert.ok(html.includes('EXPLAIN Visualizer'));
    assert.ok(html.includes('textarea'));
  });

  it('POST /explain returns HTML plan for SELECT', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/explain`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: 'SELECT * FROM users WHERE active = 1' }),
    });
    assert.equal(res.status, 200);
    const html = await res.text();
    assert.ok(html.includes('<svg'), 'Response should contain SVG');
    assert.ok(html.includes('Seq Scan'), 'Should show Seq Scan in plan');
  });

  it('POST /explain with JOIN query', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/explain`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: 'SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = 1' }),
    });
    assert.equal(res.status, 200);
    const html = await res.text();
    assert.ok(html.includes('<svg'));
    assert.ok(html.includes('Join') || html.includes('Loop'), 'Should show join in plan');
  });

  it('POST /explain with invalid SQL returns error', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/explain`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: 'NOT VALID SQL' }),
    });
    assert.equal(res.status, 400);
    const data = await res.json();
    assert.ok(data.error);
  });

  it('POST /explain without query returns error', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/explain`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    assert.equal(res.status, 400);
    const data = await res.json();
    assert.equal(data.error, 'Missing "query" field');
  });

  it('POST /query returns JSON result', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: 'SELECT COUNT(*) AS n FROM users WHERE active = 1' }),
    });
    assert.equal(res.status, 200);
    const data = await res.json();
    assert.ok(data.rows);
    assert.equal(data.rows[0].n, 25);
  });

  it('CORS headers present', async () => {
    const res = await fetch(`http://127.0.0.1:${HTTP_PORT}/explain`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: 'SELECT 1' }),
    });
    assert.equal(res.headers.get('access-control-allow-origin'), '*');
  });
});
