// server.test.js — Tests for HenryDB HTTP server
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { HenryDBServer } from './server.js';

describe('HenryDB HTTP Server', () => {
  let server;
  let baseUrl;

  before(async () => {
    server = new HenryDBServer();
    const port = await server.start();
    baseUrl = `http://localhost:${port}`;
  });

  after(async () => {
    await server.stop();
  });

  it('health check', async () => {
    const res = await fetch(`${baseUrl}/health`);
    assert.equal(res.status, 200);
    const data = await res.json();
    assert.equal(data.status, 'ok');
    assert.ok(data.uptime >= 0);
  });

  it('POST /sql — CREATE TABLE', async () => {
    const res = await fetch(`${baseUrl}/sql`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)' }),
    });
    assert.equal(res.status, 200);
    const data = await res.json();
    assert.ok(data.success);
  });

  it('POST /sql — INSERT', async () => {
    const res = await fetch(`${baseUrl}/sql`, {
      method: 'POST',
      body: JSON.stringify({ sql: "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 35)" }),
    });
    const data = await res.json();
    assert.ok(data.success);
    assert.equal(data.result.count, 3);
  });

  it('POST /sql — SELECT', async () => {
    const res = await fetch(`${baseUrl}/sql`, {
      method: 'POST',
      body: JSON.stringify({ sql: 'SELECT * FROM users WHERE age > 28 ORDER BY name' }),
    });
    const data = await res.json();
    assert.ok(data.success);
    assert.equal(data.result.rows.length, 2);
    assert.equal(data.result.rows[0].name, 'Alice');
    assert.equal(data.result.rows[1].name, 'Carol');
    assert.ok(data.timing.ms >= 0);
  });

  it('POST /sql — SQL error returns 400', async () => {
    const res = await fetch(`${baseUrl}/sql`, {
      method: 'POST',
      body: JSON.stringify({ sql: 'SELECT * FROM nonexistent' }),
    });
    assert.equal(res.status, 400);
    const data = await res.json();
    assert.ok(!data.success);
    assert.ok(data.error.length > 0);
  });

  it('POST /sql — invalid body returns 400', async () => {
    const res = await fetch(`${baseUrl}/sql`, {
      method: 'POST',
      body: 'not json',
    });
    assert.equal(res.status, 400);
  });

  it('POST /sql — missing sql field returns 400', async () => {
    const res = await fetch(`${baseUrl}/sql`, {
      method: 'POST',
      body: JSON.stringify({ query: 'SELECT 1' }),
    });
    assert.equal(res.status, 400);
    const data = await res.json();
    assert.ok(data.error.includes('Missing'));
  });

  it('GET /tables — lists tables with columns', async () => {
    const res = await fetch(`${baseUrl}/tables`);
    const data = await res.json();
    assert.ok(data.tables.length >= 1);
    const users = data.tables.find(t => t.name === 'users');
    assert.ok(users);
    assert.equal(users.columns.length, 3);
    assert.equal(users.columns[0].name, 'id');
  });

  it('GET /stats — returns database stats', async () => {
    const res = await fetch(`${baseUrl}/stats`);
    const data = await res.json();
    assert.ok(data.tables >= 1);
    assert.ok(data.requests > 0);
    assert.ok(data.uptime >= 0);
  });

  it('404 for unknown paths', async () => {
    const res = await fetch(`${baseUrl}/unknown`);
    assert.equal(res.status, 404);
    const data = await res.json();
    assert.ok(data.endpoints);
  });

  it('CORS headers present', async () => {
    const res = await fetch(`${baseUrl}/health`);
    assert.equal(res.headers.get('Access-Control-Allow-Origin'), '*');
  });

  it('complex query via API', async () => {
    // Create another table
    await fetch(`${baseUrl}/sql`, {
      method: 'POST',
      body: JSON.stringify({ sql: 'CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)' }),
    });
    await fetch(`${baseUrl}/sql`, {
      method: 'POST',
      body: JSON.stringify({ sql: "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)" }),
    });
    
    const res = await fetch(`${baseUrl}/sql`, {
      method: 'POST',
      body: JSON.stringify({ 
        sql: 'SELECT u.name, SUM(o.amount) as total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY total DESC' 
      }),
    });
    const data = await res.json();
    assert.ok(data.success);
    assert.equal(data.result.rows[0].name, 'Alice');
    assert.equal(data.result.rows[0].total, 300);
  });
});
