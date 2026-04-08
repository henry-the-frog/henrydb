// server-sessions.test.js — Session management through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15531;

describe('Session Management', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('create and query sessions', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE sessions (id TEXT, user_id INTEGER, data TEXT, expires_at TEXT, created_at TEXT)');
    
    await client.query("INSERT INTO sessions VALUES ('sess_abc123', 1, '{\"cart\":[1,2,3]}', '2026-04-09T00:00:00Z', '2026-04-08T12:00:00Z')");
    await client.query("INSERT INTO sessions VALUES ('sess_def456', 2, '{\"cart\":[]}', '2026-04-09T00:00:00Z', '2026-04-08T13:00:00Z')");
    await client.query("INSERT INTO sessions VALUES ('sess_expired', 1, '{}', '2026-04-07T00:00:00Z', '2026-04-06T12:00:00Z')");

    // Lookup session
    const result = await client.query("SELECT * FROM sessions WHERE id = 'sess_abc123'");
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(parseInt(result.rows[0].user_id), 1);

    await client.end();
  });

  it('find active (non-expired) sessions', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT * FROM sessions WHERE expires_at > '2026-04-08T00:00:00Z'");
    assert.strictEqual(result.rows.length, 2); // Two active sessions

    await client.end();
  });

  it('update session data', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query("UPDATE sessions SET data = '{\"cart\":[1,2,3,4]}' WHERE id = 'sess_abc123'");
    
    const result = await client.query("SELECT data FROM sessions WHERE id = 'sess_abc123'");
    assert.ok(result.rows[0].data.includes('[1,2,3,4]'));

    await client.end();
  });

  it('cleanup expired sessions', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query("DELETE FROM sessions WHERE expires_at < '2026-04-08T00:00:00Z'");
    
    const result = await client.query('SELECT COUNT(*) AS cnt FROM sessions');
    assert.strictEqual(parseInt(result.rows[0].cnt), 2); // Only active sessions remain

    await client.end();
  });

  it('session count per user', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT user_id, COUNT(*) AS session_count FROM sessions GROUP BY user_id');
    assert.ok(result.rows.length >= 1);

    await client.end();
  });
});
