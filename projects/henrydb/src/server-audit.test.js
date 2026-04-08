// server-audit.test.js — Audit trail and logging patterns
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15535;

describe('Audit Trail & Logging', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('create and query audit log', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE audit_log (id INTEGER, user_id INTEGER, action TEXT, resource TEXT, resource_id INTEGER, details TEXT, ip TEXT, ts TEXT)');
    
    // Log various actions
    await client.query("INSERT INTO audit_log VALUES (1, 1, 'LOGIN', 'session', NULL, '{\"method\":\"password\"}', '192.168.1.10', '2026-04-08T10:00:00Z')");
    await client.query("INSERT INTO audit_log VALUES (2, 1, 'CREATE', 'document', 42, '{\"title\":\"Q1 Report\"}', '192.168.1.10', '2026-04-08T10:05:00Z')");
    await client.query("INSERT INTO audit_log VALUES (3, 2, 'LOGIN', 'session', NULL, '{\"method\":\"sso\"}', '10.0.0.5', '2026-04-08T10:10:00Z')");
    await client.query("INSERT INTO audit_log VALUES (4, 1, 'UPDATE', 'document', 42, '{\"field\":\"status\",\"old\":\"draft\",\"new\":\"published\"}', '192.168.1.10', '2026-04-08T10:15:00Z')");
    await client.query("INSERT INTO audit_log VALUES (5, 2, 'DELETE', 'comment', 7, '{}', '10.0.0.5', '2026-04-08T10:20:00Z')");
    await client.query("INSERT INTO audit_log VALUES (6, 1, 'LOGOUT', 'session', NULL, '{}', '192.168.1.10', '2026-04-08T12:00:00Z')");

    const result = await client.query('SELECT COUNT(*) AS cnt FROM audit_log');
    assert.strictEqual(parseInt(result.rows[0].cnt), 6);

    await client.end();
  });

  it('user activity timeline', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT action, resource, ts FROM audit_log WHERE user_id = 1 ORDER BY ts");
    assert.strictEqual(result.rows.length, 4);
    assert.strictEqual(result.rows[0].action, 'LOGIN');
    assert.strictEqual(result.rows[3].action, 'LOGOUT');

    await client.end();
  });

  it('action frequency', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT action, COUNT(*) AS cnt FROM audit_log GROUP BY action ORDER BY cnt DESC');
    assert.ok(result.rows.length >= 4);

    await client.end();
  });

  it('resource change history', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT action, details, ts FROM audit_log WHERE resource = 'document' AND resource_id = 42 ORDER BY ts"
    );
    assert.strictEqual(result.rows.length, 2); // CREATE and UPDATE

    await client.end();
  });

  it('security events from IP', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT user_id, action, ts FROM audit_log WHERE ip = '10.0.0.5' ORDER BY ts"
    );
    assert.strictEqual(result.rows.length, 2); // User 2's actions

    await client.end();
  });
});
