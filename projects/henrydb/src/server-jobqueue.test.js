// server-jobqueue.test.js — Job queue pattern through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15532;

describe('Job Queue Pattern', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('enqueue and dequeue jobs', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE job_queue (id INTEGER, type TEXT, payload TEXT, status TEXT, priority INTEGER, created_at TEXT, started_at TEXT, completed_at TEXT)');
    
    // Enqueue jobs
    await client.query("INSERT INTO job_queue VALUES (1, 'email', '{\"to\":\"alice@test.com\"}', 'pending', 1, '2026-04-08T10:00:00Z', NULL, NULL)");
    await client.query("INSERT INTO job_queue VALUES (2, 'email', '{\"to\":\"bob@test.com\"}', 'pending', 2, '2026-04-08T10:01:00Z', NULL, NULL)");
    await client.query("INSERT INTO job_queue VALUES (3, 'report', '{\"type\":\"monthly\"}', 'pending', 1, '2026-04-08T10:02:00Z', NULL, NULL)");

    // Dequeue: get highest priority pending job
    const result = await client.query("SELECT * FROM job_queue WHERE status = 'pending' ORDER BY priority DESC, created_at");
    assert.ok(result.rows.length >= 2);
    assert.strictEqual(parseInt(result.rows[0].priority), 2); // Highest priority first

    await client.end();
  });

  it('claim and process a job', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Claim job 2 (highest priority)
    await client.query("UPDATE job_queue SET status = 'processing', started_at = '2026-04-08T10:05:00Z' WHERE id = 2");
    
    // Complete it
    await client.query("UPDATE job_queue SET status = 'completed', completed_at = '2026-04-08T10:05:30Z' WHERE id = 2");

    const result = await client.query("SELECT * FROM job_queue WHERE id = 2");
    assert.strictEqual(result.rows[0].status, 'completed');

    await client.end();
  });

  it('job status summary', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT status, COUNT(*) AS cnt FROM job_queue GROUP BY status ORDER BY cnt DESC');
    assert.ok(result.rows.length >= 2);

    await client.end();
  });

  it('retry failed jobs', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Fail job 1
    await client.query("UPDATE job_queue SET status = 'failed' WHERE id = 1");
    
    // Retry: reset to pending
    await client.query("UPDATE job_queue SET status = 'pending', started_at = NULL WHERE status = 'failed'");
    
    const result = await client.query("SELECT * FROM job_queue WHERE id = 1");
    assert.strictEqual(result.rows[0].status, 'pending');

    await client.end();
  });

  it('jobs by type', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT type, COUNT(*) AS cnt FROM job_queue GROUP BY type");
    assert.ok(result.rows.length >= 2); // email and report

    await client.end();
  });
});
