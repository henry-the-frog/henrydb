// server-cqrs.test.js — CQRS pattern demonstration
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15529;

describe('CQRS Pattern', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('command side: writes to event log', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Event sourcing: write side stores events
    await client.query('CREATE TABLE event_log (id INTEGER, aggregate_id TEXT, event_type TEXT, payload TEXT, version INTEGER, created_at TEXT)');
    
    // Commands produce events
    await client.query("INSERT INTO event_log VALUES (1, 'user-123', 'UserCreated', '{\"name\":\"Alice\"}', 1, '2026-04-08T10:00:00Z')");
    await client.query("INSERT INTO event_log VALUES (2, 'user-123', 'EmailChanged', '{\"email\":\"alice@new.com\"}', 2, '2026-04-08T11:00:00Z')");
    await client.query("INSERT INTO event_log VALUES (3, 'user-456', 'UserCreated', '{\"name\":\"Bob\"}', 1, '2026-04-08T12:00:00Z')");
    await client.query("INSERT INTO event_log VALUES (4, 'user-123', 'ProfileUpdated', '{\"bio\":\"Hello!\"}', 3, '2026-04-08T13:00:00Z')");

    const result = await client.query("SELECT COUNT(*) AS cnt FROM event_log");
    assert.strictEqual(parseInt(result.rows[0].cnt), 4);

    await client.end();
  });

  it('query side: materialized read model', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Read model: denormalized view for queries
    await client.query('CREATE TABLE user_profiles (aggregate_id TEXT, name TEXT, email TEXT, bio TEXT, version INTEGER)');
    
    // Build read model from events (projection)
    await client.query("INSERT INTO user_profiles VALUES ('user-123', 'Alice', 'alice@new.com', 'Hello!', 3)");
    await client.query("INSERT INTO user_profiles VALUES ('user-456', 'Bob', NULL, NULL, 1)");

    // Query the read model
    const result = await client.query('SELECT * FROM user_profiles ORDER BY name');
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].name, 'Alice');
    assert.strictEqual(result.rows[0].email, 'alice@new.com');

    await client.end();
  });

  it('event replay: rebuild read model from events', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Get all events for an aggregate in order
    const events = await client.query(
      "SELECT event_type, payload FROM event_log WHERE aggregate_id = 'user-123' ORDER BY version"
    );
    assert.strictEqual(events.rows.length, 3);
    assert.strictEqual(events.rows[0].event_type, 'UserCreated');
    assert.strictEqual(events.rows[1].event_type, 'EmailChanged');
    assert.strictEqual(events.rows[2].event_type, 'ProfileUpdated');

    await client.end();
  });

  it('aggregate event count', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT aggregate_id, COUNT(*) AS event_count, MAX(version) AS latest_version FROM event_log GROUP BY aggregate_id ORDER BY event_count DESC'
    );
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].aggregate_id, 'user-123');
    assert.strictEqual(parseInt(result.rows[0].event_count), 3);

    await client.end();
  });

  it('event timeline', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT event_type, aggregate_id, created_at FROM event_log ORDER BY created_at'
    );
    assert.strictEqual(result.rows.length, 4);
    // First event should be earliest
    assert.ok(result.rows[0].created_at <= result.rows[3].created_at);

    await client.end();
  });
});
