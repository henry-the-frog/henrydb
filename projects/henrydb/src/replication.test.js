// replication.test.js — Tests for streaming replication
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';
import { Database } from './db.js';
import { ReplicationPublisher, ReplicationSubscriber, REPLICATION_CHANNEL } from './replication.js';

const PRIMARY_PORT = 15494;

describe('Streaming Replication', () => {
  let primaryServer;
  let publisher;
  let replicaDb;
  let subscriber;

  before(async () => {
    // Start primary
    primaryServer = new HenryDBServer({ port: PRIMARY_PORT });
    await primaryServer.start();

    // Enable replication on primary
    publisher = new ReplicationPublisher(primaryServer);
    publisher.enable();

    // Create replica (in-memory, no server needed)
    replicaDb = new Database();

    // Connect replica to primary
    subscriber = new ReplicationSubscriber(replicaDb, {
      host: '127.0.0.1',
      port: PRIMARY_PORT,
      user: 'replica',
      database: 'test',
    });
    await subscriber.start();

    // Small delay for subscription to settle
    await new Promise(r => setTimeout(r, 100));
  });

  after(async () => {
    await subscriber.stop();
    await primaryServer.stop();
  });

  it('replicates CREATE TABLE', async () => {
    const client = new pg.Client({ host: '127.0.0.1', port: PRIMARY_PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE replicated (id INTEGER, name TEXT, value INTEGER)');
    await new Promise(r => setTimeout(r, 200));

    // Replica should have the table
    const result = replicaDb.execute('SELECT COUNT(*) AS cnt FROM replicated');
    assert.strictEqual(result.rows[0].cnt, 0);

    await client.end();
  });

  it('replicates INSERT operations', async () => {
    const client = new pg.Client({ host: '127.0.0.1', port: PRIMARY_PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query("INSERT INTO replicated VALUES (1, 'Alice', 100)");
    await client.query("INSERT INTO replicated VALUES (2, 'Bob', 200)");
    await client.query("INSERT INTO replicated VALUES (3, 'Charlie', 300)");
    await new Promise(r => setTimeout(r, 200));

    // Replica should have all rows
    const result = replicaDb.execute('SELECT * FROM replicated ORDER BY id');
    assert.strictEqual(result.rows.length, 3);
    assert.strictEqual(result.rows[0].name, 'Alice');
    assert.strictEqual(result.rows[2].name, 'Charlie');

    await client.end();
  });

  it('replicates UPDATE operations', async () => {
    const client = new pg.Client({ host: '127.0.0.1', port: PRIMARY_PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('UPDATE replicated SET value = 150 WHERE id = 1');
    await new Promise(r => setTimeout(r, 200));

    const result = replicaDb.execute('SELECT value FROM replicated WHERE id = 1');
    assert.strictEqual(result.rows[0].value, 150);

    await client.end();
  });

  it('replicates DELETE operations', async () => {
    const client = new pg.Client({ host: '127.0.0.1', port: PRIMARY_PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('DELETE FROM replicated WHERE id = 2');
    await new Promise(r => setTimeout(r, 200));

    const result = replicaDb.execute('SELECT COUNT(*) AS cnt FROM replicated');
    assert.strictEqual(result.rows[0].cnt, 2);

    await client.end();
  });

  it('publisher stats track operations', () => {
    const stats = publisher.getStats();
    assert.ok(stats.enabled);
    assert.ok(stats.seqNo > 0, `Expected seqNo > 0, got ${stats.seqNo}`);
    assert.ok(stats.replicaCount >= 1, `Expected at least 1 replica, got ${stats.replicaCount}`);
  });

  it('subscriber stats track replication', () => {
    assert.ok(subscriber.stats.operationsReceived > 0);
    assert.ok(subscriber.stats.operationsApplied > 0);
    assert.ok(subscriber.stats.lagMs >= 0);
  });

  it('queries on replica return same data as primary', async () => {
    const client = new pg.Client({ host: '127.0.0.1', port: PRIMARY_PORT, user: 'test', database: 'test' });
    await client.connect();

    // Query primary
    const primaryResult = await client.query('SELECT * FROM replicated ORDER BY id');

    // Query replica
    const replicaResult = replicaDb.execute('SELECT * FROM replicated ORDER BY id');

    assert.strictEqual(primaryResult.rows.length, replicaResult.rows.length);
    for (let i = 0; i < primaryResult.rows.length; i++) {
      assert.strictEqual(primaryResult.rows[i].id, replicaResult.rows[i].id);
      assert.strictEqual(primaryResult.rows[i].name, replicaResult.rows[i].name);
    }

    await client.end();
  });

  it('replicates multiple tables', async () => {
    const client = new pg.Client({ host: '127.0.0.1', port: PRIMARY_PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE orders (id INTEGER, product TEXT, qty INTEGER)');
    await client.query("INSERT INTO orders VALUES (1, 'Widget', 10)");
    await client.query("INSERT INTO orders VALUES (2, 'Gadget', 5)");
    await new Promise(r => setTimeout(r, 200));

    const result = replicaDb.execute('SELECT * FROM orders ORDER BY id');
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].product, 'Widget');

    await client.end();
  });

  it('replica can execute read queries independently', async () => {
    // Complex query on replica only
    const result = replicaDb.execute(`
      SELECT name, value FROM replicated 
      WHERE value > 100 
      ORDER BY value DESC
    `);
    assert.ok(result.rows.length > 0);
    // All returned values should be > 100
    for (const row of result.rows) {
      assert.ok(row.value > 100);
    }
  });
});
