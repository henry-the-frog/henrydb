// server-json.test.js — Tests for JSON support through the wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15498;

describe('JSON via Server', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('stores and retrieves JSON data', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE json_test (id INTEGER, data TEXT)');
    await client.query(`INSERT INTO json_test VALUES (1, '{"name": "Alice", "age": 30}')`);
    await client.query(`INSERT INTO json_test VALUES (2, '{"name": "Bob", "age": 25}')`);

    const result = await client.query('SELECT * FROM json_test ORDER BY id');
    assert.strictEqual(result.rows.length, 2);
    
    // Parse JSON from the text column
    const data1 = JSON.parse(result.rows[0].data);
    assert.strictEqual(data1.name, 'Alice');
    assert.strictEqual(data1.age, 30);

    await client.end();
  });

  it('JSON_EXTRACT through wire protocol', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT JSON_EXTRACT(data, '$.name') AS name FROM json_test WHERE id = 1");
    assert.strictEqual(result.rows[0].name, 'Alice');

    await client.end();
  });

  it('JSON_EXTRACT with nested paths', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query(`INSERT INTO json_test VALUES (3, '{"user": {"name": "Charlie", "address": {"city": "Denver"}}}')`);

    const result = await client.query("SELECT JSON_EXTRACT(data, '$.user.address.city') AS city FROM json_test WHERE id = 3");
    assert.strictEqual(result.rows[0].city, 'Denver');

    await client.end();
  });

  it('JSON data in parameterized queries', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const jsonData = JSON.stringify({ name: 'Diana', age: 28, tags: ['dev', 'design'] });
    await client.query('INSERT INTO json_test VALUES ($1, $2)', [4, jsonData]);

    const result = await client.query('SELECT data FROM json_test WHERE id = $1', [4]);
    const parsed = JSON.parse(result.rows[0].data);
    assert.strictEqual(parsed.name, 'Diana');
    assert.deepStrictEqual(parsed.tags, ['dev', 'design']);

    await client.end();
  });

  it('JSON in WHERE clause via JSON_EXTRACT', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT id FROM json_test WHERE JSON_EXTRACT(data, '$.age') > 26 ORDER BY id");
    assert.ok(result.rows.length >= 1);
    // Alice (30) and Diana (28) should qualify
    const ids = result.rows.map(r => r.id);
    assert.ok(ids.includes(1)); // Alice

    await client.end();
  });

  it('aggregation with JSON data', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT COUNT(*) AS cnt FROM json_test');
    assert.ok(parseInt(result.rows[0].cnt) >= 4);

    await client.end();
  });
});
