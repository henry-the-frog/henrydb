// server-docstore.test.js — Document store patterns (JSON CRUD through wire protocol)
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15517;

describe('Document Store (JSON)', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    // Documents table: key-value with JSON documents
    await client.query('CREATE TABLE documents (id INTEGER, collection TEXT, doc TEXT)');
    
    // Insert documents as JSON strings
    await client.query(`INSERT INTO documents VALUES (1, 'users', '{"name":"Alice","age":30,"tags":["admin","active"]}')`);
    await client.query(`INSERT INTO documents VALUES (2, 'users', '{"name":"Bob","age":25,"tags":["user"]}')`);
    await client.query(`INSERT INTO documents VALUES (3, 'users', '{"name":"Charlie","age":35,"tags":["admin"]}')`);
    await client.query(`INSERT INTO documents VALUES (4, 'products', '{"name":"Laptop","price":999,"specs":{"ram":"16GB","ssd":"512GB"}}')`);
    await client.query(`INSERT INTO documents VALUES (5, 'products', '{"name":"Phone","price":699,"specs":{"ram":"8GB","storage":"256GB"}}')`);
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('query documents by collection', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT * FROM documents WHERE collection = 'users'");
    assert.strictEqual(result.rows.length, 3);
    
    // Parse JSON
    for (const row of result.rows) {
      const doc = JSON.parse(row.doc);
      assert.ok(doc.name);
      assert.ok(doc.age !== undefined);
    }

    await client.end();
  });

  it('extract JSON fields with JSON_EXTRACT', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT JSON_EXTRACT(doc, '$.name') AS name FROM documents WHERE collection = 'users'"
    );
    assert.strictEqual(result.rows.length, 3);
    const names = result.rows.map(r => r.name);
    assert.ok(names.includes('Alice') || names.includes('"Alice"'));

    await client.end();
  });

  it('filter by JSON field value', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Filter where price > 700
    const result = await client.query(
      "SELECT JSON_EXTRACT(doc, '$.name') AS name, JSON_EXTRACT(doc, '$.price') AS price FROM documents WHERE collection = 'products'"
    );
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });

  it('nested JSON extraction', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT JSON_EXTRACT(doc, '$.specs.ram') AS ram FROM documents WHERE collection = 'products'"
    );
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });

  it('insert new document and query back', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query(`INSERT INTO documents VALUES (6, 'events', '{"type":"click","page":"/home","ts":"2026-04-08T12:00:00Z"}')`);
    
    const result = await client.query("SELECT doc FROM documents WHERE collection = 'events'");
    assert.strictEqual(result.rows.length, 1);
    const event = JSON.parse(result.rows[0].doc);
    assert.strictEqual(event.type, 'click');
    assert.strictEqual(event.page, '/home');

    await client.end();
  });

  it('aggregate over JSON documents', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT collection, COUNT(*) AS count FROM documents GROUP BY collection ORDER BY count DESC'
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });
});
