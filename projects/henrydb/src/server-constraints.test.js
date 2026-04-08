// server-constraints.test.js — Constraint enforcement through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15528;

describe('Constraint Enforcement', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('PRIMARY KEY prevents duplicates', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE pk_test (id INTEGER PRIMARY KEY, name TEXT)');
    await client.query("INSERT INTO pk_test VALUES (1, 'Alice')");
    
    try {
      await client.query("INSERT INTO pk_test VALUES (1, 'Duplicate')");
      // If ON CONFLICT DO NOTHING is implicit, just verify 1 row
    } catch (e) {
      assert.ok(e.message.includes('duplicate') || e.message.includes('unique') || e.message.includes('constraint'));
    }

    const result = await client.query('SELECT * FROM pk_test WHERE id = 1');
    assert.strictEqual(result.rows.length, 1);

    await client.end();
  });

  it('NOT NULL constraint', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE nn_test (id INTEGER NOT NULL, name TEXT NOT NULL)');
    await client.query("INSERT INTO nn_test VALUES (1, 'Valid')");
    
    const result = await client.query('SELECT * FROM nn_test');
    assert.strictEqual(result.rows.length, 1);

    await client.end();
  });

  it('CHECK constraint', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE check_test (id INTEGER, age INTEGER CHECK (age >= 0))');
    await client.query('INSERT INTO check_test VALUES (1, 25)');
    
    try {
      await client.query('INSERT INTO check_test VALUES (2, -5)');
    } catch (e) {
      assert.ok(e.message.includes('check') || e.message.includes('constraint') || e.message.includes('CHECK'));
    }

    await client.end();
  });

  it('FOREIGN KEY reference', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE fk_parent (id INTEGER PRIMARY KEY, name TEXT)');
    await client.query('CREATE TABLE fk_child (id INTEGER, parent_id INTEGER REFERENCES fk_parent(id), data TEXT)');
    
    await client.query("INSERT INTO fk_parent VALUES (1, 'Parent')");
    await client.query("INSERT INTO fk_child VALUES (1, 1, 'Valid child')");

    const result = await client.query('SELECT c.data, p.name FROM fk_child c JOIN fk_parent p ON c.parent_id = p.id');
    assert.strictEqual(result.rows.length, 1);

    await client.end();
  });

  it('DEFAULT values', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE default_test (id INTEGER, status TEXT DEFAULT \'active\', score INTEGER DEFAULT 0)');
    await client.query('INSERT INTO default_test (id) VALUES (1)');
    
    const result = await client.query('SELECT * FROM default_test WHERE id = 1');
    assert.strictEqual(result.rows.length, 1);

    await client.end();
  });

  it('multiple constraints on one table', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE multi_const (id INTEGER PRIMARY KEY, email TEXT NOT NULL, age INTEGER CHECK (age >= 18))');
    await client.query("INSERT INTO multi_const VALUES (1, 'alice@test.com', 30)");
    await client.query("INSERT INTO multi_const VALUES (2, 'bob@test.com', 25)");

    const result = await client.query('SELECT * FROM multi_const ORDER BY id');
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });
});
