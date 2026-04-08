// server-alter.test.js — Tests for ALTER TABLE through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15510;

describe('ALTER TABLE via Wire Protocol', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('ALTER TABLE ADD COLUMN', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE alter_test (id INTEGER, name TEXT)');
    await client.query("INSERT INTO alter_test VALUES (1, 'alice')");

    // Add a column
    await client.query('ALTER TABLE alter_test ADD COLUMN age INTEGER');
    
    // Insert with new column
    await client.query("INSERT INTO alter_test VALUES (2, 'bob', 25)");
    
    const result = await client.query('SELECT * FROM alter_test WHERE id = 2');
    assert.strictEqual(result.rows.length, 1);

    await client.end();
  });

  it('ALTER TABLE DROP COLUMN', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE drop_col (id INTEGER, name TEXT, extra TEXT)');
    await client.query("INSERT INTO drop_col VALUES (1, 'test', 'removed')");
    
    await client.query('ALTER TABLE drop_col DROP COLUMN extra');
    
    const result = await client.query('SELECT * FROM drop_col');
    assert.strictEqual(result.rows.length, 1);
    // extra column should not appear
    assert.ok(!('extra' in result.rows[0]) || result.rows[0].extra === null || result.rows[0].extra === undefined);

    await client.end();
  });

  it('ALTER TABLE RENAME TO', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE old_name (id INTEGER)');
    await client.query('INSERT INTO old_name VALUES (1)');
    
    await client.query('ALTER TABLE old_name RENAME TO new_name');
    
    const result = await client.query('SELECT * FROM new_name');
    assert.strictEqual(result.rows.length, 1);

    await client.end();
  });

  it('ALTER TABLE RENAME COLUMN', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE rename_col (id INTEGER, old_name TEXT)');
    await client.query("INSERT INTO rename_col VALUES (1, 'test')");
    
    await client.query('ALTER TABLE rename_col RENAME COLUMN old_name TO new_name');
    
    const result = await client.query('SELECT new_name FROM rename_col');
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(result.rows[0].new_name, 'test');

    await client.end();
  });

  it('DROP TABLE works', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE to_drop (id INTEGER)');
    await client.query('INSERT INTO to_drop VALUES (1)');
    
    await client.query('DROP TABLE to_drop');
    
    try {
      await client.query('SELECT * FROM to_drop');
      assert.fail('Should fail — table was dropped');
    } catch (e) {
      assert.ok(e.message.includes('not found') || e.message.includes('to_drop'));
    }

    await client.end();
  });

  it('DROP TABLE IF EXISTS (no error for missing table)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Should not error even if table doesn't exist
    await client.query('DROP TABLE IF EXISTS nonexistent_table');

    await client.end();
  });
});
