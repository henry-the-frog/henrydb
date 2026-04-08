// server-introspection.test.js — Tests for information_schema and catalog queries
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15495;

describe('Information Schema & Introspection', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    await client.query('CREATE TABLE users (id INTEGER, name TEXT, email TEXT)');
    await client.query('CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL, status TEXT)');
    await client.query('CREATE TABLE products (id INTEGER, name TEXT, price REAL, stock INTEGER)');
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('information_schema.tables lists all tables', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT * FROM information_schema.tables WHERE table_schema = 'public'");
    assert.ok(result.rows.length >= 3, `Expected at least 3 tables, got ${result.rows.length}`);

    const tableNames = result.rows.map(r => r.table_name);
    assert.ok(tableNames.includes('users'));
    assert.ok(tableNames.includes('orders'));
    assert.ok(tableNames.includes('products'));

    // Check columns
    assert.strictEqual(result.rows[0].table_schema, 'public');
    assert.strictEqual(result.rows[0].table_type, 'BASE TABLE');

    await client.end();
  });

  it('information_schema.columns lists columns for a table', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Use simple query (no params) to avoid extended protocol complications  
    const result = await client.query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'users'");
    
    const colNames = result.rows.map(r => r.column_name);
    assert.ok(colNames.includes('id'), `Missing 'id' column, got: ${colNames}`);
    assert.ok(colNames.includes('name'), `Missing 'name' column, got: ${colNames}`);
    assert.ok(colNames.includes('email'), `Missing 'email' column, got: ${colNames}`);

    await client.end();
  });

  it('information_schema.columns includes data types', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'orders'");
    
    const cols = {};
    for (const row of result.rows) {
      cols[row.column_name] = row.data_type;
    }
    assert.ok(cols.id, 'Expected id column');
    assert.ok(cols.amount, 'Expected amount column');

    await client.end();
  });

  it('information_schema shows new tables after creation', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Create a new table
    await client.query('CREATE TABLE dynamic_table (a INTEGER, b TEXT, c REAL)');

    // Should appear in information_schema
    const result = await client.query("SELECT * FROM information_schema.tables WHERE table_name = 'dynamic_table'");
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(result.rows[0].table_name, 'dynamic_table');

    // Columns too
    const cols = await client.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'dynamic_table'");
    const colNames = cols.rows.map(r => r.column_name);
    assert.ok(colNames.includes('a'));
    assert.ok(colNames.includes('b'));
    assert.ok(colNames.includes('c'));

    await client.end();
  });

  it('pg_catalog queries return empty (not error)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // These shouldn't throw
    const r1 = await client.query('SELECT * FROM pg_catalog.pg_type LIMIT 0');
    assert.strictEqual(r1.rows.length, 0);

    await client.end();
  });

  it('SHOW server_version works', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SHOW server_version');
    assert.strictEqual(result.rows.length, 1);
    assert.ok(result.rows[0].server_version.includes('15'));

    await client.end();
  });

  it('current_database() works', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT current_database()');
    assert.strictEqual(result.rows.length, 1);

    await client.end();
  });
});
