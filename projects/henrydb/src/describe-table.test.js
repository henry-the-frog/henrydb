// describe-table.test.js — Tests for \d tablename support
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;

describe('\\d tablename (DESCRIBE)', () => {
  let server, port, c;
  
  before(async () => {
    port = 34400 + Math.floor(Math.random() * 2000);
    server = new HenryDBServer({ port });
    await server.start();
    c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    await c.query('CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT, tier TEXT DEFAULT \'bronze\')');
    await c.query('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)');
  });
  
  after(async () => {
    if (c) await c.end();
    if (server) await server.stop();
  });

  it('DESCRIBE returns column info', async () => {
    const r = await c.query('DESCRIBE users');
    assert.equal(r.rows.length, 4);
    assert.equal(r.rows[0].column_name, 'id');
    assert.equal(r.rows[0].type, 'SERIAL');
    assert.equal(r.rows[0].primary_key, true);
    assert.equal(r.rows[1].column_name, 'name');
    assert.equal(r.rows[1].not_null, true);
  });

  it('information_schema.columns returns all columns', async () => {
    const r = await c.query("SELECT column_name, data_type, ordinal_position FROM information_schema.columns WHERE table_name = 'users' ORDER BY ordinal_position");
    assert.equal(r.rows.length, 4);
    assert.equal(r.rows[0].column_name, 'id');
    assert.equal(r.rows[0].data_type, 'SERIAL');
    assert.equal(String(r.rows[0].ordinal_position), '1');
  });

  it('information_schema.columns without filter returns all tables', async () => {
    const r = await c.query("SELECT DISTINCT table_name FROM information_schema.columns ORDER BY table_name");
    assert.ok(r.rows.length >= 2);
  });

  it('information_schema.tables lists all tables', async () => {
    const r = await c.query("SELECT table_name, table_type FROM information_schema.tables ORDER BY table_name");
    assert.ok(r.rows.length >= 2);
    const names = r.rows.map(r => r.table_name);
    assert.ok(names.includes('users'));
    assert.ok(names.includes('orders'));
  });

  it('pg_attribute query returns column metadata', async () => {
    const r = await c.query("SELECT attname, attnotnull, attnum FROM pg_catalog.pg_attribute WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'users') AND attnum > 0 AND NOT attisdropped ORDER BY attnum");
    assert.equal(r.rows.length, 4);
    assert.equal(r.rows[0].attname, 'id');
    assert.equal(String(r.rows[0].attnum), '1');
  });

  it('pg_catalog query for non-existent table returns empty', async () => {
    const r = await c.query("SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'nonexistent') AND attnum > 0");
    assert.equal(r.rows.length, 0);
  });

  it('SHOW TABLES lists table names', async () => {
    const r = await c.query('SHOW TABLES');
    assert.ok(r.rows.length >= 2);
  });
});
