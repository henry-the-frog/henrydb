// orm-compat.test.js — Tests for ORM/migration compatibility stubs
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Client } = pg;

function getPort() {
  return 37000 + Math.floor(Math.random() * 10000);
}

describe('ORM Compatibility', () => {
  let server, port, dir;
  
  before(async () => {
    port = getPort();
    dir = mkdtempSync(join(tmpdir(), 'henrydb-orm-'));
    server = new HenryDBServer({ port, dataDir: dir, transactional: true });
    await server.start();
  });
  
  after(async () => {
    if (server) await server.stop();
    if (dir) rmSync(dir, { recursive: true });
  });

  it('CREATE EXTENSION IF NOT EXISTS (uuid-ossp)', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    await client.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"');
    // Should not throw
    await client.end();
  });

  it('CREATE EXTENSION pgcrypto', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    await client.query('CREATE EXTENSION IF NOT EXISTS pgcrypto');
    await client.end();
  });

  it('DROP EXTENSION IF EXISTS', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    await client.query('DROP EXTENSION IF EXISTS pgcrypto');
    await client.end();
  });

  it('CREATE SCHEMA', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    await client.query('CREATE SCHEMA IF NOT EXISTS public');
    await client.end();
  });

  it('COMMENT ON', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    await client.query("CREATE TABLE commented (id INT)");
    await client.query("COMMENT ON TABLE commented IS 'Test table'");
    await client.query("COMMENT ON COLUMN commented.id IS 'Primary key'");
    await client.end();
  });

  it('GRANT/REVOKE', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    await client.query('GRANT ALL ON ALL TABLES IN SCHEMA public TO test');
    await client.query('REVOKE ALL ON ALL TABLES IN SCHEMA public FROM public');
    await client.end();
  });

  it('Prisma-style migration', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    // Typical Prisma migration sequence
    await client.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"');
    await client.query("CREATE TABLE users (id INT PRIMARY KEY, email TEXT, name TEXT, created_at TEXT DEFAULT 'now')");
    await client.query("CREATE TABLE posts (id INT PRIMARY KEY, title TEXT, content TEXT, author_id INT, published INT DEFAULT 0)");
    
    // Insert some data
    await client.query('INSERT INTO users VALUES (1, $1, $2, $3)', ['alice@test.com', 'Alice', '2026-04-10']);
    await client.query('INSERT INTO posts VALUES (1, $1, $2, $3, $4)', ['Hello World', 'My first post', 1, 1]);
    
    // Join query
    const result = await client.query(`
      SELECT p.title, u.name as author
      FROM posts p
      JOIN users u ON p.author_id = u.id
      WHERE p.published = 1
    `);
    
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].title, 'Hello World');
    assert.equal(result.rows[0].author, 'Alice');
    
    await client.end();
  });
});
