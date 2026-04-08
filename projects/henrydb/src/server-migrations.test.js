// server-migrations.test.js — Schema migration system through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15520;

// Simple migration runner
class MigrationRunner {
  constructor(client) {
    this.client = client;
    this.migrations = [];
  }
  
  add(version, name, up, down) {
    this.migrations.push({ version, name, up, down });
  }
  
  async setup() {
    await this.client.query('CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER, name TEXT, applied_at TEXT)');
  }
  
  async getApplied() {
    const result = await this.client.query('SELECT version FROM schema_migrations ORDER BY version');
    return new Set(result.rows.map(r => parseInt(r.version)));
  }
  
  async migrate() {
    await this.setup();
    const applied = await this.getApplied();
    let count = 0;
    
    for (const m of this.migrations) {
      if (!applied.has(m.version)) {
        for (const sql of m.up) {
          await this.client.query(sql);
        }
        await this.client.query(`INSERT INTO schema_migrations VALUES (${m.version}, '${m.name}', '${new Date().toISOString()}')`);
        count++;
      }
    }
    return count;
  }
  
  async rollback(toVersion) {
    const applied = await this.getApplied();
    const toRollback = this.migrations
      .filter(m => applied.has(m.version) && m.version > toVersion)
      .sort((a, b) => b.version - a.version);
    
    for (const m of toRollback) {
      for (const sql of m.down) {
        await this.client.query(sql);
      }
      await this.client.query(`DELETE FROM schema_migrations WHERE version = ${m.version}`);
    }
    return toRollback.length;
  }
}

describe('Schema Migrations', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('applies migrations in order', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const runner = new MigrationRunner(client);
    runner.add(1, 'create_users', [
      'CREATE TABLE mig_users (id INTEGER, name TEXT)',
    ], [
      'DROP TABLE mig_users',
    ]);
    runner.add(2, 'add_email', [
      'ALTER TABLE mig_users ADD COLUMN email TEXT',
    ], [
      'ALTER TABLE mig_users DROP COLUMN email',
    ]);
    runner.add(3, 'create_posts', [
      'CREATE TABLE mig_posts (id INTEGER, user_id INTEGER, title TEXT)',
    ], [
      'DROP TABLE mig_posts',
    ]);

    const count = await runner.migrate();
    assert.strictEqual(count, 3);

    await client.end();
  });

  it('skips already applied migrations', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const runner = new MigrationRunner(client);
    runner.add(1, 'create_users', ['CREATE TABLE mig_users (id INTEGER, name TEXT)'], ['DROP TABLE mig_users']);
    runner.add(2, 'add_email', ['ALTER TABLE mig_users ADD COLUMN email TEXT'], ['ALTER TABLE mig_users DROP COLUMN email']);
    runner.add(3, 'create_posts', ['CREATE TABLE mig_posts (id INTEGER, user_id INTEGER, title TEXT)'], ['DROP TABLE mig_posts']);

    const count = await runner.migrate();
    assert.strictEqual(count, 0); // All already applied

    await client.end();
  });

  it('applies only new migrations', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const runner = new MigrationRunner(client);
    runner.add(1, 'create_users', ['CREATE TABLE mig_users (id INTEGER, name TEXT)'], ['DROP TABLE mig_users']);
    runner.add(2, 'add_email', ['ALTER TABLE mig_users ADD COLUMN email TEXT'], ['ALTER TABLE mig_users DROP COLUMN email']);
    runner.add(3, 'create_posts', ['CREATE TABLE mig_posts (id INTEGER, user_id INTEGER, title TEXT)'], ['DROP TABLE mig_posts']);
    runner.add(4, 'add_tags', ['CREATE TABLE mig_tags (id INTEGER, post_id INTEGER, tag TEXT)'], ['DROP TABLE mig_tags']);

    const count = await runner.migrate();
    assert.strictEqual(count, 1); // Only migration 4 is new

    await client.end();
  });

  it('tracks migration history', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT * FROM schema_migrations ORDER BY version');
    assert.strictEqual(result.rows.length, 4);
    assert.strictEqual(parseInt(result.rows[0].version), 1);
    assert.strictEqual(result.rows[0].name, 'create_users');

    await client.end();
  });

  it('data persists after migration', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query("INSERT INTO mig_users VALUES (1, 'Alice')");
    await client.query("INSERT INTO mig_posts VALUES (1, 1, 'Hello')");

    const users = await client.query('SELECT * FROM mig_users');
    const posts = await client.query('SELECT * FROM mig_posts');
    assert.strictEqual(users.rows.length, 1);
    assert.strictEqual(posts.rows.length, 1);

    await client.end();
  });
});
