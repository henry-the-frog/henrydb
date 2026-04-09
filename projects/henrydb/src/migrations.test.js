// migrations.test.js — Tests for schema migration system
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { MigrationRunner } from './migrations.js';
import { Database } from './db.js';

describe('Schema Migrations', () => {
  let db, runner;

  beforeEach(() => {
    db = new Database();
    runner = new MigrationRunner(db);
  });

  it('creates tracking table on init', () => {
    const tables = db.execute('SHOW TABLES');
    assert.ok(tables.rows.some(r => (r.table_name || r.name || Object.values(r)[0]) === '_migrations'));
  });

  it('starts at version 0', () => {
    assert.equal(runner.currentVersion(), 0);
  });

  it('registers migrations', () => {
    runner.add(1, 'Create users', 'CREATE TABLE users (id INTEGER PRIMARY KEY)', 'DROP TABLE users');
    runner.add(2, 'Add email', "ALTER TABLE users ADD COLUMN email TEXT", "SELECT 1");
    assert.equal(runner.pending().length, 2);
  });

  it('applies single migration', () => {
    runner.add(1, 'Create users', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)', 'DROP TABLE users');
    const result = runner.up();
    assert.deepEqual(result.applied, [1]);
    assert.equal(result.errors.length, 0);
    assert.equal(runner.currentVersion(), 1);

    // Table should exist
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM users').rows[0].cnt, 1);
  });

  it('applies multiple migrations in order', () => {
    runner
      .add(1, 'Create users', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)', 'DROP TABLE users')
      .add(2, 'Create posts', 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)', 'DROP TABLE posts')
      .add(3, 'Create tags', 'CREATE TABLE tags (id INTEGER PRIMARY KEY, tag TEXT)', 'DROP TABLE tags');

    const result = runner.up();
    assert.deepEqual(result.applied, [1, 2, 3]);
    assert.equal(runner.currentVersion(), 3);
  });

  it('applies up to specific version', () => {
    runner
      .add(1, 'V1', 'CREATE TABLE t1 (id INTEGER PRIMARY KEY)', 'DROP TABLE t1')
      .add(2, 'V2', 'CREATE TABLE t2 (id INTEGER PRIMARY KEY)', 'DROP TABLE t2')
      .add(3, 'V3', 'CREATE TABLE t3 (id INTEGER PRIMARY KEY)', 'DROP TABLE t3');

    runner.up(2);
    assert.equal(runner.currentVersion(), 2);
    assert.equal(runner.pending().length, 1);
  });

  it('rollback last migration', () => {
    runner
      .add(1, 'Create users', 'CREATE TABLE users (id INTEGER PRIMARY KEY)', 'DROP TABLE users')
      .add(2, 'Create posts', 'CREATE TABLE posts (id INTEGER PRIMARY KEY)', 'DROP TABLE posts');

    runner.up();
    assert.equal(runner.currentVersion(), 2);

    const result = runner.down();
    assert.ok(result.rolledBack.includes(2));
    assert.equal(runner.currentVersion(), 1);
  });

  it('rollback to specific version', () => {
    runner
      .add(1, 'V1', 'CREATE TABLE t1 (id INTEGER PRIMARY KEY)', 'DROP TABLE t1')
      .add(2, 'V2', 'CREATE TABLE t2 (id INTEGER PRIMARY KEY)', 'DROP TABLE t2')
      .add(3, 'V3', 'CREATE TABLE t3 (id INTEGER PRIMARY KEY)', 'DROP TABLE t3');

    runner.up();
    runner.down(1);
    assert.equal(runner.currentVersion(), 1);
  });

  it('reset rolls back everything', () => {
    runner
      .add(1, 'V1', 'CREATE TABLE t1 (id INTEGER PRIMARY KEY)', 'DROP TABLE t1')
      .add(2, 'V2', 'CREATE TABLE t2 (id INTEGER PRIMARY KEY)', 'DROP TABLE t2');

    runner.up();
    runner.reset();
    assert.equal(runner.currentVersion(), 0);
  });

  it('redo rolls back and re-applies last migration', () => {
    runner.add(1, 'Create users', 'CREATE TABLE users (id INTEGER PRIMARY KEY)', 'DROP TABLE users');
    runner.up();
    
    const result = runner.redo();
    assert.deepEqual(result.applied, [1]);
    assert.equal(runner.currentVersion(), 1);
  });

  it('status shows correct info', () => {
    runner
      .add(1, 'V1', 'CREATE TABLE t1 (id INTEGER PRIMARY KEY)', 'DROP TABLE t1')
      .add(2, 'V2', 'CREATE TABLE t2 (id INTEGER PRIMARY KEY)', 'DROP TABLE t2');

    runner.up(1);
    const status = runner.status();
    assert.equal(status.currentVersion, 1);
    assert.equal(status.applied, 1);
    assert.equal(status.pending, 1);
    assert.equal(status.total, 2);
    assert.equal(status.migrations[0].status, 'applied');
    assert.equal(status.migrations[1].status, 'pending');
  });

  it('handles migration with multiple SQL statements', () => {
    runner.add(1, 'Setup schema', [
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
      'CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER)',
      'CREATE INDEX idx_posts_user ON posts(user_id)',
    ], [
      'DROP TABLE posts',
      'DROP TABLE users',
    ]);

    runner.up();
    assert.equal(runner.currentVersion(), 1);
    // Both tables should exist
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute('INSERT INTO posts VALUES (1, 1)');
  });

  it('stops on migration error', () => {
    runner
      .add(1, 'Good', 'CREATE TABLE t1 (id INTEGER PRIMARY KEY)', 'DROP TABLE t1')
      .add(2, 'Bad', 'INVALID SQL SYNTAX HERE', 'SELECT 1')
      .add(3, 'Never reached', 'CREATE TABLE t3 (id INTEGER PRIMARY KEY)', 'DROP TABLE t3');

    const result = runner.up();
    assert.equal(result.applied.length, 1);
    assert.equal(result.errors.length, 1);
    assert.equal(runner.currentVersion(), 1);
  });

  it('idempotent: running up twice only applies new migrations', () => {
    runner
      .add(1, 'V1', 'CREATE TABLE t1 (id INTEGER PRIMARY KEY)', 'DROP TABLE t1');

    runner.up();
    const result = runner.up(); // Should be no-op
    assert.equal(result.applied.length, 0);
    assert.equal(runner.currentVersion(), 1);
  });

  it('applied() returns migration history', () => {
    runner.add(1, 'V1', 'CREATE TABLE t1 (id INTEGER PRIMARY KEY)', 'DROP TABLE t1');
    runner.up();
    
    const history = runner.applied();
    assert.equal(history.length, 1);
    assert.equal(history[0].version, 1);
    assert.equal(history[0].name, 'V1');
    assert.ok(history[0].applied_at);
  });
});
