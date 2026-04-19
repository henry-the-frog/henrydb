// triggers-mvcc.test.js — Trigger tests through TransactionalDatabase
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-trigger-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('Triggers Through MVCC', () => {
  afterEach(cleanup);

  it('AFTER INSERT trigger fires', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('CREATE TABLE audit (msg TEXT)');
    db.execute("CREATE TRIGGER after_ins AFTER INSERT ON t INSERT INTO audit VALUES ('inserted')");
    db.execute('INSERT INTO t VALUES (1)');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM audit').rows[0].cnt, 1);
  });

  it('trigger fires for each insert', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('CREATE TABLE audit_log (msg TEXT)');
    db.execute("CREATE TRIGGER after_ins AFTER INSERT ON t INSERT INTO audit_log VALUES ('fired')");
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('INSERT INTO t VALUES (3)');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM audit_log').rows[0].cnt, 3);
  });

  it('trigger with UPDATE action', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE TABLE audit_log (msg TEXT)');
    db.execute("CREATE TRIGGER after_ins AFTER INSERT ON t INSERT INTO audit_log VALUES ('row added')");
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('INSERT INTO t VALUES (2, 200)');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM audit_log').rows[0].cnt, 2);
  });

  it('trigger persists across close/reopen', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('CREATE TABLE audit (msg TEXT)');
    db.execute("CREATE TRIGGER after_ins AFTER INSERT ON t INSERT INTO audit VALUES ('fired')");
    db.execute('INSERT INTO t VALUES (1)');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM audit').rows[0].cnt, 1);
    db.close();
    db = TransactionalDatabase.open(dir);
    // Trigger should still fire
    db.execute('INSERT INTO t VALUES (2)');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM audit').rows[0].cnt, 2);
  });

  it('trigger in session transaction', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('CREATE TABLE audit (msg TEXT)');
    db.execute("CREATE TRIGGER after_ins AFTER INSERT ON t INSERT INTO audit VALUES ('triggered')");
    const s = db.session();
    s.begin();
    s.execute('INSERT INTO t VALUES (1)');
    assert.equal(s.execute('SELECT COUNT(*) as cnt FROM audit').rows[0].cnt, 1);
    s.commit();
    s.close();
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM audit').rows[0].cnt, 1);
  });
});
