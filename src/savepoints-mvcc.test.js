// savepoints-mvcc.test.js — Savepoints through TransactionalDatabase
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-sp-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('Savepoints through MVCC', () => {
  afterEach(cleanup);

  it('basic savepoint and rollback to', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    const s = db.session();
    s.begin();
    s.execute("INSERT INTO t VALUES (1, 'a')");
    s.execute('SAVEPOINT sp1');
    s.execute("INSERT INTO t VALUES (2, 'b')");
    // Both rows visible in session
    const r1 = s.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r1.rows.length, 2);
    // Rollback to savepoint
    s.execute('ROLLBACK TO sp1');
    // Only row 1 should remain
    const r2 = s.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r2.rows.length, 1);
    assert.equal(r2.rows[0].val, 'a');
    s.commit();
    // After commit, only row 1 persists
    const r3 = db.execute('SELECT * FROM t');
    assert.equal(r3.rows.length, 1);
    s.close();
  });

  it('nested savepoints', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    const s = db.session();
    s.begin();
    s.execute('INSERT INTO t VALUES (1)');
    s.execute('SAVEPOINT sp1');
    s.execute('INSERT INTO t VALUES (2)');
    s.execute('SAVEPOINT sp2');
    s.execute('INSERT INTO t VALUES (3)');
    // 3 rows in session
    assert.equal(s.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 3);
    // Rollback to sp2: only 1, 2
    s.execute('ROLLBACK TO sp2');
    assert.equal(s.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
    // Rollback to sp1: only 1
    s.execute('ROLLBACK TO sp1');
    assert.equal(s.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
    s.commit();
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
    s.close();
  });

  it('release savepoint', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    const s = db.session();
    s.begin();
    s.execute('INSERT INTO t VALUES (1)');
    s.execute('SAVEPOINT sp1');
    s.execute('INSERT INTO t VALUES (2)');
    s.execute('RELEASE sp1');
    // Can't rollback to released savepoint
    assert.throws(() => s.execute('ROLLBACK TO sp1'), /not found/i);
    s.commit();
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
    s.close();
  });

  it('ROLLBACK TO with SAVEPOINT keyword', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    const s = db.session();
    s.begin();
    s.execute('SAVEPOINT sp1');
    s.execute('INSERT INTO t VALUES (1)');
    s.execute('ROLLBACK TO SAVEPOINT sp1');
    assert.equal(s.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 0);
    s.commit();
    s.close();
  });

  it('RELEASE SAVEPOINT with SAVEPOINT keyword', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    const s = db.session();
    s.begin();
    s.execute('SAVEPOINT sp1');
    s.execute('INSERT INTO t VALUES (1)');
    s.execute('RELEASE SAVEPOINT sp1');
    assert.throws(() => s.execute('ROLLBACK TO sp1'), /not found/i);
    s.commit();
    s.close();
  });

  it('savepoint does not affect other sessions', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s2.begin();
    s1.execute('SAVEPOINT sp1');
    s1.execute('INSERT INTO t VALUES (2)');
    // s2 doesn't see s1's uncommitted data
    assert.equal(s2.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
    s1.execute('ROLLBACK TO sp1');
    s1.commit();
    s2.commit();
    // Only original row remains
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
    s1.close();
    s2.close();
  });

  it('savepoint with DELETE rollback', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'keep')");
    db.execute("INSERT INTO t VALUES (2, 'keep')");
    const s = db.session();
    s.begin();
    s.execute('SAVEPOINT before_delete');
    s.execute('DELETE FROM t WHERE id = 1');
    assert.equal(s.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
    s.execute('ROLLBACK TO before_delete');
    assert.equal(s.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
    s.commit();
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
    s.close();
  });

  it('savepoint with UPDATE rollback', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");
    const s = db.session();
    s.begin();
    s.execute('SAVEPOINT sp1');
    s.execute("UPDATE t SET val = 'modified' WHERE id = 1");
    assert.equal(s.execute("SELECT val FROM t WHERE id = 1").rows[0].val, 'modified');
    s.execute('ROLLBACK TO sp1');
    assert.equal(s.execute("SELECT val FROM t WHERE id = 1").rows[0].val, 'original');
    s.commit();
    assert.equal(db.execute("SELECT val FROM t WHERE id = 1").rows[0].val, 'original');
    s.close();
  });

  it('error: savepoint without transaction', () => {
    db = fresh();
    const s = db.session();
    assert.throws(() => s.execute('SAVEPOINT sp1'), /no transaction/i);
    s.close();
  });

  it('error: rollback to nonexistent savepoint', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    const s = db.session();
    s.begin();
    assert.throws(() => s.execute('ROLLBACK TO nonexistent'), /not found/i);
    s.rollback();
    s.close();
  });

  it('savepoint rollback data does NOT persist across close/reopen', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    const s = db.session();
    s.begin();
    s.execute("INSERT INTO t VALUES (1, 'keep')");
    s.execute('SAVEPOINT sp1');
    s.execute("INSERT INTO t VALUES (2, 'discard')");
    s.execute('ROLLBACK TO sp1');
    s.commit();
    s.close();
    db.close();
    db = TransactionalDatabase.open(dir);
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 'keep');
  });

  it('commit after savepoints clears them', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    const s = db.session();
    s.begin();
    s.execute('SAVEPOINT sp1');
    s.execute('INSERT INTO t VALUES (1)');
    s.commit();
    // New transaction: old savepoints gone
    s.begin();
    assert.throws(() => s.execute('ROLLBACK TO sp1'), /not found/i);
    s.rollback();
    s.close();
  });
});
