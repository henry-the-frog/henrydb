// concurrent-session-stress.test.js — Stress test concurrent sessions
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-concurrent-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('Concurrent Session Stress Tests', () => {
  afterEach(cleanup);

  it('read committed: s2 sees s1 committed data', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s1.execute("INSERT INTO t VALUES (1, 'from-s1')");
    // s2 should NOT see uncommitted s1 data
    s2.begin();
    const r1 = s2.execute('SELECT * FROM t');
    assert.equal(r1.rows.length, 0, 's2 should not see uncommitted s1 data');
    s1.commit();
    s2.commit();
    // New transaction on s2 should see committed data
    s2.begin();
    const r2 = s2.execute('SELECT * FROM t');
    assert.equal(r2.rows.length, 1, 's2 should see committed s1 data in new tx');
    s2.commit();
    s1.close();
    s2.close();
  });

  it('snapshot isolation: s2 does not see s1 commit mid-transaction', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");
    const s1 = db.session();
    const s2 = db.session();
    s2.begin();
    // s2 takes snapshot
    const r1 = s2.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(r1.rows[0].val, 'original');
    // s1 updates
    s1.begin();
    s1.execute("UPDATE t SET val = 'updated' WHERE id = 1");
    s1.commit();
    // s2 still sees old value (snapshot)
    const r2 = s2.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(r2.rows[0].val, 'original', 's2 should still see original (snapshot)');
    s2.commit();
    s1.close();
    s2.close();
  });

  it('write-write conflict: both sessions insert different rows', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s2.begin();
    s1.execute("INSERT INTO t VALUES (1, 'from-s1')");
    s2.execute("INSERT INTO t VALUES (2, 'from-s2')");
    s1.commit();
    s2.commit();
    // Both rows should exist
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].val, 'from-s1');
    assert.equal(r.rows[1].val, 'from-s2');
    s1.close();
    s2.close();
  });

  it('rollback does not affect other sessions', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'keep')");
    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s1.execute("INSERT INTO t VALUES (2, 'discard')");
    s1.rollback();
    s2.begin();
    const r = s2.execute('SELECT * FROM t');
    assert.equal(r.rows.length, 1, 'rollback should not affect other sessions');
    assert.equal(r.rows[0].val, 'keep');
    s2.commit();
    s1.close();
    s2.close();
  });

  it('many concurrent sessions: 10 inserters', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    const sessions = [];
    for (let i = 0; i < 10; i++) {
      const s = db.session();
      s.begin();
      s.execute(`INSERT INTO t VALUES (${i}, 's${i}')`);
      sessions.push(s);
    }
    // Commit all
    for (const s of sessions) {
      s.commit();
      s.close();
    }
    const r = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(r.rows[0].cnt, 10);
  });

  it('interleaved read-write: reader sees consistent snapshot', () => {
    db = fresh();
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 1000)');
    db.execute('INSERT INTO accounts VALUES (2, 1000)');
    const reader = db.session();
    const writer = db.session();
    reader.begin();
    // Reader takes snapshot: total = 2000
    const r1 = reader.execute('SELECT SUM(balance) as total FROM accounts');
    assert.equal(r1.rows[0].total, 2000);
    // Writer transfers 500 from account 1 to 2
    writer.begin();
    writer.execute('UPDATE accounts SET balance = 500 WHERE id = 1');
    writer.execute('UPDATE accounts SET balance = 1500 WHERE id = 2');
    writer.commit();
    // Reader still sees total = 2000 (snapshot)
    const r2 = reader.execute('SELECT SUM(balance) as total FROM accounts');
    assert.equal(r2.rows[0].total, 2000, 'reader should see consistent snapshot');
    reader.commit();
    // New read sees updated values
    const r3 = db.execute('SELECT SUM(balance) as total FROM accounts');
    assert.equal(r3.rows[0].total, 2000); // Transfer doesn't change total
    reader.close();
    writer.close();
  });

  it('session after rollback can start new transaction', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    const s = db.session();
    s.begin();
    s.execute('INSERT INTO t VALUES (1)');
    s.rollback();
    s.begin();
    s.execute('INSERT INTO t VALUES (2)');
    s.commit();
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 2);
    s.close();
  });

  it('auto-commit interleaved with sessions', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    const s = db.session();
    s.begin();
    s.execute('INSERT INTO t VALUES (1)');
    // Auto-commit INSERT
    db.execute('INSERT INTO t VALUES (2)');
    s.execute('INSERT INTO t VALUES (3)');
    s.commit();
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 3);
    s.close();
  });

  it('DELETE in one session, INSERT in another — no interference', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s1.execute('DELETE FROM t WHERE id = 1');
    s2.begin();
    s2.execute("INSERT INTO t VALUES (3, 'c')");
    s1.commit();
    s2.commit();
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2); // row 1 deleted, rows 2 and 3 remain
    assert.equal(r.rows[0].id, 2);
    assert.equal(r.rows[1].id, 3);
    s1.close();
    s2.close();
  });

  it('concurrent sessions survive close/reopen', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s1.execute('INSERT INTO t VALUES (1)');
    s1.commit();
    s2.begin();
    s2.execute('INSERT INTO t VALUES (2)');
    s2.commit();
    s1.close();
    s2.close();
    db.close();
    db = TransactionalDatabase.open(dir);
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2);
  });
});
