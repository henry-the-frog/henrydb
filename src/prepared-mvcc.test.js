// prepared-mvcc.test.js — Prepared statements through MVCC
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { PreparedStatement } from './prepared.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-prep-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('Prepared Statements Through MVCC', () => {
  afterEach(cleanup);

  it('basic prepare + execute through MVCC', () => {
    db = fresh();
    db.execute('CREATE TABLE users (id INT, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");

    const stmt = new PreparedStatement('get_user', 'SELECT * FROM users WHERE id = $1');
    const sql = stmt.bind([1]);
    const r = db.execute(sql);
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Alice');
  });

  it('prepared INSERT through MVCC', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    const stmt = new PreparedStatement('ins', "INSERT INTO t VALUES ($1, $2)");
    for (let i = 0; i < 10; i++) {
      db.execute(stmt.bind([i, `val${i}`]));
    }
    const r = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(r.rows[0].cnt, 10);
  });

  it('prepared statement in session transaction', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    const s = db.session();
    s.begin();
    const ins = new PreparedStatement('ins', "INSERT INTO t VALUES ($1, $2)");
    s.execute(ins.bind([1, 'hello']));
    s.execute(ins.bind([2, 'world']));
    const sel = new PreparedStatement('sel', 'SELECT val FROM t WHERE id = $1');
    const r = s.execute(sel.bind([1]));
    assert.equal(r.rows[0].val, 'hello');
    s.commit();
    // Verify after commit
    const r2 = db.execute(sel.bind([2]));
    assert.equal(r2.rows[0].val, 'world');
    s.close();
  });

  it('prepared statement sees MVCC snapshot correctly', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");
    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s2.begin();
    // s1 updates
    s1.execute("UPDATE t SET val = 'modified' WHERE id = 1");
    // s2 uses prepared statement — should see original
    const sel = new PreparedStatement('sel', 'SELECT val FROM t WHERE id = $1');
    const r = s2.execute(sel.bind([1]));
    assert.equal(r.rows[0].val, 'original');
    s1.commit();
    s2.commit();
    s1.close();
    s2.close();
  });

  it('prepared UPDATE through MVCC', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'old')");
    const upd = new PreparedStatement('upd', "UPDATE t SET val = $2 WHERE id = $1");
    db.execute(upd.bind([1, 'new']));
    const r = db.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(r.rows[0].val, 'new');
  });

  it('prepared DELETE through MVCC', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('INSERT INTO t VALUES (3)');
    const del = new PreparedStatement('del', 'DELETE FROM t WHERE id = $1');
    db.execute(del.bind([2]));
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[1].id, 3);
  });

  it('prepared statement with savepoint rollback', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    const s = db.session();
    s.begin();
    const ins = new PreparedStatement('ins', "INSERT INTO t VALUES ($1, $2)");
    s.execute(ins.bind([1, 'keep']));
    s.execute('SAVEPOINT sp1');
    s.execute(ins.bind([2, 'discard']));
    s.execute('ROLLBACK TO sp1');
    s.commit();
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 'keep');
    s.close();
  });

  it('prepared statement survives close/reopen', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    const ins = new PreparedStatement('ins', "INSERT INTO t VALUES ($1, $2)");
    db.execute(ins.bind([1, 'persistent']));
    db.close();
    db = TransactionalDatabase.open(dir);
    const sel = new PreparedStatement('sel', 'SELECT val FROM t WHERE id = $1');
    const r = db.execute(sel.bind([1]));
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 'persistent');
  });

  it('concurrent readers with prepared statements', () => {
    db = fresh();
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 1000)');
    db.execute('INSERT INTO accounts VALUES (2, 1000)');
    const sel = new PreparedStatement('sel', 'SELECT balance FROM accounts WHERE id = $1');
    const upd = new PreparedStatement('upd', 'UPDATE accounts SET balance = $2 WHERE id = $1');
    const reader = db.session();
    const writer = db.session();
    reader.begin();
    writer.begin();
    // Reader takes snapshot
    const r1 = reader.execute(sel.bind([1]));
    assert.equal(r1.rows[0].balance, 1000);
    // Writer updates
    writer.execute(upd.bind([1, 500]));
    writer.execute(upd.bind([2, 1500]));
    writer.commit();
    // Reader still sees old balance
    const r2 = reader.execute(sel.bind([1]));
    assert.equal(r2.rows[0].balance, 1000);
    reader.commit();
    reader.close();
    writer.close();
  });
});
