// views-mvcc.test.js — Views and materialized views through TransactionalDatabase
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-views-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('Views Through MVCC', () => {
  afterEach(cleanup);

  it('basic view creation and query', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, name TEXT, dept TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 'Eng')");
    db.execute("INSERT INTO t VALUES (2, 'Bob', 'Mkt')");
    db.execute("CREATE VIEW eng AS SELECT * FROM t WHERE dept = 'Eng'");
    const r = db.execute('SELECT * FROM eng');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Alice');
  });

  it('view reflects underlying table changes', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, name TEXT, active INT)');
    db.execute('INSERT INTO t VALUES (1, \'Alice\', 1)');
    db.execute('INSERT INTO t VALUES (2, \'Bob\', 0)');
    db.execute('CREATE VIEW active_users AS SELECT * FROM t WHERE active = 1');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM active_users').rows[0].cnt, 1);
    // Add another active user
    db.execute('INSERT INTO t VALUES (3, \'Carol\', 1)');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM active_users').rows[0].cnt, 2);
  });

  it('view persists across close/reopen', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute('CREATE VIEW all_t AS SELECT * FROM t');
    db.close();
    db = TransactionalDatabase.open(dir);
    const r = db.execute('SELECT * FROM all_t');
    assert.equal(r.rows.length, 1);
  });

  it('DROP VIEW', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('CREATE VIEW v AS SELECT * FROM t');
    db.execute('DROP VIEW v');
    assert.throws(() => db.execute('SELECT * FROM v'), /not found|does not exist/i);
  });

  it('view in session transaction', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('CREATE VIEW high_val AS SELECT * FROM t WHERE val > 15');
    const s = db.session();
    s.begin();
    s.execute('INSERT INTO t VALUES (3, 30)');
    // View should reflect session's uncommitted data
    const r = s.execute('SELECT COUNT(*) as cnt FROM high_val');
    assert.equal(r.rows[0].cnt, 2); // 20 and 30
    s.rollback();
    s.close();
    // After rollback, view back to 1 row
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM high_val').rows[0].cnt, 1);
  });

  it('materialized view basic', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT COUNT(*) as cnt, SUM(val) as total FROM t');
    const r = db.execute('SELECT * FROM mv');
    assert.equal(r.rows[0].cnt, 10);
    assert.equal(r.rows[0].total, 550);
  });

  it('materialized view can be refreshed', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT COUNT(*) as cnt FROM t');
    // Initial matview
    const r1 = db.execute('SELECT cnt FROM mv');
    assert.ok(r1.rows[0].cnt >= 5);
    // Add more rows
    for (let i = 6; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    // Refresh should work
    db.execute('REFRESH MATERIALIZED VIEW mv');
    assert.equal(db.execute('SELECT cnt FROM mv').rows[0].cnt, 10);
  });
});
