// explain-mvcc.test.js — EXPLAIN/EXPLAIN ANALYZE through TransactionalDatabase
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-explain-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('EXPLAIN Through MVCC', () => {
  afterEach(cleanup);

  it('EXPLAIN returns plan for SELECT', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    const r = db.execute('EXPLAIN SELECT * FROM t WHERE id = 1');
    assert.equal(r.type, 'PLAN');
    assert.ok(r.plan.length > 0);
    assert.equal(r.plan[0].operation, 'TABLE_SCAN');
  });

  it('EXPLAIN ANALYZE returns actual row counts', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE id = 25');
    assert.equal(r.type, 'ANALYZE');
    assert.equal(r.actual_rows, 1);
    assert.ok(r.execution_time_ms >= 0);
  });

  it('EXPLAIN shows JOIN plan', () => {
    db = fresh();
    db.execute('CREATE TABLE a (id INT, val TEXT)');
    db.execute('CREATE TABLE b (id INT, ref_id INT)');
    db.execute("INSERT INTO a VALUES (1, 'x')");
    db.execute('INSERT INTO b VALUES (10, 1)');
    const r = db.execute('EXPLAIN SELECT * FROM a JOIN b ON a.id = b.ref_id');
    assert.equal(r.type, 'PLAN');
    const joinOp = r.plan.find(p => p.operation.includes('JOIN'));
    assert.ok(joinOp, 'should have a JOIN operation in plan');
  });

  it('EXPLAIN ANALYZE in session sees correct MVCC state', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const s = db.session();
    s.begin();
    s.execute('INSERT INTO t VALUES (100)');
    s.execute('INSERT INTO t VALUES (101)');
    // EXPLAIN ANALYZE in session should see 12 rows
    const r = s.execute('EXPLAIN ANALYZE SELECT * FROM t');
    assert.equal(r.actual_rows, 12);
    s.rollback();
    // After rollback, auto-commit EXPLAIN ANALYZE should see 10
    const r2 = db.execute('EXPLAIN ANALYZE SELECT * FROM t');
    assert.equal(r2.actual_rows, 10);
    s.close();
  });

  it('EXPLAIN ANALYZE with index scan', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE id = 50');
    assert.equal(r.actual_rows, 1);
    assert.ok(r.plan.length > 0);
  });

  it('EXPLAIN ANALYZE accuracy with filter', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, category TEXT)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, '${i % 3 === 0 ? 'A' : 'B'}')`);
    }
    const r = db.execute("EXPLAIN ANALYZE SELECT * FROM t WHERE category = 'A'");
    // About 34 rows match (0,3,6,...,99)
    assert.ok(r.actual_rows >= 30 && r.actual_rows <= 40);
  });
});
