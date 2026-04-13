// constraints-depth.test.js — CHECK/DEFAULT/NOT NULL through TransactionalDatabase
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-constr-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('Constraints Through MVCC', () => {
  afterEach(cleanup);

  describe('NOT NULL', () => {
    it('rejects NULL value', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, name TEXT NOT NULL)');
      assert.throws(() => db.execute('INSERT INTO t (id) VALUES (1)'), /null/i);
    });

    it('allows non-NULL value', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, name TEXT NOT NULL)');
      db.execute("INSERT INTO t VALUES (1, 'hello')");
      assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
    });

    it('persists across reopen', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, name TEXT NOT NULL)');
      db.execute("INSERT INTO t VALUES (1, 'hello')");
      db.close();
      db = TransactionalDatabase.open(dir);
      assert.throws(() => db.execute('INSERT INTO t (id) VALUES (2)'), /null/i);
    });

    it('works in session transaction', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, name TEXT NOT NULL)');
      const s = db.session();
      s.begin();
      assert.throws(() => s.execute('INSERT INTO t (id) VALUES (1)'), /null/i);
      s.rollback();
      s.close();
    });
  });

  describe('DEFAULT', () => {
    it('applies default on missing column', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, status TEXT DEFAULT \'active\', score INT DEFAULT 0)');
      db.execute('INSERT INTO t (id) VALUES (1)');
      const r = db.execute('SELECT * FROM t WHERE id = 1');
      assert.equal(r.rows[0].status, 'active');
      assert.equal(r.rows[0].score, 0);
    });

    it('explicit value overrides default', () => {
      db = fresh();
      db.execute("CREATE TABLE t (id INT, status TEXT DEFAULT 'active')");
      db.execute("INSERT INTO t VALUES (1, 'inactive')");
      assert.equal(db.execute('SELECT status FROM t WHERE id = 1').rows[0].status, 'inactive');
    });

    it('default persists across reopen', () => {
      db = fresh();
      db.execute("CREATE TABLE t (id INT, status TEXT DEFAULT 'active')");
      db.execute('INSERT INTO t (id) VALUES (1)');
      db.close();
      db = TransactionalDatabase.open(dir);
      // After reopen, the default should still apply for new inserts
      db.execute('INSERT INTO t (id) VALUES (2)');
      const r = db.execute('SELECT status FROM t WHERE id = 2');
      assert.equal(r.rows[0].status, 'active');
    });

    it('default in session', () => {
      db = fresh();
      db.execute("CREATE TABLE t (id INT, val TEXT DEFAULT 'default')");
      const s = db.session();
      s.begin();
      s.execute('INSERT INTO t (id) VALUES (1)');
      const r = s.execute('SELECT val FROM t WHERE id = 1');
      assert.equal(r.rows[0].val, 'default');
      s.commit();
      s.close();
    });
  });

  describe('CHECK', () => {
    it('rejects invalid value', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, age INT CHECK (age >= 0))');
      assert.throws(() => db.execute('INSERT INTO t VALUES (1, -5)'), /check/i);
    });

    it('allows valid value', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, age INT CHECK (age >= 0))');
      db.execute('INSERT INTO t VALUES (1, 25)');
      assert.equal(db.execute('SELECT age FROM t WHERE id = 1').rows[0].age, 25);
    });

    it('persists across reopen', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, age INT CHECK (age >= 0))');
      db.execute('INSERT INTO t VALUES (1, 25)');
      db.close();
      db = TransactionalDatabase.open(dir);
      assert.throws(() => db.execute('INSERT INTO t VALUES (2, -1)'), /check/i);
    });

    it('CHECK in session', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT, score INT CHECK (score >= 0 AND score <= 100))');
      const s = db.session();
      s.begin();
      s.execute('INSERT INTO t VALUES (1, 50)');
      assert.throws(() => s.execute('INSERT INTO t VALUES (2, 150)'), /check/i);
      s.commit();
      s.close();
    });
  });

  describe('Combined constraints', () => {
    it('NOT NULL + DEFAULT', () => {
      db = fresh();
      db.execute("CREATE TABLE t (id INT, name TEXT NOT NULL DEFAULT 'unknown')");
      db.execute('INSERT INTO t (id) VALUES (1)');
      assert.equal(db.execute('SELECT name FROM t WHERE id = 1').rows[0].name, 'unknown');
    });

    it('PK + CHECK', () => {
      db = fresh();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY CHECK (id > 0), name TEXT)');
      assert.throws(() => db.execute("INSERT INTO t VALUES (0, 'bad')"), /check/i);
      db.execute("INSERT INTO t VALUES (1, 'good')");
      assert.throws(() => db.execute("INSERT INTO t VALUES (1, 'dup')"), /duplicate|unique/i);
    });

    it('all constraints survive close/reopen', () => {
      db = fresh();
      db.execute("CREATE TABLE t (id INT PRIMARY KEY CHECK (id > 0), name TEXT NOT NULL DEFAULT 'anon', age INT CHECK (age >= 0))");
      db.execute("INSERT INTO t VALUES (1, 'Alice', 30)");
      db.close();
      db = TransactionalDatabase.open(dir);
      
      // PK enforced
      assert.throws(() => db.execute("INSERT INTO t VALUES (1, 'Bob', 25)"), /duplicate|unique/i);
      // CHECK enforced
      assert.throws(() => db.execute("INSERT INTO t VALUES (2, 'Bob', -1)"), /check/i);
      // NOT NULL enforced (with default)
      db.execute('INSERT INTO t (id, age) VALUES (3, 20)');
      assert.equal(db.execute('SELECT name FROM t WHERE id = 3').rows[0].name, 'anon');
    });
  });
});
