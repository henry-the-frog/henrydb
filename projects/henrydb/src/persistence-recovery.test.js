// persistence-recovery.test.js — WAL recovery and persistence tests

import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

let testDirs = [];

function makeTempDir() {
  const dir = mkdtempSync(join(tmpdir(), 'henrydb-test-'));
  testDirs.push(dir);
  return dir;
}

afterEach(() => {
  for (const dir of testDirs) {
    try { rmSync(dir, { recursive: true, force: true }); } catch {}
  }
  testDirs = [];
});

describe('WAL Recovery — Basic', () => {
  it('recovers CREATE TABLE + INSERT', () => {
    const dir = makeTempDir();
    const db = new Database({ dataDir: dir });
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    db.execute("INSERT INTO t VALUES (2, 'world')");
    
    const db2 = Database.recover(dir);
    assert.equal(db2.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
    assert.equal(db2.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 'hello');
  });

  it('recovers UPDATE', () => {
    const dir = makeTempDir();
    const db = new Database({ dataDir: dir });
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('UPDATE t SET val = 200 WHERE id = 1');
    
    const db2 = Database.recover(dir);
    assert.equal(db2.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 200);
  });

  it('recovers DELETE', () => {
    const dir = makeTempDir();
    const db = new Database({ dataDir: dir });
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');
    db.execute('DELETE FROM t WHERE id = 2');
    
    const db2 = Database.recover(dir);
    assert.equal(db2.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
  });

  it('recovers committed transaction', () => {
    const dir = makeTempDir();
    const db = new Database({ dataDir: dir });
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('INSERT INTO t VALUES (2, 200)');
    db.execute('COMMIT');
    
    const db2 = Database.recover(dir);
    assert.equal(db2.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
  });

  it('recovers multiple tables', () => {
    const dir = makeTempDir();
    const db = new Database({ dataDir: dir });
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute('INSERT INTO orders VALUES (1, 1)');
    
    const db2 = Database.recover(dir);
    assert.equal(db2.execute('SELECT name FROM users WHERE id = 1').rows[0].name, 'Alice');
    assert.equal(db2.execute('SELECT user_id FROM orders WHERE id = 1').rows[0].user_id, 1);
  });
});

describe('WAL Recovery — Data Integrity', () => {
  it('large dataset survives recovery', () => {
    const dir = makeTempDir();
    const db = new Database({ dataDir: dir });
    db.execute('CREATE TABLE big (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO big VALUES (${i}, ${i * 7})`);
    }
    
    const db2 = Database.recover(dir);
    const cnt = db2.execute('SELECT COUNT(*) as cnt FROM big').rows[0].cnt;
    assert.equal(cnt, 100);
    
    const sum = db2.execute('SELECT SUM(val) as s FROM big').rows[0].s;
    const expectedSum = Array.from({length: 100}, (_, i) => i * 7).reduce((a, b) => a + b, 0);
    assert.equal(sum, expectedSum);
  });

  it('mixed operations recover correctly', () => {
    const dir = makeTempDir();
    const db = new Database({ dataDir: dir });
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)');
    db.execute('UPDATE t SET val = val * 2 WHERE id <= 3');
    db.execute('DELETE FROM t WHERE id = 5');
    db.execute('INSERT INTO t VALUES (6, 60)');
    
    const db2 = Database.recover(dir);
    const rows = db2.execute('SELECT * FROM t ORDER BY id').rows;
    assert.equal(rows.length, 5); // 1,2,3,4,6
    assert.equal(rows[0].val, 20); // 10 * 2
    assert.equal(rows[1].val, 40); // 20 * 2
    assert.equal(rows[2].val, 60); // 30 * 2
    assert.equal(rows[3].val, 40); // unchanged
    assert.equal(rows[4].val, 60); // new insert
  });
});
