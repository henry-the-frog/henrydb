// upsert.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('UPSERT (INSERT ON CONFLICT)', () => {
  it('DO NOTHING skips duplicate', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (1, 20) ON CONFLICT (id) DO NOTHING');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 10);
  });

  it('DO UPDATE modifies existing row', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET val = 20');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 20);
  });

  it('inserts when no conflict', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10) ON CONFLICT (id) DO NOTHING');
    assert.equal(db.execute('SELECT * FROM t').rows.length, 1);
    assert.equal(db.execute('SELECT val FROM t').rows[0].val, 10);
  });

  it('multiple upserts accumulate', () => {
    const db = new Database();
    db.execute('CREATE TABLE counters (id INT PRIMARY KEY, cnt INT)');
    db.execute('INSERT INTO counters VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET cnt = cnt + 1');
    db.execute('INSERT INTO counters VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET cnt = cnt + 1');
    db.execute('INSERT INTO counters VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET cnt = cnt + 1');
    assert.equal(db.execute('SELECT cnt FROM counters WHERE id = 1').rows[0].cnt, 3);
  });
});
