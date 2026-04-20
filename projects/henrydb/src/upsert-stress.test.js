// upsert-stress.test.js — INSERT ON CONFLICT stress tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('UPSERT (INSERT ON CONFLICT)', () => {
  it('DO UPDATE on PK conflict', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT, counter INT DEFAULT 0)');
    db.execute("INSERT INTO t VALUES (1, 'original', 1)");
    db.execute("INSERT INTO t VALUES (1, 'updated', 2) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, counter = EXCLUDED.counter");
    const r = db.execute('SELECT * FROM t WHERE id = 1');
    assert.equal(r.rows[0].val, 'updated');
    assert.equal(r.rows[0].counter, 2);
  });

  it('DO NOTHING on conflict', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'first')");
    db.execute("INSERT INTO t VALUES (1, 'second') ON CONFLICT (id) DO NOTHING");
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 'first');
  });

  it('no conflict: normal insert', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'first')");
    db.execute("INSERT INTO t VALUES (2, 'second') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 2);
  });

  it('batch upsert', () => {
    const db = new Database();
    db.execute('CREATE TABLE counters (name TEXT PRIMARY KEY, count INT DEFAULT 0)');
    
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO counters VALUES ('hits', 1) ON CONFLICT (name) DO UPDATE SET count = counters.count + 1`);
    }
    
    assert.equal(db.execute("SELECT count FROM counters WHERE name = 'hits'").rows[0].count, 10);
  });

  it('upsert with RETURNING', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'first')");
    const r = db.execute("INSERT INTO t VALUES (1, 'updated') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val RETURNING val");
    assert.equal(r.rows[0].val, 'updated');
  });
});
