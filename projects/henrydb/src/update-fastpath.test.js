// update-fastpath.test.js — Tests for the UPDATE fast-path optimization
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('UPDATE fast-path basics', () => {
  it('simple PK update', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('UPDATE t SET x = 99 WHERE id = 1');
    assert.equal(db.execute('SELECT x FROM t WHERE id = 1').rows[0].x, 99);
  });

  it('updates correct row only', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');
    db.execute('UPDATE t SET x = 99 WHERE id = 2');
    const rows = db.execute('SELECT * FROM t ORDER BY id').rows;
    assert.equal(rows[0].x, 10);
    assert.equal(rows[1].x, 99);
    assert.equal(rows[2].x, 30);
  });

  it('returns correct count', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    const r = db.execute('UPDATE t SET x = 99 WHERE id = 1');
    assert.equal(r.count, 1);
  });

  it('text PK', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT PRIMARY KEY, val INTEGER)');
    db.execute("INSERT INTO t VALUES ('alice', 10), ('bob', 20)");
    db.execute("UPDATE t SET val = 99 WHERE name = 'alice'");
    assert.equal(db.execute("SELECT val FROM t WHERE name = 'alice'").rows[0].val, 99);
    assert.equal(db.execute("SELECT val FROM t WHERE name = 'bob'").rows[0].val, 20);
  });

  it('multiple column update', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10, 20)');
    db.execute('UPDATE t SET x = 100, y = 200 WHERE id = 1');
    const row = db.execute('SELECT * FROM t WHERE id = 1').rows[0];
    assert.equal(row.x, 100);
    assert.equal(row.y, 200);
  });

  it('nonexistent row', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    const r = db.execute('UPDATE t SET x = 99 WHERE id = 999');
    assert.equal(r.count || 0, 0);
  });
});

describe('UPDATE fast-path repeated updates', () => {
  it('many updates to same row preserve data', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 0)');
    for (let i = 0; i < 100; i++) {
      db.execute(`UPDATE t SET x = ${i} WHERE id = 1`);
    }
    assert.equal(db.execute('SELECT x FROM t WHERE id = 1').rows[0].x, 99);
  });

  it('1000 updates to rotating rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    for (let i = 0; i < 1000; i++) {
      db.execute(`UPDATE t SET x = ${i} WHERE id = ${i % 10}`);
    }
    
    // Verify all rows exist
    const count = db.execute('SELECT COUNT(*) as c FROM t').rows[0].c;
    assert.equal(count, 10);
    
    // Verify last update for each row
    for (let i = 0; i < 10; i++) {
      const expected = 990 + i; // last update: i = 990+i → row i gets value 990+i
      const actual = db.execute(`SELECT x FROM t WHERE id = ${i}`).rows[0].x;
      assert.equal(actual, expected, `Row ${i}: expected ${expected}, got ${actual}`);
    }
  });

  it('5000 updates preserve table integrity', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    for (let i = 0; i < 5000; i++) {
      db.execute(`UPDATE t SET x = ${i} WHERE id = ${i % 100}`);
    }
    
    const count = db.execute('SELECT COUNT(*) as c FROM t').rows[0].c;
    assert.equal(count, 100, 'Table should still have 100 rows');
    
    const row = db.execute('SELECT x FROM t WHERE id = 42').rows[0];
    assert.equal(row.x, 4942);
  });
});

describe('UPDATE fast-path falls back to full path', () => {
  it('expression in SET (e.g., SET x = x + 1)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('UPDATE t SET x = x + 1 WHERE id = 1');
    assert.equal(db.execute('SELECT x FROM t WHERE id = 1').rows[0].x, 11);
  });

  it('complex WHERE (non-equality)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');
    db.execute('UPDATE t SET x = 0 WHERE id > 1');
    const rows = db.execute('SELECT * FROM t ORDER BY id').rows;
    assert.equal(rows[0].x, 10);
    assert.equal(rows[1].x, 0);
    assert.equal(rows[2].x, 0);
  });

  it('WHERE on non-PK column', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20), (3, 10)');
    db.execute('UPDATE t SET x = 99 WHERE x = 10');
    assert.equal(db.execute('SELECT x FROM t WHERE id = 1').rows[0].x, 99);
    assert.equal(db.execute('SELECT x FROM t WHERE id = 3').rows[0].x, 99);
  });

  it('UPDATE with no WHERE (updates all)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20)');
    db.execute('UPDATE t SET x = 0');
    const rows = db.execute('SELECT * FROM t ORDER BY id').rows;
    assert.equal(rows[0].x, 0);
    assert.equal(rows[1].x, 0);
  });
});

describe('UPDATE fast-path with indexes', () => {
  it('preserves secondary index after update', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)');
    db.execute('CREATE INDEX idx_x ON t(x)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');
    db.execute('UPDATE t SET x = 25 WHERE id = 2');
    // The secondary index should still work for queries
    const rows = db.execute('SELECT * FROM t WHERE x >= 25 ORDER BY x').rows;
    assert.equal(rows.length, 2);
    assert.equal(rows[0].x, 25);
    assert.equal(rows[1].x, 30);
  });
});
