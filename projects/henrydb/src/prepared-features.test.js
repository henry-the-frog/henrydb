// prepared-features.test.js — Tests for new prepared statement features
// ? positional placeholders and stmt.executeMany()
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('? Positional Placeholders', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val REAL)');
  });

  it('basic ? placeholder insert', () => {
    db.execute('INSERT INTO t VALUES (?, ?, ?)', [1, 'alice', 3.14]);
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 1);
    assert.equal(r[0].name, 'alice');
    assert.equal(r[0].val, 3.14);
  });

  it('? in WHERE clause', () => {
    db.execute('INSERT INTO t VALUES (1, \'alice\', 3.14)');
    db.execute('INSERT INTO t VALUES (2, \'bob\', 2.71)');
    const r = rows(db.execute('SELECT * FROM t WHERE id = ?', [2]));
    assert.equal(r.length, 1);
    assert.equal(r[0].name, 'bob');
  });

  it('multiple ? in WHERE', () => {
    db.execute('INSERT INTO t VALUES (1, \'alice\', 3.14)');
    db.execute('INSERT INTO t VALUES (2, \'bob\', 2.71)');
    db.execute('INSERT INTO t VALUES (3, \'charlie\', 1.41)');
    const r = rows(db.execute('SELECT * FROM t WHERE id > ? AND val < ?', [2, 2.0]));
    assert.equal(r.length, 1);
    assert.equal(r[0].name, 'charlie');
  });

  it('? in UPDATE SET and WHERE', () => {
    db.execute('INSERT INTO t VALUES (1, \'alice\', 3.14)');
    db.execute('UPDATE t SET name = ?, val = ? WHERE id = ?', ['alice2', 9.99, 1]);
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r[0].name, 'alice2');
    assert.equal(r[0].val, 9.99);
  });

  it('? in DELETE WHERE', () => {
    db.execute('INSERT INTO t VALUES (1, \'alice\', 3.14)');
    db.execute('INSERT INTO t VALUES (2, \'bob\', 2.71)');
    db.execute('DELETE FROM t WHERE id = ?', [1]);
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 2);
  });

  it('? auto-numbers correctly with 5 params', () => {
    db.execute('DROP TABLE t');
    db.execute('CREATE TABLE t2 (a INT, b INT, c INT, d INT, e INT)');
    db.execute('INSERT INTO t2 VALUES (?, ?, ?, ?, ?)', [1, 2, 3, 4, 5]);
    const r = rows(db.execute('SELECT * FROM t2'));
    assert.deepEqual(r[0], { a: 1, b: 2, c: 3, d: 4, e: 5 });
  });

  it('mixed $ and ? is not supported (? auto-numbers independently)', () => {
    // ? auto-numbers from $1, so mixing would be confusing but shouldn't crash
    db.execute('INSERT INTO t VALUES (?, ?, ?)', [1, 'test', 1.0]);
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
  });
});

describe('Prepared Statement executeMany', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val REAL)');
  });

  it('basic executeMany insert', () => {
    const stmt = db.prepare('INSERT INTO t VALUES (?, ?, ?)');
    const result = stmt.executeMany([
      [1, 'alice', 3.14],
      [2, 'bob', 2.71],
      [3, 'charlie', 1.41],
    ]);
    assert.equal(result.count, 3);
    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 3);
    assert.equal(r[0].name, 'alice');
    assert.equal(r[2].name, 'charlie');
    stmt.close();
  });

  it('executeMany with $N placeholders', () => {
    const stmt = db.prepare('INSERT INTO t VALUES ($1, $2, $3)');
    stmt.executeMany([
      [10, 'ten', 10.0],
      [20, 'twenty', 20.0],
    ]);
    const r = rows(db.execute('SELECT count(*) as c FROM t'));
    assert.equal(r[0].c, 2);
    stmt.close();
  });

  it('executeMany returns results array', () => {
    const stmt = db.prepare('INSERT INTO t VALUES (?, ?, ?)');
    const result = stmt.executeMany([
      [1, 'a', 1.0],
      [2, 'b', 2.0],
    ]);
    assert.equal(result.results.length, 2);
    stmt.close();
  });

  it('executeMany with empty array', () => {
    const stmt = db.prepare('INSERT INTO t VALUES (?, ?, ?)');
    const result = stmt.executeMany([]);
    assert.equal(result.count, 0);
    stmt.close();
  });

  it('executeMany with single row', () => {
    const stmt = db.prepare('INSERT INTO t VALUES (?, ?, ?)');
    stmt.executeMany([[42, 'answer', 42.0]]);
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 42);
    stmt.close();
  });

  it('executeMany with large batch (100 rows)', () => {
    const stmt = db.prepare('INSERT INTO t VALUES (?, ?, ?)');
    const data = Array.from({ length: 100 }, (_, i) => [i, `name_${i}`, i * 1.1]);
    const result = stmt.executeMany(data);
    assert.equal(result.count, 100);
    const r = rows(db.execute('SELECT count(*) as c FROM t'));
    assert.equal(r[0].c, 100);
    stmt.close();
  });

  it('prepared statement reuse after executeMany', () => {
    const stmt = db.prepare('INSERT INTO t VALUES (?, ?, ?)');
    stmt.executeMany([[1, 'a', 1.0], [2, 'b', 2.0]]);
    // Single execute after batch
    stmt.execute(3, 'c', 3.0);
    const r = rows(db.execute('SELECT count(*) as c FROM t'));
    assert.equal(r[0].c, 3);
    stmt.close();
  });

  it('executeMany with prepared SELECT', () => {
    db.execute("INSERT INTO t VALUES (1, 'alice', 3.14)");
    db.execute("INSERT INTO t VALUES (2, 'bob', 2.71)");
    db.execute("INSERT INTO t VALUES (3, 'charlie', 1.41)");
    
    const stmt = db.prepare('SELECT * FROM t WHERE id = ?');
    const result = stmt.executeMany([[1], [3]]);
    assert.equal(result.count, 2);
    const r1 = rows(result.results[0]);
    const r2 = rows(result.results[1]);
    assert.equal(r1[0].name, 'alice');
    assert.equal(r2[0].name, 'charlie');
    stmt.close();
  });
});

describe('Prepared Statement Fast Bind', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
  });

  it('reuse same stmt with different params', () => {
    const stmt = db.prepare('INSERT INTO t VALUES (?, ?)');
    for (let i = 0; i < 10; i++) {
      stmt.execute(i, `name_${i}`);
    }
    const r = rows(db.execute('SELECT count(*) as c FROM t'));
    assert.equal(r[0].c, 10);
    stmt.close();
  });

  it('prepared SELECT with varying params', () => {
    for (let i = 0; i < 5; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'name_${i}')`);
    }
    const stmt = db.prepare('SELECT * FROM t WHERE id = ?');
    for (let i = 0; i < 5; i++) {
      const r = rows(stmt.execute(i));
      assert.equal(r[0].name, `name_${i}`);
    }
    stmt.close();
  });

  it('null params', () => {
    const stmt = db.prepare('INSERT INTO t VALUES (?, ?)');
    stmt.execute(1, null);
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r[0].id, 1);
    assert.equal(r[0].name, null);
    stmt.close();
  });

  it('string with special characters', () => {
    const stmt = db.prepare('INSERT INTO t VALUES (?, ?)');
    stmt.execute(1, "it's a \"test\" with \nnewlines");
    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r[0].name, "it's a \"test\" with \nnewlines");
    stmt.close();
  });
});
