import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('ARRAY_AGG and STRING_AGG ORDER BY', () => {
  it('ARRAY_AGG ORDER BY ASC', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (3),(1),(4),(1),(5)');
    const r = db.execute('SELECT ARRAY_AGG(id ORDER BY id) as sorted FROM t');
    assert.deepEqual(r.rows[0].sorted, [1, 1, 3, 4, 5]);
  });

  it('ARRAY_AGG ORDER BY DESC', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (3),(1),(4)');
    const r = db.execute('SELECT ARRAY_AGG(id ORDER BY id DESC) as rev FROM t');
    assert.deepEqual(r.rows[0].rev, [4, 3, 1]);
  });

  it('ARRAY_AGG DISTINCT ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (3),(1),(3),(1),(5)');
    const r = db.execute('SELECT ARRAY_AGG(DISTINCT id ORDER BY id) as sorted FROM t');
    assert.deepEqual(r.rows[0].sorted, [1, 3, 5]);
  });

  it('ARRAY_AGG with GROUP BY and ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',3),('A',1),('A',2),('B',6),('B',4)");
    const r = db.execute('SELECT grp, ARRAY_AGG(val ORDER BY val) as sorted FROM t GROUP BY grp ORDER BY grp');
    assert.deepEqual(r.rows[0].sorted, [1, 2, 3]);
    assert.deepEqual(r.rows[1].sorted, [4, 6]);
  });

  it('STRING_AGG ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (3,'c'),(1,'a'),(2,'b')");
    const r = db.execute("SELECT STRING_AGG(name, ', ' ORDER BY id) as names FROM t");
    assert.equal(r.rows[0].names, 'a, b, c');
  });

  it('STRING_AGG ORDER BY DESC', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT)');
    db.execute("INSERT INTO t VALUES ('c'),('a'),('b')");
    const r = db.execute("SELECT STRING_AGG(name, '-' ORDER BY name DESC) as names FROM t");
    assert.equal(r.rows[0].names, 'c-b-a');
  });
});
