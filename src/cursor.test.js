// cursor.test.js — Tests for DECLARE/FETCH/CLOSE cursor support
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CURSOR Support', () => {
  function setup() {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 30), (4, 'Dave', 40), (5, 'Eve', 50)");
    return db;
  }

  it('DECLARE and FETCH basic', () => {
    const db = setup();
    db.execute('DECLARE c CURSOR FOR SELECT * FROM t ORDER BY id');
    const r = db.execute('FETCH 2 FROM c');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[1].name, 'Bob');
    db.execute('CLOSE c');
  });

  it('FETCH advances cursor position', () => {
    const db = setup();
    db.execute('DECLARE c CURSOR FOR SELECT * FROM t ORDER BY id');
    
    db.execute('FETCH 2 FROM c');
    const r = db.execute('FETCH 2 FROM c');
    assert.equal(r.rows[0].name, 'Carol');
    assert.equal(r.rows[1].name, 'Dave');
    db.execute('CLOSE c');
  });

  it('FETCH NEXT returns one row', () => {
    const db = setup();
    db.execute('DECLARE c CURSOR FOR SELECT * FROM t ORDER BY id');
    
    const r = db.execute('FETCH NEXT FROM c');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Alice');
    db.execute('CLOSE c');
  });

  it('FETCH ALL returns remaining rows', () => {
    const db = setup();
    db.execute('DECLARE c CURSOR FOR SELECT * FROM t ORDER BY id');
    
    db.execute('FETCH 2 FROM c');
    const r = db.execute('FETCH ALL FROM c');
    assert.equal(r.rows.length, 3); // 3 remaining
    assert.equal(r.rows[0].name, 'Carol');
    assert.equal(r.rows[2].name, 'Eve');
    db.execute('CLOSE c');
  });

  it('FETCH past end returns empty', () => {
    const db = setup();
    db.execute('DECLARE c CURSOR FOR SELECT * FROM t ORDER BY id');
    
    db.execute('FETCH ALL FROM c');
    const r = db.execute('FETCH 1 FROM c');
    assert.equal(r.rows.length, 0);
    db.execute('CLOSE c');
  });

  it('FETCH FIRST resets position', () => {
    const db = setup();
    db.execute('DECLARE c CURSOR FOR SELECT * FROM t ORDER BY id');
    
    db.execute('FETCH 3 FROM c');
    const r = db.execute('FETCH FIRST FROM c');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Alice');
    db.execute('CLOSE c');
  });

  it('CLOSE removes cursor', () => {
    const db = setup();
    db.execute('DECLARE c CURSOR FOR SELECT * FROM t');
    db.execute('CLOSE c');
    
    assert.throws(() => db.execute('FETCH 1 FROM c'), /does not exist/);
  });

  it('CLOSE ALL removes all cursors', () => {
    const db = setup();
    db.execute('DECLARE c1 CURSOR FOR SELECT * FROM t');
    db.execute('DECLARE c2 CURSOR FOR SELECT * FROM t');
    db.execute('CLOSE ALL');
    
    assert.throws(() => db.execute('FETCH 1 FROM c1'), /does not exist/);
    assert.throws(() => db.execute('FETCH 1 FROM c2'), /does not exist/);
  });

  it('duplicate cursor name throws', () => {
    const db = setup();
    db.execute('DECLARE c CURSOR FOR SELECT * FROM t');
    assert.throws(() => db.execute('DECLARE c CURSOR FOR SELECT * FROM t'), /already exists/);
    db.execute('CLOSE c');
  });

  it('cursor with WHERE clause', () => {
    const db = setup();
    db.execute('DECLARE c CURSOR FOR SELECT * FROM t WHERE val > 25 ORDER BY id');
    const r = db.execute('FETCH ALL FROM c');
    assert.equal(r.rows.length, 3); // Carol(30), Dave(40), Eve(50)
    db.execute('CLOSE c');
  });

  it('cursor with aggregation', () => {
    const db = setup();
    db.execute('DECLARE c CURSOR FOR SELECT COUNT(*) as cnt, SUM(val) as total FROM t');
    const r = db.execute('FETCH 1 FROM c');
    assert.equal(r.rows[0].cnt, 5);
    assert.equal(r.rows[0].total, 150);
    db.execute('CLOSE c');
  });

  it('multiple independent cursors', () => {
    const db = setup();
    db.execute('DECLARE c1 CURSOR FOR SELECT * FROM t ORDER BY id');
    db.execute('DECLARE c2 CURSOR FOR SELECT * FROM t ORDER BY id DESC');
    
    const r1 = db.execute('FETCH 1 FROM c1');
    const r2 = db.execute('FETCH 1 FROM c2');
    assert.equal(r1.rows[0].name, 'Alice');
    assert.equal(r2.rows[0].name, 'Eve');
    
    db.execute('CLOSE ALL');
  });
});
