// sequence.test.js — Tests for SEQUENCE + COALESCE/NULLIF/GREATEST/LEAST
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Sequences', () => {
  it('CREATE and nextval', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE seq1 START WITH 1');
    const r1 = db.execute("SELECT nextval('seq1') as id");
    assert.equal(r1.rows[0].id, 1);
    const r2 = db.execute("SELECT nextval('seq1') as id");
    assert.equal(r2.rows[0].id, 2);
  });

  it('currval returns last value', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE seq1');
    db.execute("SELECT nextval('seq1')");
    db.execute("SELECT nextval('seq1')");
    const r = db.execute("SELECT currval('seq1') as val");
    assert.equal(r.rows[0].val, 2);
  });

  it('currval before nextval throws', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE seq1');
    assert.throws(() => db.execute("SELECT currval('seq1')"), /not yet defined/);
  });

  it('setval updates position', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE seq1');
    db.execute("SELECT setval('seq1', 100)");
    const r = db.execute("SELECT nextval('seq1') as id");
    assert.equal(r.rows[0].id, 101);
  });

  it('custom START and INCREMENT', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE seq1 START WITH 10 INCREMENT BY 5');
    const vals = [];
    for (let i = 0; i < 5; i++) {
      vals.push(db.execute("SELECT nextval('seq1') as id").rows[0].id);
    }
    assert.deepStrictEqual(vals, [10, 15, 20, 25, 30]);
  });

  it('nextval in INSERT', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE user_seq START WITH 1');
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    
    db.execute("INSERT INTO users VALUES (nextval('user_seq'), 'Alice')");
    db.execute("INSERT INTO users VALUES (nextval('user_seq'), 'Bob')");
    
    const r = db.execute('SELECT * FROM users ORDER BY id');
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[1].id, 2);
  });

  it('DROP SEQUENCE', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE seq1');
    db.execute('DROP SEQUENCE seq1');
    assert.throws(() => db.execute("SELECT nextval('seq1')"), /does not exist/);
  });

  it('DROP SEQUENCE IF EXISTS', () => {
    const db = new Database();
    db.execute('DROP SEQUENCE IF EXISTS nonexistent'); // Should not throw
  });

  it('duplicate sequence name throws', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE seq1');
    assert.throws(() => db.execute('CREATE SEQUENCE seq1'), /already exists/);
  });
});

describe('Expression Functions', () => {
  it('COALESCE returns first non-null', () => {
    const db = new Database();
    const r = db.execute('SELECT COALESCE(NULL, NULL, 42) as val');
    assert.equal(r.rows[0].val, 42);
  });

  it('COALESCE returns first arg if non-null', () => {
    const db = new Database();
    const r = db.execute('SELECT COALESCE(1, 2, 3) as val');
    assert.equal(r.rows[0].val, 1);
  });

  it('NULLIF returns null when equal', () => {
    const db = new Database();
    const r = db.execute('SELECT NULLIF(5, 5) as val');
    assert.equal(r.rows[0].val, null);
  });

  it('NULLIF returns first when not equal', () => {
    const db = new Database();
    const r = db.execute('SELECT NULLIF(5, 3) as val');
    assert.equal(r.rows[0].val, 5);
  });

  it('GREATEST returns maximum', () => {
    const db = new Database();
    const r = db.execute('SELECT GREATEST(1, 5, 3, 7, 2) as val');
    assert.equal(r.rows[0].val, 7);
  });

  it('LEAST returns minimum', () => {
    const db = new Database();
    const r = db.execute('SELECT LEAST(5, 1, 8, 2) as val');
    assert.equal(r.rows[0].val, 1);
  });

  it('functions work in WHERE clause', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 20), (2, 30, 5)');
    
    const r = db.execute('SELECT * FROM t WHERE GREATEST(a, b) > 15');
    assert.equal(r.rows.length, 2);
  });
});
