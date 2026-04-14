// sequence.test.js — Sequences (CREATE SEQUENCE, NEXTVAL, CURRVAL)
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Sequences', () => {
  it('creates sequence and generates values', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE s START 1 INCREMENT 1');
    assert.equal(db.execute("SELECT NEXTVAL('s') AS id").rows[0].id, 1);
    assert.equal(db.execute("SELECT NEXTVAL('s') AS id").rows[0].id, 2);
    assert.equal(db.execute("SELECT NEXTVAL('s') AS id").rows[0].id, 3);
  });

  it('CURRVAL returns current value', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE s START 10 INCREMENT 5');
    db.execute("SELECT NEXTVAL('s')");
    assert.equal(db.execute("SELECT CURRVAL('s') AS v").rows[0].v, 10);
    db.execute("SELECT NEXTVAL('s')");
    assert.equal(db.execute("SELECT CURRVAL('s') AS v").rows[0].v, 15);
  });

  it('uses sequence in INSERT', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE id_seq START 1 INCREMENT 1');
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (NEXTVAL('id_seq'), 'Alice')");
    db.execute("INSERT INTO t VALUES (NEXTVAL('id_seq'), 'Bob')");
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[1].id, 2);
  });
});
