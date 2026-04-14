// serial.test.js — SERIAL column type
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('SERIAL column type', () => {
  it('auto-increments on INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id SERIAL, name TEXT)');
    db.execute("INSERT INTO t (name) VALUES ('Alice')");
    db.execute("INSERT INTO t (name) VALUES ('Bob')");
    db.execute("INSERT INTO t (name) VALUES ('Carol')");
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.deepEqual(r.rows.map(r => r.id), [1, 2, 3]);
  });

  it('can be combined with PRIMARY KEY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t (name) VALUES ('Alice')");
    db.execute("INSERT INTO t (name) VALUES ('Bob')");
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].id, 1);
  });

  it('sequence continues after deletes', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id SERIAL, name TEXT)');
    db.execute("INSERT INTO t (name) VALUES ('Alice')");
    db.execute("INSERT INTO t (name) VALUES ('Bob')");
    db.execute('DELETE FROM t WHERE id = 2');
    db.execute("INSERT INTO t (name) VALUES ('Carol')");
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.deepEqual(r.rows.map(r => r.id), [1, 3]);
  });
});
