// limit-pushdown.test.js — LIMIT push-down optimization
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('LIMIT push-down', () => {
  it('stops scanning early for simple LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'n${i}')`);
    
    const r = db.execute('SELECT * FROM t LIMIT 5');
    assert.equal(r.rows.length, 5);
    assert.equal(r.rows[0].id, 0);
  });

  it('applies WHERE before LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    const r = db.execute('SELECT * FROM t WHERE id >= 50 LIMIT 3');
    assert.equal(r.rows.length, 3);
    assert.deepEqual(r.rows.map(r => r.id), [50, 51, 52]);
  });

  it('respects OFFSET + LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    const r = db.execute('SELECT * FROM t LIMIT 3 OFFSET 10');
    assert.deepEqual(r.rows.map(r => r.id), [10, 11, 12]);
  });

  it('does NOT push LIMIT when ORDER BY present', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    const r = db.execute('SELECT * FROM t ORDER BY id DESC LIMIT 3');
    assert.deepEqual(r.rows.map(r => r.id), [9, 8, 7]);
  });

  it('does NOT push LIMIT with GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('a', 1), ('b', 2), ('a', 3), ('b', 4), ('c', 5)");
    
    const r = db.execute('SELECT cat, SUM(val) AS total FROM t GROUP BY cat LIMIT 2');
    assert.equal(r.rows.length, 2);
  });

  it('does NOT push LIMIT with DISTINCT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1), (1), (2), (2), (3)');
    
    const r = db.execute('SELECT DISTINCT val FROM t LIMIT 2');
    assert.equal(r.rows.length, 2);
  });
});
