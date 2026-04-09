// explain-compiled.test.js — Tests for EXPLAIN COMPILED SQL integration
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('EXPLAIN COMPILED', () => {
  
  it('basic EXPLAIN COMPILED single table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);

    const r = db.execute('EXPLAIN COMPILED SELECT * FROM t WHERE val > 500');
    assert.ok(r.message.includes('Compiled Query Plan'));
    assert.ok(r.message.includes('TABLE_SCAN'));
    assert.ok(r.compiled, 'Should report as compilable');
  });

  it('EXPLAIN COMPILED join', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT, val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO a VALUES (${i}, 'a${i}')`);
    for (let i = 0; i < 200; i++) db.execute(`INSERT INTO b VALUES (${i}, ${i % 100}, ${i})`);

    const r = db.execute('EXPLAIN COMPILED SELECT * FROM a JOIN b ON a.id = b.a_id');
    assert.ok(r.message.includes('HASH_JOIN') || r.message.includes('Join'));
    assert.ok(r.compiled);
  });

  it('EXPLAIN COMPILED reports not compilable for tiny table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    for (let i = 0; i < 5; i++) db.execute(`INSERT INTO t VALUES (${i})`);

    const r = db.execute('EXPLAIN COMPILED SELECT * FROM t');
    assert.ok(r.message.includes('NO') || !r.compiled);
  });

  it('EXPLAIN COMPILED shows join strategies', () => {
    const db = new Database();
    db.execute('CREATE TABLE c (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE o (id INT PRIMARY KEY, cid INT, amt INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO c VALUES (${i}, 'c${i}')`);
    for (let i = 0; i < 500; i++) db.execute(`INSERT INTO o VALUES (${i}, ${i % 100}, ${i * 10})`);

    const r = db.execute('EXPLAIN COMPILED SELECT * FROM c JOIN o ON c.id = o.cid');
    assert.ok(r.message.includes('Join strategies'));
    assert.ok(r.estimatedCost > 0);
  });

  it('regular EXPLAIN still works', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);

    const r = db.execute('EXPLAIN SELECT * FROM t WHERE id = 5');
    assert.ok(r.type === 'PLAN' || r.type === 'ROWS' || r.plan || r.rows);
  });

  it('EXPLAIN ANALYZE still works', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);

    const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE val > 25');
    assert.ok(r.rows || r.plan || r.message);
  });
});
