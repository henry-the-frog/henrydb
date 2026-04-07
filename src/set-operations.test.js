// set-operations.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Set Operations', () => {
  it('INTERSECT returns common rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (val INT)');
    db.execute('CREATE TABLE b (val INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO a VALUES (2)');
    db.execute('INSERT INTO a VALUES (3)');
    db.execute('INSERT INTO b VALUES (2)');
    db.execute('INSERT INTO b VALUES (3)');
    db.execute('INSERT INTO b VALUES (4)');

    const r = db.execute('SELECT val FROM a INTERSECT SELECT val FROM b');
    assert.equal(r.rows.length, 2);
    const vals = r.rows.map(r => r.val).sort();
    assert.deepEqual(vals, [2, 3]);
  });

  it('EXCEPT returns rows only in left', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (val INT)');
    db.execute('CREATE TABLE b (val INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO a VALUES (2)');
    db.execute('INSERT INTO a VALUES (3)');
    db.execute('INSERT INTO b VALUES (2)');
    db.execute('INSERT INTO b VALUES (3)');

    const r = db.execute('SELECT val FROM a EXCEPT SELECT val FROM b');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 1);
  });

  it('INTERSECT with empty result', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (val INT)');
    db.execute('CREATE TABLE b (val INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO b VALUES (2)');

    const r = db.execute('SELECT val FROM a INTERSECT SELECT val FROM b');
    assert.equal(r.rows.length, 0);
  });

  it('EXCEPT returns all when no overlap', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (val INT)');
    db.execute('CREATE TABLE b (val INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO b VALUES (2)');

    const r = db.execute('SELECT val FROM a EXCEPT SELECT val FROM b');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 1);
  });
});
