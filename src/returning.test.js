// returning.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('RETURNING clause', () => {
  it('INSERT RETURNING *', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    const r = db.execute("INSERT INTO t VALUES (1, 'Alice') RETURNING *");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[0].name, 'Alice');
  });

  it('INSERT RETURNING specific columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
    const r = db.execute("INSERT INTO t VALUES (1, 'test', 42) RETURNING id, val");
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[0].val, 42);
    assert.equal(r.rows[0].name, undefined); // Not requested
  });
});
