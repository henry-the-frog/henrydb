// ctas.test.js — CREATE TABLE AS SELECT tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CREATE TABLE AS SELECT', () => {
  it('creates table from query result', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO src VALUES (1, 10)');
    db.execute('INSERT INTO src VALUES (2, 20)');
    db.execute('INSERT INTO src VALUES (3, 30)');

    db.execute('CREATE TABLE dst AS SELECT id, val FROM src WHERE val > 15');
    const r = db.execute('SELECT * FROM dst');
    assert.equal(r.rows.length, 2);
    assert.deepEqual(r.rows.map(r => r.val).sort(), [20, 30]);
  });

  it('infers column types', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (id INT PRIMARY KEY, name TEXT, score INT)');
    db.execute("INSERT INTO src VALUES (1, 'test', 95)");

    db.execute('CREATE TABLE dst AS SELECT name, score FROM src');
    const desc = db.execute('DESCRIBE dst');
    const nameCol = desc.rows.find(r => r.column_name === 'name');
    assert.equal(nameCol.type, 'TEXT');
  });

  it('creates empty table from empty result', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE dst AS SELECT * FROM src WHERE val > 1000');
    const r = db.execute('SELECT * FROM dst');
    assert.equal(r.rows.length, 0);
  });
});
