// string-functions.test.js — String function correctness tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('String Functions', () => {
  it('UPPER/LOWER', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT UPPER('hello') as r").rows[0].r, 'HELLO');
    assert.equal(db.execute("SELECT LOWER('HELLO') as r").rows[0].r, 'hello');
  });

  it('LENGTH', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT LENGTH('hello') as r").rows[0].r, 5);
    assert.equal(db.execute("SELECT LENGTH('') as r").rows[0].r, 0);
  });

  it('TRIM', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT TRIM('  hello  ') as r").rows[0].r, 'hello');
  });

  it('LTRIM/RTRIM', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT LTRIM('  hello') as r").rows[0].r, 'hello');
    assert.equal(db.execute("SELECT RTRIM('hello  ') as r").rows[0].r, 'hello');
  });

  it('LEFT/RIGHT', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT LEFT('hello', 3) as r").rows[0].r, 'hel');
    assert.equal(db.execute("SELECT RIGHT('hello', 3) as r").rows[0].r, 'llo');
  });

  it('REPEAT', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT REPEAT('ab', 3) as r").rows[0].r, 'ababab');
  });

  it('REVERSE', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT REVERSE('hello') as r").rows[0].r, 'olleh');
  });

  it('LPAD/RPAD', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT LPAD('hi', 5, '*') as r").rows[0].r, '***hi');
    assert.equal(db.execute("SELECT RPAD('hi', 5, '*') as r").rows[0].r, 'hi***');
  });

  it('REPLACE', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT REPLACE('hello world', 'world', 'there') as r").rows[0].r, 'hello there');
  });

  it('SUBSTRING/SUBSTR', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT SUBSTRING('hello', 2, 3) as r").rows[0].r, 'ell');
    assert.equal(db.execute("SELECT SUBSTR('hello', 2, 3) as r").rows[0].r, 'ell');
  });

  it('CONCAT', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT CONCAT('hello', ' ', 'world') as r").rows[0].r, 'hello world');
  });

  it('string concatenation with ||', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT 'hello' || ' ' || 'world' as r").rows[0].r, 'hello world');
  });

  it('POSITION/LOCATE', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT POSITION('ll' IN 'hello') as r").rows[0].r, 3);
  });

  it('LIKE with %', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('apple'),('banana'),('avocado'),('blueberry')");
    const r = db.execute("SELECT val FROM t WHERE val LIKE 'a%' ORDER BY val");
    assert.deepEqual(r.rows.map(r => r.val), ['apple', 'avocado']);
  });

  it('LIKE with _', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('cat'),('car'),('cap'),('cob')");
    const r = db.execute("SELECT val FROM t WHERE val LIKE 'ca_' ORDER BY val");
    assert.deepEqual(r.rows.map(r => r.val), ['cap', 'car', 'cat']);
  });

  it('ILIKE case-insensitive', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('Apple'),('BANANA'),('cherry')");
    const r = db.execute("SELECT val FROM t WHERE val ILIKE 'a%' ORDER BY val");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 'Apple');
  });

  it('INITCAP', () => {
    const db = new Database();
    const r = db.execute("SELECT INITCAP('hello world') as r");
    assert.equal(r.rows[0].r, 'Hello World');
  });

  it('string functions with table data and || operator', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (first_name TEXT, last_name TEXT)');
    db.execute("INSERT INTO users VALUES ('john','doe'),('jane','smith')");
    const r = db.execute(`
      SELECT UPPER(first_name) || ' ' || UPPER(last_name) as name,
             LENGTH(first_name) + LENGTH(last_name) as name_len
      FROM users
      ORDER BY last_name
    `);
    assert.equal(r.rows[0].name, 'JOHN DOE');
    assert.equal(r.rows[0].name_len, 7);
  });
});
