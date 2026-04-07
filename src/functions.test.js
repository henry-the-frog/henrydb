// functions.test.js — Extended function library tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('String Functions', () => {
  it('LEFT returns first N characters', () => {
    const db = new Database();
    const r = db.execute("SELECT LEFT('Hello World', 5) AS result");
    assert.equal(r.rows[0].result, 'Hello');
  });

  it('RIGHT returns last N characters', () => {
    const db = new Database();
    const r = db.execute("SELECT RIGHT('Hello World', 5) AS result");
    assert.equal(r.rows[0].result, 'World');
  });

  it('LPAD pads string on left', () => {
    const db = new Database();
    const r = db.execute("SELECT LPAD('42', 5, '0') AS result");
    assert.equal(r.rows[0].result, '00042');
  });

  it('RPAD pads string on right', () => {
    const db = new Database();
    const r = db.execute("SELECT RPAD('Hi', 5, '.') AS result");
    assert.equal(r.rows[0].result, 'Hi...');
  });

  it('REVERSE reverses a string', () => {
    const db = new Database();
    const r = db.execute("SELECT REVERSE('Hello') AS result");
    assert.equal(r.rows[0].result, 'olleH');
  });

  it('REPEAT repeats a string', () => {
    const db = new Database();
    const r = db.execute("SELECT REPEAT('ab', 3) AS result");
    assert.equal(r.rows[0].result, 'ababab');
  });
});

describe('Math Functions', () => {
  it('POWER computes exponentiation', () => {
    const db = new Database();
    const r = db.execute('SELECT POWER(2, 10) AS result');
    assert.equal(r.rows[0].result, 1024);
  });

  it('SQRT computes square root', () => {
    const db = new Database();
    const r = db.execute('SELECT SQRT(144) AS result');
    assert.equal(r.rows[0].result, 12);
  });

  it('LOG computes natural logarithm', () => {
    const db = new Database();
    const r = db.execute('SELECT ROUND(LOG(2.718281828), 0) AS result');
    assert.equal(r.rows[0].result, 1);
  });

  it('RANDOM returns a value between 0 and 1', () => {
    const db = new Database();
    const r = db.execute('SELECT RANDOM() AS result');
    assert.ok(r.rows[0].result >= 0 && r.rows[0].result < 1);
  });
});

describe('Date/Time Functions', () => {
  it('CURRENT_TIMESTAMP returns ISO date string', () => {
    const db = new Database();
    const r = db.execute('SELECT CURRENT_TIMESTAMP AS ts');
    assert.ok(r.rows[0].ts.includes('T'));
    assert.ok(r.rows[0].ts.includes('Z'));
  });

  it('CURRENT_DATE returns date only', () => {
    const db = new Database();
    const r = db.execute('SELECT CURRENT_DATE AS d');
    assert.match(r.rows[0].d, /^\d{4}-\d{2}-\d{2}$/);
  });

  it('STRFTIME formats dates', () => {
    const db = new Database();
    const r = db.execute("SELECT STRFTIME('%Y', '2026-04-06T12:00:00Z') AS yr");
    assert.equal(r.rows[0].yr, '2026');
  });
});

describe('Functions on Table Data', () => {
  it('string functions on columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Hello')");
    db.execute("INSERT INTO t VALUES (2, 'World')");

    const r = db.execute('SELECT REVERSE(name) AS rev FROM t ORDER BY id');
    assert.equal(r.rows[0].rev, 'olleH');
    assert.equal(r.rows[1].rev, 'dlroW');
  });

  it('math functions on columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 16)');
    db.execute('INSERT INTO t VALUES (2, 25)');

    const r = db.execute('SELECT SQRT(val) AS root FROM t ORDER BY id');
    assert.equal(r.rows[0].root, 4);
    assert.equal(r.rows[1].root, 5);
  });
});
