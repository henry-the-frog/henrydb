// plsql.test.js — Integration tests for PL/SQL procedural functions
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('PL/SQL: basic functions', () => {
  it('factorial with WHILE loop', () => {
    const db = new Database();
    db.execute(`
      CREATE FUNCTION factorial(n INTEGER) RETURNS INTEGER
      LANGUAGE plsql
      AS $$
        DECLARE
          result INTEGER := 1;
          i INTEGER := 1;
        BEGIN
          WHILE i <= n LOOP
            result := result * i;
            i := i + 1;
          END LOOP;
          RETURN result;
        END;
      $$
    `);
    const r = db.execute('SELECT factorial(5) as result');
    assert.strictEqual(r.rows[0].result, 120);
  });

  it('IF/ELSIF/ELSE classification', () => {
    const db = new Database();
    db.execute(`
      CREATE FUNCTION classify(x INTEGER) RETURNS TEXT
      LANGUAGE plsql
      AS $$
        BEGIN
          IF x < 0 THEN
            RETURN 'negative';
          ELSIF x = 0 THEN
            RETURN 'zero';
          ELSIF x < 10 THEN
            RETURN 'small';
          ELSE
            RETURN 'large';
          END IF;
        END;
      $$
    `);
    assert.strictEqual(db.execute('SELECT classify(-5) as c').rows[0].c, 'negative');
    assert.strictEqual(db.execute('SELECT classify(0) as c').rows[0].c, 'zero');
    assert.strictEqual(db.execute('SELECT classify(7) as c').rows[0].c, 'small');
    assert.strictEqual(db.execute('SELECT classify(42) as c').rows[0].c, 'large');
  });

  it('simple RETURN without DECLARE', () => {
    const db = new Database();
    db.execute(`
      CREATE FUNCTION double(x INTEGER) RETURNS INTEGER
      LANGUAGE plsql
      AS $$
        BEGIN
          RETURN x * 2;
        END;
      $$
    `);
    const r = db.execute('SELECT double(21) as result');
    assert.strictEqual(r.rows[0].result, 42);
  });
});

describe('PL/SQL: SQL integration', () => {
  it('SELECT INTO from table', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (name TEXT, price INTEGER)');
    db.execute("INSERT INTO items VALUES ('Widget', 10)");
    db.execute("INSERT INTO items VALUES ('Gadget', 25)");
    db.execute("INSERT INTO items VALUES ('Doohickey', 15)");

    db.execute(`
      CREATE FUNCTION max_price() RETURNS INTEGER
      LANGUAGE plsql
      AS $$
        DECLARE
          result INTEGER;
        BEGIN
          SELECT MAX(price) INTO result FROM items;
          RETURN result;
        END;
      $$
    `);
    const r = db.execute('SELECT max_price() as mp');
    assert.strictEqual(r.rows[0].mp, 25);
  });

  it('use PL/SQL function in WHERE clause', () => {
    const db = new Database();
    db.execute(`
      CREATE FUNCTION is_even(n INTEGER) RETURNS INTEGER
      LANGUAGE plsql
      AS $$
        BEGIN
          IF n % 2 = 0 THEN
            RETURN 1;
          ELSE
            RETURN 0;
          END IF;
        END;
      $$
    `);
    db.execute('CREATE TABLE nums (n INTEGER)');
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO nums VALUES (${i})`);
    }
    const r = db.execute('SELECT n FROM nums WHERE is_even(n) = 1 ORDER BY n');
    assert.deepStrictEqual(r.rows.map(r => r.n), [2, 4, 6, 8, 10]);
  });
});

describe('PL/SQL: LANGUAGE before AS (SQL standard order)', () => {
  it('LANGUAGE before AS works', () => {
    const db = new Database();
    db.execute(`
      CREATE FUNCTION add_one(x INTEGER) RETURNS INTEGER
      LANGUAGE plsql
      AS $$ BEGIN RETURN x + 1; END; $$
    `);
    assert.strictEqual(db.execute('SELECT add_one(41) as r').rows[0].r, 42);
  });
});

describe('PL/SQL: auto-detection', () => {
  it('detects PL/SQL from DECLARE keyword', () => {
    const db = new Database();
    db.execute(`
      CREATE FUNCTION sum_range(low INTEGER, high INTEGER) RETURNS INTEGER
      AS $$
        DECLARE
          total INTEGER := 0;
          i INTEGER;
        BEGIN
          i := low;
          WHILE i <= high LOOP
            total := total + i;
            i := i + 1;
          END LOOP;
          RETURN total;
        END;
      $$
    `);
    assert.strictEqual(db.execute('SELECT sum_range(1, 10) as r').rows[0].r, 55);
  });

  it('detects PL/SQL from BEGIN keyword', () => {
    const db = new Database();
    db.execute(`
      CREATE FUNCTION negate(x INTEGER) RETURNS INTEGER
      AS $$ BEGIN RETURN -x; END; $$
    `);
    assert.strictEqual(db.execute('SELECT negate(42) as r').rows[0].r, -42);
  });
});

describe('PL/SQL: recursive functions', () => {
  it('recursive factorial', () => {
    const db = new Database();
    db.execute(`
      CREATE FUNCTION rec_fact(n INTEGER) RETURNS INTEGER
      LANGUAGE plsql
      AS $$
        BEGIN
          IF n <= 1 THEN RETURN 1; END IF;
          RETURN n * rec_fact(n - 1);
        END;
      $$
    `);
    assert.strictEqual(db.execute('SELECT rec_fact(5) as r').rows[0].r, 120);
    assert.strictEqual(db.execute('SELECT rec_fact(1) as r').rows[0].r, 1);
  });
});

describe('PL/SQL: nested function calls', () => {
  it('PL/SQL function calling PL/SQL function', () => {
    const db = new Database();
    db.execute(`CREATE FUNCTION inc(x INTEGER) RETURNS INTEGER
      LANGUAGE plsql AS $$ BEGIN RETURN x + 1; END; $$`);
    db.execute(`CREATE FUNCTION inc_twice(x INTEGER) RETURNS INTEGER
      LANGUAGE plsql AS $$ BEGIN RETURN inc(inc(x)); END; $$`);
    assert.strictEqual(db.execute('SELECT inc_twice(40) as r').rows[0].r, 42);
  });
});

describe('PL/SQL: FOR loop', () => {
  it('FOR i IN range LOOP', () => {
    const db = new Database();
    db.execute(`
      CREATE FUNCTION sum_for(n INTEGER) RETURNS INTEGER
      LANGUAGE plsql AS $$
        DECLARE total INTEGER := 0;
        BEGIN
          FOR i IN 1..n LOOP
            total := total + i;
          END LOOP;
          RETURN total;
        END;
      $$
    `);
    assert.strictEqual(db.execute('SELECT sum_for(10) as r').rows[0].r, 55);
  });
});

describe('PL/SQL: string concatenation', () => {
  it('concatenate with ||', () => {
    const db = new Database();
    db.execute(`CREATE FUNCTION greet(name TEXT) RETURNS TEXT
      LANGUAGE plsql AS $$ BEGIN RETURN 'Hello, ' || name || '!'; END; $$`);
    assert.strictEqual(db.execute("SELECT greet('World') as r").rows[0].r, 'Hello, World!');
  });
});
