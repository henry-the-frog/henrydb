// plsql.test.js — Tests for PL/HenryDB procedural language interpreter
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { PLParser, PLInterpreter, PLRaise } from './plsql.js';
import { Database } from './db.js';

let db;

function run(source, params = {}) {
  const parser = new PLParser(source);
  const ast = parser.parse();
  const interp = new PLInterpreter(db);
  const result = interp.execute(ast, params);
  return { result, notices: interp.notices };
}

describe('PL/HenryDB', () => {
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)');
    db.execute("INSERT INTO items VALUES (1, 'Alpha', 10)");
    db.execute("INSERT INTO items VALUES (2, 'Beta', 20)");
    db.execute("INSERT INTO items VALUES (3, 'Gamma', 30)");
  });

  test('simple variable declaration and return', () => {
    const { result } = run(`
      DECLARE
        x INTEGER := 42;
      BEGIN
        RETURN x;
      END;
    `);
    assert.equal(result, 42);
  });

  test('assignment and arithmetic', () => {
    const { result } = run(`
      DECLARE
        x INTEGER := 10;
        y INTEGER := 20;
        z INTEGER;
      BEGIN
        z := x + y;
        RETURN z;
      END;
    `);
    assert.equal(result, 30);
  });

  test('IF/THEN/ELSE', () => {
    const { result } = run(`
      DECLARE
        x INTEGER := 15;
        result TEXT;
      BEGIN
        IF x > 10 THEN
          result := 'big';
        ELSE
          result := 'small';
        END IF;
        RETURN result;
      END;
    `);
    assert.equal(result, 'big');
  });

  test('IF/ELSIF/ELSE', () => {
    const { result } = run(`
      DECLARE
        x INTEGER := 5;
        label TEXT;
      BEGIN
        IF x > 10 THEN
          label := 'big';
        ELSIF x > 3 THEN
          label := 'medium';
        ELSE
          label := 'small';
        END IF;
        RETURN label;
      END;
    `);
    assert.equal(result, 'medium');
  });

  test('WHILE loop', () => {
    const { result } = run(`
      DECLARE
        i INTEGER := 0;
        total INTEGER := 0;
      BEGIN
        WHILE i < 5 LOOP
          total := total + i;
          i := i + 1;
        END LOOP;
        RETURN total;
      END;
    `);
    assert.equal(result, 10); // 0+1+2+3+4
  });

  test('FOR loop', () => {
    const { result } = run(`
      DECLARE
        total INTEGER := 0;
      BEGIN
        FOR i IN 1..5 LOOP
          total := total + i;
        END LOOP;
        RETURN total;
      END;
    `);
    assert.equal(result, 15); // 1+2+3+4+5
  });

  test('FOR REVERSE loop', () => {
    const { result } = run(`
      DECLARE
        s TEXT := '';
      BEGIN
        FOR i IN REVERSE 1..3 LOOP
          s := s || i;
        END LOOP;
        RETURN s;
      END;
    `);
    assert.equal(result, '321');
  });

  test('RAISE NOTICE', () => {
    const { notices } = run(`
      BEGIN
        RAISE NOTICE 'hello %', 'world';
      END;
    `);
    assert.equal(notices.length, 1);
    assert.equal(notices[0].level, 'NOTICE');
    assert.equal(notices[0].message, 'hello world');
  });

  test('RAISE EXCEPTION', () => {
    assert.throws(() => {
      run(`
        BEGIN
          RAISE EXCEPTION 'something went wrong';
        END;
      `);
    }, PLRaise);
  });

  test('parameters passed to block', () => {
    const { result } = run(`
      DECLARE
        doubled INTEGER;
      BEGIN
        doubled := n * 2;
        RETURN doubled;
      END;
    `, { n: 21 });
    assert.equal(result, 42);
  });

  test('string concatenation with ||', () => {
    const { result } = run(`
      DECLARE
        greeting TEXT;
      BEGIN
        greeting := 'Hello' || ' ' || 'World';
        RETURN greeting;
      END;
    `);
    assert.equal(result, 'Hello World');
  });

  test('NULL handling', () => {
    const { result } = run(`
      DECLARE
        x INTEGER;
      BEGIN
        IF x IS NULL THEN
          RETURN 'null';
        ELSE
          RETURN 'not null';
        END IF;
      END;
    `);
    assert.equal(result, 'null');
  });

  test('nested IF statements', () => {
    const { result } = run(`
      DECLARE
        x INTEGER := 15;
        y INTEGER := 5;
        r TEXT;
      BEGIN
        IF x > 10 THEN
          IF y > 3 THEN
            r := 'both';
          ELSE
            r := 'just x';
          END IF;
        ELSE
          r := 'neither';
        END IF;
        RETURN r;
      END;
    `);
    assert.equal(result, 'both');
  });

  test('EXECUTE dynamic SQL', () => {
    const { result } = run(`
      DECLARE
        cnt INTEGER;
      BEGIN
        EXECUTE 'SELECT COUNT(*) as cnt FROM items' INTO cnt;
        RETURN cnt;
      END;
    `);
    assert.equal(result, 3);
  });

  test('SELECT INTO captures query result', () => {
    const { result } = run(`
      DECLARE
        total INTEGER;
      BEGIN
        SELECT SUM(price) INTO total FROM items;
        RETURN total;
      END;
    `);
    assert.equal(result, 60);
  });

  test('EXCEPTION handler catches RAISE', () => {
    const { result, notices } = run(`
      DECLARE
        msg TEXT;
      BEGIN
        RAISE EXCEPTION 'oops';
      EXCEPTION
        WHEN OTHERS THEN
          msg := 'caught';
          RETURN msg;
      END;
    `);
    assert.equal(result, 'caught');
  });

  test('infinite loop protection', () => {
    assert.throws(() => {
      run(`
        DECLARE
          x INTEGER := 0;
        BEGIN
          WHILE TRUE LOOP
            x := x + 1;
          END LOOP;
        END;
      `);
    }, /infinite loop/);
  });

  test('FOUND variable after SELECT INTO', () => {
    const { result } = run(`
      DECLARE
        val INTEGER;
        was_found BOOLEAN;
      BEGIN
        SELECT price INTO val FROM items WHERE id = 1;
        was_found := FOUND;
        RETURN was_found;
      END;
    `);
    assert.equal(result, true);
  });

  test('multiple RAISE NOTICE messages', () => {
    const { notices } = run(`
      BEGIN
        RAISE NOTICE 'step %', 1;
        RAISE NOTICE 'step %', 2;
        RAISE NOTICE 'step %', 3;
      END;
    `);
    assert.equal(notices.length, 3);
    assert.equal(notices[0].message, 'step 1');
    assert.equal(notices[2].message, 'step 3');
  });

  test('PERFORM executes SQL without returning', () => {
    run(`
      BEGIN
        PERFORM INSERT INTO items VALUES (4, 'Delta', 40);
      END;
    `);
    const r = db.execute('SELECT * FROM items WHERE id = 4');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Delta');
  });
});
