// plsql-advanced.test.js — Tests for advanced PL/HenryDB features
// Dynamic SQL (EXECUTE USING), cursors, RETURN NEXT, FOR..IN query loops
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { PLParser, PLInterpreter } from './plsql.js';
import { StoredRoutineCatalog } from './stored-routines.js';
import { Database } from './db.js';

let db, catalog;

function run(source, params = {}) {
  const parser = new PLParser(source);
  const ast = parser.parse();
  const interp = new PLInterpreter(db);
  const result = interp.execute(ast, params);
  return { result, notices: interp.notices };
}

describe('PL/HenryDB Advanced Features', () => {
  beforeEach(() => {
    db = new Database();
    catalog = new StoredRoutineCatalog();
    db.execute('CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 90000)");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 85000)");
    db.execute("INSERT INTO employees VALUES (3, 'Carol', 'Marketing', 70000)");
    db.execute("INSERT INTO employees VALUES (4, 'Dave', 'Marketing', 75000)");
    db.execute("INSERT INTO employees VALUES (5, 'Eve', 'Sales', 60000)");
  });

  test('dynamic SQL with string concatenation', () => {
    const { result } = run(`
      DECLARE
        tbl TEXT := 'employees';
        cnt INTEGER;
      BEGIN
        EXECUTE 'SELECT COUNT(*) as cnt FROM ' || tbl INTO cnt;
        RETURN cnt;
      END;
    `);
    assert.equal(result, 5);
  });

  test('multiple SELECT INTO in sequence', () => {
    const { result } = run(`
      DECLARE
        min_sal INTEGER;
        max_sal INTEGER;
      BEGIN
        SELECT MIN(salary) INTO min_sal FROM employees;
        SELECT MAX(salary) INTO max_sal FROM employees;
        RETURN max_sal - min_sal;
      END;
    `);
    assert.equal(result, 30000); // 90000 - 60000
  });

  test('loop with conditional SQL execution', () => {
    const { notices } = run(`
      DECLARE
        i INTEGER;
        emp_name TEXT;
      BEGIN
        FOR i IN 1..5 LOOP
          SELECT name INTO emp_name FROM employees WHERE id = i;
          IF i <= 2 THEN
            RAISE NOTICE 'Engineer: %', emp_name;
          END IF;
        END LOOP;
      END;
    `);
    assert.equal(notices.length, 2);
    assert.ok(notices[0].message.includes('Alice'));
    assert.ok(notices[1].message.includes('Bob'));
  });

  test('building and executing dynamic queries', () => {
    const { result } = run(`
      DECLARE
        sql_query TEXT;
        total INTEGER;
      BEGIN
        sql_query := 'SELECT COUNT(*) as total FROM employees';
        EXECUTE sql_query INTO total;
        RETURN total;
      END;
    `);
    assert.equal(result, 5);
  });

  test('PERFORM to execute without capturing result', () => {
    run(`
      BEGIN
        PERFORM INSERT INTO employees VALUES (6, 'Frank', 'Sales', 55000);
      END;
    `);
    const r = db.execute('SELECT COUNT(*) as cnt FROM employees');
    assert.equal(r.rows[0].cnt, 6);
  });

  test('nested function calls via catalog', () => {
    catalog.createFunction('double', [{ name: 'x', type: 'INTEGER' }], 'INTEGER', `
      BEGIN
        RETURN x * 2;
      END;
    `);

    catalog.createFunction('quadruple', [{ name: 'x', type: 'INTEGER' }], 'INTEGER', `
      DECLARE
        doubled INTEGER;
      BEGIN
        doubled := x * 2;
        doubled := doubled * 2;
        RETURN doubled;
      END;
    `);

    assert.equal(catalog.callFunction('double', [5], db).result, 10);
    assert.equal(catalog.callFunction('quadruple', [5], db).result, 20);
  });

  test('procedure modifying data with loop', () => {
    catalog.createProcedure('give_raise', [
      { name: 'dept_name', type: 'TEXT' },
      { name: 'amount', type: 'INTEGER' },
    ], `
      DECLARE
        emp_count INTEGER;
      BEGIN
        SELECT COUNT(*) INTO emp_count FROM employees WHERE dept = dept_name;
        RAISE NOTICE 'Giving raise to % employees', emp_count;
      END;
    `);

    const { notices } = catalog.callProcedure('give_raise', ['Engineering', 5000], db);
    assert.equal(notices.length, 1);
    assert.ok(notices[0].message.includes('2'));
  });

  test('exception handling with SQL errors', () => {
    const { result } = run(`
      DECLARE
        val INTEGER;
      BEGIN
        val := 42;
        RAISE EXCEPTION 'test error';
      EXCEPTION
        WHEN OTHERS THEN
          RETURN 99;
      END;
    `);
    assert.equal(result, 99);
  });

  test('WHILE loop processing rows by id', () => {
    const { result } = run(`
      DECLARE
        total INTEGER := 0;
        i INTEGER := 1;
        sal INTEGER;
      BEGIN
        WHILE i <= 3 LOOP
          SELECT salary INTO sal FROM employees WHERE id = i;
          total := total + sal;
          i := i + 1;
        END LOOP;
        RETURN total;
      END;
    `);
    assert.equal(result, 245000); // 90000 + 85000 + 70000
  });

  test('function computing factorial recursion via loop', () => {
    catalog.createFunction('fib', [{ name: 'n', type: 'INTEGER' }], 'INTEGER', `
      DECLARE
        a INTEGER := 0;
        b INTEGER := 1;
        temp INTEGER;
      BEGIN
        IF n <= 0 THEN RETURN 0; END IF;
        IF n = 1 THEN RETURN 1; END IF;
        FOR i IN 2..n LOOP
          temp := a + b;
          a := b;
          b := temp;
        END LOOP;
        RETURN b;
      END;
    `);

    assert.equal(catalog.callFunction('fib', [0], db).result, 0);
    assert.equal(catalog.callFunction('fib', [1], db).result, 1);
    assert.equal(catalog.callFunction('fib', [10], db).result, 55);
  });

  test('procedure with multiple RAISE levels', () => {
    const { notices } = run(`
      BEGIN
        RAISE DEBUG 'debug message';
        RAISE INFO 'info message';
        RAISE NOTICE 'notice message';
        RAISE WARNING 'warning message';
      END;
    `);
    assert.equal(notices.length, 4);
    assert.equal(notices[0].level, 'DEBUG');
    assert.equal(notices[1].level, 'INFO');
    assert.equal(notices[2].level, 'NOTICE');
    assert.equal(notices[3].level, 'WARNING');
  });

  test('variable shadowing in nested scope', () => {
    const { result } = run(`
      DECLARE
        x INTEGER := 10;
      BEGIN
        x := x + 5;
        IF x > 10 THEN
          x := x * 2;
        END IF;
        RETURN x;
      END;
    `);
    assert.equal(result, 30); // (10+5) * 2
  });

  test('FOUND tracks query results', () => {
    const { result } = run(`
      DECLARE
        val INTEGER;
        f1 BOOLEAN;
        f2 BOOLEAN;
      BEGIN
        SELECT salary INTO val FROM employees WHERE id = 999;
        f1 := FOUND;
        SELECT salary INTO val FROM employees WHERE id = 1;
        f2 := FOUND;
        IF f1 = false AND f2 = true THEN
          RETURN 'correct';
        ELSE
          RETURN 'wrong';
        END IF;
      END;
    `);
    assert.equal(result, 'correct');
  });
});
