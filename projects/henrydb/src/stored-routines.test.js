// stored-routines.test.js — Tests for CREATE FUNCTION/PROCEDURE, CALL, stored routine catalog
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { StoredRoutineCatalog, parseCreateRoutine, parseCall } from './stored-routines.js';

let db, catalog;

describe('StoredRoutineCatalog', () => {
  beforeEach(() => {
    db = new Database();
    catalog = new StoredRoutineCatalog();
    db.execute('CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, balance INTEGER)');
    db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000)");
    db.execute("INSERT INTO accounts VALUES (2, 'Bob', 500)");
  });

  test('create and call a function', () => {
    catalog.createFunction('add_nums', [
      { name: 'a', type: 'INTEGER' },
      { name: 'b', type: 'INTEGER' },
    ], 'INTEGER', `
      BEGIN
        RETURN a + b;
      END;
    `);

    const { result } = catalog.callFunction('add_nums', [3, 4], db);
    assert.equal(result, 7);
  });

  test('create and call a procedure', () => {
    catalog.createProcedure('reset_balance', [
      { name: 'account_id', type: 'INTEGER' },
      { name: 'new_balance', type: 'INTEGER' },
    ], `
      BEGIN
        PERFORM UPDATE accounts SET balance = 0 WHERE id = 1;
      END;
    `);

    const { notices } = catalog.callProcedure('reset_balance', [1, 0], db);
    assert.ok(notices !== undefined);
  });

  test('function with default parameters', () => {
    catalog.createFunction('greet', [
      { name: 'name', type: 'TEXT' },
      { name: 'greeting', type: 'TEXT', default: 'Hello' },
    ], 'TEXT', `
      BEGIN
        RETURN greeting || ' ' || name;
      END;
    `);

    const { result: r1 } = catalog.callFunction('greet', ['World'], db);
    assert.equal(r1, 'Hello World');

    const { result: r2 } = catalog.callFunction('greet', ['World', 'Hi'], db);
    assert.equal(r2, 'Hi World');
  });

  test('function with SQL query', () => {
    catalog.createFunction('get_balance', [
      { name: 'aid', type: 'INTEGER' },
    ], 'INTEGER', `
      DECLARE
        bal INTEGER;
      BEGIN
        SELECT balance INTO bal FROM accounts WHERE id = aid;
        RETURN bal;
      END;
    `);

    const { result } = catalog.callFunction('get_balance', [1], db);
    assert.equal(result, 1000);
  });

  test('procedure with RAISE NOTICE', () => {
    catalog.createProcedure('log_action', [
      { name: 'msg', type: 'TEXT' },
    ], `
      BEGIN
        RAISE NOTICE 'Action: %', msg;
      END;
    `);

    const { notices } = catalog.callProcedure('log_action', ['test'], db);
    assert.equal(notices.length, 1);
    assert.equal(notices[0].message, 'Action: test');
  });

  test('drop function', () => {
    catalog.createFunction('temp_fn', [], 'INTEGER', 'BEGIN RETURN 1; END;');
    assert.ok(catalog.hasFunction('temp_fn'));
    catalog.dropFunction('temp_fn');
    assert.ok(!catalog.hasFunction('temp_fn'));
  });

  test('drop function IF EXISTS', () => {
    const result = catalog.dropFunction('nonexistent', true);
    assert.equal(result, false);
  });

  test('OR REPLACE function', () => {
    catalog.createFunction('my_fn', [], 'INTEGER', 'BEGIN RETURN 1; END;');
    assert.throws(() => {
      catalog.createFunction('my_fn', [], 'INTEGER', 'BEGIN RETURN 2; END;');
    });
    catalog.createFunction('my_fn', [], 'INTEGER', 'BEGIN RETURN 2; END;', { orReplace: true });
    const { result } = catalog.callFunction('my_fn', [], db);
    assert.equal(result, 2);
  });

  test('listRoutines', () => {
    catalog.createFunction('fn1', [], 'INTEGER', 'BEGIN RETURN 1; END;');
    catalog.createProcedure('proc1', [], 'BEGIN NULL; END;');
    const routines = catalog.listRoutines();
    assert.equal(routines.length, 2);
    assert.ok(routines.some(r => r.type === 'FUNCTION'));
    assert.ok(routines.some(r => r.type === 'PROCEDURE'));
  });

  test('function with conditional logic', () => {
    catalog.createFunction('classify_balance', [
      { name: 'aid', type: 'INTEGER' },
    ], 'TEXT', `
      DECLARE
        bal INTEGER;
        label TEXT;
      BEGIN
        SELECT balance INTO bal FROM accounts WHERE id = aid;
        IF bal >= 1000 THEN
          label := 'high';
        ELSIF bal >= 500 THEN
          label := 'medium';
        ELSE
          label := 'low';
        END IF;
        RETURN label;
      END;
    `);

    assert.equal(catalog.callFunction('classify_balance', [1], db).result, 'high');
    assert.equal(catalog.callFunction('classify_balance', [2], db).result, 'medium');
  });

  test('function with loop', () => {
    catalog.createFunction('factorial', [
      { name: 'n', type: 'INTEGER' },
    ], 'INTEGER', `
      DECLARE
        result INTEGER := 1;
      BEGIN
        FOR i IN 1..n LOOP
          result := result * i;
        END LOOP;
        RETURN result;
      END;
    `);

    assert.equal(catalog.callFunction('factorial', [5], db).result, 120);
    assert.equal(catalog.callFunction('factorial', [0], db).result, 1);
  });
});

describe('parseCreateRoutine', () => {
  test('parse CREATE FUNCTION with dollar quoting', () => {
    const parsed = parseCreateRoutine(`
      CREATE FUNCTION add(a INTEGER, b INTEGER)
      RETURNS INTEGER
      LANGUAGE plhenrydb
      AS $$ BEGIN RETURN a + b; END; $$
    `);
    assert.equal(parsed.kind, 'FUNCTION');
    assert.equal(parsed.name, 'add');
    assert.equal(parsed.params.length, 2);
    assert.equal(parsed.returnType, 'INTEGER');
    assert.ok(parsed.body.includes('RETURN'));
  });

  test('parse CREATE OR REPLACE FUNCTION', () => {
    const parsed = parseCreateRoutine(`
      CREATE OR REPLACE FUNCTION greet(name TEXT)
      RETURNS TEXT
      LANGUAGE plhenrydb
      AS $$ BEGIN RETURN name; END; $$
    `);
    assert.equal(parsed.orReplace, true);
    assert.equal(parsed.name, 'greet');
  });

  test('parse CREATE PROCEDURE', () => {
    const parsed = parseCreateRoutine(`
      CREATE PROCEDURE do_stuff(x INTEGER)
      LANGUAGE plhenrydb
      AS $$ BEGIN NULL; END; $$
    `);
    assert.equal(parsed.kind, 'PROCEDURE');
    assert.equal(parsed.name, 'do_stuff');
    assert.equal(parsed.returnType, null);
  });

  test('parse IMMUTABLE function', () => {
    const parsed = parseCreateRoutine(`
      CREATE FUNCTION pure(x INTEGER)
      RETURNS INTEGER
      LANGUAGE plhenrydb
      IMMUTABLE
      AS $$ BEGIN RETURN x * 2; END; $$
    `);
    assert.equal(parsed.volatile, 'IMMUTABLE');
  });
});

describe('parseCall', () => {
  test('parse CALL with arguments', () => {
    const parsed = parseCall("CALL reset_balance(1, 0)");
    assert.equal(parsed.name, 'reset_balance');
    assert.deepEqual(parsed.args, [1, 0]);
  });

  test('parse CALL with string arguments', () => {
    const parsed = parseCall("CALL log_msg('hello world')");
    assert.equal(parsed.name, 'log_msg');
    assert.deepEqual(parsed.args, ['hello world']);
  });

  test('parse CALL with no arguments', () => {
    const parsed = parseCall("CALL do_stuff()");
    assert.equal(parsed.name, 'do_stuff');
    assert.deepEqual(parsed.args, []);
  });
});
