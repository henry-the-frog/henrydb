import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Identifier Quoting', () => {
  test('backtick-quoted table name', () => {
    const db = new Database();
    db.execute('CREATE TABLE `my table` (id INT, name TEXT)');
    db.execute("INSERT INTO `my table` VALUES (1, 'Alice')");
    const r = db.execute('SELECT * FROM `my table`');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[0].name, 'Alice');
  });

  test('backtick-quoted reserved words as column names', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (`order` INT, `select` TEXT, `from` TEXT)');
    db.execute("INSERT INTO t (`order`, `select`, `from`) VALUES (1, 'hello', 'world')");
    const r = db.execute('SELECT `order`, `select`, `from` FROM t');
    assert.equal(r.rows.length, 1);
    // Column names are uppercased internally
    const row = r.rows[0];
    const vals = Object.values(row);
    assert.ok(vals.includes(1));
    assert.ok(vals.includes('hello'));
    assert.ok(vals.includes('world'));
  });

  test('double-quoted column names', () => {
    const db = new Database();
    db.execute('CREATE TABLE t ("First Name" TEXT, "Last Name" TEXT)');
    db.execute("INSERT INTO t VALUES ('Alice', 'Smith')");
    const r = db.execute('SELECT "First Name", "Last Name" FROM t');
    assert.equal(r.rows.length, 1);
  });

  test('double-quoted reserved words', () => {
    const db = new Database();
    db.execute('CREATE TABLE t ("group" INT, "having" TEXT)');
    db.execute("INSERT INTO t VALUES (42, 'test')");
    const r = db.execute('SELECT "group", "having" FROM t');
    assert.equal(r.rows.length, 1);
  });

  test('backtick in WHERE clause', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (`key` INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    const r = db.execute('SELECT * FROM t WHERE `key` = 2');
    assert.equal(r.rows.length, 1);
  });

  test('backtick in ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (`order` INT)');
    db.execute('INSERT INTO t VALUES (3), (1), (2)');
    const r = db.execute('SELECT * FROM t ORDER BY `order`');
    const vals = r.rows.map(row => Object.values(row)[0]);
    assert.deepEqual(vals, [1, 2, 3]);
  });

  test('mixed quoting styles', () => {
    const db = new Database();
    db.execute('CREATE TABLE `t` ("a" INT, `b` TEXT)');
    db.execute("INSERT INTO `t` VALUES (1, 'hello')");
    const r = db.execute('SELECT "a", `b` FROM `t`');
    assert.equal(r.rows.length, 1);
  });

  test('quoting with spaces in column names', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (`first name` TEXT, age INT)');
    db.execute("INSERT INTO t VALUES ('Bob', 30)");
    const r = db.execute('SELECT `first name` FROM t');
    assert.equal(r.rows.length, 1);
  });

  test('regular unquoted identifiers still work', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'test')");
    const r = db.execute('SELECT id, name FROM t');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
  });

  test('backtick-quoted alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (42)');
    const r = db.execute('SELECT id AS `my id` FROM t');
    assert.equal(r.rows.length, 1);
  });
});
