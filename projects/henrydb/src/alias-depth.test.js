// alias-depth.test.js — Aliasing depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-alias-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Table Aliases', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('table alias in JOIN', () => {
    db.execute('CREATE TABLE orders (id INT, customer_id INT)');
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute('INSERT INTO orders VALUES (1, 1)');

    const r = rows(db.execute(
      'SELECT o.id AS order_id, c.name FROM orders o INNER JOIN customers c ON o.customer_id = c.id'
    ));
    assert.equal(r.length, 1);
    assert.equal(r[0].name, 'Alice');
  });

  it('same table aliased differently', () => {
    db.execute('CREATE TABLE t (id INT, parent_id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, NULL, 'root')");
    db.execute("INSERT INTO t VALUES (2, 1, 'child')");

    const r = rows(db.execute(
      'SELECT a.name AS child, b.name AS parent ' +
      'FROM t a INNER JOIN t b ON a.parent_id = b.id'
    ));
    assert.equal(r.length, 1);
    assert.equal(r[0].child, 'child');
    assert.equal(r[0].parent, 'root');
  });
});

describe('Column Aliases', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('column alias in SELECT', () => {
    db.execute('CREATE TABLE t (id INT, value INT)');
    db.execute('INSERT INTO t VALUES (1, 42)');

    const r = rows(db.execute('SELECT value AS v FROM t'));
    assert.equal(r[0].v, 42);
  });

  it('column alias in ORDER BY', () => {
    db.execute('CREATE TABLE t (id INT, score INT)');
    db.execute('INSERT INTO t VALUES (1, 30)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, 20)');

    const r = rows(db.execute('SELECT id, score AS s FROM t ORDER BY s'));
    assert.equal(r[0].id, 2);
    assert.equal(r[1].id, 3);
    assert.equal(r[2].id, 1);
  });

  it('expression alias', () => {
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (3, 4)');

    const r = rows(db.execute('SELECT a + b AS total FROM t'));
    assert.equal(r[0].total, 7);
  });
});

describe('Subquery Aliases', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('derived table with alias', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');

    const r = rows(db.execute(
      'SELECT sub.total FROM (SELECT SUM(val) AS total FROM t) AS sub'
    ));
    assert.equal(r[0].total, 60);
  });
});
