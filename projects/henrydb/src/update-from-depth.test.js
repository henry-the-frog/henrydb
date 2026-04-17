// update-from-depth.test.js — UPDATE FROM / multi-table update depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-uf-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('UPDATE FROM', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('UPDATE with FROM clause joins tables', () => {
    db.execute('CREATE TABLE products (id INT, price INT)');
    db.execute('CREATE TABLE adjustments (product_id INT, new_price INT)');
    db.execute('INSERT INTO products VALUES (1, 100)');
    db.execute('INSERT INTO products VALUES (2, 200)');
    db.execute('INSERT INTO adjustments VALUES (1, 150)');

    db.execute('UPDATE products SET price = adjustments.new_price FROM adjustments WHERE products.id = adjustments.product_id');

    const r = rows(db.execute('SELECT * FROM products ORDER BY id'));
    assert.equal(r[0].price, 150); // Updated
    assert.equal(r[1].price, 200); // Unchanged
  });

  it('UPDATE FROM with expression', () => {
    db.execute('CREATE TABLE employees (id INT, salary INT)');
    db.execute('CREATE TABLE raises (emp_id INT, pct INT)');
    db.execute('INSERT INTO employees VALUES (1, 50000)');
    db.execute('INSERT INTO employees VALUES (2, 60000)');
    db.execute('INSERT INTO raises VALUES (1, 10)');

    db.execute('UPDATE employees SET salary = employees.salary + employees.salary * raises.pct / 100 FROM raises WHERE employees.id = raises.emp_id');

    const r = rows(db.execute('SELECT * FROM employees ORDER BY id'));
    assert.equal(r[0].salary, 55000); // 50000 + 10% = 55000
    assert.equal(r[1].salary, 60000); // No raise
  });

  it('UPDATE FROM with no matching rows', () => {
    db.execute('CREATE TABLE t1 (id INT, val INT)');
    db.execute('CREATE TABLE t2 (id INT, val INT)');
    db.execute('INSERT INTO t1 VALUES (1, 100)');
    db.execute('INSERT INTO t2 VALUES (99, 999)');

    db.execute('UPDATE t1 SET val = t2.val FROM t2 WHERE t1.id = t2.id');

    const r = rows(db.execute('SELECT * FROM t1'));
    assert.equal(r[0].val, 100); // Unchanged
  });
});

describe('DELETE with subquery', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('DELETE WHERE IN subquery', () => {
    db.execute('CREATE TABLE orders (id INT, status TEXT)');
    db.execute('CREATE TABLE cancelled (order_id INT)');
    db.execute("INSERT INTO orders VALUES (1, 'active')");
    db.execute("INSERT INTO orders VALUES (2, 'active')");
    db.execute("INSERT INTO orders VALUES (3, 'active')");
    db.execute('INSERT INTO cancelled VALUES (1)');
    db.execute('INSERT INTO cancelled VALUES (3)');

    db.execute('DELETE FROM orders WHERE id IN (SELECT order_id FROM cancelled)');

    const r = rows(db.execute('SELECT * FROM orders'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 2);
  });
});
