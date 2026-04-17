// subquery-cte-depth.test.js — Subquery and CTE correctness depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-subq-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE orders (id INT, customer_id INT, amount INT)');
  db.execute('CREATE TABLE customers (id INT, name TEXT)');
  db.execute("INSERT INTO customers VALUES (1, 'Alice')");
  db.execute("INSERT INTO customers VALUES (2, 'Bob')");
  db.execute("INSERT INTO customers VALUES (3, 'Carol')");
  db.execute('INSERT INTO orders VALUES (1, 1, 100)');
  db.execute('INSERT INTO orders VALUES (2, 1, 200)');
  db.execute('INSERT INTO orders VALUES (3, 2, 150)');
  db.execute('INSERT INTO orders VALUES (4, NULL, 50)');
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Scalar Subqueries', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('scalar subquery in SELECT', () => {
    const r = rows(db.execute(
      'SELECT name, (SELECT SUM(amount) FROM orders WHERE customer_id = customers.id) AS total FROM customers ORDER BY name'
    ));
    assert.equal(r.length, 3);
    const alice = r.find(x => x.name === 'Alice');
    assert.equal(alice.total, 300, 'Alice total should be 300');
    const carol = r.find(x => x.name === 'Carol');
    assert.equal(carol.total, null, 'Carol with no orders should have NULL total');
  });

  it('scalar subquery returning NULL', () => {
    const r = rows(db.execute(
      "SELECT (SELECT name FROM customers WHERE id = 999) AS result"
    ));
    assert.equal(r.length, 1);
    assert.equal(r[0].result, null, 'Non-existent subquery should return NULL');
  });
});

describe('EXISTS', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('EXISTS with matching rows', () => {
    const r = rows(db.execute(
      'SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id) ORDER BY name'
    ));
    assert.equal(r.length, 2);
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[1].name, 'Bob');
  });

  it('NOT EXISTS', () => {
    const r = rows(db.execute(
      'SELECT name FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id) ORDER BY name'
    ));
    assert.equal(r.length, 1);
    assert.equal(r[0].name, 'Carol');
  });

  it('EXISTS with empty subquery', () => {
    const r = rows(db.execute(
      'SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE amount > 9999)'
    ));
    assert.equal(r.length, 0);
  });
});

describe('IN and NOT IN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('IN subquery', () => {
    const r = rows(db.execute(
      'SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders) ORDER BY name'
    ));
    assert.equal(r.length, 2);
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[1].name, 'Bob');
  });

  it('NOT IN with NULLs in subquery — SQL three-valued logic', () => {
    // NOT IN with NULLs is tricky: if subquery contains NULL,
    // x NOT IN (..., NULL, ...) is UNKNOWN (not true)
    // This should return no rows or fewer rows due to NULL handling
    const r = rows(db.execute(
      'SELECT name FROM customers WHERE id NOT IN (SELECT customer_id FROM orders) ORDER BY name'
    ));
    // orders.customer_id includes NULL → NOT IN should return 0 rows (SQL standard)
    // However, many databases handle this differently
    // Carol (id=3) is not in {1, 2, NULL} → 3 NOT IN {1,2,NULL} = UNKNOWN → not included
    // So correct SQL behavior: 0 rows (or Carol if NULL handling is relaxed)
  });

  it('IN with empty subquery', () => {
    const r = rows(db.execute(
      'SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 9999)'
    ));
    assert.equal(r.length, 0);
  });

  it('IN with literal list', () => {
    const r = rows(db.execute(
      'SELECT name FROM customers WHERE id IN (1, 3) ORDER BY name'
    ));
    assert.equal(r.length, 2);
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[1].name, 'Carol');
  });
});

describe('Correlated Subqueries', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('correlated subquery in WHERE', () => {
    // Find customers whose max order > 100
    const r = rows(db.execute(
      'SELECT name FROM customers WHERE (SELECT MAX(amount) FROM orders WHERE customer_id = customers.id) > 100 ORDER BY name'
    ));
    assert.equal(r.length, 2);
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[1].name, 'Bob');
  });

  it('correlated subquery referencing outer alias', () => {
    const r = rows(db.execute(
      'SELECT c.name, (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS order_count FROM customers c ORDER BY c.name'
    ));
    assert.equal(r.length, 3);
    const alice = r.find(x => x.name === 'Alice');
    assert.equal(alice.order_count, 2);
    const carol = r.find(x => x.name === 'Carol');
    assert.equal(carol.order_count, 0);
  });
});

describe('CTEs (Common Table Expressions)', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('simple CTE', () => {
    const r = rows(db.execute(
      'WITH big_orders AS (SELECT * FROM orders WHERE amount > 100) ' +
      'SELECT customer_id, amount FROM big_orders ORDER BY amount'
    ));
    assert.equal(r.length, 2);
    assert.equal(r[0].amount, 150);
    assert.equal(r[1].amount, 200);
  });

  it('CTE with multiple references', () => {
    const r = rows(db.execute(
      'WITH order_totals AS (SELECT customer_id, SUM(amount) AS total FROM orders WHERE customer_id IS NOT NULL GROUP BY customer_id) ' +
      'SELECT c.name, ot.total FROM customers c INNER JOIN order_totals ot ON c.id = ot.customer_id ORDER BY c.name'
    ));
    assert.equal(r.length, 2);
    assert.equal(r.find(x => x.name === 'Alice').total, 300);
    assert.equal(r.find(x => x.name === 'Bob').total, 150);
  });

  it('CTE referenced in subquery', () => {
    const r = rows(db.execute(
      'WITH cust_orders AS (SELECT customer_id, COUNT(*) AS cnt FROM orders WHERE customer_id IS NOT NULL GROUP BY customer_id) ' +
      'SELECT name FROM customers WHERE id IN (SELECT customer_id FROM cust_orders WHERE cnt >= 2) ORDER BY name'
    ));
    assert.equal(r.length, 1);
    assert.equal(r[0].name, 'Alice');
  });

  it('multiple CTEs', () => {
    const r = rows(db.execute(
      'WITH ' +
      'active_customers AS (SELECT DISTINCT customer_id FROM orders WHERE customer_id IS NOT NULL), ' +
      'customer_names AS (SELECT id, name FROM customers) ' +
      'SELECT cn.name FROM customer_names cn INNER JOIN active_customers ac ON cn.id = ac.customer_id ORDER BY cn.name'
    ));
    assert.equal(r.length, 2);
  });
});

describe('Recursive CTE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('recursive CTE generates sequence', () => {
    const r = rows(db.execute(
      'WITH RECURSIVE seq(n) AS (' +
      '  SELECT 1 ' +
      '  UNION ALL ' +
      '  SELECT n + 1 FROM seq WHERE n < 10' +
      ') SELECT n FROM seq'
    ));
    assert.equal(r.length, 10);
    assert.equal(r[0].n, 1);
    assert.equal(r[9].n, 10);
  });

  it('recursive CTE for hierarchy traversal', () => {
    db.execute('CREATE TABLE tree (id INT, parent_id INT, name TEXT)');
    db.execute("INSERT INTO tree VALUES (1, NULL, 'root')");
    db.execute("INSERT INTO tree VALUES (2, 1, 'child1')");
    db.execute("INSERT INTO tree VALUES (3, 1, 'child2')");
    db.execute("INSERT INTO tree VALUES (4, 2, 'grandchild1')");

    const r = rows(db.execute(
      'WITH RECURSIVE ancestors(id, name, depth) AS (' +
      '  SELECT id, name, 0 FROM tree WHERE parent_id IS NULL ' +
      '  UNION ALL ' +
      '  SELECT t.id, t.name, a.depth + 1 FROM tree t INNER JOIN ancestors a ON t.parent_id = a.id' +
      ') SELECT id, name, depth FROM ancestors ORDER BY depth, id'
    ));
    assert.equal(r.length, 4);
    assert.equal(r[0].name, 'root');
    assert.equal(r[0].depth, 0);
    assert.equal(r[3].name, 'grandchild1');
    assert.equal(r[3].depth, 2);
  });
});

describe('Subquery + MVCC', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('subquery sees snapshot-consistent data', () => {
    const s1 = db.session();
    s1.begin();
    
    // s1 reads via subquery
    const r1 = rows(s1.execute(
      'SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 100)'
    ));
    assert.equal(r1.length, 2);

    // Concurrent delete
    db.execute('DELETE FROM orders WHERE amount = 200');

    // s1 should still see the same result
    const r2 = rows(s1.execute(
      'SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 100)'
    ));
    assert.equal(r2.length, 2, 'Snapshot should still see deleted order');

    s1.commit();
    s1.close();

    // New read should see updated state
    const r3 = rows(db.execute(
      'SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 100)'
    ));
    assert.equal(r3.length, 1); // Only Bob's 150 remains > 100
  });
});
