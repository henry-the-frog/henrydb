// having-depth.test.js — HAVING clause depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-hav-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE orders (id INT, customer TEXT, amount INT)');
  db.execute("INSERT INTO orders VALUES (1, 'Alice', 100)");
  db.execute("INSERT INTO orders VALUES (2, 'Alice', 200)");
  db.execute("INSERT INTO orders VALUES (3, 'Bob', 50)");
  db.execute("INSERT INTO orders VALUES (4, 'Bob', 75)");
  db.execute("INSERT INTO orders VALUES (5, 'Carol', 300)");
  db.execute("INSERT INTO orders VALUES (6, 'Carol', 150)");
  db.execute("INSERT INTO orders VALUES (7, 'Carol', 100)");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Basic HAVING', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('HAVING filters groups by aggregate', () => {
    const r = rows(db.execute(
      'SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer HAVING SUM(amount) > 200 ORDER BY customer'
    ));
    // Alice: 300, Bob: 125, Carol: 550
    assert.equal(r.length, 2); // Alice and Carol
    assert.equal(r[0].customer, 'Alice');
    assert.equal(r[1].customer, 'Carol');
  });

  it('HAVING with COUNT', () => {
    const r = rows(db.execute(
      'SELECT customer, COUNT(*) AS cnt FROM orders GROUP BY customer HAVING COUNT(*) > 2'
    ));
    // Carol has 3 orders
    assert.equal(r.length, 1);
    assert.equal(r[0].customer, 'Carol');
  });

  it('HAVING with AVG', () => {
    const r = rows(db.execute(
      'SELECT customer, AVG(amount) AS avg_amt FROM orders GROUP BY customer HAVING AVG(amount) > 100 ORDER BY customer'
    ));
    // Alice avg=150, Bob avg=62.5, Carol avg≈183
    assert.equal(r.length, 2);
    assert.ok(r.some(x => x.customer === 'Alice'));
    assert.ok(r.some(x => x.customer === 'Carol'));
  });
});

describe('HAVING with Multiple Conditions', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('HAVING with AND', () => {
    const r = rows(db.execute(
      'SELECT customer, SUM(amount) AS total, COUNT(*) AS cnt ' +
      'FROM orders GROUP BY customer ' +
      'HAVING SUM(amount) > 200 AND COUNT(*) >= 2'
    ));
    // Alice: sum=300, cnt=2 ✓
    // Carol: sum=550, cnt=3 ✓
    assert.equal(r.length, 2);
  });

  it('HAVING with OR', () => {
    const r = rows(db.execute(
      'SELECT customer FROM orders GROUP BY customer ' +
      'HAVING SUM(amount) > 400 OR COUNT(*) = 2 ORDER BY customer'
    ));
    // Alice: cnt=2 ✓, Bob: cnt=2 ✓, Carol: sum=550 ✓
    assert.equal(r.length, 3);
  });
});

describe('HAVING Edge Cases', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('HAVING filters all groups', () => {
    const r = rows(db.execute(
      'SELECT customer FROM orders GROUP BY customer HAVING SUM(amount) > 10000'
    ));
    assert.equal(r.length, 0);
  });

  it('HAVING passes all groups', () => {
    const r = rows(db.execute(
      'SELECT customer FROM orders GROUP BY customer HAVING SUM(amount) > 0'
    ));
    assert.equal(r.length, 3);
  });

  it('HAVING + ORDER BY + LIMIT', () => {
    const r = rows(db.execute(
      'SELECT customer, SUM(amount) AS total FROM orders ' +
      'GROUP BY customer HAVING SUM(amount) > 100 ' +
      'ORDER BY total DESC LIMIT 1'
    ));
    assert.equal(r.length, 1);
    assert.equal(r[0].customer, 'Carol');
  });
});
