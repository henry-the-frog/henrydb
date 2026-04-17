// insert-select-depth.test.js — INSERT SELECT + CTAS tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-inssel-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('INSERT INTO ... SELECT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('basic INSERT SELECT', () => {
    db.execute('CREATE TABLE src (id INT, val TEXT)');
    db.execute('CREATE TABLE dst (id INT, val TEXT)');
    db.execute("INSERT INTO src VALUES (1, 'a')");
    db.execute("INSERT INTO src VALUES (2, 'b')");

    db.execute('INSERT INTO dst SELECT * FROM src');

    const r = rows(db.execute('SELECT * FROM dst ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].val, 'a');
  });

  it('INSERT SELECT with WHERE', () => {
    db.execute('CREATE TABLE src (id INT, val INT)');
    db.execute('CREATE TABLE dst (id INT, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO src VALUES (${i}, ${i * 10})`);

    db.execute('INSERT INTO dst SELECT * FROM src WHERE val > 50');

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM dst'));
    assert.equal(r[0].c, 5); // ids 6-10
  });

  it('INSERT SELECT with aggregate', () => {
    db.execute('CREATE TABLE src (dept TEXT, salary INT)');
    db.execute('CREATE TABLE summary (dept TEXT, total INT)');
    db.execute("INSERT INTO src VALUES ('eng', 100)");
    db.execute("INSERT INTO src VALUES ('eng', 200)");
    db.execute("INSERT INTO src VALUES ('sales', 150)");

    db.execute('INSERT INTO summary SELECT dept, SUM(salary) FROM src GROUP BY dept');

    const r = rows(db.execute('SELECT * FROM summary ORDER BY dept'));
    assert.equal(r.length, 2);
    assert.equal(r[0].total, 300);
    assert.equal(r[1].total, 150);
  });

  it('INSERT SELECT from same table (self-duplicate)', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");

    // Insert a copy with different id
    db.execute("INSERT INTO t SELECT id + 100, val FROM t");

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[1].id, 101);
    assert.equal(r[1].val, 'original');
  });

  it('INSERT SELECT with JOIN', () => {
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute('CREATE TABLE orders (customer_id INT, amount INT)');
    db.execute('CREATE TABLE report (name TEXT, total INT)');
    
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    db.execute('INSERT INTO orders VALUES (1, 100)');
    db.execute('INSERT INTO orders VALUES (1, 200)');
    db.execute('INSERT INTO orders VALUES (2, 150)');

    db.execute(
      'INSERT INTO report SELECT c.name, SUM(o.amount) FROM customers c ' +
      'INNER JOIN orders o ON c.id = o.customer_id GROUP BY c.name'
    );

    const r = rows(db.execute('SELECT * FROM report ORDER BY name'));
    assert.equal(r.length, 2);
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[0].total, 300);
  });
});

describe('CREATE TABLE AS SELECT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('basic CTAS', () => {
    db.execute('CREATE TABLE src (id INT, val TEXT)');
    db.execute("INSERT INTO src VALUES (1, 'hello')");
    db.execute("INSERT INTO src VALUES (2, 'world')");

    db.execute('CREATE TABLE dst AS SELECT * FROM src');

    const r = rows(db.execute('SELECT * FROM dst ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].val, 'hello');
  });

  it('CTAS with expression columns', () => {
    db.execute('CREATE TABLE src (id INT, price INT, qty INT)');
    db.execute('INSERT INTO src VALUES (1, 100, 5)');
    db.execute('INSERT INTO src VALUES (2, 200, 3)');

    db.execute('CREATE TABLE totals AS SELECT id, price * qty AS total FROM src');

    const r = rows(db.execute('SELECT * FROM totals ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].total, 500);
    assert.equal(r[1].total, 600);
  });

  it('CTAS with aggregate', () => {
    db.execute('CREATE TABLE src (dept TEXT, salary INT)');
    db.execute("INSERT INTO src VALUES ('eng', 100)");
    db.execute("INSERT INTO src VALUES ('eng', 200)");
    db.execute("INSERT INTO src VALUES ('sales', 150)");

    db.execute('CREATE TABLE summary AS SELECT dept, AVG(salary) AS avg_sal FROM src GROUP BY dept');

    const r = rows(db.execute('SELECT * FROM summary ORDER BY dept'));
    assert.equal(r.length, 2);
  });
});

describe('INSERT SELECT + MVCC', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('INSERT SELECT sees snapshot-consistent source data', () => {
    db.execute('CREATE TABLE src (id INT, val INT)');
    db.execute('CREATE TABLE dst (id INT, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO src VALUES (${i}, ${i})`);

    const s1 = db.session();
    s1.begin();
    
    // Start reading source
    const count1 = rows(s1.execute('SELECT COUNT(*) AS c FROM src'))[0].c;
    assert.equal(count1, 10);

    // Concurrent modification
    db.execute('INSERT INTO src VALUES (11, 11)');

    // INSERT SELECT in s1 should only see 10 rows
    s1.execute('INSERT INTO dst SELECT * FROM src');
    s1.commit();
    s1.close();

    const dstCount = rows(db.execute('SELECT COUNT(*) AS c FROM dst'))[0].c;
    assert.equal(dstCount, 10, 'INSERT SELECT should see snapshot-consistent source');
  });
});
