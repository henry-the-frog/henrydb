import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function query(db, sql) {
  const r = db.execute(sql);
  return r && r.rows ? r.rows : r;
}

describe('SQL Scalar Functions (CREATE FUNCTION)', () => {

  it('basic arithmetic function', () => {
    const db = new Database();
    db.execute("CREATE FUNCTION double(x INTEGER) RETURNS INTEGER AS 'SELECT x * 2'");
    const rows = query(db, 'SELECT double(5) AS result');
    assert.equal(rows[0].result, 10);
  });

  it('string concatenation with dollar-quoting', () => {
    const db = new Database();
    // Dollar-quoting allows single quotes in function body
    const sql = "CREATE FUNCTION full_name(first TEXT, last TEXT) RETURNS TEXT AS " + "$$" + "SELECT first || ' ' || last" + "$$";
    db.execute(sql);
    const rows = query(db, "SELECT full_name('Alice', 'Smith') AS name");
    assert.equal(rows[0].name, 'Alice Smith');
  });

  it('function used in WHERE clause', () => {
    const db = new Database();
    db.execute("CREATE FUNCTION double(x INTEGER) RETURNS INTEGER AS 'SELECT x * 2'");
    db.execute('CREATE TABLE people (name TEXT, age INTEGER)');
    db.execute("INSERT INTO people VALUES ('Alice', 25)");
    db.execute("INSERT INTO people VALUES ('Bob', 15)");
    db.execute("INSERT INTO people VALUES ('Carol', 30)");
    const rows = query(db, 'SELECT name, double(age) AS dbl FROM people WHERE age > 20');
    assert.equal(rows.length, 2);
    assert.equal(rows[0].dbl, 50); // Alice: 25*2
    assert.equal(rows[1].dbl, 60); // Carol: 30*2
  });

  it('NULL argument handling', () => {
    const db = new Database();
    db.execute("CREATE FUNCTION double(x INTEGER) RETURNS INTEGER AS 'SELECT x * 2'");
    const rows = query(db, 'SELECT double(NULL) AS result');
    assert.equal(rows[0].result, null);
  });

  it('nested function calls', () => {
    const db = new Database();
    db.execute("CREATE FUNCTION double(x INTEGER) RETURNS INTEGER AS 'SELECT x * 2'");
    db.execute("CREATE FUNCTION triple(x INTEGER) RETURNS INTEGER AS 'SELECT x * 3'");
    // double(triple(5)) = double(15) = 30
    const rows = query(db, 'SELECT double(triple(5)) AS result');
    assert.equal(rows[0].result, 30);
  });

  it('OR REPLACE replaces existing function', () => {
    const db = new Database();
    db.execute("CREATE FUNCTION add1(x INTEGER) RETURNS INTEGER AS 'SELECT x + 1'");
    let rows = query(db, 'SELECT add1(5) AS result');
    assert.equal(rows[0].result, 6);

    db.execute("CREATE OR REPLACE FUNCTION add1(x INTEGER) RETURNS INTEGER AS 'SELECT x + 10'");
    rows = query(db, 'SELECT add1(5) AS result');
    assert.equal(rows[0].result, 15);
  });

  it('function with table data', () => {
    const db = new Database();
    db.execute("CREATE FUNCTION discount(price NUMERIC, pct NUMERIC) RETURNS NUMERIC AS 'SELECT price * (100 - pct) / 100'");
    db.execute('CREATE TABLE products (name TEXT, price NUMERIC)');
    db.execute("INSERT INTO products VALUES ('Widget', 100)");
    db.execute("INSERT INTO products VALUES ('Gadget', 50)");
    const rows = query(db, 'SELECT name, discount(price, 20) AS sale_price FROM products');
    assert.equal(rows[0].sale_price, 80);
    assert.equal(rows[1].sale_price, 40);
  });

  it('duplicate CREATE FUNCTION throws error', () => {
    const db = new Database();
    db.execute("CREATE FUNCTION foo(x INTEGER) RETURNS INTEGER AS 'SELECT x'");
    assert.throws(() => {
      db.execute("CREATE FUNCTION foo(x INTEGER) RETURNS INTEGER AS 'SELECT x'");
    }, /already exists/);
  });

  it('function with CASE expression', () => {
    const db = new Database();
    db.execute("CREATE FUNCTION is_positive(n INTEGER) RETURNS BOOLEAN AS 'SELECT CASE WHEN n > 0 THEN 1 ELSE 0 END'");
    const rows = query(db, 'SELECT is_positive(5) AS r1, is_positive(-3) AS r2, is_positive(0) AS r3');
    assert.equal(rows[0].r1, 1);
    assert.equal(rows[0].r2, 0);
    assert.equal(rows[0].r3, 0);
  });

  it('function in ORDER BY', () => {
    const db = new Database();
    db.execute("CREATE FUNCTION neg(x INTEGER) RETURNS INTEGER AS 'SELECT x * -1'");
    db.execute('CREATE TABLE nums (val INTEGER)');
    db.execute('INSERT INTO nums VALUES (3)');
    db.execute('INSERT INTO nums VALUES (1)');
    db.execute('INSERT INTO nums VALUES (2)');
    const rows = query(db, 'SELECT val, neg(val) AS negval FROM nums ORDER BY neg(val)');
    // Ordering by neg(val): -3, -2, -1 → vals: 3, 2, 1
    assert.equal(rows[0].val, 3);
    assert.equal(rows[1].val, 2);
    assert.equal(rows[2].val, 1);
  });
});
