// ctas-insert-select.test.js — CREATE TABLE AS SELECT and INSERT INTO ... SELECT
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('CREATE TABLE AS SELECT', () => {
  it('basic CTAS', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.execute("INSERT INTO users VALUES (3, 'Carol', 35)");
    
    db.execute('CREATE TABLE seniors AS SELECT id, name FROM users WHERE age > 28');
    const result = db.execute('SELECT * FROM seniors ORDER BY id');
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].name, 'Alice');
    assert.strictEqual(result.rows[1].name, 'Carol');
  });

  it('CTAS with aggregation', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT)');
    for (let i = 0; i < 20; i++) {
      const region = ['US', 'EU', 'APAC', 'LATAM'][i % 4];
      db.execute(`INSERT INTO sales VALUES (${i}, '${region}', ${(i * 17) % 100})`);
    }
    
    db.execute('CREATE TABLE region_totals AS SELECT region, SUM(amount) as total, COUNT(*) as cnt FROM sales GROUP BY region');
    const result = db.execute('SELECT * FROM region_totals ORDER BY region');
    assert.strictEqual(result.rows.length, 4);
    for (const row of result.rows) {
      assert.strictEqual(row.cnt, 5);
    }
  });

  it('CTAS with JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total INT)');
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    db.execute('INSERT INTO orders VALUES (100, 1, 50)');
    db.execute('INSERT INTO orders VALUES (101, 1, 30)');
    db.execute('INSERT INTO orders VALUES (102, 2, 70)');
    
    db.execute('CREATE TABLE order_summary AS SELECT c.name, SUM(o.total) as total FROM customers c JOIN orders o ON o.customer_id = c.id GROUP BY c.name');
    const result = db.execute('SELECT * FROM order_summary ORDER BY name');
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].name, 'Alice');
    assert.strictEqual(result.rows[0].total, 80);
    assert.strictEqual(result.rows[1].name, 'Bob');
    assert.strictEqual(result.rows[1].total, 70);
  });

  it('CTAS with empty result', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE empty_copy AS SELECT * FROM src');
    const result = db.execute('SELECT COUNT(*) as cnt FROM empty_copy');
    assert.strictEqual(result.rows[0].cnt, 0);
  });
});

describe('INSERT INTO ... SELECT', () => {
  it('basic INSERT SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE source (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE target (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO source VALUES (${i}, ${i * 10})`);
    
    db.execute('INSERT INTO target SELECT * FROM source WHERE val > 50');
    const result = db.execute('SELECT * FROM target ORDER BY id');
    assert.strictEqual(result.rows.length, 4);
    assert.strictEqual(result.rows[0].val, 60);
  });

  it('INSERT SELECT with column mapping', () => {
    const db = new Database();
    db.execute('CREATE TABLE detailed (id INT PRIMARY KEY, name TEXT, val INT, extra TEXT)');
    db.execute('CREATE TABLE summary (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO detailed VALUES (1, 'Alice', 100, 'x')");
    db.execute("INSERT INTO detailed VALUES (2, 'Bob', 200, 'y')");
    
    db.execute('INSERT INTO summary SELECT id, name FROM detailed');
    const result = db.execute('SELECT * FROM summary ORDER BY id');
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].name, 'Alice');
  });

  it('INSERT SELECT with aggregation', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute('CREATE TABLE totals (grp TEXT, total INT)');
    for (let i = 0; i < 20; i++) {
      db.execute(`INSERT INTO data VALUES (${i}, 'g${i % 4}', ${i * 5})`);
    }
    
    db.execute('INSERT INTO totals SELECT grp, SUM(val) FROM data GROUP BY grp');
    const result = db.execute('SELECT * FROM totals ORDER BY grp');
    assert.strictEqual(result.rows.length, 4);
  });

  it('INSERT SELECT appends to existing data', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'existing')");
    
    db.execute('CREATE TABLE src (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO src VALUES (2, 'new1')");
    db.execute("INSERT INTO src VALUES (3, 'new2')");
    
    db.execute('INSERT INTO t SELECT * FROM src');
    const result = db.execute('SELECT * FROM t ORDER BY id');
    assert.strictEqual(result.rows.length, 3);
    assert.strictEqual(result.rows[0].val, 'existing');
    assert.strictEqual(result.rows[2].val, 'new2');
  });
});
