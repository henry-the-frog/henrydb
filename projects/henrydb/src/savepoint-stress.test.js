// savepoint-stress.test.js — Stress tests for ROLLBACK TO with complex schemas
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Savepoint ROLLBACK TO — Complex Schemas', () => {
  it('rollback preserves PRIMARY KEY index integrity', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'alice')");
    db.execute("INSERT INTO users VALUES (2, 'bob')");
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO users VALUES (3, 'charlie')");
    
    db.execute('ROLLBACK TO sp1');
    
    // Should be able to re-insert id=3 since it was rolled back
    db.execute("INSERT INTO users VALUES (3, 'dave')");
    assert.equal(db.execute('SELECT COUNT(*) as c FROM users').rows[0].c, 3);
    assert.equal(db.execute('SELECT name FROM users WHERE id = 3').rows[0].name, 'dave');
  });

  it('rollback preserves UNIQUE constraint integrity', () => {
    const db = new Database();
    db.execute('CREATE TABLE emails (id INT PRIMARY KEY, email TEXT UNIQUE)');
    db.execute("INSERT INTO emails VALUES (1, 'a@test.com')");
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO emails VALUES (2, 'b@test.com')");
    
    db.execute('ROLLBACK TO sp1');
    
    // b@test.com should be available again
    db.execute("INSERT INTO emails VALUES (3, 'b@test.com')");
    assert.equal(db.execute('SELECT COUNT(*) as c FROM emails').rows[0].c, 2);
  });

  it('rollback with secondary index preserves index state', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, price INT)');
    db.execute('CREATE INDEX idx_price ON products(price)');
    db.execute('INSERT INTO products VALUES (1, 100)');
    db.execute('INSERT INTO products VALUES (2, 200)');
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO products VALUES (3, 150)');
    db.execute('UPDATE products SET price = 250 WHERE id = 1');
    
    db.execute('ROLLBACK TO sp1');
    
    // Verify data is restored
    const rows = db.execute('SELECT * FROM products ORDER BY id').rows;
    assert.equal(rows.length, 2);
    assert.equal(rows[0].price, 100);
    assert.equal(rows[1].price, 200);
  });

  it('rollback with NOT NULL + DEFAULT columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE config (key TEXT PRIMARY KEY, val TEXT NOT NULL DEFAULT \'default\')');
    db.execute("INSERT INTO config VALUES ('a', 'val_a')");
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO config VALUES ('b', 'val_b')");
    db.execute("UPDATE config SET val = 'changed' WHERE key = 'a'");
    
    db.execute('ROLLBACK TO sp1');
    
    assert.equal(db.execute('SELECT COUNT(*) as c FROM config').rows[0].c, 1);
    assert.equal(db.execute("SELECT val FROM config WHERE key = 'a'").rows[0].val, 'val_a');
  });

  it('rollback after DELETE + INSERT of same PK', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");
    db.execute('SAVEPOINT sp1');
    db.execute('DELETE FROM t WHERE id = 1');
    db.execute("INSERT INTO t VALUES (1, 'replaced')");
    
    db.execute('ROLLBACK TO sp1');
    
    // Should see original row
    const rows = db.execute('SELECT * FROM t').rows;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].val, 'original');
  });

  it('rollback with CHECK constraints', () => {
    const db = new Database();
    db.execute('CREATE TABLE ranges (id INT, low INT, high INT, CHECK (low < high))');
    db.execute('INSERT INTO ranges VALUES (1, 10, 20)');
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO ranges VALUES (2, 30, 40)');
    
    db.execute('ROLLBACK TO sp1');
    
    assert.equal(db.execute('SELECT COUNT(*) as c FROM ranges').rows[0].c, 1);
    // Verify CHECK still works after rollback
    assert.throws(() => db.execute('INSERT INTO ranges VALUES (3, 50, 40)'), /CHECK/i);
  });

  it('rollback with sequence (SERIAL column)', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE users_id_seq');
    db.execute('CREATE TABLE users (id INT DEFAULT nextval(\'users_id_seq\'), name TEXT)');
    db.execute("INSERT INTO users (name) VALUES ('alice')"); // id=1
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO users (name) VALUES ('bob')"); // id=2
    db.execute("INSERT INTO users (name) VALUES ('charlie')"); // id=3
    
    db.execute('ROLLBACK TO sp1');
    
    // Only alice should exist
    const rows = db.execute('SELECT * FROM users').rows;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].name, 'alice');
    
    // Sequence should continue from where it was (NOT rolled back - this is PG behavior)
    db.execute("INSERT INTO users (name) VALUES ('dave')");
    const dave = db.execute("SELECT id FROM users WHERE name = 'dave'").rows[0];
    assert.ok(dave.id >= 4, 'sequence should not be rolled back');
  });

  it('nested savepoints with different schemas', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, ref INT, data TEXT)');
    
    db.execute("INSERT INTO t1 VALUES (1, 'a')");
    db.execute('SAVEPOINT outer');
    db.execute("INSERT INTO t1 VALUES (2, 'b')");
    db.execute("INSERT INTO t2 VALUES (1, 1, 'x')");
    
    db.execute('SAVEPOINT inner');
    db.execute("INSERT INTO t1 VALUES (3, 'c')");
    db.execute("INSERT INTO t2 VALUES (2, 2, 'y')");
    db.execute("UPDATE t2 SET data = 'z' WHERE id = 1");
    
    // Rollback inner: t1 should have 1,2; t2 should have (1,1,'x')
    db.execute('ROLLBACK TO inner');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t1').rows[0].c, 2);
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t2').rows[0].c, 1);
    assert.equal(db.execute('SELECT data FROM t2 WHERE id = 1').rows[0].data, 'x');
    
    // Rollback outer: t1 should have only 1; t2 should be empty
    db.execute('ROLLBACK TO outer');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t1').rows[0].c, 1);
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t2').rows[0].c, 0);
  });

  it('100-row stress: insert, update, delete, rollback', () => {
    const db = new Database();
    db.execute('CREATE TABLE stress (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO stress VALUES (${i}, ${i * 10})`);
    }
    
    db.execute('SAVEPOINT sp1');
    
    // Do lots of mutations
    for (let i = 51; i <= 100; i++) {
      db.execute(`INSERT INTO stress VALUES (${i}, ${i * 10})`);
    }
    db.execute('DELETE FROM stress WHERE id <= 10');
    db.execute('UPDATE stress SET val = val + 1 WHERE id > 40 AND id <= 50');
    
    // Verify mutations happened
    const preRollback = db.execute('SELECT COUNT(*) as c FROM stress').rows[0].c;
    assert.equal(preRollback, 90); // 50 + 50 - 10
    
    db.execute('ROLLBACK TO sp1');
    
    // Should be back to original 50 rows
    const postRollback = db.execute('SELECT COUNT(*) as c FROM stress').rows[0].c;
    assert.equal(postRollback, 50);
    
    // Verify specific values
    assert.equal(db.execute('SELECT val FROM stress WHERE id = 1').rows[0].val, 10);
    assert.equal(db.execute('SELECT val FROM stress WHERE id = 50').rows[0].val, 500);
    assert.equal(db.execute('SELECT val FROM stress WHERE id = 45').rows[0].val, 450);
  });

  it('savepoint + JOIN correctness after rollback', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total INT)');
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'alice'), (2, 'bob')");
    db.execute('INSERT INTO orders VALUES (1, 1, 100), (2, 2, 200)');
    
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO orders VALUES (3, 1, 300)');
    db.execute('DELETE FROM orders WHERE id = 2');
    
    db.execute('ROLLBACK TO sp1');
    
    // JOIN should work correctly with restored data
    const r = db.execute(`
      SELECT c.name, SUM(o.total) as total_orders
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      GROUP BY c.name
      ORDER BY c.name
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'alice');
    assert.equal(r.rows[0].total_orders, 100);
    assert.equal(r.rows[1].name, 'bob');
    assert.equal(r.rows[1].total_orders, 200);
  });
});
