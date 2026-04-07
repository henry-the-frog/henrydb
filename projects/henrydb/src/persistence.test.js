// persistence.test.js — Database serialization tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Database Persistence', () => {
  it('serialize and restore basic table', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    db1.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db1.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    
    const json = db1.save();
    const db2 = Database.fromSerialized(json);
    
    const rows = db2.execute('SELECT * FROM users ORDER BY id').rows;
    assert.equal(rows.length, 2);
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[1].name, 'Bob');
  });

  it('preserves multiple tables', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)');
    db1.execute('CREATE TABLE t2 (id INT PRIMARY KEY, num INT)');
    db1.execute("INSERT INTO t1 VALUES (1, 'hello')");
    db1.execute('INSERT INTO t2 VALUES (1, 42)');
    
    const db2 = Database.fromSerialized(db1.save());
    
    assert.equal(db2.execute('SELECT val FROM t1').rows[0].val, 'hello');
    assert.equal(db2.execute('SELECT num FROM t2').rows[0].num, 42);
  });

  it('preserves schema (NOT NULL, PRIMARY KEY)', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)');
    db1.execute("INSERT INTO t VALUES (1, 'test')");
    
    const db2 = Database.fromSerialized(db1.save());
    const schema = db2.tables.get('t').schema;
    
    assert.ok(schema[0].primaryKey);
    assert.ok(schema[1].notNull);
  });

  it('round-trip with queries after restore', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)');
    db1.execute("INSERT INTO products VALUES (1, 'Widget', 25)");
    db1.execute("INSERT INTO products VALUES (2, 'Gadget', 50)");
    
    const db2 = Database.fromSerialized(db1.save());
    
    // Test various queries on restored DB
    assert.equal(db2.execute('SELECT COUNT(*) AS cnt FROM products').rows[0].cnt, 2);
    assert.equal(db2.execute('SELECT SUM(price) AS total FROM products').rows[0].total, 75);
    assert.equal(db2.execute("SELECT name FROM products WHERE price > 30").rows[0].name, 'Gadget');
  });

  it('restored DB supports new operations', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db1.execute('INSERT INTO t VALUES (1, 10)');
    
    const db2 = Database.fromSerialized(db1.save());
    
    // Insert new data into restored DB
    db2.execute('INSERT INTO t VALUES (2, 20)');
    assert.equal(db2.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 2);
    
    // Update existing data
    db2.execute('UPDATE t SET val = 99 WHERE id = 1');
    assert.equal(db2.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 99);
  });

  it('preserves triggers', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE audit (id INT, msg TEXT)');
    db1.execute('CREATE TABLE items (id INT PRIMARY KEY)');
    db1.execute("CREATE TRIGGER t1 AFTER INSERT ON items EXECUTE INSERT INTO audit VALUES (1, 'fired')");
    
    const db2 = Database.fromSerialized(db1.save());
    assert.equal(db2.triggers.length, 1);
    assert.equal(db2.triggers[0].name, 't1');
  });

  it('empty database round-trip', () => {
    const db1 = new Database();
    const db2 = Database.fromSerialized(db1.save());
    assert.equal(db2.tables.size, 0);
  });
});
