// triggers.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Triggers', () => {
  it('AFTER INSERT trigger fires', () => {
    const db = new Database();
    db.execute('CREATE TABLE audit (id INT, action TEXT)');
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    db.execute("CREATE TRIGGER log_insert AFTER INSERT ON users EXECUTE INSERT INTO audit VALUES (1, 'added')");
    
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    assert.equal(db.execute('SELECT * FROM audit').rows.length, 1);
    assert.equal(db.execute('SELECT action FROM audit').rows[0].action, 'added');
  });

  it('trigger fires for each row', () => {
    const db = new Database();
    db.execute('CREATE TABLE counter (val INT)');
    db.execute('INSERT INTO counter VALUES (0)');
    db.execute('CREATE TABLE items (id INT PRIMARY KEY)');
    db.execute("CREATE TRIGGER count_items AFTER INSERT ON items EXECUTE UPDATE counter SET val = val + 1");
    
    db.execute('INSERT INTO items VALUES (1)');
    db.execute('INSERT INTO items VALUES (2)');
    db.execute('INSERT INTO items VALUES (3)');
    
    assert.equal(db.execute('SELECT val FROM counter').rows[0].val, 3);
  });

  it('multiple triggers on same event', () => {
    const db = new Database();
    db.execute('CREATE TABLE log1 (msg TEXT)');
    db.execute('CREATE TABLE log2 (msg TEXT)');
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    
    db.execute("CREATE TRIGGER t1 AFTER INSERT ON t EXECUTE INSERT INTO log1 VALUES ('fired')");
    db.execute("CREATE TRIGGER t2 AFTER INSERT ON t EXECUTE INSERT INTO log2 VALUES ('fired')");
    
    db.execute('INSERT INTO t VALUES (1)');
    assert.equal(db.execute('SELECT * FROM log1').rows.length, 1);
    assert.equal(db.execute('SELECT * FROM log2').rows.length, 1);
  });
});
