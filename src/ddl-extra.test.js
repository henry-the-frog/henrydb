// ddl-extra.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('TRUNCATE TABLE', () => {
  it('removes all rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    
    db.execute('TRUNCATE TABLE t');
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 0);
  });

  it('table structure preserved after truncate', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'test')");
    
    db.execute('TRUNCATE TABLE t');
    
    // Can still insert
    db.execute("INSERT INTO t VALUES (1, 'new')");
    assert.equal(db.execute('SELECT name FROM t').rows[0].name, 'new');
  });

  it('throws for nonexistent table', () => {
    const db = new Database();
    assert.throws(() => db.execute('TRUNCATE TABLE nope'), /not found/);
  });
});

describe('RENAME TABLE', () => {
  it('renames a table', () => {
    const db = new Database();
    db.execute('CREATE TABLE old_name (id INT PRIMARY KEY)');
    db.execute('INSERT INTO old_name VALUES (1)');
    
    db.execute('RENAME TABLE old_name TO new_name');
    
    assert.throws(() => db.execute('SELECT * FROM old_name'));
    assert.equal(db.execute('SELECT * FROM new_name').rows.length, 1);
  });

  it('throws for nonexistent source', () => {
    const db = new Database();
    assert.throws(() => db.execute('RENAME TABLE nope TO whatever'), /not found/);
  });

  it('throws if target exists', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY)');
    assert.throws(() => db.execute('RENAME TABLE a TO b'), /already exists/);
  });
});

describe('DESCRIBE', () => {
  it('DESCRIBE is alias for SHOW COLUMNS FROM', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)');
    
    const r = db.execute('DESCRIBE t');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].column_name, 'id');
  });
});
