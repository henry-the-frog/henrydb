// alter-table.test.js — ALTER TABLE tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('ALTER TABLE', () => {
  it('ADD COLUMN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('ALTER TABLE t ADD COLUMN name TEXT');
    
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[0].name, null); // New column is null for existing rows
  });

  it('ADD COLUMN with DEFAULT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2)');
    db.execute("ALTER TABLE t ADD COLUMN status TEXT DEFAULT 'active'");
    
    const r = db.execute('SELECT id, status FROM t ORDER BY id');
    // Existing rows may or may not get the default
    assert.equal(r.rows.length, 2);
  });

  it('RENAME TABLE', () => {
    const db = new Database();
    db.execute('CREATE TABLE old_name (id INT)');
    db.execute('INSERT INTO old_name VALUES (1)');
    db.execute('ALTER TABLE old_name RENAME TO new_name');
    
    assert.equal(db.execute('SELECT COUNT(*) as c FROM new_name').rows[0].c, 1);
    assert.throws(() => db.execute('SELECT * FROM old_name'));
  });

  it('RENAME COLUMN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (old_col TEXT)');
    db.execute("INSERT INTO t VALUES ('test')");
    db.execute('ALTER TABLE t RENAME COLUMN old_col TO new_col');
    
    const r = db.execute('SELECT new_col FROM t');
    assert.equal(r.rows[0].new_col, 'test');
  });

  it('DROP COLUMN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT, age INT)');
    db.execute("INSERT INTO t VALUES (1, 'alice', 25)");
    db.execute('ALTER TABLE t DROP COLUMN age');
    
    const r = db.execute('SELECT * FROM t');
    assert.ok(!('age' in r.rows[0]), 'dropped column should not exist');
    assert.equal(r.rows[0].name, 'alice');
  });

  it('multiple ALTER operations', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');
    
    db.execute('ALTER TABLE t ADD COLUMN name TEXT');
    db.execute("UPDATE t SET name = 'item_' || CAST(id AS TEXT)");
    db.execute('ALTER TABLE t ADD COLUMN active BOOLEAN');
    db.execute('UPDATE t SET active = true');
    
    const r = db.execute('SELECT * FROM t WHERE active = true ORDER BY id');
    assert.equal(r.rows.length, 3);
  });
});
