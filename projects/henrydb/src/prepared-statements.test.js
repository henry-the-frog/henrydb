// prepared-statements.test.js — Tests for PREPARE/EXECUTE/DEALLOCATE
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Prepared Statements', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");
  });

  it('PREPARE and EXECUTE basic query', () => {
    db.execute('PREPARE find AS SELECT * FROM users WHERE id = $1');
    const result = db.execute('EXECUTE find (2)');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Bob');
  });

  it('EXECUTE with different parameters', () => {
    db.execute('PREPARE find AS SELECT * FROM users WHERE id = $1');
    const r1 = db.execute('EXECUTE find (1)');
    const r2 = db.execute('EXECUTE find (3)');
    assert.equal(r1.rows[0].name, 'Alice');
    assert.equal(r2.rows[0].name, 'Charlie');
  });

  it('EXECUTE with multiple parameters', () => {
    db.execute("PREPARE find AS SELECT * FROM users WHERE name = $1 AND age = $2");
    const result = db.execute("EXECUTE find ('Alice', 30)");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 1);
  });

  it('DEALLOCATE removes prepared statement', () => {
    db.execute('PREPARE stmt AS SELECT * FROM users');
    db.execute('DEALLOCATE stmt');
    assert.throws(() => db.execute('EXECUTE stmt ()'), /not found/);
  });

  it('DEALLOCATE ALL removes all statements', () => {
    db.execute('PREPARE s1 AS SELECT * FROM users WHERE id = $1');
    db.execute('PREPARE s2 AS SELECT * FROM users WHERE name = $1');
    db.execute('DEALLOCATE ALL');
    assert.throws(() => db.execute('EXECUTE s1 (1)'), /not found/);
    assert.throws(() => db.execute('EXECUTE s2 (1)'), /not found/);
  });

  it('EXECUTE nonexistent statement throws', () => {
    assert.throws(() => db.execute('EXECUTE nope (1)'), /not found/);
  });

  it('PREPARE with INSERT', () => {
    db.execute("PREPARE ins AS INSERT INTO users VALUES ($1, $2, $3)");
    db.execute("EXECUTE ins (4, 'Dave', 28)");
    const result = db.execute('SELECT * FROM users WHERE id = 4');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Dave');
  });

  it('PREPARE with UPDATE', () => {
    db.execute("PREPARE upd AS UPDATE users SET age = $1 WHERE id = $2");
    db.execute("EXECUTE upd (99, 1)");
    const result = db.execute('SELECT age FROM users WHERE id = 1');
    assert.equal(result.rows[0].age, 99);
  });

  it('PREPARE with DELETE', () => {
    db.execute("PREPARE del AS DELETE FROM users WHERE id = $1");
    db.execute("EXECUTE del (2)");
    const result = db.execute('SELECT * FROM users');
    assert.equal(result.rows.length, 2);
  });

  it('multiple executions of same statement', () => {
    db.execute('PREPARE q AS SELECT name FROM users WHERE id = $1');
    for (let id = 1; id <= 3; id++) {
      const result = db.execute(`EXECUTE q (${id})`);
      assert.equal(result.rows.length, 1);
    }
  });
});
