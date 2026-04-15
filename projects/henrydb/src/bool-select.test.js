import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Boolean expressions in SELECT list', () => {
  it('IS NULL in SELECT returns true/false', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1,'hello'),(2,NULL),(3,'world')");

    const r = db.execute('SELECT id, val IS NULL as is_null FROM t ORDER BY id');
    assert.deepEqual(r.rows, [
      { id: 1, is_null: false },
      { id: 2, is_null: true },
      { id: 3, is_null: false }
    ]);
  });

  it('IS NOT NULL in SELECT returns true/false', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1,'x'),(2,NULL)");

    const r = db.execute('SELECT id, val IS NOT NULL as not_null FROM t ORDER BY id');
    assert.deepEqual(r.rows, [
      { id: 1, not_null: true },
      { id: 2, not_null: false }
    ]);
  });

  it('comparison operators in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');

    const r = db.execute('SELECT id, id > 2 as gt2, id = 1 as eq1 FROM t ORDER BY id');
    assert.equal(r.rows[0].gt2, false);
    assert.equal(r.rows[0].eq1, true);
    assert.equal(r.rows[2].gt2, true);
    assert.equal(r.rows[2].eq1, false);
  });

  it('LIKE in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT)');
    db.execute("INSERT INTO t VALUES ('Alice'),('Bob'),('Anna')");

    const r = db.execute("SELECT name, name LIKE 'A%' as starts_a FROM t ORDER BY name");
    assert.equal(r.rows[0].starts_a, true);  // Alice
    assert.equal(r.rows[1].starts_a, true);  // Anna
    assert.equal(r.rows[2].starts_a, false); // Bob
  });

  it('IN in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');

    const r = db.execute('SELECT id, id IN (1, 3) as in_set FROM t ORDER BY id');
    assert.equal(r.rows[0].in_set, true);
    assert.equal(r.rows[1].in_set, false);
    assert.equal(r.rows[2].in_set, true);
  });

  it('IS NULL without alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('x'),(NULL)");

    const r = db.execute('SELECT val IS NULL FROM t ORDER BY val');
    // Should have a column with the boolean result
    assert.equal(r.rows.length, 2);
  });
});
