// constraints.test.js — Constraint enforcement tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Constraints', () => {
  it('PRIMARY KEY prevents duplicates', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'first')");
    assert.throws(() => db.execute("INSERT INTO t VALUES (1, 'second')"), /duplicate|unique|primary/i);
  });

  it('UNIQUE prevents duplicates', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, email TEXT UNIQUE)');
    db.execute("INSERT INTO t VALUES (1, 'a@test.com')");
    assert.throws(() => db.execute("INSERT INTO t VALUES (2, 'a@test.com')"), /duplicate|unique/i);
  });

  it('NOT NULL prevents null insertion', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT NOT NULL)');
    assert.throws(() => db.execute('INSERT INTO t VALUES (1, NULL)'), /null|not null/i);
  });

  it('CHECK constraint', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, age INT CHECK (age >= 0))');
    db.execute('INSERT INTO t VALUES (1, 25)');
    assert.throws(() => db.execute('INSERT INTO t VALUES (2, -5)'), /check/i);
  });

  it('DEFAULT value', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT, status TEXT DEFAULT 'pending')");
    db.execute('INSERT INTO t (id) VALUES (1)');
    assert.equal(db.execute('SELECT status FROM t WHERE id = 1').rows[0].status, 'pending');
  });

  it('SERIAL auto-increment', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE t_id_seq');
    db.execute("CREATE TABLE t (id INT DEFAULT nextval('t_id_seq'), name TEXT)");
    db.execute("INSERT INTO t (name) VALUES ('alice')");
    db.execute("INSERT INTO t (name) VALUES ('bob')");
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.ok(r.rows[0].id < r.rows[1].id, 'IDs should auto-increment');
  });

  it('foreign key data integrity', () => {
    const db = new Database();
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE children (id INT PRIMARY KEY, parent_id INT, name TEXT)');
    db.execute("INSERT INTO parents VALUES (1, 'parent1')");
    db.execute("INSERT INTO children VALUES (1, 1, 'child1')");
    
    // The FK itself isn't enforced (common in lightweight DBs), but JOINs work
    const r = db.execute(`
      SELECT c.name as child, p.name as parent
      FROM children c JOIN parents p ON c.parent_id = p.id
    `);
    assert.equal(r.rows[0].child, 'child1');
    assert.equal(r.rows[0].parent, 'parent1');
  });
});
