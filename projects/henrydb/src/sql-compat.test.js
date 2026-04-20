// sql-compat.test.js — SQL compatibility verification
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('SQL Compatibility — DDL', () => {
  it('CREATE TABLE with common column types', () => {
    const db = new Database();
    db.execute(`CREATE TABLE all_types (
      a INT, b BIGINT, c SMALLINT, d FLOAT, e REAL,
      g TEXT, j BOOLEAN, n SERIAL PRIMARY KEY
    )`);
    db.execute("INSERT INTO all_types (a,b,c,d,e,g,j) VALUES (1,2,3,1.5,2.5,'hi',true)");
    const r = db.execute('SELECT * FROM all_types');
    assert.equal(r.rows.length, 1);
  });

  it('CREATE TABLE with CHECK constraint', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT CHECK (val > 0))');
    db.execute('INSERT INTO t VALUES (1)');
    assert.throws(() => db.execute('INSERT INTO t VALUES (-1)'), /CHECK/i);
  });

  it('ALTER TABLE ADD COLUMN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('ALTER TABLE t ADD COLUMN name TEXT');
    db.execute("UPDATE t SET name = 'alice' WHERE id = 1");
    assert.equal(db.execute('SELECT name FROM t WHERE id = 1').rows[0].name, 'alice');
  });

  it('DROP TABLE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('DROP TABLE t');
    assert.throws(() => db.execute('SELECT * FROM t'));
  });

  it('DROP TABLE IF EXISTS', () => {
    const db = new Database();
    db.execute('DROP TABLE IF EXISTS nonexistent'); // Should not throw
  });
});

describe('SQL Compatibility — DML', () => {
  it('INSERT ... RETURNING', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)');
    const r = db.execute("INSERT INTO t (name) VALUES ('alice') RETURNING id, name");
    assert.equal(r.rows[0].name, 'alice');
    assert.ok(r.rows[0].id > 0);
  });

  it('UPDATE ... RETURNING', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'alice')");
    const r = db.execute("UPDATE t SET name = 'bob' WHERE id = 1 RETURNING name");
    assert.equal(r.rows[0].name, 'bob');
  });

  it('DELETE ... RETURNING', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'alice'),(2, 'bob')");
    const r = db.execute('DELETE FROM t WHERE id = 1 RETURNING name');
    assert.equal(r.rows[0].name, 'alice');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 1);
  });

  it('INSERT ... ON CONFLICT (UPSERT)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");
    db.execute("INSERT INTO t VALUES (1, 'updated') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 'updated');
  });
});

describe('SQL Compatibility — Expressions', () => {
  it('CAST / type casting', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT CAST('42' AS INT) as r").rows[0].r, 42);
    assert.equal(db.execute("SELECT CAST(3.14 AS INT) as r").rows[0].r, 3);
    assert.equal(db.execute("SELECT '42'::INT as r").rows[0].r, 42);
  });

  it('BETWEEN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3),(4),(5)');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t WHERE val BETWEEN 2 AND 4').rows[0].c, 3);
  });

  it('CASE WHEN with NULL', () => {
    const db = new Database();
    const r = db.execute('SELECT CASE WHEN NULL THEN 1 ELSE 0 END as r');
    assert.equal(r.rows[0].r, 0);
  });

  it('COALESCE chain', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT COALESCE(NULL, NULL, 3, 4) as r').rows[0].r, 3);
  });

  it('GEN_RANDOM_UUID() format', () => {
    const db = new Database();
    const uuid = db.execute('SELECT GEN_RANDOM_UUID() as id').rows[0].id;
    assert.ok(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(uuid));
  });
});

describe('SQL Compatibility — Set Operations', () => {
  it('UNION ALL', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (val INT)');
    db.execute('CREATE TABLE b (val INT)');
    db.execute('INSERT INTO a VALUES (1),(2)');
    db.execute('INSERT INTO b VALUES (2),(3)');
    const r = db.execute('SELECT val FROM a UNION ALL SELECT val FROM b ORDER BY val');
    assert.equal(r.rows.length, 4); // 1,2,2,3
  });

  it('UNION (distinct)', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (val INT)');
    db.execute('CREATE TABLE b (val INT)');
    db.execute('INSERT INTO a VALUES (1),(2)');
    db.execute('INSERT INTO b VALUES (2),(3)');
    const r = db.execute('SELECT val FROM a UNION SELECT val FROM b ORDER BY val');
    assert.equal(r.rows.length, 3); // 1,2,3
  });

  it('INTERSECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (val INT)');
    db.execute('CREATE TABLE b (val INT)');
    db.execute('INSERT INTO a VALUES (1),(2),(3)');
    db.execute('INSERT INTO b VALUES (2),(3),(4)');
    const r = db.execute('SELECT val FROM a INTERSECT SELECT val FROM b ORDER BY val');
    assert.deepEqual(r.rows.map(r => r.val), [2, 3]);
  });

  it('EXCEPT', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (val INT)');
    db.execute('CREATE TABLE b (val INT)');
    db.execute('INSERT INTO a VALUES (1),(2),(3)');
    db.execute('INSERT INTO b VALUES (2),(3),(4)');
    const r = db.execute('SELECT val FROM a EXCEPT SELECT val FROM b ORDER BY val');
    assert.deepEqual(r.rows.map(r => r.val), [1]);
  });
});
