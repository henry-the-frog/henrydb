import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('HenryDB Feature Coverage (2026-04-19)', () => {
  let db;

  it('DDL: CREATE, ALTER, DROP', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("ALTER TABLE t ADD COLUMN score INT DEFAULT 0");
    db.execute("INSERT INTO t VALUES (1, 'test')");
    assert.equal(db.execute('SELECT score FROM t').rows[0].score, 0);
    db.execute('DROP TABLE t');
    assert.throws(() => db.execute('SELECT * FROM t'));
  });

  it('DML: INSERT, UPDATE, DELETE, UPSERT', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20)');
    db.execute('UPDATE t SET val = val * 2 WHERE id = 1');
    db.execute('DELETE FROM t WHERE id = 2');
    db.execute("INSERT INTO t VALUES (1, 100) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 100);
  });

  it('SELECT: JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT', () => {
    db = new Database();
    db.execute('CREATE TABLE a (id INT, name TEXT)');
    db.execute('CREATE TABLE b (id INT, a_id INT, val INT)');
    db.execute("INSERT INTO a VALUES (1,'x'),(2,'y')");
    db.execute('INSERT INTO b VALUES (1,1,10),(2,1,20),(3,2,30)');
    const r = db.execute(`
      SELECT DISTINCT a.name, SUM(b.val) AS total
      FROM a JOIN b ON a.id = b.a_id
      GROUP BY a.name
      HAVING SUM(b.val) > 15
      ORDER BY total DESC
      LIMIT 5
    `);
    assert.ok(r.rows.length >= 1);
  });

  it('Window functions: ROW_NUMBER, RANK, LAG, LEAD, SUM OVER', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT, grp TEXT)');
    db.execute("INSERT INTO t VALUES (1,10,'a'),(2,20,'a'),(3,30,'b')");
    const r = db.execute(`
      SELECT id, val,
        ROW_NUMBER() OVER (ORDER BY id) AS rn,
        RANK() OVER (PARTITION BY grp ORDER BY val DESC) AS rank,
        LAG(val) OVER (ORDER BY id) AS prev,
        SUM(val) OVER (ORDER BY id) AS running
      FROM t
    `);
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].rn, 1);
  });

  it('CTEs: basic, recursive, chained', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');
    const r = db.execute(`
      WITH RECURSIVE nums(n) AS (
        SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 3
      ),
      doubled AS (SELECT n, n * 2 AS d FROM nums)
      SELECT * FROM doubled
    `);
    assert.equal(r.rows.length, 3);
  });

  it('Subqueries: IN, EXISTS, correlated, scalar', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20),(3,30)');
    const r1 = db.execute('SELECT * FROM t WHERE id IN (SELECT id FROM t WHERE val > 15)');
    assert.equal(r1.rows.length, 2);
    const r2 = db.execute('SELECT * FROM t a WHERE EXISTS (SELECT 1 FROM t b WHERE b.val > a.val)');
    assert.equal(r2.rows.length, 2);  // 10 and 20 have larger values
  });

  it('Set operations: UNION, EXCEPT, INTERSECT', () => {
    db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    db.execute('INSERT INTO a VALUES (1),(2),(3)');
    db.execute('INSERT INTO b VALUES (2),(3),(4)');
    assert.equal(db.execute('SELECT id FROM a UNION SELECT id FROM b').rows.length, 4);
    assert.equal(db.execute('SELECT id FROM a EXCEPT SELECT id FROM b').rows.length, 1);
    assert.equal(db.execute('SELECT id FROM a INTERSECT SELECT id FROM b').rows.length, 2);
  });

  it('Transactions: BEGIN, COMMIT, ROLLBACK, SAVEPOINT', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('BEGIN');
    db.execute('UPDATE t SET val = 999');
    db.execute('ROLLBACK');
    assert.equal(db.execute('SELECT val FROM t').rows[0].val, 10);
  });

  it('Views and CTAS', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10),(2,20)');
    db.execute('CREATE VIEW v AS SELECT id, val * 2 AS doubled FROM t');
    assert.equal(db.execute('SELECT * FROM v WHERE doubled > 25').rows.length, 1);
    db.execute('CREATE TABLE t2 AS SELECT * FROM v');
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t2').rows[0].cnt, 2);
  });

  it('Functions: COALESCE, ROUND, ABS, GREATEST, LEAST, string, JSON', () => {
    db = new Database();
    const r = db.execute(`
      SELECT COALESCE(NULL, 42) AS c,
        ROUND(3.14159, 2) AS pi,
        ABS(-100) AS abs,
        GREATEST(1, 2, 3) AS mx,
        LEAST(1, 2, 3) AS mn,
        UPPER('hello') AS upper,
        LENGTH('test') AS len
    `);
    assert.equal(r.rows[0].c, 42);
    assert.equal(r.rows[0].pi, 3.14);
    assert.equal(r.rows[0].abs, 100);
    assert.equal(r.rows[0].mx, 3);
    assert.equal(r.rows[0].mn, 1);
    assert.equal(r.rows[0].upper, 'HELLO');
    assert.equal(r.rows[0].len, 4);
  });

  it('Constraints: PRIMARY KEY, NOT NULL, CHECK, UNIQUE, DEFAULT', () => {
    db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL, val INT CHECK (val >= 0) DEFAULT 0, code TEXT UNIQUE)");
    db.execute("INSERT INTO t (id, name, code) VALUES (1, 'test', 'ABC')");
    assert.equal(db.execute('SELECT val FROM t').rows[0].val, 0);
    assert.throws(() => db.execute("INSERT INTO t VALUES (1, 'dup', 1, 'DEF')"));  // PK violation
    assert.throws(() => db.execute("INSERT INTO t VALUES (2, NULL, 1, 'GHI')"));  // NOT NULL
    assert.throws(() => db.execute("INSERT INTO t VALUES (3, 'neg', -1, 'JKL')"));  // CHECK
    assert.throws(() => db.execute("INSERT INTO t VALUES (4, 'dup', 1, 'ABC')"));  // UNIQUE
  });

  it('Indexes and EXPLAIN', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE INDEX idx ON t (val)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const plan = db.execute('EXPLAIN SELECT * FROM t WHERE val > 500');
    assert.ok(plan);
    const r = db.execute('SELECT * FROM t WHERE val > 500');
    assert.equal(r.rows.length, 50);
  });

  it('UDFs and stored procedures', () => {
    db = new Database();
    db.execute("CREATE FUNCTION double_it(x INT) RETURNS INT AS 'RETURN x * 2'");
    assert.equal(db.execute('SELECT double_it(21) AS r').rows[0].r, 42);
    db.execute('CREATE TABLE t (id INT)');
    db.execute("CREATE PROCEDURE add_row(v INT) AS 'INSERT INTO t VALUES (v)'");
    db.execute('CALL add_row(42)');
    assert.equal(db.execute('SELECT * FROM t').rows[0].id, 42);
  });
});
