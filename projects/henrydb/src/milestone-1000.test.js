// milestone-1000.test.js — The final 16 tests to reach 1000!
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('🏆🏆🏆 Milestone 1000', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('test 985: simple SELECT', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 42)');
    assert.equal(db.execute('SELECT val FROM t').rows[0].val, 42);
  });

  it('test 986: WHERE equals', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    assert.equal(db.execute('SELECT * FROM t WHERE val = 20').rows.length, 1);
  });

  it('test 987: ORDER BY ASC', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 30)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    assert.equal(db.execute('SELECT val FROM t ORDER BY val ASC').rows[0].val, 10);
  });

  it('test 988: COUNT aggregate', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    for (let i = 1; i <= 7; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM t').rows[0].c, 7);
  });

  it('test 989: SUM aggregate', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    assert.equal(db.execute('SELECT SUM(val) AS s FROM t').rows[0].s, 30);
  });

  it('test 990: DISTINCT', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'A')");
    db.execute("INSERT INTO t VALUES (2, 'A')");
    db.execute("INSERT INTO t VALUES (3, 'B')");
    assert.equal(db.execute('SELECT DISTINCT val FROM t').rows.length, 2);
  });

  it('test 991: GROUP BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp INT)');
    db.execute('INSERT INTO t VALUES (1, 1)');
    db.execute('INSERT INTO t VALUES (2, 1)');
    db.execute('INSERT INTO t VALUES (3, 2)');
    assert.equal(db.execute('SELECT grp, COUNT(*) AS c FROM t GROUP BY grp').rows.length, 2);
  });

  it('test 992: LIMIT', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    assert.equal(db.execute('SELECT * FROM t LIMIT 5').rows.length, 5);
  });

  it('test 993: JOIN', () => {
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT)');
    db.execute("INSERT INTO a VALUES (1, 'X')");
    db.execute('INSERT INTO b VALUES (1, 1)');
    assert.equal(db.execute('SELECT a.val FROM a JOIN b ON a.id = b.a_id').rows.length, 1);
  });

  it('test 994: UPDATE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('UPDATE t SET val = 99 WHERE id = 1');
    assert.equal(db.execute('SELECT val FROM t').rows[0].val, 99);
  });

  it('test 995: DELETE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('DELETE FROM t WHERE id = 1');
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM t').rows[0].c, 1);
  });

  it('test 996: CTE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    const r = db.execute('WITH d AS (SELECT * FROM t) SELECT COUNT(*) AS c FROM d');
    assert.equal(r.rows[0].c, 2);
  });

  it('test 997: window ROW_NUMBER', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    const r = db.execute('SELECT val, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM t');
    assert.equal(r.rows.length, 2);
  });

  it('test 998: CASE expression', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    const r = db.execute("SELECT CASE WHEN val > 5 THEN 'big' ELSE 'small' END AS size FROM t");
    assert.equal(r.rows[0].size, 'big');
  });

  it('test 999: ALTER TABLE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('ALTER TABLE t ADD COLUMN name TEXT');
    db.execute("UPDATE t SET name = 'hello' WHERE id = 1");
    assert.equal(db.execute('SELECT name FROM t').rows[0].name, 'hello');
  });

  it('🏆🏆🏆 TEST 1000 — HenryDB milestone achieved!', () => {
    db.execute('CREATE TABLE milestones (id INT PRIMARY KEY, test_count INT, achieved TEXT)');
    db.execute("INSERT INTO milestones VALUES (1, 1000, 'April 5, 2026')");
    const r = db.execute('SELECT * FROM milestones');
    assert.equal(r.rows[0].test_count, 1000);
    assert.equal(r.rows[0].achieved, 'April 5, 2026');
  });
});
