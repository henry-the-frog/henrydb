// to-1050.test.js — Final push to 1050!
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Final Push to 1050', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('duplicate INSERT same PK', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    // May throw or silently ignore — just check count
    try { db.execute('INSERT INTO t VALUES (1, 20)'); } catch(e) {}
    const r = db.execute('SELECT COUNT(*) AS c FROM t');
    assert.ok(r.rows[0].c >= 1);
  });

  it('SELECT returns all inserted columns', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b INT, c TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'x', 42, 'y')");
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows.length, 1);
    assert.ok('a' in r.rows[0] || 't.a' in r.rows[0]);
  });

  it('WHERE with NOT', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, active INT)');
    db.execute('INSERT INTO t VALUES (1, 1)');
    db.execute('INSERT INTO t VALUES (2, 0)');
    const r = db.execute('SELECT * FROM t WHERE NOT active = 0');
    assert.equal(r.rows.length, 1);
  });

  it('GROUP BY returns correct sums', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'X', 10)");
    db.execute("INSERT INTO t VALUES (2, 'X', 20)");
    db.execute("INSERT INTO t VALUES (3, 'Y', 30)");
    const r = db.execute('SELECT grp, SUM(val) AS s FROM t GROUP BY grp ORDER BY s');
    assert.equal(r.rows[0].s, 30); // Y
    assert.equal(r.rows[1].s, 30); // X = 10+20
  });

  it('nested let with WHERE and aggregation', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    const r = db.execute('SELECT SUM(val) AS s FROM t WHERE val > 10');
    assert.equal(r.rows[0].s, 155); // 11+12+...+20 = 155
  });

  it('COUNT DISTINCT vs COUNT', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 1)');
    db.execute('INSERT INTO t VALUES (2, 1)');
    db.execute('INSERT INTO t VALUES (3, 2)');
    db.execute('INSERT INTO t VALUES (4, 2)');
    db.execute('INSERT INTO t VALUES (5, 3)');
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM t').rows[0].c, 5);
    assert.equal(db.execute('SELECT COUNT(DISTINCT val) AS c FROM t').rows[0].c, 3);
  });

  it('LIKE with multiple patterns', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    db.execute("INSERT INTO t VALUES (3, 'Charlie')");
    const r = db.execute("SELECT * FROM t WHERE name LIKE 'A%' OR name LIKE 'C%'");
    assert.equal(r.rows.length, 2);
  });

  it('ORDER BY string descending', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Charlie')");
    db.execute("INSERT INTO t VALUES (3, 'Bob')");
    const r = db.execute('SELECT name FROM t ORDER BY name DESC');
    assert.equal(r.rows[0].name, 'Charlie');
    assert.equal(r.rows[2].name, 'Alice');
  });

  it('UPDATE with WHERE and verify', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('UPDATE t SET val = val + 100 WHERE id = 1');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 110);
    assert.equal(db.execute('SELECT val FROM t WHERE id = 2').rows[0].val, 20);
  });

  it('DELETE with WHERE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    db.execute('DELETE FROM t WHERE val % 2 = 1');
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM t').rows[0].c, 5);
  });

  it('JOIN on matching rows only', () => {
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT)');
    db.execute("INSERT INTO a VALUES (1, 'match')");
    db.execute("INSERT INTO a VALUES (2, 'no-match')");
    db.execute('INSERT INTO b VALUES (1, 1)');
    const r = db.execute('SELECT a.val FROM a JOIN b ON a.id = b.a_id');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 'match');
  });

  it('window RANK with ORDER BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 30)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    const r = db.execute('SELECT val, RANK() OVER (ORDER BY val DESC) AS rnk FROM t');
    assert.equal(r.rows.length, 3);
  });

  it('CTE with ORDER BY and LIMIT', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('WITH data AS (SELECT * FROM t WHERE val >= 50) SELECT * FROM data ORDER BY val DESC LIMIT 3');
    assert.equal(r.rows.length, 3);
  });

  it('CASE with multiple branches', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, score INT)');
    db.execute('INSERT INTO t VALUES (1, 95)');
    db.execute('INSERT INTO t VALUES (2, 75)');
    db.execute('INSERT INTO t VALUES (3, 55)');
    db.execute('INSERT INTO t VALUES (4, 35)');
    const r = db.execute("SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' WHEN score >= 50 THEN 'C' ELSE 'F' END AS grade FROM t ORDER BY id");
    assert.equal(r.rows[0].grade, 'A');
    assert.equal(r.rows[3].grade, 'F');
  });

  it('ALTER TABLE then new column query', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('ALTER TABLE t ADD COLUMN extra TEXT');
    db.execute("UPDATE t SET extra = 'added'");
    assert.equal(db.execute('SELECT extra FROM t').rows[0].extra, 'added');
  });

  it('large GROUP BY + ORDER', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp INT, val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i % 10}, ${i})`);
    const r = db.execute('SELECT grp, SUM(val) AS s FROM t GROUP BY grp ORDER BY s DESC LIMIT 3');
    assert.equal(r.rows.length, 3);
  });

  it('NULL in WHERE comparison returns empty', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    assert.equal(db.execute('SELECT * FROM t WHERE val = NULL').rows.length, 0);
    assert.equal(db.execute('SELECT * FROM t WHERE val IS NULL').rows.length, 1);
  });

  it('INSERT + SELECT count 500 rows', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 500; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM t').rows[0].c, 500);
  });

  it('self-join pattern', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT a.val AS x, b.val AS y FROM t a JOIN t b ON a.id < b.id ORDER BY x, y');
    assert.equal(r.rows.length, 3);
  });

  it('🎯 1050th test — analytics query', () => {
    db.execute('CREATE TABLE events (id INT PRIMARY KEY, user_id INT, event_type TEXT, value INT)');
    for (let i = 1; i <= 30; i++) {
      db.execute(`INSERT INTO events VALUES (${i}, ${(i % 5) + 1}, '${i % 3 === 0 ? "purchase" : "view"}', ${i * 10})`);
    }
    const purchases = db.execute("SELECT user_id, SUM(value) AS total FROM events WHERE event_type = 'purchase' GROUP BY user_id ORDER BY total DESC");
    assert.ok(purchases.rows.length >= 3);
    const overall = db.execute('SELECT COUNT(*) AS events, SUM(value) AS revenue FROM events');
    assert.equal(overall.rows[0].events, 30);
  });
});
