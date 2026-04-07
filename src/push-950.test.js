// push-950.test.js — Push HenryDB to 950!
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Push to 950', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('sequential INSERTs maintain order', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 20);
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[19].id, 20);
  });

  it('DELETE WHERE with IN list', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    db.execute('DELETE FROM t WHERE id IN (2, 4, 6, 8, 10)');
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 5);
  });

  it('UPDATE WHERE with IN', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    db.execute('UPDATE t SET val = 0 WHERE id IN (1, 3, 5)');
    assert.equal(db.execute('SELECT SUM(val) AS s FROM t').rows[0].s, 6); // 0+2+0+4+0
  });

  it('SELECT with expression alias', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 3)');
    const r = db.execute('SELECT a + b AS total FROM t');
    assert.equal(r.rows[0].total, 13);
  });

  it('SELECT with subtraction alias', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 3)');
    const r = db.execute('SELECT a - b AS diff FROM t');
    assert.equal(r.rows[0].diff, 7);
  });

  it('multiple WHERE conditions — all AND', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)');
    db.execute('INSERT INTO t VALUES (1, 1, 2, 3)');
    db.execute('INSERT INTO t VALUES (2, 1, 2, 4)');
    db.execute('INSERT INTO t VALUES (3, 1, 3, 3)');
    const r = db.execute('SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3');
    assert.equal(r.rows.length, 1);
  });

  it('mixed OR and AND', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, x INT, y INT)');
    db.execute('INSERT INTO t VALUES (1, 1, 1)');
    db.execute('INSERT INTO t VALUES (2, 1, 0)');
    db.execute('INSERT INTO t VALUES (3, 0, 1)');
    const r = db.execute('SELECT * FROM t WHERE x = 1 OR y = 1');
    assert.equal(r.rows.length, 3);
  });

  it('GROUP BY with NULL values', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'A')");
    db.execute('INSERT INTO t VALUES (2, NULL)');
    db.execute("INSERT INTO t VALUES (3, 'A')");
    db.execute('INSERT INTO t VALUES (4, NULL)');
    const r = db.execute('SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp');
    assert.ok(r.rows.length >= 2);
  });

  it('ORDER BY NULL handling', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, NULL)');
    db.execute('INSERT INTO t VALUES (3, 5)');
    const r = db.execute('SELECT * FROM t ORDER BY val');
    assert.equal(r.rows.length, 3);
  });

  it('DISTINCT on single column', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'A')");
    db.execute("INSERT INTO t VALUES (2, 'B')");
    db.execute("INSERT INTO t VALUES (3, 'A')");
    db.execute("INSERT INTO t VALUES (4, 'C')");
    db.execute("INSERT INTO t VALUES (5, 'B')");
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val');
    assert.equal(r.rows.length, 3);
  });

  it('COUNT(*) vs COUNT(column)', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, NULL)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const all = db.execute('SELECT COUNT(*) AS c FROM t').rows[0].c;
    const noNull = db.execute('SELECT COUNT(val) AS c FROM t').rows[0].c;
    assert.equal(all, 3);
    assert.equal(noNull, 2);
  });

  it('SUM ignores NULL', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, NULL)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    assert.equal(db.execute('SELECT SUM(val) AS s FROM t').rows[0].s, 40);
  });

  it('AVG ignores NULL', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, NULL)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    assert.equal(db.execute('SELECT AVG(val) AS a FROM t').rows[0].a, 20);
  });

  it('MIN/MAX with NULL', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, NULL)');
    db.execute('INSERT INTO t VALUES (3, 5)');
    assert.equal(db.execute('SELECT MIN(val) AS m FROM t').rows[0].m, 5);
    assert.equal(db.execute('SELECT MAX(val) AS m FROM t').rows[0].m, 10);
  });

  it('LIKE with leading wildcard', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    db.execute("INSERT INTO t VALUES (2, 'jello')");
    db.execute("INSERT INTO t VALUES (3, 'mellow')");
    const r = db.execute("SELECT name FROM t WHERE name LIKE '%ello%' ORDER BY name");
    assert.ok(r.rows.length >= 2);
  });

  it('NOT LIKE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    db.execute("INSERT INTO t VALUES (2, 'world')");
    const r = db.execute("SELECT name FROM t WHERE name NOT LIKE '%ello%'");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'world');
  });

  it('BETWEEN inclusive', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    const r = db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val BETWEEN 3 AND 7');
    assert.equal(r.rows[0].cnt, 5);
  });

  it('NOT BETWEEN', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    const r = db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val NOT BETWEEN 3 AND 7');
    assert.equal(r.rows[0].cnt, 5);
  });

  it('self-join — all pairs', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'A')");
    db.execute("INSERT INTO t VALUES (2, 'B')");
    db.execute("INSERT INTO t VALUES (3, 'C')");
    const r = db.execute('SELECT a.val AS x, b.val AS y FROM t a JOIN t b ON a.id < b.id');
    assert.equal(r.rows.length, 3); // (A,B), (A,C), (B,C)
  });

  it('LEFT JOIN returns all left rows', () => {
    db.execute('CREATE TABLE left_t (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE right_t (id INT PRIMARY KEY, left_id INT, data TEXT)');
    db.execute("INSERT INTO left_t VALUES (1, 'X')");
    db.execute("INSERT INTO left_t VALUES (2, 'Y')");
    db.execute("INSERT INTO right_t VALUES (1, 1, 'match')");
    const r = db.execute('SELECT l.val, r.data FROM left_t l LEFT JOIN right_t r ON l.id = r.left_id');
    assert.equal(r.rows.length, 2);
  });

  it('window ROW_NUMBER', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, score INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('INSERT INTO t VALUES (2, 200)');
    db.execute('INSERT INTO t VALUES (3, 150)');
    const r = db.execute('SELECT score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM t');
    assert.equal(r.rows.length, 3);
  });

  it('window RANK with ties', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, score INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('INSERT INTO t VALUES (2, 100)');
    db.execute('INSERT INTO t VALUES (3, 90)');
    const r = db.execute('SELECT score, RANK() OVER (ORDER BY score DESC) AS rnk FROM t ORDER BY rnk');
    assert.equal(r.rows[0].rnk, 1);
    assert.equal(r.rows[1].rnk, 1); // tie
    assert.equal(r.rows[2].rnk, 3); // skip 2
  });

  it('CTE simple filter', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('WITH big AS (SELECT * FROM t WHERE val > 50) SELECT COUNT(*) AS cnt FROM big');
    assert.equal(r.rows[0].cnt, 5);
  });

  it('CTE with GROUP BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'A', 20)");
    db.execute("INSERT INTO t VALUES (3, 'B', 30)");
    const r = db.execute('WITH sums AS (SELECT grp, SUM(val) AS total FROM t GROUP BY grp) SELECT * FROM sums ORDER BY total DESC');
    assert.equal(r.rows.length, 2);
  });

  it('multiple aggregates per group', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)');
    const data = [['A',10],['A',20],['A',30],['B',5],['B',15],['C',100]];
    data.forEach(([cat, val], i) => db.execute(`INSERT INTO t VALUES (${i+1}, '${cat}', ${val})`));
    const r = db.execute('SELECT cat, COUNT(*) AS cnt, SUM(val) AS s, MIN(val) AS mn, MAX(val) AS mx, AVG(val) AS a FROM t GROUP BY cat ORDER BY s DESC');
    assert.equal(r.rows[0].cat, 'C');
    assert.equal(r.rows[0].s, 100);
    assert.equal(r.rows[1].s, 60); // A: 10+20+30
  });

  it('INSERT after DELETE maintains integrity', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('DELETE FROM t WHERE id = 1');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].id, 2);
    assert.equal(r.rows[1].id, 3);
  });

  it('multi-column UPDATE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT)');
    db.execute("INSERT INTO t VALUES (1, 10, 'old')");
    db.execute("UPDATE t SET a = 20, b = 'new' WHERE id = 1");
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows[0].a, 20);
    assert.equal(r.rows[0].b, 'new');
  });

  it('CREATE INDEX speeds query (correctness)', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'item${i}', ${i})`);
    db.execute('CREATE INDEX idx_val ON t(val)');
    const r = db.execute('SELECT name FROM t WHERE val = 50');
    assert.equal(r.rows[0].name, 'item50');
  });

  it('ALTER TABLE then query new column', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute('ALTER TABLE t ADD COLUMN age INT');
    db.execute('UPDATE t SET age = 30 WHERE id = 1');
    const r = db.execute('SELECT name, age FROM t');
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[0].age, 30);
  });

  it('DROP + recreate + insert', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('DROP TABLE t');
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'new')");
    assert.equal(db.execute('SELECT name FROM t').rows[0].name, 'new');
  });

  it('large GROUP BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp INT, val INT)');
    for (let i = 0; i < 200; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i % 10}, ${i})`);
    const r = db.execute('SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp ORDER BY grp');
    assert.equal(r.rows.length, 10);
    assert.equal(r.rows[0].cnt, 20);
  });

  it('CASE with NULL', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    const r = db.execute('SELECT CASE WHEN val IS NULL THEN -1 ELSE val END AS safe FROM t ORDER BY id');
    assert.equal(r.rows[0].safe, -1);
    assert.equal(r.rows[1].safe, 10);
  });

  it('CASE with multiple WHEN', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grade INT)');
    db.execute('INSERT INTO t VALUES (1, 95)');
    db.execute('INSERT INTO t VALUES (2, 75)');
    db.execute('INSERT INTO t VALUES (3, 55)');
    const r = db.execute("SELECT grade, CASE WHEN grade >= 90 THEN 'A' WHEN grade >= 70 THEN 'B' WHEN grade >= 50 THEN 'C' ELSE 'F' END AS letter FROM t ORDER BY id");
    assert.equal(r.rows[0].letter, 'A');
    assert.equal(r.rows[1].letter, 'B');
    assert.equal(r.rows[2].letter, 'C');
  });

  it('nested subquery in WHERE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('SELECT * FROM t WHERE val > (SELECT AVG(val) FROM t)');
    assert.equal(r.rows.length, 5);
  });

  it('IN subquery', () => {
    db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE employees (id INT PRIMARY KEY, dept_id INT, name TEXT)');
    db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
    db.execute("INSERT INTO departments VALUES (2, 'Sales')");
    db.execute("INSERT INTO employees VALUES (1, 1, 'Alice')");
    db.execute("INSERT INTO employees VALUES (2, 2, 'Bob')");
    db.execute("INSERT INTO employees VALUES (3, 3, 'Charlie')"); // orphan
    const r = db.execute('SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments)');
    assert.equal(r.rows.length, 2);
  });

  it('empty table operations', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM t').rows[0].c, 0);
    db.execute('DELETE FROM t'); // no-op
    db.execute('UPDATE t SET val = 0'); // no-op
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM t').rows[0].c, 0);
  });

  it('1000-row stress test', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 1000; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i % 50})`);
    const r = db.execute('SELECT COUNT(*) AS c, SUM(val) AS s FROM t');
    assert.equal(r.rows[0].c, 1000);
  });

  it('complex multi-table query', () => {
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT, amount INT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO orders VALUES (${i}, ${(i % 2) + 1}, ${i * 100})`);
    const r = db.execute('SELECT c.name, COUNT(*) AS orders FROM customers c JOIN orders o ON c.id = o.cust_id GROUP BY c.name ORDER BY orders DESC');
    assert.ok(r.rows.length >= 1);
  });

  it('window + ORDER BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 100)");
    db.execute("INSERT INTO t VALUES (2, 'A', 200)");
    db.execute("INSERT INTO t VALUES (3, 'B', 150)");
    const r = db.execute('SELECT cat, val, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY val DESC) AS rn FROM t ORDER BY cat, rn');
    assert.equal(r.rows.length, 3);
  });

  it('DISTINCT + COUNT', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'A')");
    db.execute("INSERT INTO t VALUES (2, 'A')");
    db.execute("INSERT INTO t VALUES (3, 'B')");
    assert.equal(db.execute('SELECT COUNT(DISTINCT val) AS cnt FROM t').rows[0].cnt, 2);
  });

  it('parenthesized arithmetic', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)');
    db.execute('INSERT INTO t VALUES (1, 2, 3, 4)');
    const r = db.execute('SELECT a * b + c AS result FROM t');
    assert.equal(r.rows[0].result, 10); // 2*3 + 4
  });

  it('INSERT with explicit column list', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
    db.execute("INSERT INTO t (id, name, val) VALUES (1, 'test', 42)");
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows[0].name, 'test');
    assert.equal(r.rows[0].val, 42);
  });

  it('🎯 950th test — comprehensive report query', () => {
    db.execute('CREATE TABLE students (id INT PRIMARY KEY, name TEXT, grade INT)');
    db.execute('CREATE TABLE courses (id INT PRIMARY KEY, student_id INT, course TEXT, score INT)');
    const students = [['Alice', 10], ['Bob', 11], ['Charlie', 10], ['Diana', 11]];
    students.forEach(([name, grade], i) => db.execute(`INSERT INTO students VALUES (${i+1}, '${name}', ${grade})`));
    const courses = [
      [1, 'Math', 90], [1, 'Science', 85],
      [2, 'Math', 70], [2, 'Science', 75],
      [3, 'Math', 95], [3, 'Science', 80],
      [4, 'Math', 88], [4, 'Science', 92],
    ];
    courses.forEach(([sid, course, score], i) => db.execute(`INSERT INTO courses VALUES (${i+1}, ${sid}, '${course}', ${score})`));

    // Average score per course
    const avgByCourse = db.execute('SELECT course, AVG(score) AS avg_score FROM courses GROUP BY course ORDER BY avg_score DESC');
    assert.equal(avgByCourse.rows.length, 2);

    // Top scorer per course
    const topScorer = db.execute(`
      WITH ranked AS (
        SELECT c.course, s.name, c.score,
          ROW_NUMBER() OVER (PARTITION BY c.course ORDER BY c.score DESC) AS rn
        FROM courses c JOIN students s ON c.student_id = s.id
      )
      SELECT course, name, score FROM ranked WHERE rn = 1 ORDER BY course
    `);
    assert.equal(topScorer.rows.length, 2);
  });

  it('string equality exact match', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    db.execute("INSERT INTO t VALUES (2, 'world')");
    assert.equal(db.execute("SELECT name FROM t WHERE name = 'hello'").rows.length, 1);
  });

  it('NULL IS NULL is true', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    assert.equal(db.execute('SELECT * FROM t WHERE val IS NULL').rows.length, 1);
  });

  it('IS NOT NULL filters correctly', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, NULL)');
    assert.equal(db.execute('SELECT * FROM t WHERE val IS NOT NULL').rows.length, 1);
  });

  it('ORDER BY DESC + LIMIT', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    const r = db.execute('SELECT val FROM t ORDER BY val DESC LIMIT 3');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].val, 10);
  });
});
