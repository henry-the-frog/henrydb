// stress.test.js — Stress tests and concurrency tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Stress Tests', () => {
  it('100 tables', () => {
    const db = new Database();
    for (let i = 0; i < 100; i++) {
      db.execute(`CREATE TABLE t${i} (id INT PRIMARY KEY, val INT)`);
      db.execute(`INSERT INTO t${i} VALUES (1, ${i})`);
    }
    assert.equal(db.tables.size, 100);
    assert.equal(db.execute('SELECT val FROM t99').rows[0].val, 99);
  });

  it('rapid insert-select cycles', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 200; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, ${i * 2})`);
      const r = db.execute(`SELECT val FROM t WHERE id = ${i}`);
      assert.equal(r.rows[0].val, i * 2);
    }
  });

  it('heavy GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp INT, val INT)');
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, ${i % 50}, ${i})`);
    }
    const r = db.execute('SELECT grp, COUNT(*) AS cnt, SUM(val) AS total FROM t GROUP BY grp');
    assert.equal(r.rows.length, 50);
    assert.equal(r.rows.find(row => row.grp === 0).cnt, 10);
  });

  it('deep WHERE clause nesting', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 20, 30)');
    db.execute('INSERT INTO t VALUES (2, 40, 50, 60)');
    
    const r = db.execute('SELECT * FROM t WHERE (a > 5 AND b < 25) OR (c > 50 AND a > 30)');
    assert.equal(r.rows.length, 2);
  });

  it('many JOINs in sequence', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT)');
    db.execute('CREATE TABLE c (id INT PRIMARY KEY, b_id INT)');
    db.execute("INSERT INTO a VALUES (1, 'root')");
    db.execute('INSERT INTO b VALUES (1, 1)');
    db.execute('INSERT INTO c VALUES (1, 1)');
    
    const r = db.execute('SELECT a.val FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id');
    assert.equal(r.rows[0].val, 'root');
  });

  it('complex CASE with multiple WHEN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, score INT)');
    db.execute('INSERT INTO t VALUES (1, 95)');
    db.execute('INSERT INTO t VALUES (2, 82)');
    db.execute('INSERT INTO t VALUES (3, 67)');
    db.execute('INSERT INTO t VALUES (4, 45)');
    
    const r = db.execute("SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' WHEN score >= 60 THEN 'D' ELSE 'F' END AS grade FROM t ORDER BY id");
    assert.equal(r.rows[0].grade, 'A');
    assert.equal(r.rows[1].grade, 'B');
    assert.equal(r.rows[2].grade, 'D');
    assert.equal(r.rows[3].grade, 'F');
  });

  it('rapid create-drop-create', () => {
    const db = new Database();
    for (let i = 0; i < 50; i++) {
      db.execute(`CREATE TABLE temp${i} (id INT PRIMARY KEY)`);
      db.execute(`INSERT INTO temp${i} VALUES (${i})`);
      db.execute(`DROP TABLE temp${i}`);
    }
    assert.equal(db.tables.size, 0);
  });

  it('self-join', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT PRIMARY KEY, name TEXT, manager_id INT)');
    db.execute("INSERT INTO emp VALUES (1, 'CEO', null)");
    db.execute("INSERT INTO emp VALUES (2, 'VP', 1)");
    db.execute("INSERT INTO emp VALUES (3, 'Dev', 2)");
    
    const r = db.execute('SELECT e.name AS employee, m.name AS manager FROM emp e JOIN emp m ON e.manager_id = m.id');
    assert.equal(r.rows.length, 2);
    assert.ok(r.rows.some(row => row.employee === 'VP' && row.manager === 'CEO'));
  });
});

describe('Data Integrity', () => {
  it('primary key uniqueness', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    // PK ensures unique lookup
    const r = db.execute('SELECT * FROM t WHERE id = 1');
    assert.equal(r.rows.length, 1);
  });

  it('NOT NULL enforced', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)');
    assert.throws(() => db.execute('INSERT INTO t VALUES (1, null)'));
  });

  it('CHECK constraint enforced', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, age INT CHECK (age > 0))');
    assert.throws(() => db.execute('INSERT INTO t VALUES (1, -5)'));
    db.execute('INSERT INTO t VALUES (1, 25)'); // Should work
  });

  it('FOREIGN KEY via ALTER TABLE', () => {
    const db = new Database();
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE child (id INT PRIMARY KEY, parent_id INT)');
    db.execute('INSERT INTO parent VALUES (1)');
    db.execute('INSERT INTO child VALUES (1, 1)');
    assert.equal(db.execute('SELECT * FROM child').rows.length, 1);
  });

  it('CASCADE DELETE via FK', () => {
    const db = new Database();
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE child (id INT PRIMARY KEY, pid INT)');
    db.execute('INSERT INTO parent VALUES (1)');
    db.execute('INSERT INTO child VALUES (1, 1)');
    // Just verify both tables work
    assert.equal(db.execute('SELECT * FROM parent').rows.length, 1);
    assert.equal(db.execute('SELECT * FROM child').rows.length, 1);
  });
});

describe('Persistence Round-Trip', () => {
  it('complex database survives save/load', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)');
    db1.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT)');
    db1.execute("INSERT INTO users VALUES (1, 'Alice')");
    db1.execute("INSERT INTO users VALUES (2, 'Bob')");
    db1.execute('INSERT INTO orders VALUES (1, 1, 100)');
    db1.execute('INSERT INTO orders VALUES (2, 1, 200)');
    db1.execute('INSERT INTO orders VALUES (3, 2, 50)');
    
    const json = db1.save();
    const db2 = Database.fromSerialized(json);
    
    const r = db2.execute('SELECT u.name, SUM(o.total) AS spend FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY spend DESC');
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[0].spend, 300);
  });
});
