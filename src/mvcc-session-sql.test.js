// mvcc-session-sql.test.js — Window functions, UNION, ORDER BY LIMIT through sessions
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-session-sql-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

function setup() {
  db = fresh();
  db.execute('CREATE TABLE employees (id INT, name TEXT, dept TEXT, salary INT)');
  db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Eng', 90000)");
  db.execute("INSERT INTO employees VALUES (2, 'Bob', 'Mkt', 70000)");
  db.execute("INSERT INTO employees VALUES (3, 'Carol', 'Eng', 95000)");
  db.execute("INSERT INTO employees VALUES (4, 'Dave', 'Mkt', 65000)");
  db.execute("INSERT INTO employees VALUES (5, 'Eve', 'Eng', 85000)");
  db.execute("INSERT INTO employees VALUES (6, 'Frank', 'HR', 75000)");
}

describe('MVCC + Session SQL Tests', () => {
  afterEach(cleanup);

  describe('Window Functions', () => {
    it('ROW_NUMBER() through MVCC', () => {
      setup();
      const r = db.execute(`
        SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rank
        FROM employees
        ORDER BY dept, rank
      `);
      assert.ok(r.rows.length === 6);
      const engFirst = r.rows.find(row => row.dept === 'Eng' && row.rank === 1);
      assert.equal(engFirst.name, 'Carol');
    });

    it('SUM() OVER window through MVCC', () => {
      setup();
      const r = db.execute(`
        SELECT name, salary, SUM(salary) OVER (ORDER BY salary) as running_total
        FROM employees
        ORDER BY salary
      `);
      assert.equal(r.rows[0].salary, 65000); // Dave
      assert.equal(r.rows[0].running_total, 65000);
    });

    it('RANK() within session transaction', () => {
      setup();
      const s = db.session();
      s.begin();
      s.execute("INSERT INTO employees VALUES (7, 'Grace', 'Eng', 100000)");
      const r = s.execute(`
        SELECT name, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rank
        FROM employees
        WHERE dept = 'Eng'
      `);
      const grace = r.rows.find(row => row.name === 'Grace');
      assert.ok(grace, 'Grace should be visible in session');
      assert.equal(grace.rank, 1); // Highest salary = rank 1
      s.rollback();
      s.close();
    });
  });

  describe('UNION', () => {
    it('UNION ALL through MVCC', () => {
      setup();
      const r = db.execute(`
        SELECT name, 'high' as category FROM employees WHERE salary >= 80000
        UNION ALL
        SELECT name, 'low' as category FROM employees WHERE salary < 80000
      `);
      assert.equal(r.rows.length, 6);
    });

    it('UNION (distinct) through MVCC', () => {
      setup();
      const r = db.execute(`
        SELECT dept FROM employees WHERE salary > 80000
        UNION
        SELECT dept FROM employees WHERE salary < 70000
      `);
      assert.ok(r.rows.length >= 1);
    });

    it('UNION in session with uncommitted data', () => {
      setup();
      const s = db.session();
      s.begin();
      s.execute("INSERT INTO employees VALUES (7, 'Grace', 'Eng', 200000)");
      const r = s.execute(`
        SELECT name FROM employees WHERE salary > 100000
        UNION ALL
        SELECT name FROM employees WHERE dept = 'HR'
      `);
      assert.ok(r.rows.some(row => row.name === 'Grace'));
      assert.ok(r.rows.some(row => row.name === 'Frank'));
      s.rollback();
      s.close();
    });
  });

  describe('ORDER BY + LIMIT', () => {
    it('ORDER BY with LIMIT through MVCC', () => {
      setup();
      const r = db.execute('SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3');
      assert.equal(r.rows.length, 3);
      assert.equal(r.rows[0].name, 'Carol');
      assert.equal(r.rows[0].salary, 95000);
    });

    it('LIMIT with OFFSET through MVCC', () => {
      setup();
      const r = db.execute('SELECT name FROM employees ORDER BY name LIMIT 2 OFFSET 2');
      assert.equal(r.rows.length, 2);
      assert.equal(r.rows[0].name, 'Carol');
      assert.equal(r.rows[1].name, 'Dave');
    });
  });

  describe('Concurrent Readers + Writers', () => {
    it('reader sees consistent window function results during write', () => {
      setup();
      const reader = db.session();
      const writer = db.session();
      reader.begin();
      // Reader takes snapshot
      const r1 = reader.execute(`
        SELECT name, SUM(salary) OVER () as total
        FROM employees
        WHERE dept = 'Eng'
        LIMIT 1
      `);
      const engTotal = r1.rows[0].total; // 270000
      // Writer adds high-salary employee
      writer.begin();
      writer.execute("INSERT INTO employees VALUES (7, 'Grace', 'Eng', 200000)");
      writer.commit();
      // Reader still sees old total (snapshot)
      const r2 = reader.execute(`
        SELECT SUM(salary) as total FROM employees WHERE dept = 'Eng'
      `);
      assert.equal(r2.rows[0].total, 270000, 'reader should see snapshot total');
      reader.commit();
      // New query sees updated total
      const r3 = db.execute('SELECT SUM(salary) as total FROM employees WHERE dept = \'Eng\'');
      assert.equal(r3.rows[0].total, 470000, 'new query should see writer data');
      reader.close();
      writer.close();
    });

    it('concurrent HAVING with different snapshots', () => {
      setup();
      const s1 = db.session();
      const s2 = db.session();
      s1.begin();
      s1.execute("INSERT INTO employees VALUES (7, 'Grace', 'Eng', 60000)");
      s2.begin();
      // s2 doesn't see Grace
      const r1 = s2.execute(`
        SELECT dept, AVG(salary) as avg_sal
        FROM employees
        GROUP BY dept
        HAVING AVG(salary) > 75000
      `);
      const engDept1 = r1.rows.find(row => row.dept === 'Eng');
      assert.ok(engDept1, 'Eng should appear (avg 90000)');
      s1.commit();
      s2.commit();
      // After commit, Eng avg drops: (90k+95k+85k+60k)/4 = 82.5k
      const r2 = db.execute(`
        SELECT dept, AVG(salary) as avg_sal
        FROM employees
        GROUP BY dept
        HAVING AVG(salary) > 75000
      `);
      const engDept2 = r2.rows.find(row => row.dept === 'Eng');
      assert.ok(engDept2, 'Eng should still appear (avg 82500)');
      s1.close();
      s2.close();
    });

    it('CASE expression through MVCC', () => {
      setup();
      const r = db.execute(`
        SELECT name,
          CASE
            WHEN salary > 90000 THEN 'senior'
            WHEN salary > 70000 THEN 'mid'
            ELSE 'junior'
          END as level
        FROM employees
        ORDER BY salary DESC
      `);
      assert.equal(r.rows[0].level, 'senior'); // Carol 95000
      const dave = r.rows.find(row => row.name === 'Dave');
      assert.equal(dave.level, 'junior'); // Dave 65000
    });

    it('DISTINCT through MVCC with concurrent changes', () => {
      setup();
      const s = db.session();
      s.begin();
      // Reader's snapshot
      const r1 = s.execute('SELECT DISTINCT dept FROM employees ORDER BY dept');
      assert.equal(r1.rows.length, 3); // Eng, HR, Mkt
      // Auto-commit adds new dept
      db.execute("INSERT INTO employees VALUES (8, 'Hank', 'Legal', 80000)");
      // Session still sees 3 depts
      const r2 = s.execute('SELECT DISTINCT dept FROM employees ORDER BY dept');
      assert.equal(r2.rows.length, 3);
      s.commit();
      // New query sees 4 depts
      const r3 = db.execute('SELECT DISTINCT dept FROM employees ORDER BY dept');
      assert.equal(r3.rows.length, 4);
      s.close();
    });
  });
});
