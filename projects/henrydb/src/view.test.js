// view.test.js — View tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Views', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100000)");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 90000)");
    db.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Marketing', 80000)");
    db.execute("INSERT INTO employees VALUES (4, 'Diana', 'Marketing', 85000)");
    db.execute("INSERT INTO employees VALUES (5, 'Eve', 'Sales', 70000)");
  });

  describe('CREATE VIEW', () => {
    it('creates a simple view', () => {
      const result = db.execute('CREATE VIEW eng AS SELECT * FROM employees WHERE dept = \'Engineering\'');
      assert.equal(result.type, 'OK');
    });

    it('errors on duplicate view name', () => {
      db.execute('CREATE VIEW eng AS SELECT * FROM employees WHERE dept = \'Engineering\'');
      assert.throws(() => {
        db.execute('CREATE VIEW eng AS SELECT * FROM employees WHERE dept = \'Engineering\'');
      }, /already exists/);
    });
  });

  describe('Query from view', () => {
    it('SELECT * from view', () => {
      db.execute('CREATE VIEW eng AS SELECT * FROM employees WHERE dept = \'Engineering\'');
      const result = db.execute('SELECT * FROM eng');
      assert.equal(result.rows.length, 2);
      assert.ok(result.rows.every(r => r.dept === 'Engineering'));
    });

    it('SELECT specific columns from view', () => {
      db.execute('CREATE VIEW eng AS SELECT name, salary FROM employees WHERE dept = \'Engineering\'');
      const result = db.execute('SELECT name FROM eng');
      assert.equal(result.rows.length, 2);
      assert.ok(result.rows[0].name !== undefined);
    });

    it('WHERE on view', () => {
      db.execute('CREATE VIEW high_earners AS SELECT * FROM employees WHERE salary > 80000');
      const result = db.execute('SELECT * FROM high_earners WHERE dept = \'Engineering\'');
      assert.equal(result.rows.length, 2); // Alice(100k) + Bob(90k)
    });

    it('ORDER BY on view', () => {
      db.execute('CREATE VIEW all_emp AS SELECT * FROM employees');
      const result = db.execute('SELECT * FROM all_emp ORDER BY salary DESC');
      assert.equal(result.rows[0].name, 'Alice');
    });

    it('LIMIT on view', () => {
      db.execute('CREATE VIEW all_emp AS SELECT * FROM employees');
      const result = db.execute('SELECT * FROM all_emp LIMIT 2');
      assert.equal(result.rows.length, 2);
    });

    it('view reflects underlying data changes', () => {
      db.execute('CREATE VIEW eng AS SELECT * FROM employees WHERE dept = \'Engineering\'');
      db.execute("INSERT INTO employees VALUES (6, 'Frank', 'Engineering', 95000)");
      const result = db.execute('SELECT * FROM eng');
      assert.equal(result.rows.length, 3);
    });

    it('view with aggregates', () => {
      db.execute('CREATE VIEW dept_stats AS SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM employees GROUP BY dept');
      const result = db.execute('SELECT * FROM dept_stats');
      assert.equal(result.rows.length, 3);
      const eng = result.rows.find(r => r.dept === 'Engineering');
      assert.equal(eng.cnt, 2);
    });
  });

  describe('DROP VIEW', () => {
    it('drops existing view', () => {
      db.execute('CREATE VIEW eng AS SELECT * FROM employees WHERE dept = \'Engineering\'');
      const result = db.execute('DROP VIEW eng');
      assert.equal(result.type, 'OK');
    });

    it('errors on non-existent view', () => {
      assert.throws(() => {
        db.execute('DROP VIEW ghost');
      }, /not found/);
    });

    it('dropped view cannot be queried', () => {
      db.execute('CREATE VIEW eng AS SELECT * FROM employees WHERE dept = \'Engineering\'');
      db.execute('DROP VIEW eng');
      assert.throws(() => {
        db.execute('SELECT * FROM eng');
      }, /not found/);
    });
  });
});
