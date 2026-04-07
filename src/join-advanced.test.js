// join-advanced.test.js — Advanced JOIN tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Advanced JOINs', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
    db.execute("INSERT INTO departments VALUES (2, 'Marketing')");
    db.execute("INSERT INTO departments VALUES (3, 'HR')");

    db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT, salary INT)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 1, 100000)");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 1, 90000)");
    db.execute("INSERT INTO employees VALUES (3, 'Charlie', 2, 80000)");
    db.execute("INSERT INTO employees VALUES (4, 'Diana', NULL, 85000)"); // no dept

    db.execute('CREATE TABLE projects (id INT PRIMARY KEY, name TEXT, lead_id INT, dept_id INT)');
    db.execute("INSERT INTO projects VALUES (1, 'Alpha', 1, 1)");
    db.execute("INSERT INTO projects VALUES (2, 'Beta', 3, 2)");
    db.execute("INSERT INTO projects VALUES (3, 'Gamma', 1, 1)");
  });

  describe('LEFT JOIN', () => {
    it('includes unmatched rows from left', () => {
      const r = db.execute('SELECT employees.name, departments.name AS dept FROM employees LEFT JOIN departments ON employees.dept_id = departments.id ORDER BY employees.id');
      assert.equal(r.rows.length, 4);
      const diana = r.rows[3]; // Diana is last (id=4)
      assert.equal(diana.dept, null);
    });

    it('all departments including empty ones', () => {
      const r = db.execute('SELECT departments.name AS dept, COUNT(employees.id) AS emp_count FROM departments LEFT JOIN employees ON departments.id = employees.dept_id GROUP BY departments.name ORDER BY departments.name');
      assert.ok(r.rows.length >= 2);
    });
  });

  describe('INNER JOIN', () => {
    it('excludes unmatched rows', () => {
      const r = db.execute('SELECT employees.name, departments.name AS dept FROM employees JOIN departments ON employees.dept_id = departments.id');
      assert.equal(r.rows.length, 3); // Diana excluded (no dept)
    });

    it('JOIN with WHERE', () => {
      const r = db.execute("SELECT employees.name FROM employees JOIN departments ON employees.dept_id = departments.id WHERE departments.name = 'Engineering'");
      assert.equal(r.rows.length, 2);
    });

    it('JOIN with ORDER BY', () => {
      const r = db.execute('SELECT employees.name AS ename, employees.salary FROM employees JOIN departments ON employees.dept_id = departments.id ORDER BY employees.salary DESC');
      assert.equal(r.rows[0].ename, 'Alice');
    });
  });

  describe('Multi-table queries', () => {
    it('three-table JOIN', () => {
      const r = db.execute('SELECT projects.name AS project, employees.name AS lead, departments.name AS dept FROM projects JOIN employees ON projects.lead_id = employees.id JOIN departments ON projects.dept_id = departments.id');
      assert.equal(r.rows.length, 3);
    });

    it('three-table JOIN with WHERE', () => {
      const r = db.execute("SELECT projects.name FROM projects JOIN employees ON projects.lead_id = employees.id JOIN departments ON projects.dept_id = departments.id WHERE departments.name = 'Engineering'");
      assert.equal(r.rows.length, 2); // Alpha and Gamma
    });
  });

  describe('Self JOIN', () => {
    it('self join works with aliases', () => {
      db.execute('CREATE TABLE tree (id INT PRIMARY KEY, name TEXT, parent_id INT)');
      db.execute("INSERT INTO tree VALUES (1, 'Root', NULL)");
      db.execute("INSERT INTO tree VALUES (2, 'Child1', 1)");
      db.execute("INSERT INTO tree VALUES (3, 'Child2', 1)");
      db.execute("INSERT INTO tree VALUES (4, 'Grandchild', 2)");
      // Self-join to find parent names
      const r = db.execute('SELECT t1.name AS child, t2.name AS parent FROM tree t1 JOIN tree t2 ON t1.parent_id = t2.id ORDER BY t1.id');
      assert.equal(r.rows.length, 3); // 3 rows with parents (Root has no parent)
      assert.equal(r.rows[0].child, 'Child1');
      assert.equal(r.rows[0].parent, 'Root');
    });
  });

  describe('JOIN edge cases', () => {
    it('JOIN on empty table', () => {
      db.execute('CREATE TABLE empty (id INT PRIMARY KEY, val TEXT)');
      const r = db.execute('SELECT * FROM employees JOIN empty ON employees.id = empty.id');
      assert.equal(r.rows.length, 0);
    });

    it('LEFT JOIN on empty table', () => {
      db.execute('CREATE TABLE empty (id INT PRIMARY KEY, val TEXT)');
      const r = db.execute('SELECT employees.name FROM employees LEFT JOIN empty ON employees.id = empty.id');
      assert.equal(r.rows.length, 4); // all employees, empty cols are null
    });

    it('JOIN with aggregate', () => {
      const r = db.execute('SELECT departments.name, COUNT(*) AS cnt FROM employees JOIN departments ON employees.dept_id = departments.id GROUP BY departments.name ORDER BY cnt DESC');
      assert.ok(r.rows.length > 0);
      assert.equal(r.rows[0].cnt, 2); // Engineering has most
    });
  });
});
