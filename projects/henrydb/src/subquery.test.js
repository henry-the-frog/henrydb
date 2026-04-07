// subquery.test.js — Subquery tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Subqueries', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
    db.execute("INSERT INTO departments VALUES (2, 'Marketing')");
    db.execute("INSERT INTO departments VALUES (3, 'Sales')");

    db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT, salary INT)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 1, 100000)");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 1, 90000)");
    db.execute("INSERT INTO employees VALUES (3, 'Charlie', 2, 80000)");
    db.execute("INSERT INTO employees VALUES (4, 'Diana', 2, 85000)");
    db.execute("INSERT INTO employees VALUES (5, 'Eve', 3, 95000)");
  });

  describe('IN (SELECT ...)', () => {
    it('basic IN subquery', () => {
      const result = db.execute("SELECT * FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Engineering')");
      assert.equal(result.rows.length, 2);
      assert.ok(result.rows.every(r => r.dept_id === 1));
    });

    it('IN subquery with multiple results', () => {
      const result = db.execute("SELECT * FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name != 'Sales')");
      assert.equal(result.rows.length, 4); // Engineering(2) + Marketing(2)
    });

    it('IN subquery with no results', () => {
      const result = db.execute("SELECT * FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'HR')");
      assert.equal(result.rows.length, 0);
    });

    it('NOT IN subquery', () => {
      const result = db.execute("SELECT * FROM employees WHERE dept_id NOT IN (SELECT id FROM departments WHERE name = 'Engineering')");
      assert.equal(result.rows.length, 3); // Marketing(2) + Sales(1)
    });
  });

  describe('IN (value list)', () => {
    it('IN with literal values', () => {
      const result = db.execute('SELECT * FROM employees WHERE dept_id IN (1, 3)');
      assert.equal(result.rows.length, 3);
    });

    it('NOT IN with literal values', () => {
      const result = db.execute('SELECT * FROM employees WHERE dept_id NOT IN (1)');
      assert.equal(result.rows.length, 3);
    });

    it('IN with strings', () => {
      const result = db.execute("SELECT * FROM employees WHERE name IN ('Alice', 'Eve')");
      assert.equal(result.rows.length, 2);
    });
  });

  describe('EXISTS', () => {
    it('basic EXISTS', () => {
      const result = db.execute("SELECT * FROM departments WHERE EXISTS (SELECT * FROM employees WHERE salary > 99000)");
      // EXISTS is uncorrelated, so it returns all departments if any high-earner exists
      assert.equal(result.rows.length, 3);
    });

    it('NOT EXISTS when subquery has results', () => {
      const result = db.execute("SELECT * FROM departments WHERE NOT EXISTS (SELECT * FROM employees WHERE salary > 99000)");
      assert.equal(result.rows.length, 0);
    });

    it('NOT EXISTS when subquery is empty', () => {
      const result = db.execute("SELECT * FROM departments WHERE NOT EXISTS (SELECT * FROM employees WHERE salary > 999999)");
      assert.equal(result.rows.length, 3);
    });
  });

  describe('Scalar subqueries', () => {
    it('comparison with scalar subquery', () => {
      // Find employees earning more than the average
      const result = db.execute('SELECT * FROM employees WHERE salary > (SELECT AVG(salary) AS avg FROM employees)');
      // Avg salary = (100000+90000+80000+85000+95000)/5 = 90000
      assert.ok(result.rows.length > 0);
      assert.ok(result.rows.every(r => r.salary > 90000));
    });

    it('equality with scalar subquery', () => {
      const result = db.execute('SELECT * FROM employees WHERE salary = (SELECT MAX(salary) AS m FROM employees)');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Alice');
    });

    it('scalar subquery returning NULL', () => {
      const result = db.execute("SELECT * FROM employees WHERE salary = (SELECT salary FROM employees WHERE name = 'Nobody')");
      assert.equal(result.rows.length, 0);
    });
  });

  describe('Combined', () => {
    it('subquery with AND', () => {
      const result = db.execute("SELECT * FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Engineering') AND salary > 95000");
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Alice');
    });

    it('subquery with ORDER BY', () => {
      const result = db.execute("SELECT * FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Engineering') ORDER BY salary DESC");
      assert.equal(result.rows.length, 2);
      assert.equal(result.rows[0].name, 'Alice');
      assert.equal(result.rows[1].name, 'Bob');
    });
  });
});
