// decorrelate.test.js — Subquery decorrelation tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Subquery Decorrelation', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT, salary INT)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 1, 100000)");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 1, 90000)");
    db.execute("INSERT INTO employees VALUES (3, 'Carol', 2, 80000)");
    db.execute("INSERT INTO employees VALUES (4, 'Dave', 2, 70000)");
    db.execute("INSERT INTO employees VALUES (5, 'Eve', 3, 60000)");

    db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT, budget INT)');
    db.execute("INSERT INTO departments VALUES (1, 'Engineering', 500000)");
    db.execute("INSERT INTO departments VALUES (2, 'Sales', 200000)");
    db.execute("INSERT INTO departments VALUES (3, 'Marketing', 100000)");
  });

  describe('Uncorrelated IN Subquery', () => {
    it('basic IN subquery returns correct results', () => {
      const result = db.execute(`
        SELECT name FROM employees
        WHERE dept_id IN (SELECT id FROM departments WHERE budget > 150000)
      `);
      assert.equal(result.rows.length, 4); // dept 1 (Alice, Bob) and dept 2 (Carol, Dave)
      const names = result.rows.map(r => r.name).sort();
      assert.deepEqual(names, ['Alice', 'Bob', 'Carol', 'Dave']);
    });

    it('IN subquery with single match', () => {
      const result = db.execute(`
        SELECT name FROM employees
        WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Marketing')
      `);
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Eve');
    });

    it('IN subquery with no matches', () => {
      const result = db.execute(`
        SELECT name FROM employees
        WHERE dept_id IN (SELECT id FROM departments WHERE budget > 1000000)
      `);
      assert.equal(result.rows.length, 0);
    });
  });

  describe('Correlated EXISTS Subquery', () => {
    it('EXISTS with correlated condition', () => {
      const result = db.execute(`
        SELECT name FROM employees e
        WHERE EXISTS (SELECT 1 FROM departments d WHERE d.id = e.dept_id AND d.budget > 150000)
      `);
      assert.equal(result.rows.length, 4); // depts with budget > 150k (dept 1 + dept 2)
      const names = result.rows.map(r => r.name).sort();
      assert.deepEqual(names, ['Alice', 'Bob', 'Carol', 'Dave']);
    });
  });

  describe('Performance — Uncorrelated Should Not Re-execute', () => {
    it('large IN subquery is efficient (hash lookup)', () => {
      // Create a large lookup table
      db.execute('CREATE TABLE codes (id INT PRIMARY KEY, code INT)');
      for (let i = 0; i < 1000; i++) {
        db.execute(`INSERT INTO codes VALUES (${i}, ${i * 3})`);
      }

      db.execute('CREATE TABLE items (id INT PRIMARY KEY, code INT)');
      for (let i = 0; i < 100; i++) {
        db.execute(`INSERT INTO items VALUES (${i}, ${i * 3})`);
      }

      const start = performance.now();
      const result = db.execute(`
        SELECT id FROM items
        WHERE code IN (SELECT code FROM codes WHERE code < 300)
      `);
      const elapsed = performance.now() - start;

      // Should return items with code < 300 (id 0-99)
      assert.equal(result.rows.length, 100);
      // Should be fast — under 500ms (without decorrelation it would be ~50K subquery executions)
      assert.ok(elapsed < 500, `Query took ${elapsed.toFixed(0)}ms — may not be decorrelated`);
    });
  });
});
