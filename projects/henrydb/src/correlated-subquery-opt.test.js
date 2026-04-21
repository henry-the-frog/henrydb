// correlated-subquery-opt.test.js — Tests for correlated subquery materialization
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Correlated Subquery Materialization', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE employees (id INT PRIMARY KEY, dept_id INT, name TEXT, salary INT)');
    db.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')");
    db.execute("INSERT INTO employees VALUES (1, 1, 'Alice', 100), (2, 1, 'Bob', 120), (3, 2, 'Carol', 80)");
  });

  it('COUNT(*) with correlation returns correct counts', () => {
    const r = db.execute('SELECT d.name, (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS cnt FROM departments d ORDER BY d.name');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows.find(x => x.name === 'Engineering').cnt, 2);
    assert.equal(r.rows.find(x => x.name === 'Sales').cnt, 1);
    assert.equal(r.rows.find(x => x.name === 'HR').cnt, 0); // COUNT defaults to 0
  });

  it('SUM with correlation', () => {
    const r = db.execute('SELECT d.name, (SELECT SUM(salary) FROM employees e WHERE e.dept_id = d.id) AS total FROM departments d ORDER BY d.name');
    assert.equal(r.rows.find(x => x.name === 'Engineering').total, 220);
    assert.equal(r.rows.find(x => x.name === 'Sales').total, 80);
    assert.equal(r.rows.find(x => x.name === 'HR').total, null); // SUM of no rows = null
  });

  it('MAX with correlation', () => {
    const r = db.execute('SELECT d.name, (SELECT MAX(salary) FROM employees e WHERE e.dept_id = d.id) AS max_sal FROM departments d ORDER BY d.name');
    assert.equal(r.rows.find(x => x.name === 'Engineering').max_sal, 120);
    assert.equal(r.rows.find(x => x.name === 'Sales').max_sal, 80);
    assert.equal(r.rows.find(x => x.name === 'HR').max_sal, null);
  });

  it('performance: materialized is much faster than naive', () => {
    // Create larger dataset
    for (let i = 4; i < 100; i++) {
      db.execute(`INSERT INTO departments VALUES (${i}, 'Dept${i}')`);
    }
    for (let i = 4; i < 5000; i++) {
      db.execute(`INSERT INTO employees VALUES (${i}, ${i % 100}, 'Emp${i}', ${50 + (i % 100)})`);
    }

    const start = performance.now();
    const r = db.execute('SELECT id, (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS cnt FROM departments d ORDER BY id');
    const elapsed = performance.now() - start;
    
    assert.equal(r.rows.length, 99); // departments 1-3 + 4-99 = 99 rows
    // With materialization, this should be fast (<50ms)
    // Without materialization, it would be ~500ms+
    assert.ok(elapsed < 200, `Should be fast: ${elapsed.toFixed(1)}ms`);
  });

  it('multiple correlated subqueries in same SELECT', () => {
    const r = db.execute(`
      SELECT d.name, 
        (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS cnt,
        (SELECT MAX(salary) FROM employees e WHERE e.dept_id = d.id) AS max_sal
      FROM departments d ORDER BY d.name
    `);
    assert.equal(r.rows.length, 3);
    const eng = r.rows.find(x => x.name === 'Engineering');
    assert.equal(eng.cnt, 2);
    assert.equal(eng.max_sal, 120);
  });

  it('works with WHERE on outer query', () => {
    const r = db.execute("SELECT d.name, (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS cnt FROM departments d WHERE d.name = 'Engineering'");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].cnt, 2);
  });
});
