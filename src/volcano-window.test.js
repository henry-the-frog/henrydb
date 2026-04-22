// volcano-window.test.js — Window function support in volcano planner
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { buildPlan } from './volcano-planner.js';
import { parse } from './sql.js';

describe('Volcano Window Functions', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, dept TEXT, salary INT)');
    db.execute("INSERT INTO emp VALUES (1, 'Alice', 'Eng', 100)");
    db.execute("INSERT INTO emp VALUES (2, 'Bob', 'Eng', 80)");
    db.execute("INSERT INTO emp VALUES (3, 'Charlie', 'Sales', 90)");
    db.execute("INSERT INTO emp VALUES (4, 'Diana', 'Sales', 70)");
    db.execute("INSERT INTO emp VALUES (5, 'Eve', 'Eng', 95)");
  });

  function volcanoQuery(sql) {
    const ast = parse(sql);
    const plan = buildPlan(ast, db.tables);
    return plan.toArray();
  }

  it('ROW_NUMBER() OVER (ORDER BY)', () => {
    const rows = volcanoQuery('SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) as rn FROM emp');
    assert.equal(rows.length, 5);
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[0].rn, 1);
    assert.equal(rows[4].rn, 5);
  });

  it('RANK() OVER (PARTITION BY ... ORDER BY)', () => {
    const rows = volcanoQuery('SELECT name, dept, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rnk FROM emp');
    assert.equal(rows.length, 5);
    // Eng: Alice(100)=1, Eve(95)=2, Bob(80)=3
    const alice = rows.find(r => r.name === 'Alice');
    assert.equal(alice.rnk, 1);
    const bob = rows.find(r => r.name === 'Bob');
    assert.equal(bob.rnk, 3);
    // Sales: Charlie(90)=1, Diana(70)=2
    const charlie = rows.find(r => r.name === 'Charlie');
    assert.equal(charlie.rnk, 1);
  });

  it('DENSE_RANK() OVER (ORDER BY)', () => {
    const rows = volcanoQuery('SELECT name, DENSE_RANK() OVER (ORDER BY salary DESC) as dr FROM emp');
    assert.equal(rows[0].dr, 1);
  });

  it('SUM() OVER (ORDER BY) — running total', () => {
    const rows = volcanoQuery('SELECT name, salary, SUM(salary) OVER (ORDER BY salary) as running FROM emp');
    assert.equal(rows.length, 5);
    // Sorted by salary: 70, 80, 90, 95, 100
    assert.equal(rows[0].running, 70);
    assert.equal(rows[1].running, 150);
    assert.equal(rows[4].running, 435);
  });

  it('COUNT() OVER (PARTITION BY)', () => {
    const rows = volcanoQuery('SELECT name, dept, COUNT(*) OVER (PARTITION BY dept) as dept_count FROM emp');
    const alice = rows.find(r => r.name === 'Alice');
    assert.equal(alice.dept_count, 3); // 3 Eng employees
    const charlie = rows.find(r => r.name === 'Charlie');
    assert.equal(charlie.dept_count, 2); // 2 Sales employees
  });

  it('window function with regular columns', () => {
    const rows = volcanoQuery('SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) as rn FROM emp');
    assert.equal(rows.length, 5);
    assert.equal(rows[0].id, 1);
    assert.equal(rows[0].rn, 1);
  });

  it('window function with WHERE clause', () => {
    const rows = volcanoQuery("SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) as rn FROM emp WHERE dept = 'Eng'");
    assert.equal(rows.length, 3);
  });
});
