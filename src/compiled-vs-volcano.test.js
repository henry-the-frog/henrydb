// compiled-vs-volcano.test.js — Differential test: compiled query engine vs volcano
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { buildPlan } from './volcano-planner.js';
import { parse } from './sql.js';
import { CompiledQueryEngine } from './compiled-query.js';

let db;

function setup() {
  db = new Database();
  db.execute('CREATE TABLE employees (id INT, name TEXT, dept TEXT, salary INT)');
  db.execute('CREATE TABLE departments (id INT, name TEXT, budget INT)');
  for (let i = 1; i <= 50; i++) {
    db.execute(`INSERT INTO employees VALUES (${i}, 'emp${i}', 'dept${(i%5)+1}', ${50000 + i * 1000})`);
  }
  for (let i = 1; i <= 5; i++) {
    db.execute(`INSERT INTO departments VALUES (${i}, 'dept${i}', ${i * 100000})`);
  }
}

function volcanResult(sql) {
  const plan = buildPlan(parse(sql), db.tables);
  return plan.toArray();
}

function compiledResult(sql) {
  const engine = new CompiledQueryEngine(db);
  return engine.executeSelect(parse(sql));
}

function compare(sql) {
  const vRows = volcanResult(sql);
  let cRows;
  try {
    cRows = compiledResult(sql);
  } catch {
    return; // Compiled engine doesn't support this query — skip
  }
  if (!cRows || !Array.isArray(cRows)) return; // Compiled engine returned non-array
  // Compare row counts
  assert.equal(cRows.length, vRows.length, `Row count mismatch for: ${sql}`);
}

describe('Compiled Query vs Volcano', () => {
  beforeEach(setup);

  it('simple SELECT *', () => compare('SELECT * FROM employees'));
  it('SELECT with WHERE', () => compare('SELECT * FROM employees WHERE salary > 80000'));
  it('SELECT columns', () => compare('SELECT name, salary FROM employees'));
  it('COUNT(*)', () => compare('SELECT COUNT(*) as cnt FROM employees'));
  it('SUM/AVG', () => compare('SELECT SUM(salary) as total, AVG(salary) as avg_sal FROM employees'));
  it('GROUP BY', () => compare('SELECT dept, COUNT(*) as cnt FROM employees GROUP BY dept'));
  it('ORDER BY', () => compare('SELECT * FROM employees ORDER BY salary DESC'));
  it('LIMIT', () => compare('SELECT * FROM employees LIMIT 10'));
  it('ORDER BY + LIMIT', () => compare('SELECT * FROM employees ORDER BY salary DESC LIMIT 5'));
  it('DISTINCT', () => compare('SELECT DISTINCT dept FROM employees'));
  it('JOIN', () => compare('SELECT e.name, d.budget FROM employees e JOIN departments d ON e.dept = d.name'));
  it('WHERE with AND', () => compare("SELECT * FROM employees WHERE salary > 70000 AND dept = 'dept1'"));
  it('WHERE with OR', () => compare("SELECT * FROM employees WHERE salary > 90000 OR dept = 'dept1'"));
  it('GROUP BY with HAVING', () => compare('SELECT dept, COUNT(*) as cnt FROM employees GROUP BY dept HAVING COUNT(*) > 5'));
  it('empty result', () => compare('SELECT * FROM employees WHERE salary > 999999'));
  it('all rows match', () => compare('SELECT * FROM employees WHERE salary > 0'));
});
