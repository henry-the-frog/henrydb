// volcano-benchmark.test.js — Verify benchmark correctness
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { buildPlan } from './volcano-planner.js';
import { parse } from './sql.js';

describe('Volcano Benchmark Correctness', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE employees (id INT, name TEXT, age INT, dept TEXT, salary INT)');
    db.execute('CREATE TABLE departments (id INT, name TEXT, budget INT)');
    const depts = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'];
    for (const [i, d] of depts.entries()) {
      db.execute(`INSERT INTO departments VALUES (${i + 1}, '${d}', ${(i + 1) * 100000})`);
    }
    for (let i = 0; i < 100; i++) {
      const age = 20 + (i % 40);
      const dept = depts[i % depts.length];
      const salary = 40000 + (i % 60) * 1000;
      db.execute(`INSERT INTO employees VALUES (${i}, 'emp_${i}', ${age}, '${dept}', ${salary})`);
    }
  });

  function compareRowCounts(sql) {
    const stdRows = db.execute(sql).rows;
    const ast = parse(sql);
    const volRows = buildPlan(ast, db.tables).toArray();
    return { stdCount: stdRows.length, volCount: volRows.length };
  }

  const queries = [
    'SELECT * FROM employees',
    'SELECT * FROM employees WHERE age > 50',
    'SELECT name, salary FROM employees',
    'SELECT name, salary FROM employees ORDER BY salary DESC',
    'SELECT name FROM employees LIMIT 10',
    'SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 10',
    'SELECT COUNT(*) as cnt FROM employees',
    'SELECT dept, COUNT(*) as cnt FROM employees GROUP BY dept',
    'SELECT dept, SUM(salary) as total FROM employees GROUP BY dept',
    'SELECT DISTINCT dept FROM employees',
    'SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept = d.name',
    "SELECT e.name, d.budget FROM employees e JOIN departments d ON e.dept = d.name WHERE e.age > 50",
  ];

  for (const sql of queries) {
    it(`row count match: ${sql.substring(0, 60)}`, () => {
      const { stdCount, volCount } = compareRowCounts(sql);
      assert.equal(stdCount, volCount, `Row count mismatch for: ${sql}`);
    });
  }

  it('LIMIT returns correct number of rows', () => {
    const ast = parse('SELECT name FROM employees LIMIT 10');
    const rows = buildPlan(ast, db.tables).toArray();
    assert.equal(rows.length, 10);
  });

  it('volcano LIMIT is genuinely faster (does not read all rows)', () => {
    // Standard engine reads all 100 rows then takes 10
    // Volcano engine reads only 10 rows
    let heapReads = 0;
    const table = db.tables.get('employees');
    const origScan = table.heap.scan.bind(table.heap);
    table.heap.scan = function*() {
      for (const row of origScan()) {
        heapReads++;
        yield row;
      }
    };

    const ast = parse('SELECT name FROM employees LIMIT 10');
    buildPlan(ast, db.tables).toArray();

    assert.ok(heapReads <= 10, `Expected ≤10 heap reads, got ${heapReads}`);

    // Restore
    table.heap.scan = origScan;
  });
});
