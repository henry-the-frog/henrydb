#!/usr/bin/env node
// example.js — HenryDB feature demonstration
// Run: node examples/example.js

import { Database } from '../src/db.js';

const db = new Database();
const log = (title, result) => {
  console.log(`\n${title}`);
  console.table(result.rows);
};

console.log('🗄️  HenryDB Feature Demo\n');

// Schema
db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT NOT NULL, budget REAL)');
db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT NOT NULL, dept_id INT, salary REAL, hire_date TEXT)');

// Data
const depts = [
  [1, 'Engineering', 500000],
  [2, 'Sales', 300000],
  [3, 'Marketing', 200000],
];
for (const [id, name, budget] of depts) {
  db.execute(`INSERT INTO departments VALUES (${id}, '${name}', ${budget})`);
}

const emps = [
  [1, 'Alice', 1, 95000, '2021-01-15'],
  [2, 'Bob', 2, 65000, '2022-03-20'],
  [3, 'Carol', 1, 110000, '2020-06-01'],
  [4, 'Dave', 3, 72000, '2023-01-10'],
  [5, 'Eve', 1, 88000, '2021-09-01'],
  [6, 'Frank', 2, 58000, '2023-06-15'],
  [7, 'Grace', 1, 125000, '2019-03-01'],
  [8, 'Henry', 3, 68000, '2022-11-01'],
];
for (const [id, name, dept, salary, date] of emps) {
  db.execute(`INSERT INTO employees VALUES (${id}, '${name}', ${dept}, ${salary}, '${date}')`);
}

// 1. JOIN + GROUP BY + HAVING
log('1. Department summary (avg salary > $60k)', db.execute(`
  SELECT d.name AS department, COUNT(*) AS headcount, 
         ROUND(AVG(e.salary)) AS avg_salary, MAX(e.salary) AS top_salary
  FROM employees e
  JOIN departments d ON e.dept_id = d.id
  GROUP BY d.name
  HAVING AVG(e.salary) > 60000
  ORDER BY avg_salary DESC
`));

// 2. Window functions
log('2. Salary rank within department', db.execute(`
  SELECT name, salary,
    ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dept_rank,
    LAG(salary) OVER (PARTITION BY dept_id ORDER BY salary DESC) AS prev_salary
  FROM employees
`));

// 3. LATERAL JOIN (top earner per dept)
log('3. Top earner per department (LATERAL JOIN)', db.execute(`
  SELECT d.name AS department, top.name AS top_earner, top.salary
  FROM departments d
  CROSS JOIN LATERAL (
    SELECT name, salary FROM employees WHERE dept_id = d.id ORDER BY salary DESC LIMIT 1
  ) top
`));

// 4. CTE + Recursive
log('4. Fibonacci via recursive CTE', db.execute(`
  WITH RECURSIVE fib AS (
    SELECT 1 AS n, 1 AS value, 0 AS prev
    UNION ALL
    SELECT n + 1, value + prev, value FROM fib WHERE n < 10
  )
  SELECT n, value FROM fib
`));

// 5. information_schema
log('5. Database schema (information_schema)', db.execute(`
  SELECT table_name, column_name, data_type, is_nullable
  FROM information_schema.columns
  WHERE table_name = 'employees'
  ORDER BY ordinal_position
`));

// 6. CASE + Subquery
log('6. Salary classification', db.execute(`
  SELECT name, salary,
    CASE
      WHEN salary > (SELECT AVG(salary) FROM employees) * 1.2 THEN 'Above average'
      WHEN salary < (SELECT AVG(salary) FROM employees) * 0.8 THEN 'Below average'
      ELSE 'Average'
    END AS classification
  FROM employees
  ORDER BY salary DESC
`));

// 7. UNION
log('7. High and low earners', db.execute(`
  SELECT name, salary, 'High' AS tier FROM employees WHERE salary > 100000
  UNION ALL
  SELECT name, salary, 'Low' AS tier FROM employees WHERE salary < 65000
  ORDER BY salary DESC
`));

console.log('\n✅ All features demonstrated!\n');
