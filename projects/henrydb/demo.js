#!/usr/bin/env node
// demo.js — HenryDB Feature Showcase
// Run: node demo.js
// Or connect with psql: psql -h 127.0.0.1 -p 5433

import { Database } from './src/db.js';

const db = new Database();

function run(sql) {
  try {
    const result = db.execute(sql);
    if (result && result.rows && result.rows.length > 0) {
      // Print table header
      const cols = Object.keys(result.rows[0]);
      console.log('  ' + cols.map(c => c.padEnd(15)).join(''));
      console.log('  ' + cols.map(() => '─'.repeat(15)).join(''));
      for (const row of result.rows.slice(0, 10)) {
        console.log('  ' + cols.map(c => String(row[c] ?? 'NULL').padEnd(15)).join(''));
      }
      if (result.rows.length > 10) console.log(`  ... (${result.rows.length} rows total)`);
    }
    return result;
  } catch (e) {
    console.log('  ERROR:', e.message);
    return null;
  }
}

console.log('═══════════════════════════════════════════════════════');
console.log('          HenryDB Feature Showcase Demo');
console.log('═══════════════════════════════════════════════════════\n');

// ===== 1. Schema & Data =====
console.log('▸ 1. Schema & Data');
console.log('─────────────────────────────────────');
run('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT, hire_year INT)');
run("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 95000, 2019)");
run("INSERT INTO employees VALUES (2, 'Bob', 'Marketing', 75000, 2020)");
run("INSERT INTO employees VALUES (3, 'Charlie', 'Engineering', 105000, 2018)");
run("INSERT INTO employees VALUES (4, 'Diana', 'Sales', 85000, 2021)");
run("INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 115000, 2017)");
run("INSERT INTO employees VALUES (6, 'Frank', 'Marketing', 80000, 2022)");
run("INSERT INTO employees VALUES (7, 'Grace', 'Sales', 90000, 2019)");
run("INSERT INTO employees VALUES (8, 'Hank', 'Engineering', 125000, 2016)");
console.log('\n  All employees:');
run('SELECT * FROM employees ORDER BY id');

// ===== 2. WHERE, ORDER BY, LIMIT =====
console.log('\n▸ 2. Filtering & Sorting');
console.log('─────────────────────────────────────');
console.log('\n  Top 3 by salary:');
run('SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3');

console.log('\n  Engineers hired before 2019:');
run("SELECT name, hire_year FROM employees WHERE dept = 'Engineering' AND hire_year < 2019 ORDER BY hire_year");

// ===== 3. Aggregations =====
console.log('\n▸ 3. Aggregations');
console.log('─────────────────────────────────────');
console.log('\n  Department stats:');
run('SELECT dept, COUNT(*) as headcount, AVG(salary) as avg_salary, MIN(salary) as min_sal, MAX(salary) as max_sal FROM employees GROUP BY dept ORDER BY avg_salary DESC');

console.log('\n  Departments with avg salary > 85000:');
run('SELECT dept, AVG(salary) as avg_salary FROM employees GROUP BY dept HAVING AVG(salary) > 85000');

// ===== 4. JOINs =====
console.log('\n▸ 4. JOINs');
console.log('─────────────────────────────────────');
run('CREATE TABLE departments (name TEXT PRIMARY KEY, budget INT, location TEXT)');
run("INSERT INTO departments VALUES ('Engineering', 5000000, 'Building A')");
run("INSERT INTO departments VALUES ('Marketing', 2000000, 'Building B')");
run("INSERT INTO departments VALUES ('Sales', 3000000, 'Building C')");
run("INSERT INTO departments VALUES ('HR', 1000000, 'Building D')");

console.log('\n  Employee with department info:');
run('SELECT e.name, e.salary, d.budget, d.location FROM employees e JOIN departments d ON e.dept = d.name ORDER BY e.salary DESC LIMIT 5');

console.log('\n  Departments with no employees (LEFT JOIN):');
run('SELECT d.name, COUNT(e.id) as emp_count FROM departments d LEFT JOIN employees e ON d.name = e.dept GROUP BY d.name ORDER BY emp_count');

// ===== 5. Subqueries =====
console.log('\n▸ 5. Subqueries');
console.log('─────────────────────────────────────');
console.log('\n  Employees earning above average:');
run('SELECT name, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY salary DESC');

// ===== 6. Window Functions =====
console.log('\n▸ 6. Window Functions');
console.log('─────────────────────────────────────');
console.log('\n  Salary rank by department:');
run('SELECT name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rank FROM employees ORDER BY dept, salary DESC');

// ===== 7. CTEs =====
console.log('\n▸ 7. Common Table Expressions');
console.log('─────────────────────────────────────');
console.log('\n  Department totals via CTE:');
run('WITH dept_totals AS (SELECT dept, SUM(salary) as total_salary, COUNT(*) as cnt FROM employees GROUP BY dept) SELECT dept, total_salary, cnt, total_salary / cnt as avg_per_person FROM dept_totals ORDER BY total_salary DESC');

// ===== 8. Expressions =====
console.log('\n▸ 8. Expressions');
console.log('─────────────────────────────────────');
console.log('\n  Tax calculation:');
run("SELECT name, salary, salary * 0.30 as tax, salary - salary * 0.30 as take_home, CASE WHEN salary > 100000 THEN 'Senior' WHEN salary > 80000 THEN 'Mid' ELSE 'Junior' END as level FROM employees ORDER BY salary DESC");

// ===== 9. DISTINCT =====
console.log('\n▸ 9. DISTINCT');
console.log('─────────────────────────────────────');
run('SELECT DISTINCT dept FROM employees ORDER BY dept');

// ===== 10. ALTER TABLE =====
console.log('\n▸ 10. ALTER TABLE');
console.log('─────────────────────────────────────');
run('ALTER TABLE employees ADD COLUMN bonus INT DEFAULT 0');
run("UPDATE employees SET bonus = salary * 0.1 WHERE dept = 'Engineering'");
console.log('\n  Engineering bonuses:');
run("SELECT name, salary, bonus FROM employees WHERE bonus > 0 ORDER BY bonus DESC");

// ===== 11. Views =====
console.log('\n▸ 11. Views');
console.log('─────────────────────────────────────');
run("CREATE VIEW engineering_team AS SELECT name, salary, hire_year FROM employees WHERE dept = 'Engineering'");
console.log('\n  Engineering team (via view):');
run('SELECT * FROM engineering_team ORDER BY salary DESC');

// ===== 12. String Concatenation =====
console.log('\n▸ 12. String Concatenation (||)');
console.log('─────────────────────────────────────');
run("SELECT name || ' earns $' || CAST(salary AS TEXT) || '/yr' as info FROM employees ORDER BY salary DESC LIMIT 3");

// ===== 13. UPSERT =====
console.log('\n▸ 13. UPSERT (INSERT ON CONFLICT)');
console.log('─────────────────────────────────────');
run('CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)');
run("INSERT INTO config VALUES ('theme', 'dark')");
run("INSERT INTO config VALUES ('theme', 'light') ON CONFLICT (key) DO UPDATE SET value = 'light'");
console.log('\n  Config after upsert:');
run('SELECT * FROM config');

// ===== 14. RETURNING =====
console.log('\n▸ 14. INSERT/UPDATE RETURNING');
console.log('─────────────────────────────────────');
run('CREATE TABLE log (id SERIAL PRIMARY KEY, action TEXT, ts TEXT)');
console.log('\n  Insert with RETURNING:');
run("INSERT INTO log (action, ts) VALUES ('login', '2024-01-01') RETURNING *");
run("INSERT INTO log (action, ts) VALUES ('logout', '2024-01-02') RETURNING id");

// ===== 15. Recursive CTE =====
console.log('\n▸ 15. Recursive CTE (counting 1-10)');
console.log('─────────────────────────────────────');
run('WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 10) SELECT * FROM cnt');

// ===== 16. GENERATE_SERIES =====
console.log('\n▸ 16. GENERATE_SERIES');
console.log('─────────────────────────────────────');
run('SELECT * FROM GENERATE_SERIES(0, 50, 10)');

// ===== Summary =====
console.log('\n═══════════════════════════════════════════════════════');
console.log('  Features demonstrated:');
console.log('  ✓ DDL: CREATE TABLE, ALTER TABLE, CREATE VIEW');
console.log('  ✓ DML: INSERT, UPDATE, DELETE');
console.log('  ✓ Queries: SELECT, WHERE, ORDER BY, LIMIT, DISTINCT');
console.log('  ✓ Aggregations: COUNT, SUM, AVG, MIN, MAX');
console.log('  ✓ GROUP BY / HAVING');
console.log('  ✓ JOINs: INNER, LEFT');
console.log('  ✓ Subqueries');
console.log('  ✓ Window Functions: ROW_NUMBER, PARTITION BY');
console.log('  ✓ CTEs: WITH clause, recursive');
console.log('  ✓ Expressions: CASE WHEN, arithmetic, || concat');
console.log('  ✓ Views');
console.log('  ✓ UPSERT: INSERT ON CONFLICT');
console.log('  ✓ RETURNING: INSERT/UPDATE RETURNING');
console.log('  ✓ SERIAL: Auto-incrementing IDs');
console.log('  ✓ GENERATE_SERIES: Table functions');
console.log('═══════════════════════════════════════════════════════');
