#!/usr/bin/env node
// HenryDB Feature Showcase — demonstrates all major SQL capabilities
// Usage: node showcase.js

import { Database } from './src/db.js';

const db = new Database();
let section = 0;

function title(name) {
  section++;
  console.log(`\n${'━'.repeat(60)}`);
  console.log(`  ${section}. ${name}`);
  console.log(`${'━'.repeat(60)}`);
}

function show(sql) {
  console.log(`  > ${sql}`);
  try {
    const r = db.execute(sql);
    if (r.rows?.length > 0) {
      const cols = Object.keys(r.rows[0]);
      const widths = cols.map(c => Math.max(c.length, ...r.rows.map(r => String(r[c] ?? 'NULL').length)));
      console.log('  ' + cols.map((c, i) => c.padEnd(widths[i])).join(' | '));
      console.log('  ' + widths.map(w => '─'.repeat(w)).join('─┼─'));
      for (const row of r.rows) {
        console.log('  ' + cols.map((c, i) => String(row[c] ?? 'NULL').padEnd(widths[i])).join(' | '));
      }
    } else if (r.count !== undefined) {
      console.log(`  → ${r.count} row(s) affected`);
    } else {
      console.log('  → OK');
    }
  } catch (e) {
    console.log(`  ✗ Error: ${e.message}`);
  }
  console.log();
}

console.log('╔══════════════════════════════════════════════════════════╗');
console.log('║           HenryDB Feature Showcase                     ║');
console.log('║     A PostgreSQL-compatible database in JavaScript      ║');
console.log('╚══════════════════════════════════════════════════════════╝');

// --- Schema ---
title('Schema Definition');
show('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT, hire_date TEXT)');
show("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 120000, '2023-01-15')");
show("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 110000, '2023-03-20')");
show("INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 90000, '2023-06-01')");
show("INSERT INTO employees VALUES (4, 'Diana', 'Sales', 95000, '2022-11-10')");
show("INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 130000, '2022-08-05')");
show("INSERT INTO employees VALUES (6, 'Frank', 'Marketing', 85000, '2023-09-01')");
show("INSERT INTO employees VALUES (7, 'Grace', 'Marketing', 92000, '2023-02-14')");

// --- Basic Queries ---
title('Basic Queries');
show('SELECT * FROM employees ORDER BY salary DESC LIMIT 3');
show("SELECT name, salary FROM employees WHERE dept = 'Engineering' ORDER BY salary DESC");

// --- Aggregation ---
title('Aggregation');
show('SELECT dept, COUNT(*) as headcount, AVG(salary) as avg_salary, MAX(salary) as max_salary FROM employees GROUP BY dept ORDER BY avg_salary DESC');
show('SELECT dept, STRING_AGG(name, \', \') as team FROM employees GROUP BY dept ORDER BY dept');

// --- Window Functions ---
title('Window Functions');
show('SELECT name, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as dept_rank FROM employees ORDER BY dept, dept_rank');
show('SELECT name, salary, SUM(salary) OVER (ORDER BY hire_date) as running_total FROM employees ORDER BY hire_date');

// --- JOINs ---
title('JOINs');
show('CREATE TABLE projects (id INT PRIMARY KEY, name TEXT, lead_id INT, budget INT)');
show("INSERT INTO projects VALUES (1, 'Alpha', 1, 500000)");
show("INSERT INTO projects VALUES (2, 'Beta', 5, 300000)");
show("INSERT INTO projects VALUES (3, 'Gamma', 3, 200000)");
show('SELECT p.name as project, e.name as lead, p.budget FROM projects p JOIN employees e ON p.lead_id = e.id ORDER BY p.budget DESC');

// --- Subqueries ---
title('Subqueries');
show('SELECT name, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY salary DESC');
show('SELECT dept, (SELECT COUNT(*) FROM projects WHERE lead_id IN (SELECT id FROM employees e2 WHERE e2.dept = employees.dept)) as projects FROM employees GROUP BY dept');

// --- CTEs ---
title('Common Table Expressions');
show("WITH dept_stats AS (SELECT dept, AVG(salary) as avg_sal, COUNT(*) as size FROM employees GROUP BY dept) SELECT dept, avg_sal, size, CASE WHEN avg_sal > 100000 THEN 'Premium' ELSE 'Standard' END as tier FROM dept_stats ORDER BY avg_sal DESC");

// --- GENERATE_SERIES ---
title('GENERATE_SERIES');
show('SELECT value, value * value as square FROM GENERATE_SERIES(1, 10) ORDER BY value');
show('SELECT value % 3 as grp, SUM(value) as total FROM GENERATE_SERIES(1, 30) GROUP BY value % 3 ORDER BY grp');

// --- CASE + COALESCE ---
title('Conditional Expressions');
show("SELECT name, salary, CASE WHEN salary >= 120000 THEN 'Senior' WHEN salary >= 100000 THEN 'Mid' ELSE 'Junior' END as level FROM employees ORDER BY salary DESC");

// --- String Functions ---
title('String Functions');
show("SELECT UPPER(name) as upper_name, LENGTH(name) as name_len, REPLACE(dept, 'ing', 'ING') as dept_caps FROM employees LIMIT 3");

// --- Math ---
title('Mathematical Functions');
show('SELECT ABS(-42) as abs_val, CEIL(4.2) as ceil_val, FLOOR(4.8) as floor_val, ROUND(3.14159) as round_val, POWER(2, 10) as power_val');

// --- Operator Precedence ---
title('Operator Precedence (Fixed!)');
show('SELECT 2 + 3 * 4 as correct_14, (2 + 3) * 4 as paren_20, 10 - 2 * 3 as correct_4');

// --- UNION ---
title('Set Operations');
show("SELECT name, 'high' as category FROM employees WHERE salary > 100000 UNION SELECT name, 'low' as category FROM employees WHERE salary <= 100000 ORDER BY category, name");

// --- JSON ---
title('JSON Support');
show('CREATE TABLE configs (id INT, data TEXT)');
show("INSERT INTO configs VALUES (1, '{\"theme\":\"dark\",\"lang\":\"en\"}')");
show("INSERT INTO configs VALUES (2, '{\"theme\":\"light\",\"lang\":\"fr\"}')");
show("SELECT id, JSON_EXTRACT(data, '$.theme') as theme, JSON_EXTRACT(data, '$.lang') as lang FROM configs");

// --- Views ---
title('Views');
show("CREATE VIEW engineering_team AS SELECT name, salary FROM employees WHERE dept = 'Engineering'");
show('SELECT * FROM engineering_team ORDER BY salary DESC');

// --- EXPLAIN ---
title('Query Execution Plans');
show('EXPLAIN SELECT e.name, p.name FROM employees e JOIN projects p ON e.id = p.lead_id');

console.log(`\n${'═'.repeat(60)}`);
console.log(`  Showcase complete. ${section} features demonstrated.`);
console.log(`${'═'.repeat(60)}\n`);
