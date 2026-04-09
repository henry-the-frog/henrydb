-- demo.sql — HenryDB Feature Showcase
-- Run: node src/server.js & node src/cli.js < demo.sql

-- ==========================================
-- 1. Schema & Data
-- ==========================================

CREATE TABLE departments (
  id INT PRIMARY KEY,
  name TEXT,
  budget INT
);

CREATE TABLE employees (
  id INT PRIMARY KEY,
  name TEXT,
  dept_id INT,
  salary INT,
  hire_date TEXT
);

INSERT INTO departments VALUES (1, 'Engineering', 500000);
INSERT INTO departments VALUES (2, 'Marketing', 200000);
INSERT INTO departments VALUES (3, 'Sales', 300000);

INSERT INTO employees VALUES (1, 'Alice', 1, 120000, '2020-01-15');
INSERT INTO employees VALUES (2, 'Bob', 2, 80000, '2021-03-22');
INSERT INTO employees VALUES (3, 'Carol', 1, 130000, '2019-07-10');
INSERT INTO employees VALUES (4, 'Dave', 3, 95000, '2022-11-01');
INSERT INTO employees VALUES (5, 'Eve', 1, 110000, '2023-02-28');
INSERT INTO employees VALUES (6, 'Frank', 2, 85000, '2021-08-15');
INSERT INTO employees VALUES (7, 'Grace', 3, 105000, '2020-06-20');
INSERT INTO employees VALUES (8, 'Henry', 1, 140000, '2018-04-01');
INSERT INTO employees VALUES (9, 'Ivy', 3, 92000, '2023-09-10');
INSERT INTO employees VALUES (10, 'Jack', 2, 78000, '2024-01-05');

-- ==========================================
-- 2. Basic Queries
-- ==========================================

-- Simple SELECT with WHERE
SELECT name, salary FROM employees WHERE salary > 100000;

-- ORDER BY and LIMIT
SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 5;

-- ==========================================
-- 3. Aggregation
-- ==========================================

-- Group by department
SELECT d.name as department, COUNT(*) as headcount, AVG(e.salary) as avg_salary, MAX(e.salary) as top_salary
FROM employees e
JOIN departments d ON e.dept_id = d.id
GROUP BY d.name
ORDER BY avg_salary DESC;

-- HAVING clause
SELECT d.name, COUNT(*) as cnt FROM employees e JOIN departments d ON e.dept_id = d.id GROUP BY d.name HAVING COUNT(*) > 2;

-- ==========================================
-- 4. Joins
-- ==========================================

-- INNER JOIN
SELECT e.name, d.name as department, d.budget
FROM employees e
JOIN departments d ON e.dept_id = d.id
WHERE d.budget > 250000
ORDER BY e.salary DESC;

-- ==========================================
-- 5. Indexes
-- ==========================================

CREATE INDEX idx_salary ON employees (salary);
CREATE INDEX idx_dept ON employees (dept_id);

-- Verify index usage
EXPLAIN SELECT * FROM employees WHERE salary > 100000;

-- ==========================================
-- 6. Window Functions
-- ==========================================

-- Rank employees by salary within department
SELECT name, salary,
  ROW_NUMBER() OVER (ORDER BY salary DESC) as overall_rank,
  RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) as dept_rank
FROM employees;

-- ==========================================
-- 7. Subqueries
-- ==========================================

-- Employees earning above average
SELECT name, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees);

-- ==========================================
-- 8. Common Table Expressions (CTE)
-- ==========================================

WITH dept_stats AS (
  SELECT dept_id, AVG(salary) as avg_sal, COUNT(*) as cnt
  FROM employees
  GROUP BY dept_id
)
SELECT d.name, ds.avg_sal, ds.cnt
FROM dept_stats ds
JOIN departments d ON d.id = ds.dept_id
ORDER BY ds.avg_sal DESC;

-- ==========================================
-- 9. Updates & Deletes
-- ==========================================

-- Give Engineering a raise
UPDATE employees SET salary = salary + 5000 WHERE dept_id = 1;

-- Verify
SELECT name, salary FROM employees WHERE dept_id = 1 ORDER BY salary DESC;

-- ==========================================
-- 10. Maintenance Commands
-- ==========================================

ANALYZE employees;
VACUUM employees;

-- ==========================================
-- 11. System Catalog
-- ==========================================

SHOW TABLES;
DESCRIBE employees;

-- ==========================================
-- 12. EXPLAIN ANALYZE
-- ==========================================

EXPLAIN ANALYZE SELECT e.name, d.name as dept, e.salary
FROM employees e
JOIN departments d ON e.dept_id = d.id
WHERE e.salary > 90000
ORDER BY e.salary DESC;

-- ==========================================
-- Done! 🐸
-- ==========================================
SELECT 'HenryDB demo complete!' as message;
