// integration-mega.test.js — Mega integration test combining ALL features
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Mega integration test', () => {
  
  it('full pipeline: CREATE → INDEX → ANALYZE → CTE → WINDOW → JOIN → GROUP → HAVING → ORDER → LIMIT', () => {
    const db = new Database();
    
    // Schema
    db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT, salary INT, hire_date TEXT)');
    db.execute('CREATE TABLE projects (id INT PRIMARY KEY, name TEXT, dept_id INT, budget INT)');
    
    // Data
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO departments VALUES (${i}, 'Dept ${i}')`);
    for (let i = 1; i <= 100; i++) {
      const dept = (i % 5) + 1;
      const salary = 30000 + (i * 500);
      const month = String((i % 12) + 1).padStart(2, '0');
      db.execute(`INSERT INTO employees VALUES (${i}, 'Employee ${i}', ${dept}, ${salary}, '2020-${month}-01')`);
    }
    for (let i = 1; i <= 20; i++) {
      db.execute(`INSERT INTO projects VALUES (${i}, 'Project ${i}', ${(i % 5) + 1}, ${i * 10000})`);
    }
    
    // Indexes
    db.execute('CREATE INDEX idx_emp_dept ON employees (dept_id)');
    db.execute('CREATE INDEX idx_proj_dept ON projects (dept_id)');
    db.execute('CREATE INDEX idx_emp_salary ON employees (salary)');
    
    // Analyze
    db.execute('ANALYZE TABLE departments');
    db.execute('ANALYZE TABLE employees');
    db.execute('ANALYZE TABLE projects');
    
    // Complex query using ALL features
    const r = db.execute(`
      WITH dept_stats AS (
        SELECT d.name as dept_name, d.id as dept_id,
          COUNT(e.id) as emp_count,
          AVG(e.salary) as avg_salary,
          SUM(CASE WHEN e.salary > 50000 THEN 1 ELSE 0 END) as high_earners
        FROM departments d
        JOIN employees e ON d.id = e.dept_id
        GROUP BY d.name, d.id
        HAVING COUNT(e.id) >= 10
      ),
      dept_projects AS (
        SELECT dept_id, COUNT(*) as proj_count, SUM(budget) as total_budget
        FROM projects GROUP BY dept_id
      )
      SELECT ds.dept_name,
        ds.emp_count,
        ds.avg_salary,
        ds.high_earners,
        dp.proj_count,
        dp.total_budget,
        ROW_NUMBER() OVER (ORDER BY ds.avg_salary DESC) as salary_rank
      FROM dept_stats ds
      JOIN dept_projects dp ON ds.dept_id = dp.dept_id
      ORDER BY salary_rank
      LIMIT 3
    `);
    
    assert.ok(r.rows.length <= 3);
    assert.ok(r.rows.length > 0);
    
    // Verify structure
    for (const row of r.rows) {
      assert.ok(row.dept_name);
      assert.ok(row.emp_count >= 10);
      assert.ok(row.avg_salary > 0);
      assert.ok(row.proj_count > 0);
      assert.ok(row.salary_rank > 0);
    }
    
    // Ranks should be 1, 2, 3
    assert.strictEqual(r.rows[0].salary_rank, 1);
    
    // First should have highest avg_salary
    if (r.rows.length >= 2) {
      assert.ok(r.rows[0].avg_salary >= r.rows[1].avg_salary);
    }
  });

  it('transaction safety with complex operations', () => {
    const db = new Database();
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 10000)');
    db.execute('INSERT INTO accounts VALUES (2, 5000)');
    
    // Successful transfer
    db.execute('BEGIN');
    db.execute('UPDATE accounts SET balance = balance - 3000 WHERE id = 1');
    db.execute('UPDATE accounts SET balance = balance + 3000 WHERE id = 2');
    const mid = db.execute('SELECT SUM(balance) as total FROM accounts');
    assert.strictEqual(mid.rows[0].total, 15000); // Conserved
    db.execute('COMMIT');
    
    // Failed transfer (rollback)
    db.execute('BEGIN');
    db.execute('UPDATE accounts SET balance = balance - 99999 WHERE id = 1');
    // Realize the transfer is bad
    db.execute('ROLLBACK');
    
    const final = db.execute('SELECT * FROM accounts ORDER BY id');
    assert.strictEqual(final.rows[0].balance, 7000); // 10000 - 3000
    assert.strictEqual(final.rows[1].balance, 8000); // 5000 + 3000
    assert.strictEqual(final.rows[0].balance + final.rows[1].balance, 15000); // Conserved
  });
});
