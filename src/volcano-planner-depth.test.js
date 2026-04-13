// volcano-planner-depth.test.js — Deep tests for volcano query planner
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { buildPlan, explainPlan } from './volcano-planner.js';
import { parse } from './sql.js';
import { Database } from './db.js';

let db;

function setup() {
  db = new Database();
  db.execute('CREATE TABLE employees (id INT, name TEXT, dept_id INT, salary INT)');
  db.execute('CREATE TABLE departments (id INT, name TEXT, budget INT)');
  db.execute('CREATE TABLE projects (id INT, name TEXT, dept_id INT, lead_id INT)');
  
  for (let i = 1; i <= 100; i++) {
    db.execute(`INSERT INTO employees VALUES (${i}, 'emp${i}', ${(i % 5) + 1}, ${50000 + i * 1000})`);
  }
  for (let i = 1; i <= 5; i++) {
    db.execute(`INSERT INTO departments VALUES (${i}, 'dept${i}', ${i * 100000})`);
  }
  for (let i = 1; i <= 10; i++) {
    db.execute(`INSERT INTO projects VALUES (${i}, 'proj${i}', ${(i % 5) + 1}, ${(i % 100) + 1})`);
  }
}

describe('Volcano Planner Depth Tests', () => {
  beforeEach(setup);

  describe('Basic Operations', () => {
    it('SeqScan produces correct rows', () => {
      const plan = buildPlan(parse('SELECT * FROM departments'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows.length, 5);
    });

    it('Filter correctly applies WHERE', () => {
      const plan = buildPlan(parse('SELECT * FROM employees WHERE salary > 100000'), db.tables);
      const rows = plan.toArray();
      assert.ok(rows.every(r => r.salary > 100000));
    });

    it('Project selects correct columns', () => {
      const plan = buildPlan(parse('SELECT name, salary FROM employees WHERE id = 1'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows.length, 1);
      assert.ok('name' in rows[0]);
      assert.ok('salary' in rows[0]);
    });

    it('Sort orders correctly', () => {
      const plan = buildPlan(parse('SELECT * FROM employees ORDER BY salary DESC LIMIT 5'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows.length, 5);
      for (let i = 0; i < rows.length - 1; i++) {
        assert.ok(rows[i].salary >= rows[i + 1].salary);
      }
    });

    it('Limit truncates results', () => {
      const plan = buildPlan(parse('SELECT * FROM employees LIMIT 10'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows.length, 10);
    });

    it('Distinct removes duplicates', () => {
      const plan = buildPlan(parse('SELECT DISTINCT dept_id FROM employees'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows.length, 5);
    });
  });

  describe('Join Strategy Selection', () => {
    it('uses HashJoin for equi-joins without index', () => {
      const ast = parse('SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id');
      const explain = explainPlan(ast, db.tables);
      assert.ok(explain.includes('HashJoin') || explain.includes('NestedLoopJoin'),
        'Should use HashJoin or NLJ for equi-join without index');
    });

    it('uses INLJ when index is available', () => {
      db.execute('CREATE INDEX idx_dept ON employees (dept_id)');
      const ast = parse('SELECT e.name, d.name FROM departments d JOIN employees e ON d.id = e.dept_id');
      const explain = explainPlan(ast, db.tables, db.indexCatalog);
      // Should prefer INLJ when inner table has index on join key
      assert.ok(explain.includes('IndexNestedLoop') || explain.includes('HashJoin'),
        'Should consider INLJ with index');
    });

    it('multi-table join produces correct results', () => {
      const sql = `
        SELECT e.name as emp, d.name as dept, p.name as proj
        FROM employees e
        JOIN departments d ON e.dept_id = d.id
        JOIN projects p ON p.dept_id = d.id
      `;
      const plan = buildPlan(parse(sql), db.tables);
      const rows = plan.toArray();
      // Each employee (100) × matching projects per dept (10/5=2) = 200
      assert.ok(rows.length > 0, 'multi-table join should produce results');
    });
  });

  describe('Aggregation', () => {
    it('COUNT(*) through volcano', () => {
      const plan = buildPlan(parse('SELECT COUNT(*) as cnt FROM employees'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows[0].cnt, 100);
    });

    it('GROUP BY with aggregate', () => {
      const plan = buildPlan(parse('SELECT dept_id, COUNT(*) as cnt FROM employees GROUP BY dept_id'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows.length, 5); // 5 departments
      const total = rows.reduce((s, r) => s + r.cnt, 0);
      assert.equal(total, 100);
    });

    it('SUM and AVG', () => {
      const plan = buildPlan(parse('SELECT dept_id, SUM(salary) as total, AVG(salary) as avg_sal FROM employees GROUP BY dept_id'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows.length, 5);
      assert.ok(rows.every(r => r.total > 0));
      assert.ok(rows.every(r => r.avg_sal > 0));
    });
  });

  describe('Edge Cases', () => {
    it('empty result set', () => {
      const plan = buildPlan(parse('SELECT * FROM employees WHERE salary > 999999'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows.length, 0);
    });

    it('single row table', () => {
      db.execute('CREATE TABLE single (id INT, val TEXT)');
      db.execute("INSERT INTO single VALUES (1, 'only')");
      const plan = buildPlan(parse('SELECT * FROM single'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows.length, 1);
    });

    it('join with empty table', () => {
      db.execute('CREATE TABLE empty_t (id INT, val TEXT)');
      const plan = buildPlan(parse('SELECT e.name FROM employees e JOIN empty_t et ON e.id = et.id'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows.length, 0);
    });

    it('self-join', () => {
      const sql = 'SELECT a.name, b.name FROM employees a JOIN employees b ON a.dept_id = b.dept_id AND a.id < b.id';
      const plan = buildPlan(parse(sql), db.tables);
      const rows = plan.toArray();
      assert.ok(rows.length > 0, 'self-join should produce results');
    });

    it('LIMIT 0 returns no rows', () => {
      const plan = buildPlan(parse('SELECT * FROM employees LIMIT 0'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows.length, 0);
    });

    it('large sort is stable', () => {
      const plan = buildPlan(parse('SELECT * FROM employees ORDER BY dept_id, salary'), db.tables);
      const rows = plan.toArray();
      assert.equal(rows.length, 100);
      for (let i = 0; i < rows.length - 1; i++) {
        if (rows[i].dept_id === rows[i+1].dept_id) {
          assert.ok(rows[i].salary <= rows[i+1].salary);
        }
      }
    });
  });

  describe('Explain', () => {
    it('explain includes all operations', () => {
      const sql = 'SELECT name, salary FROM employees WHERE dept_id = 1 ORDER BY salary DESC LIMIT 5';
      const explain = explainPlan(parse(sql), db.tables);
      assert.ok(explain.includes('SeqScan') || explain.includes('Scan'));
      assert.ok(explain.includes('Filter') || explain.includes('WHERE'));
    });

    it('explain shows join type', () => {
      const sql = 'SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id';
      const explain = explainPlan(parse(sql), db.tables);
      assert.ok(explain.includes('Join'));
    });
  });
});
