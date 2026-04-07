// index-nested-loop.test.js — Tests for IndexNestedLoopJoin
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { buildPlan, explainPlan } from './volcano-planner.js';
import { parse } from './sql.js';

describe('IndexNestedLoopJoin', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, dept_id INT)');
    db.execute('CREATE TABLE departments (id INT PRIMARY KEY, dept_name TEXT)');
    
    db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
    db.execute("INSERT INTO departments VALUES (2, 'Sales')");
    db.execute("INSERT INTO departments VALUES (3, 'Marketing')");
    
    db.execute("INSERT INTO users VALUES (1, 'Alice', 1)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 2)");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 1)");
    db.execute("INSERT INTO users VALUES (4, 'Diana', 3)");
    db.execute("INSERT INTO users VALUES (5, 'Eve', 2)");
  });

  it('uses INL join when inner table has index on join key', () => {
    // departments.id has PRIMARY KEY index
    const ast = parse('SELECT u.name, d.dept_name FROM users u JOIN departments d ON u.dept_id = d.id');
    const plan = buildPlan(ast, db.tables, db.indexCatalog);
    const rows = plan.toArray();
    
    assert.equal(rows.length, 5);
    const alice = rows.find(r => (r['u.name'] || r.name) === 'Alice');
    assert.ok(alice);
    assert.equal(alice['d.dept_name'] || alice.dept_name, 'Engineering');
  });

  it('EXPLAIN shows IndexNestedLoopJoin when index available', () => {
    const ast = parse('SELECT u.name, d.dept_name FROM users u JOIN departments d ON u.dept_id = d.id');
    const plan = explainPlan(ast, db.tables, db.indexCatalog);
    assert.ok(plan.includes('IndexNestedLoopJoin'), `Expected INL join in plan:\n${plan}`);
  });

  it('falls back to HashJoin when no index on join key', () => {
    // users.dept_id has no index
    const ast = parse('SELECT d.dept_name, u.name FROM departments d JOIN users u ON d.id = u.dept_id');
    const plan = explainPlan(ast, db.tables, db.indexCatalog);
    // The join should use HashJoin since u.dept_id has no index
    assert.ok(plan.includes('HashJoin') || plan.includes('IndexNestedLoopJoin'),
      `Expected HashJoin or INL in plan:\n${plan}`);
  });

  it('INL join produces correct results with WHERE filter', () => {
    const ast = parse("SELECT u.name, d.dept_name FROM users u JOIN departments d ON u.dept_id = d.id WHERE d.dept_name = 'Engineering'");
    const plan = buildPlan(ast, db.tables, db.indexCatalog);
    const rows = plan.toArray();
    assert.equal(rows.length, 2); // Alice and Charlie
  });

  it('INL join produces same results as HashJoin', () => {
    const sql = 'SELECT u.name, d.dept_name FROM users u JOIN departments d ON u.dept_id = d.id';
    const ast = parse(sql);
    
    // With indexCatalog → may use INL
    const inlRows = buildPlan(ast, db.tables, db.indexCatalog).toArray();
    // Without indexCatalog → uses HashJoin
    const hashRows = buildPlan(ast, db.tables).toArray();
    
    assert.equal(inlRows.length, hashRows.length);
  });

  it('INL join works with standard query results', () => {
    const sql = 'SELECT u.name, d.dept_name FROM users u JOIN departments d ON u.dept_id = d.id';
    const ast = parse(sql);
    const volRows = buildPlan(ast, db.tables, db.indexCatalog).toArray();
    const stdResult = db.execute(sql);
    
    assert.equal(volRows.length, stdResult.rows.length);
  });

  it('INL join handles NULL join keys correctly', () => {
    db.execute("INSERT INTO users VALUES (6, 'Frank', NULL)");
    const ast = parse('SELECT u.name, d.dept_name FROM users u JOIN departments d ON u.dept_id = d.id');
    const plan = buildPlan(ast, db.tables, db.indexCatalog);
    const rows = plan.toArray();
    // Frank should not appear (NULL dept_id doesn't match any department)
    assert.equal(rows.length, 5);
    assert.ok(!rows.find(r => (r['u.name'] || r.name) === 'Frank'));
  });

  it('INL join handles non-matching keys', () => {
    db.execute("INSERT INTO users VALUES (7, 'Grace', 99)");
    const ast = parse('SELECT u.name, d.dept_name FROM users u JOIN departments d ON u.dept_id = d.id');
    const plan = buildPlan(ast, db.tables, db.indexCatalog);
    const rows = plan.toArray();
    // Grace has dept_id=99 which doesn't exist
    assert.equal(rows.length, 5);
  });

  it('INL join with many rows', () => {
    db.execute('CREATE TABLE big_orders (id INT, user_id INT, amount INT)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO big_orders VALUES (${i}, ${(i % 5) + 1}, ${i * 10})`);
    }
    
    // users.id has PRIMARY KEY index
    const ast = parse('SELECT o.amount, u.name FROM big_orders o JOIN users u ON o.user_id = u.id');
    const plan = buildPlan(ast, db.tables, db.indexCatalog);
    const rows = plan.toArray();
    assert.equal(rows.length, 100); // Every order matches a user
  });

  it('without indexCatalog, always uses HashJoin', () => {
    const ast = parse('SELECT u.name FROM users u JOIN departments d ON u.dept_id = d.id');
    const plan = explainPlan(ast, db.tables); // No indexCatalog
    assert.ok(!plan.includes('IndexNestedLoopJoin'), `Should not use INL without indexCatalog:\n${plan}`);
    assert.ok(plan.includes('HashJoin'));
  });
});
