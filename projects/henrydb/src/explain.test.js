// explain.test.js — EXPLAIN tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('EXPLAIN', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, dept TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30, 'Engineering')");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25, 'Marketing')");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35, 'Engineering')");
  });

  it('returns plan type', () => {
    const result = db.execute('EXPLAIN SELECT * FROM users');
    assert.equal(result.type, 'PLAN');
    assert.ok(Array.isArray(result.plan));
  });

  it('shows TABLE_SCAN for simple query', () => {
    const result = db.execute('EXPLAIN SELECT * FROM users');
    const scan = result.plan.find(p => p.operation === 'TABLE_SCAN');
    assert.ok(scan);
    assert.equal(scan.table, 'users');
  });

  it('shows FILTER for WHERE', () => {
    const result = db.execute('EXPLAIN SELECT * FROM users WHERE age > 25');
    const filter = result.plan.find(p => p.operation === 'FILTER');
    assert.ok(filter);
  });

  it('shows INDEX_SCAN when index exists', () => {
    db.execute('CREATE INDEX idx_name ON users (name)');
    const result = db.execute("EXPLAIN SELECT * FROM users WHERE name = 'Alice'");
    const scan = result.plan.find(p => p.operation === 'INDEX_SCAN');
    assert.ok(scan);
    assert.equal(scan.index, 'name');
  });

  it('shows INDEX_SCAN for PK lookup', () => {
    const result = db.execute('EXPLAIN SELECT * FROM users WHERE id = 1');
    const scan = result.plan.find(p => p.operation === 'INDEX_SCAN');
    assert.ok(scan);
    assert.equal(scan.index, 'id');
  });

  it('shows SORT for ORDER BY', () => {
    const result = db.execute('EXPLAIN SELECT * FROM users ORDER BY age DESC');
    const sort = result.plan.find(p => p.operation === 'SORT');
    assert.ok(sort);
    assert.deepEqual(sort.columns, ['age DESC']);
  });

  it('shows LIMIT', () => {
    const result = db.execute('EXPLAIN SELECT * FROM users LIMIT 10');
    const limit = result.plan.find(p => p.operation === 'LIMIT');
    assert.ok(limit);
    assert.equal(limit.count, 10);
  });

  it('shows HASH_GROUP_BY for GROUP BY', () => {
    const result = db.execute('EXPLAIN SELECT dept, COUNT(*) FROM users GROUP BY dept');
    const group = result.plan.find(p => p.operation === 'HASH_GROUP_BY');
    assert.ok(group);
    assert.deepEqual(group.columns, ['dept']);
  });

  it('shows AGGREGATE for aggregate without GROUP BY', () => {
    const result = db.execute('EXPLAIN SELECT COUNT(*) FROM users');
    const agg = result.plan.find(p => p.operation === 'AGGREGATE');
    assert.ok(agg);
  });

  it('shows DISTINCT', () => {
    const result = db.execute('EXPLAIN SELECT DISTINCT dept FROM users');
    const dist = result.plan.find(p => p.operation === 'DISTINCT');
    assert.ok(dist);
  });

  it('shows NESTED_LOOP_JOIN', () => {
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)');
    const result = db.execute('EXPLAIN SELECT * FROM users JOIN orders ON users.id = orders.user_id');
    const joinOp = result.plan.find(p => p.operation === 'NESTED_LOOP_JOIN' || p.operation === 'HASH_JOIN');
    assert.ok(joinOp, 'Should have a join operation');
  });

  it('shows VIEW_SCAN for views', () => {
    db.execute("CREATE VIEW eng AS SELECT * FROM users WHERE dept = 'Engineering'");
    const result = db.execute('EXPLAIN SELECT * FROM eng');
    const view = result.plan.find(p => p.operation === 'VIEW_SCAN');
    assert.ok(view);
    assert.equal(view.view, 'eng');
  });

  it('shows WINDOW_FUNCTION', () => {
    const result = db.execute('EXPLAIN SELECT name, ROW_NUMBER() OVER (ORDER BY age) AS rn FROM users');
    const win = result.plan.find(p => p.operation === 'WINDOW_FUNCTION');
    assert.ok(win);
  });

  it('complex query plan has multiple steps', () => {
    db.execute('CREATE INDEX idx_dept ON users (dept)');
    const result = db.execute("EXPLAIN SELECT dept, COUNT(*) AS cnt FROM users WHERE dept = 'Engineering' GROUP BY dept ORDER BY cnt DESC LIMIT 5");
    assert.ok(result.plan.length >= 3);
    // Should have: INDEX_SCAN or TABLE_SCAN, HASH_GROUP_BY, SORT, LIMIT
    const ops = result.plan.map(p => p.operation);
    assert.ok(ops.includes('HASH_GROUP_BY'));
    assert.ok(ops.includes('SORT'));
    assert.ok(ops.includes('LIMIT'));
  });

  it('EXPLAIN with CTE', () => {
    const result = db.execute("EXPLAIN WITH eng AS (SELECT * FROM users WHERE dept = 'Engineering') SELECT * FROM eng");
    const cte = result.plan.find(p => p.operation === 'CTE');
    assert.ok(cte);
    assert.equal(cte.name, 'eng');
  });
});
