// volcano-explain.test.js — Tests for EXPLAIN VOLCANO
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { explainPlan, buildPlan } from './volcano-planner.js';
import { parse } from './sql.js';

describe('EXPLAIN VOLCANO', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT, name TEXT, age INT, dept TEXT)');
    db.execute('CREATE TABLE orders (id INT, user_id INT, amount INT, product TEXT)');
  });

  it('explains simple SELECT', () => {
    const plan = explainPlan(parse('SELECT * FROM users'), db.tables);
    assert.ok(plan.includes('SeqScan'));
    assert.ok(plan.includes('users'));
  });

  it('explains SELECT with WHERE', () => {
    const plan = explainPlan(parse('SELECT name FROM users WHERE age > 25'), db.tables);
    assert.ok(plan.includes('Filter'));
    assert.ok(plan.includes('SeqScan'));
    assert.ok(plan.includes('Project'));
  });

  it('explains ORDER BY + LIMIT', () => {
    const plan = explainPlan(parse('SELECT name FROM users ORDER BY age LIMIT 5'), db.tables);
    assert.ok(plan.includes('Limit'));
    assert.ok(plan.includes('limit=5'));
    assert.ok(plan.includes('Sort'));
    assert.ok(plan.includes('age'));
  });

  it('explains JOIN', () => {
    const plan = explainPlan(parse('SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id'), db.tables);
    assert.ok(plan.includes('HashJoin'));
    assert.ok(plan.includes('SeqScan'));
  });

  it('explains GROUP BY with aggregates', () => {
    const plan = explainPlan(parse('SELECT dept, COUNT(*) as cnt FROM users GROUP BY dept'), db.tables);
    assert.ok(plan.includes('HashAggregate'));
    assert.ok(plan.includes('COUNT'));
  });

  it('explains DISTINCT', () => {
    const plan = explainPlan(parse('SELECT DISTINCT dept FROM users'), db.tables);
    assert.ok(plan.includes('Distinct'));
  });

  it('explains complex pipeline', () => {
    const plan = explainPlan(
      parse('SELECT u.name, SUM(o.amount) as total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name HAVING SUM(o.amount) > 100 ORDER BY total DESC LIMIT 5'),
      db.tables
    );
    assert.ok(plan.includes('Limit'));
    assert.ok(plan.includes('Sort'));
    assert.ok(plan.includes('Filter')); // HAVING
    assert.ok(plan.includes('HashAggregate'));
    assert.ok(plan.includes('HashJoin'));
    assert.ok(plan.includes('SeqScan'));
  });

  it('tree has correct indentation (children indented)', () => {
    const plan = explainPlan(parse('SELECT name FROM users WHERE age > 25 LIMIT 5'), db.tables);
    const lines = plan.split('\n');
    // Root should have no indent
    assert.ok(lines[0].startsWith('→'));
    // Children should be indented
    assert.ok(lines.some(l => l.startsWith('  →')));
    assert.ok(lines.some(l => l.startsWith('    →')));
  });

  it('describe() returns correct structure', () => {
    const plan = buildPlan(parse('SELECT name FROM users WHERE age > 25 LIMIT 5'), db.tables);
    const desc = plan.describe();
    assert.equal(desc.type, 'Limit');
    assert.equal(desc.details.limit, 5);
    assert.ok(desc.children.length > 0);
  });

  it('each operator type is identifiable', () => {
    const queries = [
      ['SELECT * FROM users', 'SeqScan'],
      ['SELECT name FROM users', 'Project'],
      ['SELECT * FROM users WHERE age > 25', 'Filter'],
      ['SELECT * FROM users LIMIT 10', 'Limit'],
      ['SELECT DISTINCT dept FROM users', 'Distinct'],
      ['SELECT dept, COUNT(*) as c FROM users GROUP BY dept', 'HashAggregate'],
      ['SELECT * FROM users ORDER BY age', 'Sort'],
      ['SELECT * FROM users u JOIN orders o ON u.id = o.user_id', 'HashJoin'],
    ];
    for (const [sql, expectedOp] of queries) {
      const plan = explainPlan(parse(sql), db.tables);
      assert.ok(plan.includes(expectedOp), `Expected ${expectedOp} in plan for: ${sql}\nGot: ${plan}`);
    }
  });
});
