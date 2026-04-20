// volcano-planner.test.js — Tests for volcano query planner
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { buildPlan, explainPlan } from './volcano-planner.js';
import { parse } from './sql.js';
import { HeapFile } from './page.js';

function createTable(name, rows, schema) {
  const heap = new HeapFile(name);
  for (const row of rows) {
    heap.insert(row);
  }
  const schemaObj = schema.map(s => typeof s === 'string' ? { name: s } : s);
  return { heap, schema: schemaObj, indexes: new Map() };
}

function setupDB() {
  const tables = new Map();
  tables.set('users', createTable('users', 
    [[1, 'alice', 25], [2, 'bob', 30], [3, 'charlie', 35]],
    ['id', 'name', 'age']
  ));
  tables.set('orders', createTable('orders',
    [[1, 1, 100], [2, 1, 200], [3, 2, 150]],
    ['id', 'user_id', 'amount']
  ));
  return tables;
}

describe('Volcano Planner — Plan Building', () => {
  it('simple SELECT builds SeqScan', () => {
    const tables = setupDB();
    const ast = parse('SELECT * FROM users');
    const plan = buildPlan(ast, tables);
    assert.ok(plan, 'should build a plan');
    // Execute the plan
    plan.open();
    const rows = [];
    let row;
    while ((row = plan.next()) !== null) rows.push(row);
    plan.close();
    assert.equal(rows.length, 3);
  });

  it('SELECT with WHERE builds Filter', () => {
    const tables = setupDB();
    const ast = parse('SELECT * FROM users WHERE age > 27');
    const plan = buildPlan(ast, tables);
    plan.open();
    const rows = [];
    let row;
    while ((row = plan.next()) !== null) rows.push(row);
    plan.close();
    assert.equal(rows.length, 2); // bob(30) and charlie(35)
  });

  it('SELECT with LIMIT builds Limit node', () => {
    const tables = setupDB();
    const ast = parse('SELECT * FROM users LIMIT 2');
    const plan = buildPlan(ast, tables);
    plan.open();
    const rows = [];
    let row;
    while ((row = plan.next()) !== null) rows.push(row);
    plan.close();
    assert.equal(rows.length, 2);
  });

  it('SELECT with ORDER BY builds Sort node', () => {
    const tables = setupDB();
    const ast = parse('SELECT * FROM users ORDER BY age DESC');
    const plan = buildPlan(ast, tables);
    plan.open();
    const rows = [];
    let row;
    while ((row = plan.next()) !== null) rows.push(row);
    plan.close();
    assert.equal(rows.length, 3);
    // Should be sorted by age DESC: charlie(35), bob(30), alice(25)
    assert.ok(rows[0].age >= rows[1].age, 'should be sorted DESC');
  });

  it('JOIN builds join node', () => {
    const tables = setupDB();
    const ast = parse('SELECT * FROM users JOIN orders ON users.id = orders.user_id');
    const plan = buildPlan(ast, tables);
    plan.open();
    const rows = [];
    let row;
    while ((row = plan.next()) !== null) rows.push(row);
    plan.close();
    assert.equal(rows.length, 3); // alice has 2 orders, bob has 1
  });

  it('GROUP BY builds HashAggregate node', () => {
    const tables = setupDB();
    const ast = parse('SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id');
    const plan = buildPlan(ast, tables);
    plan.open();
    const rows = [];
    let row;
    while ((row = plan.next()) !== null) rows.push(row);
    plan.close();
    assert.equal(rows.length, 2); // 2 unique user_ids
  });

  it('DISTINCT builds Distinct node', () => {
    const tables = setupDB();
    const ast = parse('SELECT DISTINCT user_id FROM orders');
    const plan = buildPlan(ast, tables);
    plan.open();
    const rows = [];
    let row;
    while ((row = plan.next()) !== null) rows.push(row);
    plan.close();
    assert.equal(rows.length, 2);
  });

  it('explainPlan returns string', () => {
    const tables = setupDB();
    const ast = parse('SELECT * FROM users WHERE age > 25 ORDER BY age LIMIT 2');
    const explain = explainPlan(ast, tables);
    assert.ok(typeof explain === 'string');
    assert.ok(explain.length > 0);
    assert.ok(explain.includes('Scan') || explain.includes('scan'), 'should mention scan');
  });

  it('complex query: JOIN + WHERE + ORDER BY + LIMIT', () => {
    const tables = setupDB();
    const ast = parse(`
      SELECT * FROM users 
      JOIN orders ON users.id = orders.user_id 
      WHERE amount > 100 
      ORDER BY amount DESC 
      LIMIT 5
    `);
    const plan = buildPlan(ast, tables);
    plan.open();
    const rows = [];
    let row;
    while ((row = plan.next()) !== null) rows.push(row);
    plan.close();
    // amount > 100: (1,1,200) and (3,2,150)
    assert.equal(rows.length, 2);
    // Sorted DESC: 200, 150
    assert.ok(rows[0].amount >= rows[1].amount);
  });
});
