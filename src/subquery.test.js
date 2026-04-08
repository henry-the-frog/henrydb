// subquery.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SubqueryEngine } from './subquery.js';

describe('Correlated Subqueries', () => {
  const engine = new SubqueryEngine();
  engine.addTable('employees', [
    { id: 1, name: 'Alice', dept_id: 1, salary: 120000 },
    { id: 2, name: 'Bob', dept_id: 1, salary: 110000 },
    { id: 3, name: 'Charlie', dept_id: 2, salary: 95000 },
    { id: 4, name: 'Dana', dept_id: 3, salary: 85000 },
    { id: 5, name: 'Eve', dept_id: 1, salary: 130000 },
  ]);
  engine.addTable('departments', [
    { id: 1, name: 'Engineering' },
    { id: 2, name: 'Sales' },
    { id: 3, name: 'HR' },
    { id: 4, name: 'Marketing' },
  ]);
  engine.addTable('orders', [
    { id: 1, emp_id: 1, amount: 500 },
    { id: 2, emp_id: 1, amount: 300 },
    { id: 3, emp_id: 3, amount: 200 },
    { id: 4, emp_id: 5, amount: 1000 },
  ]);

  it('EXISTS: employees who have orders', () => {
    const result = engine.exists('employees', {
      table: 'orders',
      where: (emp, order) => emp.id === order.emp_id,
    });
    assert.equal(result.length, 3); // Alice, Charlie, Eve
    assert.deepEqual(result.map(r => r.name).sort(), ['Alice', 'Charlie', 'Eve']);
  });

  it('NOT EXISTS: employees without orders', () => {
    const result = engine.notExists('employees', {
      table: 'orders',
      where: (emp, order) => emp.id === order.emp_id,
    });
    assert.equal(result.length, 2); // Bob, Dana
    assert.deepEqual(result.map(r => r.name).sort(), ['Bob', 'Dana']);
  });

  it('IN subquery: depts that have employees', () => {
    const result = engine.inSubquery('departments', 'id', {
      table: 'employees',
      select: 'dept_id',
    });
    assert.equal(result.length, 3); // Engineering, Sales, HR
    assert.ok(!result.find(r => r.name === 'Marketing'));
  });

  it('NOT IN subquery: depts without employees', () => {
    const result = engine.notInSubquery('departments', 'id', {
      table: 'employees',
      select: 'dept_id',
    });
    assert.equal(result.length, 1);
    assert.equal(result[0].name, 'Marketing');
  });

  it('IN with filtered subquery', () => {
    // Departments with high-earning employees (salary > 100K)
    const result = engine.inSubquery('departments', 'id', {
      table: 'employees',
      select: 'dept_id',
      where: row => row.salary > 100000,
    });
    assert.equal(result.length, 1); // Only Engineering
    assert.equal(result[0].name, 'Engineering');
  });

  it('scalar subquery: COUNT orders per employee', () => {
    const result = engine.scalarSubquery('employees', 'order_count', {
      table: 'orders',
      agg: 'COUNT',
      where: (emp, order) => emp.id === order.emp_id,
    });
    assert.equal(result.find(r => r.name === 'Alice').order_count, 2);
    assert.equal(result.find(r => r.name === 'Bob').order_count, 0);
    assert.equal(result.find(r => r.name === 'Eve').order_count, 1);
  });

  it('scalar subquery: SUM order amounts', () => {
    const result = engine.scalarSubquery('employees', 'total_orders', {
      table: 'orders',
      select: 'amount',
      agg: 'SUM',
      where: (emp, order) => emp.id === order.emp_id,
    });
    assert.equal(result.find(r => r.name === 'Alice').total_orders, 800);
    assert.equal(result.find(r => r.name === 'Eve').total_orders, 1000);
  });

  it('scalar subquery: MAX order', () => {
    const result = engine.scalarSubquery('employees', 'max_order', {
      table: 'orders',
      select: 'amount',
      agg: 'MAX',
      where: (emp, order) => emp.id === order.emp_id,
    });
    assert.equal(result.find(r => r.name === 'Alice').max_order, 500);
    assert.equal(result.find(r => r.name === 'Dana').max_order, null);
  });

  it('lateral join: top order per employee', () => {
    const result = engine.lateral('employees', {
      table: 'orders',
      where: (emp, order) => emp.id === order.emp_id,
    });
    // Should have 4 joined rows (Alice:2 + Charlie:1 + Eve:1)
    assert.equal(result.length, 4);
  });
});
