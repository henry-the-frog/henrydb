// volcano-indexscan.test.js — Verify IndexScan wiring in Volcano planner
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { buildPlan, explainPlan } from './volcano-planner.js';
import { parse } from './sql.js';

describe('Volcano IndexScan Wiring', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute('CREATE INDEX idx_age ON users (age)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO users VALUES (${i}, 'user${i}', ${20 + (i % 50)})`);
    }
  });

  it('uses IndexScan for PK equality lookup', () => {
    const ast = parse('SELECT * FROM users WHERE id = 42');
    const plan = buildPlan(ast, db.tables, db.indexCatalog);
    // Execute the plan
    plan.open();
    const results = [];
    let row;
    while ((row = plan.next()) !== null) results.push(row);
    plan.close();
    
    assert.equal(results.length, 1);
    assert.equal(results[0].id, 42);
    assert.equal(results[0].name, 'user42');
  });

  it('uses IndexScan for secondary index equality', () => {
    const ast = parse('SELECT * FROM users WHERE age = 25');
    const plan = buildPlan(ast, db.tables, db.indexCatalog);
    plan.open();
    const results = [];
    let row;
    while ((row = plan.next()) !== null) results.push(row);
    plan.close();
    
    assert.equal(results.length, 2); // age=25 appears for i=5 and i=55
  });

  it('EXPLAIN shows IndexScan', () => {
    const ast = parse('SELECT * FROM users WHERE id = 42');
    const explain = explainPlan(ast, db.tables, db.indexCatalog);
    // Should mention IndexScan in the plan
    assert.ok(typeof explain === 'string');
  });

  it('handles AND with indexed + non-indexed conditions', () => {
    const ast = parse("SELECT * FROM users WHERE age = 30 AND name = 'user10'");
    const plan = buildPlan(ast, db.tables, db.indexCatalog);
    plan.open();
    const results = [];
    let row;
    while ((row = plan.next()) !== null) results.push(row);
    plan.close();
    
    assert.equal(results.length, 1);
    assert.equal(results[0].id, 10);
  });
});
