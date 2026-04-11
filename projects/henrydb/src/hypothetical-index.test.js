// hypothetical-index.test.js — Tests for hypothetical index analysis
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { PlanBuilder, PlanFormatter } from './query-plan.js';
import { IndexAdvisor } from './index-advisor.js';
import { parse } from './sql.js';

describe('Hypothetical indexes in PlanBuilder', () => {
  it('uses hypothetical index for equality scan', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, total REAL)');
    for (let i = 1; i <= 500; i++) db.execute(`INSERT INTO orders VALUES (${i}, 'pending', ${i * 1.5})`);

    // Without hypothetical index: should be Seq Scan
    const builderNoIdx = new PlanBuilder(db);
    const planNoIdx = builderNoIdx.buildPlan(parse("SELECT * FROM orders WHERE status = 'shipped'"));
    assert.equal(planNoIdx.type, 'Seq Scan');

    // With hypothetical index: should be Index Scan
    const builderWithIdx = new PlanBuilder(db, {
      hypotheticalIndexes: [{ table: 'orders', columns: ['status'], name: 'idx_orders_status' }],
    });
    const planWithIdx = builderWithIdx.buildPlan(parse("SELECT * FROM orders WHERE status = 'shipped'"));
    assert.equal(planWithIdx.type, 'Index Scan', `Expected Index Scan, got ${planWithIdx.type}`);
    assert.ok(planWithIdx.properties?.hypothetical, 'Should be marked as hypothetical');
  });

  it('hypothetical index has lower cost than seq scan', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);

    const noIdx = new PlanBuilder(db);
    const withIdx = new PlanBuilder(db, {
      hypotheticalIndexes: [{ table: 't', columns: ['val'] }],
    });

    const planNoIdx = noIdx.buildPlan(parse("SELECT * FROM t WHERE val = 'v500'"));
    const planWithIdx = withIdx.buildPlan(parse("SELECT * FROM t WHERE val = 'v500'"));

    assert.ok(planWithIdx.estimatedCost < planNoIdx.estimatedCost,
      `Index plan (${planWithIdx.estimatedCost}) should be cheaper than seq scan (${planNoIdx.estimatedCost})`);
  });

  it('hypothetical index does not affect non-matching queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'a${i}', 'b${i}')`);

    const builder = new PlanBuilder(db, {
      hypotheticalIndexes: [{ table: 't', columns: ['a'] }],
    });

    // Query on column 'b' should NOT use the hypothetical index on 'a'
    const plan = builder.buildPlan(parse("SELECT * FROM t WHERE b = 'b50'"));
    assert.equal(plan.type, 'Seq Scan');
  });
});

describe('IndexAdvisor.compareWithIndex', () => {
  it('shows cost reduction with proposed index', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, total REAL)');
    for (let i = 1; i <= 500; i++) db.execute(`INSERT INTO orders VALUES (${i}, 'pending', ${i * 1.5})`);

    const advisor = new IndexAdvisor(db);
    const result = advisor.compareWithIndex(
      "SELECT * FROM orders WHERE status = 'shipped'",
      { table: 'orders', columns: ['status'] }
    );

    assert.ok(result);
    assert.ok(result.before.cost > 0);
    assert.ok(result.after.cost > 0);
    assert.ok(result.costReduction > 0, `Expected cost reduction, got ${result.costReduction}%`);
    assert.equal(result.before.type, 'Seq Scan');
    assert.equal(result.after.type, 'Index Scan');
    assert.ok(result.sql.includes('CREATE INDEX'));
  });

  it('shows no improvement when index does not help', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'a${i}', 'b${i}')`);

    const advisor = new IndexAdvisor(db);
    // Index on 'a' doesn't help query on 'b'
    const result = advisor.compareWithIndex(
      "SELECT * FROM t WHERE b = 'b50'",
      { table: 't', columns: ['a'] }
    );

    assert.ok(result);
    assert.equal(result.costReduction, 0);
  });

  it('handles unparseable SQL gracefully', () => {
    const db = new Database();
    const advisor = new IndexAdvisor(db);
    const result = advisor.compareWithIndex('INVALID SQL', { table: 't', columns: ['x'] });
    assert.equal(result, null);
  });

  it('provides before and after plan text', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)');
    for (let i = 1; i <= 200; i++) db.execute(`INSERT INTO users VALUES (${i}, 'u${i}@test.com')`);

    const advisor = new IndexAdvisor(db);
    const result = advisor.compareWithIndex(
      "SELECT * FROM users WHERE email = 'u100@test.com'",
      { table: 'users', columns: ['email'], name: 'idx_users_email' }
    );

    assert.ok(result.before.plan.includes('Seq Scan'));
    assert.ok(result.after.plan.includes('Index Scan'));
    assert.ok(result.after.plan.includes('idx_users_email'));
  });
});
