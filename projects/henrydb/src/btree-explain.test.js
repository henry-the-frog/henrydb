// btree-explain.test.js — Tests for EXPLAIN showing BTree engine details
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('EXPLAIN with BTree engine', () => {
  it('EXPLAIN shows btree engine for BTree table scan', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT) USING BTREE');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    
    const result = db.execute('EXPLAIN SELECT * FROM t');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('engine=btree'), `Expected engine=btree in: ${plan}`);
  });

  it('EXPLAIN shows heap engine for HeapFile table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    
    const result = db.execute('EXPLAIN SELECT * FROM t');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('engine=heap'), `Expected engine=heap in: ${plan}`);
  });

  it('EXPLAIN shows BTree PK Lookup for WHERE pk=value', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT) USING BTREE');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO products VALUES (${i}, 'p${i}')`);
    
    const result = db.execute('EXPLAIN SELECT * FROM products WHERE id = 5');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('BTree PK Lookup'), `Expected BTree PK Lookup in: ${plan}`);
  });

  it('EXPLAIN shows Sort Eliminated for ORDER BY PK ASC on BTree', () => {
    const db = new Database();
    db.execute('CREATE TABLE sorted (id INTEGER PRIMARY KEY, val TEXT) USING BTREE');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO sorted VALUES (${i}, 'v${i}')`);
    
    const result = db.execute('EXPLAIN SELECT * FROM sorted ORDER BY id ASC');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('Sort Eliminated'), `Expected Sort Eliminated in: ${plan}`);
    assert.ok(plan.includes('BTree PK ordering'), `Expected reason in: ${plan}`);
  });

  it('EXPLAIN shows Sort for ORDER BY DESC on BTree', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT) USING BTREE');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    
    const result = db.execute('EXPLAIN SELECT * FROM t ORDER BY id DESC');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    // DESC should NOT eliminate sort
    assert.ok(plan.includes('Sort  (keys:'), `Expected Sort in: ${plan}`);
    assert.ok(!plan.includes('Sort Eliminated'), `Should not have Sort Eliminated in: ${plan}`);
  });

  it('EXPLAIN shows Sort for non-PK ORDER BY on BTree', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT) USING BTREE');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, 'name${i}')`);
    
    const result = db.execute('EXPLAIN SELECT * FROM t ORDER BY name ASC');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('Sort  (keys:'), `Expected Sort in: ${plan}`);
  });

  it('EXPLAIN plan includes engine and btreeLookup in plan object', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT) USING BTREE');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    
    const result = db.execute('EXPLAIN SELECT * FROM t WHERE id = 1');
    // The plan array is in result.plan
    assert.ok(result.plan, 'Expected plan array in result');
    assert.ok(result.plan.some(step => step.operation === 'BTREE_PK_LOOKUP'), 'Expected BTREE_PK_LOOKUP in plan');
    assert.ok(result.plan.some(step => step.engine === 'btree'), 'Expected engine=btree in plan');
  });

  it('EXPLAIN shows complete plan for complex BTree query', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER) USING BTREE');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO orders VALUES (${i}, ${i * 10})`);
    
    const result = db.execute('EXPLAIN SELECT id, amount FROM orders WHERE amount > 500 ORDER BY id ASC LIMIT 10');
    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');
    
    // Should show: Seq Scan (engine=btree) + Filter + Sort Eliminated + Limit
    assert.ok(plan.includes('engine=btree'), 'Expected btree engine');
    assert.ok(plan.includes('Sort Eliminated'), 'Expected sort eliminated');
    assert.ok(plan.includes('Limit'), 'Expected limit');
    
    console.log('  Plan:\n' + plan.split('\n').map(l => '    ' + l).join('\n'));
  });

  it('EXPLAIN ANALYZE still works with BTree tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT) USING BTREE');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    
    const result = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE id = 25');
    assert.ok(result);
    assert.ok(result.rows.length > 0);
  });
});
