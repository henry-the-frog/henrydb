// compiled-aggregation.test.js — Tests for compiled aggregation in query pipeline
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CompiledQueryEngine } from './compiled-query.js';
import { Database } from './db.js';

function setupDB() {
  const db = new Database();
  db.execute('CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, category TEXT, amount INT, quantity INT)');
  
  const regions = ['US', 'EU', 'APAC', 'LATAM'];
  const categories = ['electronics', 'books', 'clothing', 'food'];
  
  for (let i = 0; i < 500; i++) {
    const region = regions[i % 4];
    const category = categories[i % 4];
    const amount = (i * 17 + 100) % 1000;
    const quantity = (i % 10) + 1;
    db.execute(`INSERT INTO sales VALUES (${i}, '${region}', '${category}', ${amount}, ${quantity})`);
  }

  db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT)');
  db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region TEXT)');

  for (let i = 0; i < 200; i++) {
    db.execute(`INSERT INTO customers VALUES (${i}, 'Customer ${i}', '${regions[i % 4]}')`);
  }
  for (let i = 0; i < 600; i++) {
    const custId = i % 200;
    const amount = (i * 23 + 50) % 5000;
    const status = ['pending', 'shipped', 'delivered'][i % 3];
    db.execute(`INSERT INTO orders VALUES (${i}, ${custId}, ${amount}, '${status}')`);
  }

  return db;
}

describe('Compiled Aggregation', () => {
  
  it('COUNT(*) with GROUP BY', () => {
    const db = setupDB();
    const engine = new CompiledQueryEngine(db);

    const result = engine._compiledAggregate(
      Array.from({ length: 500 }, (_, i) => ({ region: ['US', 'EU', 'APAC', 'LATAM'][i % 4], amount: i })),
      ['region'],
      [{ fn: 'COUNT', column: '*', alias: 'cnt' }]
    );

    assert.equal(result.length, 4);
    assert.ok(result.every(r => r.cnt === 125));
  });

  it('SUM with GROUP BY', () => {
    const db = setupDB();
    const engine = new CompiledQueryEngine(db);

    const rows = [
      { region: 'US', amount: 100 },
      { region: 'US', amount: 200 },
      { region: 'EU', amount: 300 },
      { region: 'EU', amount: 400 },
    ];

    const result = engine._compiledAggregate(rows, ['region'], [
      { fn: 'SUM', column: 'amount', alias: 'total' }
    ]);

    assert.equal(result.length, 2);
    const us = result.find(r => r.region === 'US');
    const eu = result.find(r => r.region === 'EU');
    assert.equal(us.total, 300);
    assert.equal(eu.total, 700);
  });

  it('AVG with GROUP BY', () => {
    const db = setupDB();
    const engine = new CompiledQueryEngine(db);

    const rows = [
      { cat: 'A', val: 10 },
      { cat: 'A', val: 20 },
      { cat: 'A', val: 30 },
      { cat: 'B', val: 100 },
    ];

    const result = engine._compiledAggregate(rows, ['cat'], [
      { fn: 'AVG', column: 'val', alias: 'avg_val' }
    ]);

    const a = result.find(r => r.cat === 'A');
    const b = result.find(r => r.cat === 'B');
    assert.equal(a.avg_val, 20);
    assert.equal(b.avg_val, 100);
  });

  it('MIN and MAX', () => {
    const db = setupDB();
    const engine = new CompiledQueryEngine(db);

    const rows = [
      { grp: 'X', val: 50 },
      { grp: 'X', val: 10 },
      { grp: 'X', val: 90 },
      { grp: 'Y', val: 30 },
      { grp: 'Y', val: 70 },
    ];

    const result = engine._compiledAggregate(rows, ['grp'], [
      { fn: 'MIN', column: 'val', alias: 'min_val' },
      { fn: 'MAX', column: 'val', alias: 'max_val' },
    ]);

    const x = result.find(r => r.grp === 'X');
    const y = result.find(r => r.grp === 'Y');
    assert.equal(x.min_val, 10);
    assert.equal(x.max_val, 90);
    assert.equal(y.min_val, 30);
    assert.equal(y.max_val, 70);
  });

  it('multiple aggregates in one query', () => {
    const db = setupDB();
    const engine = new CompiledQueryEngine(db);

    const rows = Array.from({ length: 100 }, (_, i) => ({
      region: ['US', 'EU'][i % 2],
      amount: (i + 1) * 10,
      qty: (i % 5) + 1,
    }));

    const result = engine._compiledAggregate(rows, ['region'], [
      { fn: 'COUNT', column: '*', alias: 'cnt' },
      { fn: 'SUM', column: 'amount', alias: 'total' },
      { fn: 'AVG', column: 'amount', alias: 'avg_amt' },
      { fn: 'MIN', column: 'qty', alias: 'min_qty' },
      { fn: 'MAX', column: 'qty', alias: 'max_qty' },
    ]);

    assert.equal(result.length, 2);
    const us = result.find(r => r.region === 'US');
    assert.equal(us.cnt, 50);
    assert.equal(us.min_qty, 1);
    assert.equal(us.max_qty, 5);
  });

  it('no GROUP BY: whole-table aggregate', () => {
    const db = setupDB();
    const engine = new CompiledQueryEngine(db);

    const rows = [{ val: 10 }, { val: 20 }, { val: 30 }];
    
    // No group-by columns = one group for all rows
    const result = engine._compiledAggregate(rows, [], [
      { fn: 'COUNT', column: '*', alias: 'cnt' },
      { fn: 'SUM', column: 'val', alias: 'total' },
      { fn: 'AVG', column: 'val', alias: 'avg_val' },
    ]);

    assert.equal(result.length, 1);
    assert.equal(result[0].cnt, 3);
    assert.equal(result[0].total, 60);
    assert.equal(result[0].avg_val, 20);
  });

  it('executeSelectWithAggregation: single table GROUP BY', () => {
    const db = setupDB();
    const engine = new CompiledQueryEngine(db);

    const ast = {
      type: 'SELECT',
      columns: [
        { name: 'region' },
        { aggregate: 'COUNT', fn: 'COUNT', args: [{ name: '*' }], alias: 'cnt' },
        { aggregate: 'SUM', fn: 'SUM', args: [{ name: 'amount' }], column: 'amount', alias: 'total' },
      ],
      from: { table: 'sales' },
      groupBy: ['region'],
    };

    const result = engine.executeSelectWithAggregation(ast);
    assert.ok(result, 'Should compile');
    assert.equal(result.rows.length, 4); // 4 regions
    assert.ok(result.rows.every(r => r.cnt === 125)); // 500/4 = 125 each
    assert.ok(result.rows.every(r => r.total > 0));
  });

  it('executeSelectWithAggregation: join + GROUP BY', () => {
    const db = setupDB();
    const engine = new CompiledQueryEngine(db);

    const ast = {
      type: 'SELECT',
      columns: [
        { name: 'region', table: 'c' },
        { aggregate: 'COUNT', fn: 'COUNT', args: [{ name: '*' }], alias: 'order_count' },
        { aggregate: 'SUM', fn: 'SUM', args: [{ name: 'amount' }], column: 'amount', alias: 'total_amount' },
      ],
      from: { table: 'customers', alias: 'c' },
      joins: [{
        table: 'orders',
        alias: 'o',
        joinType: 'INNER',
        on: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', table: 'c', name: 'id' },
          right: { type: 'column_ref', table: 'o', name: 'customer_id' }
        }
      }],
      groupBy: [{ name: 'region' }],
    };

    const result = engine.executeSelectWithAggregation(ast);
    assert.ok(result, 'Should compile join + aggregation');
    assert.equal(result.rows.length, 4); // 4 regions
    
    const total = result.rows.reduce((s, r) => s + r.order_count, 0);
    assert.equal(total, 600); // 600 orders total
  });

  it('benchmark: compiled aggregation vs standard', () => {
    const db = setupDB();
    const engine = new CompiledQueryEngine(db);

    // Compiled
    const ast = {
      type: 'SELECT',
      columns: [
        { name: 'region' },
        { aggregate: 'COUNT', fn: 'COUNT', args: [{ name: '*' }], alias: 'cnt' },
        { aggregate: 'SUM', fn: 'SUM', args: [{ name: 'amount' }], column: 'amount', alias: 'total' },
        { aggregate: 'AVG', fn: 'AVG', args: [{ name: 'amount' }], column: 'amount', alias: 'avg_amt' },
      ],
      from: { table: 'sales' },
      groupBy: ['region'],
    };

    const t0 = Date.now();
    const compiled = engine.executeSelectWithAggregation(ast);
    const compiledMs = Date.now() - t0;

    const t1 = Date.now();
    const standard = db.execute('SELECT region, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amt FROM sales GROUP BY region');
    const standardMs = Date.now() - t1;

    console.log(`    Aggregation: Compiled ${compiledMs}ms vs Standard ${standardMs}ms`);
    assert.ok(compiled);
    assert.equal(compiled.rows.length, 4);
  });

  it('handles NULL values in aggregation', () => {
    const db = new Database();
    const engine = new CompiledQueryEngine(db);

    const rows = [
      { grp: 'A', val: 10 },
      { grp: 'A', val: null },
      { grp: 'A', val: 30 },
      { grp: 'B', val: null },
    ];

    const result = engine._compiledAggregate(rows, ['grp'], [
      { fn: 'COUNT', column: 'val', alias: 'cnt' },
      { fn: 'SUM', column: 'val', alias: 'total' },
      { fn: 'AVG', column: 'val', alias: 'avg_val' },
    ]);

    const a = result.find(r => r.grp === 'A');
    assert.equal(a.cnt, 2); // NULL not counted
    assert.equal(a.total, 40);
    assert.equal(a.avg_val, 20); // AVG of 10 and 30

    const b = result.find(r => r.grp === 'B');
    assert.equal(b.cnt, 0); // All NULL
    assert.equal(b.total, 0);
    assert.equal(b.avg_val, null); // No non-null values
  });

  it('large dataset aggregation (1000 groups)', () => {
    const db = new Database();
    const engine = new CompiledQueryEngine(db);

    const rows = Array.from({ length: 100000 }, (_, i) => ({
      grp: `g${i % 1000}`,
      val: i,
    }));

    const t0 = Date.now();
    const result = engine._compiledAggregate(rows, ['grp'], [
      { fn: 'COUNT', column: '*', alias: 'cnt' },
      { fn: 'SUM', column: 'val', alias: 'total' },
      { fn: 'AVG', column: 'val', alias: 'avg_val' },
    ]);
    const ms = Date.now() - t0;

    assert.equal(result.length, 1000);
    assert.ok(result.every(r => r.cnt === 100));
    console.log(`    100K rows, 1000 groups: ${ms}ms`);
  });
});
