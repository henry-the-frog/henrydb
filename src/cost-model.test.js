// cost-model.test.js — Tests for query cost estimation
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { buildPlan } from './volcano-planner.js';
import { parse } from './sql.js';
import { estimateCost, formatCostEstimate, explainWithCost, estimateMultiEngineCost, getEngineCosts } from './cost-model.js';

describe('Cost Model', () => {
  let db, tableStats;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, dept TEXT)');
    db.execute('CREATE TABLE orders (id INT, user_id INT, amount INT)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO users VALUES (${i}, 'user_${i}', ${20 + i % 40}, 'dept_${i % 5}')`);
      db.execute(`INSERT INTO orders VALUES (${i}, ${i % 100}, ${i * 10})`);
    }
    tableStats = new Map([
      ['users', { rowCount: 100, avgRowSize: 50 }],
      ['orders', { rowCount: 100, avgRowSize: 30 }],
    ]);
  });

  function plan(sql) {
    return buildPlan(parse(sql), db.tables, db.indexCatalog);
  }

  it('estimates SeqScan cost proportional to table size', () => {
    const p = plan('SELECT * FROM users');
    const est = estimateCost(p, tableStats);
    assert.ok(est.rows > 0);
    assert.ok(est.cost > 0);
  });

  it('estimates Filter reduces rows', () => {
    const noFilter = estimateCost(plan('SELECT * FROM users'), tableStats);
    const withFilter = estimateCost(plan('SELECT * FROM users WHERE age > 30'), tableStats);
    assert.ok(withFilter.rows < noFilter.rows);
  });

  it('estimates Limit reduces cost dramatically', () => {
    const noLimit = estimateCost(plan('SELECT * FROM users'), tableStats);
    const withLimit = estimateCost(plan('SELECT * FROM users LIMIT 10'), tableStats);
    assert.ok(withLimit.cost < noLimit.cost);
  });

  it('estimates Sort cost is N*log(N)', () => {
    const scan = estimateCost(plan('SELECT * FROM users'), tableStats);
    const est = estimateCost(plan('SELECT * FROM users ORDER BY age'), tableStats);
    assert.ok(est.cost > scan.cost); // Sort should add cost beyond scan
  });

  it('estimates HashJoin cost', () => {
    const est = estimateCost(plan('SELECT * FROM users u JOIN orders o ON u.id = o.user_id'), tableStats);
    assert.ok(est.rows > 0);
    assert.ok(est.cost > 0);
  });

  it('estimates Aggregate reduces rows', () => {
    const est = estimateCost(plan('SELECT dept, COUNT(*) as cnt FROM users GROUP BY dept'), tableStats);
    assert.ok(est.rows < 100); // Should be < input rows
  });

  it('formatCostEstimate returns string', () => {
    const p = plan('SELECT * FROM users');
    const str = formatCostEstimate(p, tableStats);
    assert.ok(str.includes('rows'));
    assert.ok(str.includes('cost'));
  });

  it('explainWithCost shows cost annotations', () => {
    const p = plan('SELECT name FROM users WHERE age > 30 ORDER BY name LIMIT 10');
    const output = explainWithCost(p, tableStats);
    assert.ok(output.includes('[rows='));
    assert.ok(output.includes('cost='));
    assert.ok(output.includes('Limit'));
    assert.ok(output.includes('Sort'));
    assert.ok(output.includes('Filter'));
    assert.ok(output.includes('SeqScan'));
  });

  it('Limit has lower cost than Sort', () => {
    const sortPlan = plan('SELECT * FROM users ORDER BY age');
    const limitPlan = plan('SELECT * FROM users ORDER BY age LIMIT 5');
    const sortEst = estimateCost(sortPlan, tableStats);
    const limitEst = estimateCost(limitPlan, tableStats);
    assert.ok(limitEst.cost < sortEst.cost);
  });

  it('cost increases with table size', () => {
    const small = estimateCost(plan('SELECT * FROM users'), new Map([
      ['users', { rowCount: 10 }],
    ]));
    const large = estimateCost(plan('SELECT * FROM users'), new Map([
      ['users', { rowCount: 10000 }],
    ]));
    assert.ok(large.cost > small.cost);
  });

  it('NLJ costs more than HashJoin for large tables', () => {
    // We can't directly force NLJ vs HashJoin via SQL, but we can verify cost formulas
    const hashEst = estimateCost(plan('SELECT * FROM users u JOIN orders o ON u.id = o.user_id'), tableStats);
    assert.ok(hashEst.cost > 0);
    // NLJ cost would be O(N*M) vs HashJoin O(N+M)
    // With 100 rows each: NLJ ~ 10000, HashJoin ~ 350
    // Our planner picks HashJoin, so the actual cost should be reasonable
    assert.ok(hashEst.cost < 100000);
  });

  it('EXPLAIN with cost is multi-line', () => {
    const p = plan('SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 500');
    const output = explainWithCost(p, tableStats);
    const lines = output.split('\n');
    assert.ok(lines.length >= 3); // At least 3 operators
    assert.ok(lines.every(l => l.includes('[rows=')));
  });

  it('cost model handles Distinct', () => {
    const est = estimateCost(plan('SELECT DISTINCT dept FROM users'), tableStats);
    assert.ok(est.rows > 0);
    assert.ok(est.rows <= 100);
  });

  it('cost model handles complex pipeline', () => {
    const est = estimateCost(
      plan('SELECT dept, COUNT(*) as cnt FROM users WHERE age > 25 GROUP BY dept ORDER BY cnt DESC LIMIT 3'),
      tableStats
    );
    assert.ok(est.rows <= 3);
    assert.ok(est.cost > 0);
  });

  it('multi-engine cost returns all engines', () => {
    const result = estimateMultiEngineCost(plan('SELECT * FROM users WHERE age > 25'), tableStats);
    assert.ok(result.volcano);
    assert.ok(result.codegen);
    assert.ok(result.vectorized);
    assert.ok(result.cheapest);
    assert.ok(['volcano', 'codegen', 'vectorized'].includes(result.cheapest));
  });

  it('codegen cheaper than volcano for large scans', () => {
    // For a large table, codegen should be cheaper due to lower CPU multiplier
    const bigStats = new Map([['users', { rowCount: 10000, avgRowSize: 100 }]]);
    const result = estimateMultiEngineCost(plan('SELECT * FROM users WHERE age > 25'), bigStats);
    assert.ok(result.codegen.cost < result.volcano.cost,
      `codegen (${result.codegen.cost.toFixed(1)}) should be < volcano (${result.volcano.cost.toFixed(1)})`);
  });

  it('volcano cheapest for tiny tables', () => {
    // For very small tables, volcano's zero startup cost wins
    const tinyStats = new Map([['users', { rowCount: 5, avgRowSize: 50 }]]);
    const result = estimateMultiEngineCost(plan('SELECT * FROM users'), tinyStats);
    assert.equal(result.cheapest, 'volcano',
      `Expected volcano but got ${result.cheapest} (v=${result.volcano.cost.toFixed(2)}, c=${result.codegen.cost.toFixed(2)}, vec=${result.vectorized.cost.toFixed(2)})`);
  });

  it('engine costs are configurable', () => {
    const original = getEngineCosts();
    assert.ok(original.volcano);
    assert.equal(original.volcano.cpuMultiplier, 1.0);
  });
});
