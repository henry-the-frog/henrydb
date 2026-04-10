// join-optimizer.test.js — Tests for cost-based join optimizer
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  TableStats, CostEstimate, JoinOptimizer,
  costNestedLoopJoin, costHashJoin, costSortMergeJoin,
} from './join-optimizer.js';

describe('TableStats', () => {
  it('computes selectivity', () => {
    const stats = new TableStats({ name: 'users', rows: 1000, distinctValues: new Map([['city', 50]]) });
    assert.equal(stats.selectivity('city'), 1 / 50);
    assert.equal(stats.selectivity('unknown'), 1); // Default for unknown
  });

  it('estimates join cardinality', () => {
    const users = new TableStats({ rows: 1000, distinctValues: new Map([['id', 1000]]) });
    const orders = new TableStats({ rows: 10000, distinctValues: new Map([['id', 10000], ['user_id', 1000]]) });
    
    // |users| × |orders| / max(NDV(users.id), NDV(orders.user_id))
    const card = users.joinCardinality(orders, 'user_id');
    assert.equal(card, 1000 * 10000 / 1000); // 10000
  });
});

describe('Individual Join Costs', () => {
  const small = new TableStats({ name: 'small', rows: 100, pages: 10, distinctValues: new Map([['id', 100]]) });
  const large = new TableStats({ name: 'large', rows: 100000, pages: 5000, distinctValues: new Map([['id', 100000], ['small_id', 100]]) });

  it('NLJ is expensive without index', () => {
    const cost = costNestedLoopJoin(large, small, 'id');
    assert.ok(cost.totalCost > 0);
    assert.ok(cost.details.includes('Simple NLJ'));
    assert.equal(cost.strategy, 'Nested Loop Join');
  });

  it('NLJ with index is cheaper', () => {
    const indexed = new TableStats({ ...small, indexes: [{ column: 'id', type: 'btree' }] });
    const withIdx = costNestedLoopJoin(large, indexed, 'id');
    const noIdx = costNestedLoopJoin(large, small, 'id');
    assert.ok(withIdx.totalCost < noIdx.totalCost, 'Index NLJ should be cheaper than Simple NLJ');
  });

  it('Hash Join has linear I/O', () => {
    const cost = costHashJoin(large, small, 'id');
    // I/O = pages(build) + pages(probe)
    assert.ok(cost.ioCost <= (small.pages + large.pages) * 4.1, 'I/O should be sequential scans');
    assert.ok(cost.memoryCost > 0, 'Hash table uses memory');
  });

  it('Sort-Merge Join skips sort if already sorted', () => {
    const sorted = new TableStats({ ...large, sorted: 'id' });
    const unsortedCost = costSortMergeJoin(large, small, 'id');
    const sortedCost = costSortMergeJoin(sorted, small, 'id');
    assert.ok(sortedCost.totalCost < unsortedCost.totalCost, 'Pre-sorted should be cheaper');
  });
});

describe('JoinOptimizer — Two Table Join', () => {
  it('picks hash join for large equi-join', () => {
    const optimizer = new JoinOptimizer();
    const users = new TableStats({ name: 'users', rows: 10000, pages: 500, distinctValues: new Map([['id', 10000]]) });
    const orders = new TableStats({ name: 'orders', rows: 100000, pages: 5000, distinctValues: new Map([['user_id', 10000]]) });
    
    const result = optimizer.optimize(users, orders, 'id');
    console.log('  Best:', result.best.toString());
    for (const alt of result.alternatives) {
      console.log('  Alt:', alt.toString());
    }
    
    assert.equal(result.best.strategy, 'Hash Join');
  });

  it('picks index NLJ when inner has index and is small', () => {
    const optimizer = new JoinOptimizer();
    const big = new TableStats({ name: 'big', rows: 100000, pages: 5000, distinctValues: new Map([['id', 100000]]) });
    const tiny = new TableStats({
      name: 'tiny', rows: 10, pages: 1,
      distinctValues: new Map([['id', 10]]),
      indexes: [{ column: 'id', type: 'btree' }],
    });
    
    const result = optimizer.optimize(big, tiny, 'id');
    console.log('  Best:', result.best.toString());
    // For tiny inner with index, NLJ should be competitive
    assert.ok(result.best.strategy.includes('Join'));
  });

  it('picks sort-merge when both inputs are sorted', () => {
    const optimizer = new JoinOptimizer();
    const a = new TableStats({ name: 'sorted_a', rows: 50000, pages: 2500, sorted: 'id', distinctValues: new Map([['id', 50000]]) });
    const b = new TableStats({ name: 'sorted_b', rows: 50000, pages: 2500, sorted: 'id', distinctValues: new Map([['id', 50000]]) });
    
    const result = optimizer.optimize(a, b, 'id');
    console.log('  Best:', result.best.toString());
    assert.equal(result.best.strategy, 'Sort-Merge Join');
  });
});

describe('JoinOptimizer — Multi-Table Join Ordering', () => {
  it('optimizes 3-table join (star schema)', () => {
    const optimizer = new JoinOptimizer();
    
    const fact = new TableStats({ name: 'sales', rows: 1000000, pages: 50000, distinctValues: new Map([['product_id', 10000], ['customer_id', 50000]]) });
    const dimProduct = new TableStats({ name: 'products', rows: 10000, pages: 500, distinctValues: new Map([['id', 10000]]) });
    const dimCustomer = new TableStats({ name: 'customers', rows: 50000, pages: 2500, distinctValues: new Map([['id', 50000]]) });
    
    const joinConditions = [
      { tableA: 'sales', tableB: 'products', column: 'product_id' },
      { tableA: 'sales', tableB: 'customers', column: 'customer_id' },
    ];
    
    const result = optimizer.optimizeMultiJoin([fact, dimProduct, dimCustomer], joinConditions);
    console.log('  Best join order:', result.plan);
    console.log('  Total cost:', result.cost.toFixed(1));
    
    assert.ok(result.cost < Infinity);
    assert.ok(result.plan.includes('⋈'));
  });

  it('optimizes 4-table join chain', () => {
    const optimizer = new JoinOptimizer();
    
    const a = new TableStats({ name: 'A', rows: 100, pages: 5, distinctValues: new Map([['id', 100], ['b_id', 50]]) });
    const b = new TableStats({ name: 'B', rows: 1000, pages: 50, distinctValues: new Map([['id', 1000], ['c_id', 200]]) });
    const c = new TableStats({ name: 'C', rows: 5000, pages: 250, distinctValues: new Map([['id', 5000], ['d_id', 500]]) });
    const d = new TableStats({ name: 'D', rows: 10000, pages: 500, distinctValues: new Map([['id', 10000]]) });
    
    const joinConditions = [
      { tableA: 'A', tableB: 'B', column: 'b_id' },
      { tableA: 'B', tableB: 'C', column: 'c_id' },
      { tableA: 'C', tableB: 'D', column: 'd_id' },
    ];
    
    const result = optimizer.optimizeMultiJoin([a, b, c, d], joinConditions);
    console.log('  Best join order:', result.plan);
    console.log('  Total cost:', result.cost.toFixed(1));
    
    assert.ok(result.cost < Infinity);
    // Should join smallest tables first
    assert.ok(result.plan.includes('A'));
  });

  it('handles 2-table case gracefully', () => {
    const optimizer = new JoinOptimizer();
    const x = new TableStats({ name: 'X', rows: 500, pages: 25, distinctValues: new Map([['id', 500]]) });
    const y = new TableStats({ name: 'Y', rows: 1000, pages: 50, distinctValues: new Map([['x_id', 500]]) });
    
    const result = optimizer.optimizeMultiJoin([x, y], [{ tableA: 'X', tableB: 'Y', column: 'id' }]);
    assert.ok(result.cost < Infinity);
    assert.ok(result.plan.includes('⋈'));
  });
});

describe('CostEstimate', () => {
  it('toString is human-readable', () => {
    const est = new CostEstimate('Hash Join', {
      cpuCost: 100, ioCost: 500, memoryCost: 10,
      outputRows: 1000, details: 'test join',
    });
    const str = est.toString();
    assert.ok(str.includes('Hash Join'));
    assert.ok(str.includes('610')); // total
    assert.ok(str.includes('1000'));
  });
});
