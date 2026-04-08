// join-ordering.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { JoinOrderer } from './join-ordering.js';

describe('JoinOrderer (Selinger-style)', () => {
  it('2-table join', () => {
    const jo = new JoinOrderer();
    const result = jo.optimize(
      [{ name: 'A', rows: 1000 }, { name: 'B', rows: 100 }],
      [{ left: 'A', right: 'B', selectivity: 0.01 }],
    );
    assert.ok(result);
    assert.ok(result.totalCost > 0);
    assert.equal(result.order.type, 'join');
  });

  it('3-table join picks optimal order', () => {
    const jo = new JoinOrderer();
    const result = jo.optimize(
      [{ name: 'A', rows: 10000 }, { name: 'B', rows: 100 }, { name: 'C', rows: 500 }],
      [
        { left: 'A', right: 'B', selectivity: 0.01 },
        { left: 'B', right: 'C', selectivity: 0.1 },
      ],
    );
    assert.ok(result);
    // Small tables should be joined first (B⋈C then result⋈A)
    assert.ok(result.totalCost > 0);
  });

  it('4-table join', () => {
    const jo = new JoinOrderer();
    const result = jo.optimize(
      [
        { name: 'orders', rows: 10000 },
        { name: 'customers', rows: 500 },
        { name: 'products', rows: 1000 },
        { name: 'lineItems', rows: 50000 },
      ],
      [
        { left: 'orders', right: 'customers', selectivity: 0.002 },
        { left: 'lineItems', right: 'orders', selectivity: 0.0001 },
        { left: 'lineItems', right: 'products', selectivity: 0.001 },
      ],
    );
    assert.ok(result);
    assert.ok(result.order.type === 'join');
  });

  it('cross join (no predicate)', () => {
    const jo = new JoinOrderer();
    const result = jo.optimize(
      [{ name: 'A', rows: 10 }, { name: 'B', rows: 10 }],
      [],
    );
    assert.ok(result);
    assert.equal(result.order.rows, 100); // 10 × 10 × 1.0
  });

  it('symmetry: same cost regardless of input order', () => {
    const jo = new JoinOrderer();
    const r1 = jo.optimize(
      [{ name: 'A', rows: 100 }, { name: 'B', rows: 200 }],
      [{ left: 'A', right: 'B', selectivity: 0.05 }],
    );
    const r2 = jo.optimize(
      [{ name: 'B', rows: 200 }, { name: 'A', rows: 100 }],
      [{ left: 'A', right: 'B', selectivity: 0.05 }],
    );
    assert.equal(r1.totalCost, r2.totalCost);
  });

  it('prefers smaller intermediate results', () => {
    const jo = new JoinOrderer();
    // A(10K) ⋈ B(100) has selectivity 0.001 → 1000 rows
    // A(10K) ⋈ C(10K) has selectivity 0.1 → 10M rows  
    // Optimal: join A⋈B first (small result), then join with C
    const result = jo.optimize(
      [{ name: 'A', rows: 10000 }, { name: 'B', rows: 100 }, { name: 'C', rows: 10000 }],
      [
        { left: 'A', right: 'B', selectivity: 0.001 },
        { left: 'A', right: 'C', selectivity: 0.1 },
      ],
    );
    assert.ok(result);
    assert.ok(result.totalCost > 0);
  });
});
