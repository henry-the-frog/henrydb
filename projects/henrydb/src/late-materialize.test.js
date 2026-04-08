// late-materialize.test.js — Tests for late materialization
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { VirtualRelation, JoinedRelation } from './late-materialize.js';

function setupCustomers(n = 500) {
  const id = [], name = [], region = [], tier = [];
  for (let i = 0; i < n; i++) {
    id.push(i);
    name.push(`Customer ${i}`);
    region.push(['US', 'EU', 'APAC'][i % 3]);
    tier.push((i % 4) + 1);
  }
  return { id, name, region, tier };
}

function setupOrders(n = 500) {
  const id = [], customer_id = [], amount = [], status = [];
  for (let i = 0; i < n * 3; i++) {
    id.push(i);
    customer_id.push(i % n);
    amount.push((i * 17 + 13) % 1000);
    status.push(['pending', 'shipped', 'delivered'][i % 3]);
  }
  return { id, customer_id, amount, status };
}

describe('Late Materialization', () => {

  it('VirtualRelation: full scan', () => {
    const cols = setupCustomers(100);
    const vr = new VirtualRelation(cols);
    assert.equal(vr.length, 100);

    const rows = vr.materialize(['id', 'name'], 5);
    assert.equal(rows.length, 5);
    assert.equal(rows[0].id, 0);
  });

  it('filter without materializing', () => {
    const cols = setupCustomers(300);
    const vr = new VirtualRelation(cols);

    const filtered = vr.filterEquals('region', 'US');
    assert.equal(filtered.length, 100); // 300/3

    // Still no row objects created
    const rows = filtered.materialize(['id', 'region']);
    assert.ok(rows.every(r => r.region === 'US'));
  });

  it('chained filters', () => {
    const cols = setupCustomers(400);
    const vr = new VirtualRelation(cols);

    const result = vr
      .filterEquals('region', 'EU')
      .filterGT('tier', 2);

    const rows = result.materialize(['id', 'region', 'tier']);
    assert.ok(rows.every(r => r.region === 'EU' && r.tier > 2));
  });

  it('hash join without materializing', () => {
    const custCols = setupCustomers(100);
    const orderCols = setupOrders(100);

    const customers = new VirtualRelation(custCols);
    const orders = new VirtualRelation(orderCols);

    const joined = customers.hashJoin(orders, 'id', 'customer_id');
    assert.equal(joined.length, 300); // 100 customers × 3 orders each
  });

  it('filter → join → materialize', () => {
    const custCols = setupCustomers(200);
    const orderCols = setupOrders(200);

    const customers = new VirtualRelation(custCols);
    const orders = new VirtualRelation(orderCols);

    // Filter customers to US only, then join with orders
    const usCustomers = customers.filterEquals('region', 'US');
    const joined = usCustomers.hashJoin(orders, 'id', 'customer_id');

    // Only now materialize
    const rows = joined.materialize(['name', 'region'], ['amount', 'status'], 10);
    assert.equal(rows.length, 10);
    assert.ok(rows.every(r => r.region === 'US'));
    assert.ok(rows.every(r => r.amount !== undefined));
  });

  it('aggregate without materializing', () => {
    const cols = setupCustomers(300);
    const vr = new VirtualRelation(cols);

    const result = vr.aggregate('region', 'tier', 'SUM');
    assert.equal(result.length, 3);
    assert.ok(result.every(r => r.count === 100));
  });

  it('filter → aggregate', () => {
    const cols = setupCustomers(300);
    const vr = new VirtualRelation(cols);

    const filtered = vr.filterGT('tier', 2);
    const result = filtered.aggregate('region', 'tier', 'AVG');
    assert.ok(result.every(r => r.avg > 2));
  });

  it('LEFT JOIN preserves unmatched', () => {
    const left = new VirtualRelation({
      id: [1, 2, 3],
      name: ['Alice', 'Bob', 'Charlie'],
    });
    const right = new VirtualRelation({
      a_id: [1],
      val: ['matched'],
    });

    const joined = left.hashJoin(right, 'id', 'a_id', 'LEFT');
    assert.equal(joined.length, 3);

    const rows = joined.materialize(['id', 'name'], ['val']);
    assert.equal(rows[0].val, 'matched');
    assert.equal(rows[1].val, null); // Bob unmatched
    assert.equal(rows[2].val, null); // Charlie unmatched
  });

  it('benchmark: late vs early materialization', () => {
    const custCols = setupCustomers(1000);
    const orderCols = setupOrders(1000);

    // Late materialization: filter → join → materialize at end
    const t0 = Date.now();
    const customers = new VirtualRelation(custCols);
    const orders = new VirtualRelation(orderCols);
    const usCustomers = customers.filterEquals('region', 'US');
    const joined = usCustomers.hashJoin(orders, 'id', 'customer_id');
    const lateRows = joined.materialize(['name'], ['amount'], 100);
    const lateMs = Date.now() - t0;

    // Early materialization: create objects at every step
    const t1 = Date.now();
    const earlyFilteredRows = [];
    for (let i = 0; i < 1000; i++) {
      if (custCols.region[i] === 'US') {
        earlyFilteredRows.push({
          id: custCols.id[i],
          name: custCols.name[i],
          region: custCols.region[i],
          tier: custCols.tier[i],
        });
      }
    }
    const ht = new Map();
    for (let i = 0; i < orderCols.id.length; i++) {
      const key = orderCols.customer_id[i];
      if (!ht.has(key)) ht.set(key, []);
      ht.get(key).push(i);
    }
    const earlyJoinedRows = [];
    for (const row of earlyFilteredRows) {
      const matches = ht.get(row.id) || [];
      for (const mi of matches) {
        earlyJoinedRows.push({
          ...row,
          amount: orderCols.amount[mi],
          status: orderCols.status[mi],
        });
        if (earlyJoinedRows.length >= 100) break;
      }
      if (earlyJoinedRows.length >= 100) break;
    }
    const earlyMs = Date.now() - t1;

    console.log(`    Late: ${lateMs}ms vs Early: ${earlyMs}ms (${(earlyMs / Math.max(lateMs, 0.1)).toFixed(1)}x)`);
    assert.equal(lateRows.length, 100);
    assert.equal(earlyJoinedRows.length, 100);
  });
});
