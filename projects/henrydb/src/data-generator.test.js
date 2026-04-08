// data-generator.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DataGenerator } from './data-generator.js';

describe('DataGenerator', () => {
  it('customers have required fields', () => {
    const customers = DataGenerator.customers(100);
    assert.equal(customers.length, 100);
    assert.ok(customers[0].custkey);
    assert.ok(customers[0].name);
    assert.ok(customers[0].nation);
    assert.ok(customers[0].segment);
    assert.ok(typeof customers[0].acctbal === 'number');
  });

  it('orders reference valid customers', () => {
    const orders = DataGenerator.orders(100, 50);
    assert.equal(orders.length, 100);
    assert.ok(orders.every(o => o.custkey >= 1 && o.custkey <= 50));
    assert.ok(orders[0].orderdate.match(/\d{4}-\d{2}-\d{2}/));
  });

  it('line items reference valid orders', () => {
    const items = DataGenerator.lineItems(500, 100);
    assert.equal(items.length, 500);
    assert.ok(items.every(l => l.orderkey >= 1 && l.orderkey <= 100));
    assert.ok(items.every(l => l.quantity >= 1 && l.quantity <= 50));
  });

  it('parts have consistent pricing', () => {
    const parts = DataGenerator.parts(100);
    assert.equal(parts.length, 100);
    assert.ok(parts.every(p => p.retailprice > 0));
    assert.ok(parts.every(p => p.size >= 1 && p.size <= 50));
  });

  it('suppliers have valid fields', () => {
    const suppliers = DataGenerator.suppliers(50);
    assert.equal(suppliers.length, 50);
    assert.ok(suppliers.every(s => s.suppkey >= 1));
  });

  it('large scale: 100K line items', () => {
    const t0 = Date.now();
    const items = DataGenerator.lineItems(100000, 10000);
    const ms = Date.now() - t0;
    console.log(`    100K line items: ${ms}ms`);
    assert.equal(items.length, 100000);
    assert.ok(ms < 5000);
  });

  it('data is randomized (not all same)', () => {
    const customers = DataGenerator.customers(100);
    const nations = new Set(customers.map(c => c.nation));
    assert.ok(nations.size > 1);
    const segments = new Set(customers.map(c => c.segment));
    assert.ok(segments.size > 1);
  });

  it('TPC-H scale factor 0.01 (basic)', () => {
    const customers = DataGenerator.customers(150);
    const orders = DataGenerator.orders(1500, 150);
    const lineItems = DataGenerator.lineItems(6000, 1500);
    const parts = DataGenerator.parts(200);
    const suppliers = DataGenerator.suppliers(10);
    
    assert.equal(customers.length, 150);
    assert.equal(orders.length, 1500);
    assert.equal(lineItems.length, 6000);
    assert.equal(parts.length, 200);
    assert.equal(suppliers.length, 10);
  });
});
