// column-store.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ColumnStore } from './column-store.js';

describe('ColumnStore', () => {
  function createSalesStore() {
    const store = new ColumnStore([
      { name: 'product', type: 'text' },
      { name: 'region', type: 'text' },
      { name: 'amount', type: 'int' },
      { name: 'qty', type: 'int' },
    ]);
    store.insertBatch([
      { product: 'Widget', region: 'North', amount: 100, qty: 5 },
      { product: 'Gadget', region: 'South', amount: 200, qty: 3 },
      { product: 'Widget', region: 'South', amount: 150, qty: 7 },
      { product: 'Gadget', region: 'North', amount: 80, qty: 2 },
      { product: 'Widget', region: 'North', amount: 120, qty: 4 },
    ]);
    return store;
  }

  it('stores and retrieves column data', () => {
    const store = createSalesStore();
    assert.equal(store.rowCount, 5);
    assert.equal(store.columnCount, 4);
    const amounts = store.getColumn('amount');
    assert.deepEqual(amounts, [100, 200, 150, 80, 120]);
  });

  it('projection scan reads only requested columns', () => {
    const store = createSalesStore();
    const rows = store.scan(['product', 'amount']);
    assert.equal(rows.length, 5);
    assert.ok('product' in rows[0]);
    assert.ok('amount' in rows[0]);
    assert.ok(!('region' in rows[0]));
  });

  it('filtered scan', () => {
    const store = createSalesStore();
    const rows = store.scan(['product', 'amount'], row => row.region === 'North');
    assert.equal(rows.length, 3);
  });

  it('aggregate sum', () => {
    const store = createSalesStore();
    assert.equal(store.aggregate('amount', 'sum'), 650);
  });

  it('aggregate avg', () => {
    const store = createSalesStore();
    assert.equal(store.aggregate('amount', 'avg'), 130);
  });

  it('aggregate min/max', () => {
    const store = createSalesStore();
    assert.equal(store.aggregate('amount', 'min'), 80);
    assert.equal(store.aggregate('amount', 'max'), 200);
  });

  it('group by aggregation', () => {
    const store = createSalesStore();
    const result = store.groupBy('product', 'amount', 'sum');
    const widget = result.find(r => r.product === 'Widget');
    assert.equal(widget.sum, 370);
    const gadget = result.find(r => r.product === 'Gadget');
    assert.equal(gadget.sum, 280);
  });

  it('group by with count', () => {
    const store = createSalesStore();
    const result = store.groupBy('region', 'amount', 'count');
    const north = result.find(r => r.region === 'North');
    assert.equal(north.count, 3);
  });

  it('RLE compression', () => {
    const store = new ColumnStore([{ name: 'status', type: 'text' }]);
    for (let i = 0; i < 10; i++) store.insert({ status: 'active' });
    for (let i = 0; i < 5; i++) store.insert({ status: 'inactive' });
    for (let i = 0; i < 3; i++) store.insert({ status: 'active' });
    
    const rle = store.rleEncode('status');
    assert.equal(rle.length, 3);
    assert.deepEqual(rle[0], { value: 'active', count: 10 });
    assert.deepEqual(rle[1], { value: 'inactive', count: 5 });
    assert.deepEqual(rle[2], { value: 'active', count: 3 });
  });

  it('dictionary encoding', () => {
    const store = createSalesStore();
    const encoded = store.dictEncode('product');
    assert.equal(encoded.dictionary.length, 2); // Widget, Gadget
    assert.equal(encoded.codes.length, 5);
    assert.ok(encoded.compressionRatio < 1); // Compressed
  });

  it('analytics at scale', () => {
    const store = new ColumnStore([
      { name: 'user_id', type: 'int' },
      { name: 'event', type: 'text' },
      { name: 'duration', type: 'int' },
    ]);
    
    const events = ['click', 'view', 'purchase', 'scroll'];
    for (let i = 0; i < 1000; i++) {
      store.insert({
        user_id: i % 100,
        event: events[i % 4],
        duration: Math.floor(Math.random() * 1000),
      });
    }
    
    assert.equal(store.rowCount, 1000);
    assert.equal(store.aggregate('duration', 'count'), 1000);
    
    const byEvent = store.groupBy('event', 'duration', 'avg');
    assert.equal(byEvent.length, 4);
  });
});
