// column-store-dict.test.js — Tests for column store with dictionary encoding
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ColumnStore } from './column-store.js';

describe('ColumnStore Dictionary Encoding', () => {

  it('autoDictEncode detects low-cardinality text columns', () => {
    const store = new ColumnStore([
      { name: 'id', type: 'INT' },
      { name: 'region', type: 'TEXT' },
      { name: 'amount', type: 'INT' },
    ]);

    for (let i = 0; i < 1000; i++) {
      store.insert({
        id: i,
        region: ['US', 'EU', 'APAC', 'LATAM'][i % 4],
        amount: i * 10,
      });
    }

    const result = store.autoDictEncode();
    assert.equal(result.encoded.length, 1);
    assert.equal(result.encoded[0].column, 'region');
    assert.equal(result.encoded[0].cardinality, 4);
  });

  it('dictFilter uses dictionary encoding', () => {
    const store = new ColumnStore([
      { name: 'region', type: 'TEXT' },
      { name: 'val', type: 'INT' },
    ]);

    for (let i = 0; i < 500; i++) {
      store.insert({ region: ['US', 'EU', 'APAC'][i % 3], val: i });
    }

    store.autoDictEncode();

    const indices = store.dictFilter('region', 'US');
    assert.ok(indices.length > 100);
    // Verify correctness
    for (const idx of indices) {
      assert.equal(store.getColumn('region')[idx], 'US');
    }
  });

  it('dictGroupBy uses dictionary encoding', () => {
    const store = new ColumnStore([
      { name: 'status', type: 'TEXT' },
      { name: 'amount', type: 'INT' },
    ]);

    for (let i = 0; i < 300; i++) {
      store.insert({ status: ['pending', 'shipped', 'delivered'][i % 3], amount: i * 10 });
    }

    store.autoDictEncode();

    const groups = store.dictGroupBy('status');
    assert.equal(groups.size, 3);
    assert.equal(groups.get('pending').length, 100);
    assert.equal(groups.get('shipped').length, 100);
    assert.equal(groups.get('delivered').length, 100);
  });

  it('falls back when column not dict-encoded', () => {
    const store = new ColumnStore([
      { name: 'name', type: 'TEXT' },
      { name: 'val', type: 'INT' },
    ]);

    for (let i = 0; i < 100; i++) {
      store.insert({ name: `unique_name_${i}`, val: i }); // High cardinality
    }

    store.autoDictEncode();

    // name has cardinality 100/100 = 100% > threshold
    assert.equal(store.getDictColumn('name'), null);

    // dictFilter falls back to standard scan
    const indices = store.dictFilter('name', 'unique_name_5');
    assert.equal(indices.length, 1);
    assert.equal(indices[0], 5);
  });

  it('benchmark: dictFilter vs standard scan on 100K rows', () => {
    const store = new ColumnStore([
      { name: 'region', type: 'TEXT' },
      { name: 'amount', type: 'INT' },
    ]);

    for (let i = 0; i < 100000; i++) {
      store.insert({ region: ['US', 'EU', 'APAC', 'LATAM', 'AFRICA'][i % 5], amount: i });
    }

    store.autoDictEncode();

    // Dictionary filter
    const t0 = Date.now();
    const dictResult = store.dictFilter('region', 'US');
    const dictMs = Date.now() - t0;

    // Standard column scan (without dict)
    const t1 = Date.now();
    const col = store.getColumn('region');
    const stdResult = [];
    for (let i = 0; i < col.length; i++) {
      if (col[i] === 'US') stdResult.push(i);
    }
    const stdMs = Date.now() - t1;

    console.log(`    DictFilter: ${dictMs}ms vs Standard: ${stdMs}ms (${(stdMs / Math.max(dictMs, 0.1)).toFixed(1)}x)`);
    assert.equal(dictResult.length, stdResult.length);
    assert.equal(dictResult.length, 20000);
  });

  it('combined: dict group-by + vectorized aggregate', () => {
    const store = new ColumnStore([
      { name: 'region', type: 'TEXT' },
      { name: 'amount', type: 'INT' },
    ]);

    for (let i = 0; i < 1000; i++) {
      store.insert({ region: ['US', 'EU', 'APAC'][i % 3], amount: (i * 17 + 50) % 1000 });
    }

    store.autoDictEncode();
    const groups = store.dictGroupBy('region');
    const amountCol = store.getColumn('amount');

    // Compute SUM per group using vectorized column access
    const result = [];
    for (const [region, indices] of groups) {
      let sum = 0;
      for (const idx of indices) sum += amountCol[idx];
      result.push({ region, sum, count: indices.length });
    }

    assert.equal(result.length, 3);
    assert.ok(result.every(r => r.count > 300));
    assert.ok(result.every(r => r.sum > 0));
  });
});
