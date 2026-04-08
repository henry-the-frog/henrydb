// vectorized-aggregation.test.js — Tests for vectorized aggregation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { VectorizedCodeGen } from './vectorized-codegen.js';
import { Database } from './db.js';

describe('Vectorized Aggregation', () => {

  it('COUNT(*) with GROUP BY', () => {
    const db = new Database();
    const vec = new VectorizedCodeGen(db);

    const rows = Array.from({ length: 1000 }, (_, i) => ({
      region: ['US', 'EU', 'APAC', 'LATAM'][i % 4],
      amount: i,
    }));

    const result = vec.vectorizedAggregate(rows, ['region'], [
      { fn: 'COUNT', column: '*', alias: 'cnt' }
    ]);

    assert.equal(result.length, 4);
    assert.ok(result.every(r => r.cnt === 250));
  });

  it('SUM with GROUP BY', () => {
    const db = new Database();
    const vec = new VectorizedCodeGen(db);

    const rows = [
      { grp: 'A', val: 10 }, { grp: 'A', val: 20 },
      { grp: 'B', val: 30 }, { grp: 'B', val: 40 },
    ];

    const result = vec.vectorizedAggregate(rows, ['grp'], [
      { fn: 'SUM', column: 'val', alias: 'total' }
    ]);

    const a = result.find(r => r.grp === 'A');
    const b = result.find(r => r.grp === 'B');
    assert.equal(a.total, 30);
    assert.equal(b.total, 70);
  });

  it('AVG with GROUP BY', () => {
    const db = new Database();
    const vec = new VectorizedCodeGen(db);

    const rows = [
      { grp: 'X', val: 10 }, { grp: 'X', val: 20 }, { grp: 'X', val: 30 },
      { grp: 'Y', val: 100 },
    ];

    const result = vec.vectorizedAggregate(rows, ['grp'], [
      { fn: 'AVG', column: 'val', alias: 'avg_val' }
    ]);

    assert.equal(result.find(r => r.grp === 'X').avg_val, 20);
    assert.equal(result.find(r => r.grp === 'Y').avg_val, 100);
  });

  it('MIN and MAX', () => {
    const db = new Database();
    const vec = new VectorizedCodeGen(db);

    const rows = [
      { grp: 'A', val: 50 }, { grp: 'A', val: 10 }, { grp: 'A', val: 90 },
      { grp: 'B', val: 30 }, { grp: 'B', val: 70 },
    ];

    const result = vec.vectorizedAggregate(rows, ['grp'], [
      { fn: 'MIN', column: 'val', alias: 'min_v' },
      { fn: 'MAX', column: 'val', alias: 'max_v' },
    ]);

    const a = result.find(r => r.grp === 'A');
    assert.equal(a.min_v, 10);
    assert.equal(a.max_v, 90);
  });

  it('multiple aggregates', () => {
    const db = new Database();
    const vec = new VectorizedCodeGen(db);

    const rows = Array.from({ length: 200 }, (_, i) => ({
      region: ['US', 'EU'][i % 2],
      amount: (i + 1) * 10,
      qty: (i % 5) + 1,
    }));

    const result = vec.vectorizedAggregate(rows, ['region'], [
      { fn: 'COUNT', column: '*', alias: 'cnt' },
      { fn: 'SUM', column: 'amount', alias: 'total' },
      { fn: 'AVG', column: 'amount', alias: 'avg_amt' },
      { fn: 'MIN', column: 'qty', alias: 'min_qty' },
      { fn: 'MAX', column: 'qty', alias: 'max_qty' },
    ]);

    assert.equal(result.length, 2);
    const us = result.find(r => r.region === 'US');
    assert.equal(us.cnt, 100);
    assert.equal(us.min_qty, 1);
    assert.equal(us.max_qty, 5);
  });

  it('no GROUP BY: whole-table aggregate', () => {
    const db = new Database();
    const vec = new VectorizedCodeGen(db);

    const rows = [{ val: 10 }, { val: 20 }, { val: 30 }];

    const result = vec.vectorizedAggregate(rows, [], [
      { fn: 'COUNT', column: '*', alias: 'cnt' },
      { fn: 'SUM', column: 'val', alias: 'total' },
      { fn: 'AVG', column: 'val', alias: 'avg_val' },
    ]);

    assert.equal(result.length, 1);
    assert.equal(result[0].cnt, 3);
    assert.equal(result[0].total, 60);
    assert.equal(result[0].avg_val, 20);
  });

  it('NULL handling', () => {
    const db = new Database();
    const vec = new VectorizedCodeGen(db);

    const rows = [
      { grp: 'A', val: 10 }, { grp: 'A', val: null }, { grp: 'A', val: 30 },
      { grp: 'B', val: null },
    ];

    const result = vec.vectorizedAggregate(rows, ['grp'], [
      { fn: 'COUNT', column: 'val', alias: 'cnt' },
      { fn: 'SUM', column: 'val', alias: 'total' },
      { fn: 'AVG', column: 'val', alias: 'avg_val' },
    ]);

    const a = result.find(r => r.grp === 'A');
    assert.equal(a.cnt, 2);
    assert.equal(a.total, 40);
    assert.equal(a.avg_val, 20);

    const b = result.find(r => r.grp === 'B');
    assert.equal(b.cnt, 0);
    assert.equal(b.avg_val, null);
  });

  it('large dataset: 100K rows, 1000 groups', () => {
    const db = new Database();
    const vec = new VectorizedCodeGen(db);

    const rows = Array.from({ length: 100000 }, (_, i) => ({
      grp: `g${i % 1000}`,
      val: i,
    }));

    const t0 = Date.now();
    const result = vec.vectorizedAggregate(rows, ['grp'], [
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
