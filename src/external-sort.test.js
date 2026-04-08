// external-sort.test.js — Tests for external sort
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ExternalSort } from './external-sort.js';

describe('ExternalSort', () => {

  it('small data: in-memory sort', () => {
    const es = new ExternalSort({ runSize: 100 });
    const data = [5, 3, 1, 4, 2];
    const result = es.sort(data, (a, b) => a - b);
    assert.deepEqual(result, [1, 2, 3, 4, 5]);
    assert.equal(es.stats.runs, 1);
  });

  it('external sort: multiple runs', () => {
    const es = new ExternalSort({ runSize: 3 });
    const data = [9, 5, 1, 8, 3, 7, 2, 6, 4];
    const result = es.sort(data, (a, b) => a - b);
    assert.deepEqual(result, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert.equal(es.stats.runs, 3);
  });

  it('sortByColumn', () => {
    const rows = [
      { name: 'Charlie', score: 85 },
      { name: 'Alice', score: 92 },
      { name: 'Bob', score: 78 },
    ];

    const es = new ExternalSort({ runSize: 100 });
    const result = es.sortByColumn(rows, 'score', 'ASC');
    assert.equal(result[0].name, 'Bob');
    assert.equal(result[1].name, 'Charlie');
    assert.equal(result[2].name, 'Alice');
  });

  it('sortByColumn DESC', () => {
    const rows = [
      { score: 10 }, { score: 30 }, { score: 20 },
    ];

    const es = new ExternalSort({ runSize: 100 });
    const result = es.sortByColumn(rows, 'score', 'DESC');
    assert.equal(result[0].score, 30);
    assert.equal(result[1].score, 20);
    assert.equal(result[2].score, 10);
  });

  it('sortByColumns: multi-key sort', () => {
    const rows = [
      { region: 'US', score: 80 },
      { region: 'EU', score: 90 },
      { region: 'US', score: 95 },
      { region: 'EU', score: 70 },
    ];

    const es = new ExternalSort({ runSize: 100 });
    const result = es.sortByColumns(rows, [
      { name: 'region', direction: 'ASC' },
      { name: 'score', direction: 'DESC' },
    ]);

    assert.equal(result[0].region, 'EU');
    assert.equal(result[0].score, 90);
    assert.equal(result[1].region, 'EU');
    assert.equal(result[1].score, 70);
    assert.equal(result[2].region, 'US');
    assert.equal(result[2].score, 95);
  });

  it('topK: only first K elements', () => {
    const data = [50, 30, 10, 90, 70, 20, 40, 80, 60];
    const es = new ExternalSort();

    const result = es.topK(data, 3, (a, b) => a - b);
    assert.equal(result.length, 3);
    assert.deepEqual(result, [10, 20, 30]);
  });

  it('large external sort: 100K rows, 1K run size', () => {
    const n = 100000;
    const data = Array.from({ length: n }, () => Math.floor(Math.random() * 1000000));

    const es = new ExternalSort({ runSize: 1000 });
    const t0 = Date.now();
    const result = es.sort(data, (a, b) => a - b);
    const ms = Date.now() - t0;

    // Verify sorted
    for (let i = 1; i < result.length; i++) {
      assert.ok(result[i] >= result[i - 1], `Not sorted at index ${i}`);
    }

    console.log(`    100K external sort (${es.stats.runs} runs): ${ms}ms`);
    assert.equal(result.length, n);
  });

  it('benchmark: external sort vs Array.sort', () => {
    const n = 50000;
    const data = Array.from({ length: n }, () => Math.floor(Math.random() * 1000000));
    const dataCopy = [...data];

    // External sort
    const es = new ExternalSort({ runSize: 5000 });
    const t0 = Date.now();
    const extResult = es.sort(data, (a, b) => a - b);
    const extMs = Date.now() - t0;

    // Array.sort
    const t1 = Date.now();
    dataCopy.sort((a, b) => a - b);
    const nativeMs = Date.now() - t1;

    console.log(`    External: ${extMs}ms (${es.stats.runs} runs) vs Native: ${nativeMs}ms`);
    assert.equal(extResult.length, n);
    assert.deepEqual(extResult[0], dataCopy[0]);
    assert.deepEqual(extResult[n - 1], dataCopy[n - 1]);
  });

  it('stats tracked', () => {
    const es = new ExternalSort({ runSize: 5 });
    es.sort([10, 5, 1, 8, 3, 7, 2, 6, 4, 9], (a, b) => a - b);

    const stats = es.getStats();
    assert.equal(stats.totalRows, 10);
    assert.equal(stats.runs, 2);
    assert.ok(stats.sortTimeMs >= 0);
    assert.ok(stats.mergePassRows > 0);
  });
});
