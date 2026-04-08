// morsel-parallel.test.js — Tests for morsel-driven parallel execution
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MorselExecutor } from './morsel-parallel.js';

describe('MorselExecutor', () => {

  it('parallelFilter: basic filter', () => {
    const data = Array.from({ length: 1000 }, (_, i) => i);
    const exec = new MorselExecutor({ morselSize: 100 });

    const result = exec.parallelFilter(data, v => v > 500);
    assert.equal(result.length, 499); // 501..999
    assert.ok(result instanceof Uint32Array);
  });

  it('parallelFilter: empty result', () => {
    const data = Array.from({ length: 100 }, (_, i) => i);
    const exec = new MorselExecutor({ morselSize: 50 });

    const result = exec.parallelFilter(data, v => v > 999);
    assert.equal(result.length, 0);
  });

  it('parallelFilter: all match', () => {
    const data = Array.from({ length: 100 }, () => 1);
    const exec = new MorselExecutor({ morselSize: 50 });

    const result = exec.parallelFilter(data, v => v === 1);
    assert.equal(result.length, 100);
  });

  it('parallelSum: basic sum', () => {
    const data = Array.from({ length: 10000 }, (_, i) => i);
    const exec = new MorselExecutor({ morselSize: 1000 });

    const result = exec.parallelSum(data);
    assert.equal(result, (10000 * 9999) / 2); // Sum of 0..9999
  });

  it('parallelGroupBy: SUM', () => {
    const n = 1000;
    const groups = Array.from({ length: n }, (_, i) => ['A', 'B', 'C'][i % 3]);
    const values = Array.from({ length: n }, (_, i) => i);

    const exec = new MorselExecutor({ morselSize: 100 });
    const result = exec.parallelGroupBy(groups, values, 'SUM');

    assert.equal(result.length, 3);
    const totalSum = result.reduce((s, r) => s + r.value, 0);
    assert.equal(totalSum, (n * (n - 1)) / 2);
  });

  it('parallelGroupBy: COUNT', () => {
    const groups = Array.from({ length: 600 }, (_, i) => ['X', 'Y', 'Z'][i % 3]);
    const values = Array.from({ length: 600 }, () => 1);

    const exec = new MorselExecutor({ morselSize: 100 });
    const result = exec.parallelGroupBy(groups, values, 'COUNT');

    assert.equal(result.length, 3);
    assert.ok(result.every(r => r.count === 200));
  });

  it('parallelGroupBy: AVG', () => {
    const groups = ['A', 'A', 'B', 'B'];
    const values = [10, 20, 30, 40];

    const exec = new MorselExecutor({ morselSize: 2 });
    const result = exec.parallelGroupBy(groups, values, 'AVG');

    const a = result.find(r => r.group === 'A');
    const b = result.find(r => r.group === 'B');
    assert.equal(a.value, 15);
    assert.equal(b.value, 35);
  });

  it('parallelGroupBy: MIN/MAX', () => {
    const groups = ['A', 'A', 'A', 'B', 'B'];
    const values = [30, 10, 50, 20, 40];

    const exec = new MorselExecutor({ morselSize: 2 });
    
    const minResult = exec.parallelGroupBy(groups, values, 'MIN');
    assert.equal(minResult.find(r => r.group === 'A').value, 10);
    assert.equal(minResult.find(r => r.group === 'B').value, 20);

    const maxResult = exec.parallelGroupBy(groups, values, 'MAX');
    assert.equal(maxResult.find(r => r.group === 'A').value, 50);
    assert.equal(maxResult.find(r => r.group === 'B').value, 40);
  });

  it('parallelBuildHash: correct hash table', () => {
    const keys = [1, 2, 3, 1, 2, 3, 1, 2, 3];
    const exec = new MorselExecutor({ morselSize: 3 });

    const ht = exec.parallelBuildHash(keys);
    assert.equal(ht.get(1).length, 3);
    assert.equal(ht.get(2).length, 3);
    assert.equal(ht.get(3).length, 3);
    assert.deepEqual(ht.get(1).sort(), [0, 3, 6]);
  });

  it('benchmark: morsel filter on 1M rows', () => {
    const n = 1000000;
    const data = Array.from({ length: n }, (_, i) => i);

    // Morsel filter
    const exec = new MorselExecutor({ morselSize: 50000 });
    const t0 = Date.now();
    const morselResult = exec.parallelFilter(data, v => v > 900000);
    const morselMs = Date.now() - t0;

    // Sequential filter
    const t1 = Date.now();
    const seqResult = [];
    for (let i = 0; i < n; i++) {
      if (data[i] > 900000) seqResult.push(i);
    }
    const seqMs = Date.now() - t1;

    console.log(`    Morsel: ${morselMs}ms (${exec.stats.morsels} morsels, ${exec.stats.workers} workers) vs Sequential: ${seqMs}ms`);
    assert.equal(morselResult.length, 99999);
  });

  it('benchmark: morsel group-by on 500K rows', () => {
    const n = 500000;
    const groups = Array.from({ length: n }, (_, i) => `g${i % 100}`);
    const values = Array.from({ length: n }, (_, i) => i % 1000);

    const exec = new MorselExecutor({ morselSize: 50000 });
    const t0 = Date.now();
    const result = exec.parallelGroupBy(groups, values, 'SUM');
    const ms = Date.now() - t0;

    console.log(`    Morsel GroupBy 500K/100 groups: ${ms}ms`);
    assert.equal(result.length, 100);
  });

  it('stats tracked', () => {
    const data = Array.from({ length: 500 }, (_, i) => i);
    const exec = new MorselExecutor({ morselSize: 100, maxWorkers: 4 });

    exec.parallelFilter(data, v => v > 250);
    const stats = exec.getStats();
    assert.equal(stats.morsels, 5);
    assert.ok(stats.totalMs >= 0);
  });
});
