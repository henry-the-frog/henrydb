// partitioning.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RangePartitioner } from './partitioning.js';

describe('Range Partitioning', () => {
  function createDatePartitioner() {
    return new RangePartitioner('month', [
      { name: 'q1', minValue: 1, maxValue: 3 },
      { name: 'q2', minValue: 4, maxValue: 6 },
      { name: 'q3', minValue: 7, maxValue: 9 },
      { name: 'q4', minValue: 10, maxValue: 12 },
    ]);
  }

  it('routes rows to correct partition', () => {
    const p = createDatePartitioner();
    assert.equal(p.route({ month: 2 }), 'q1');
    assert.equal(p.route({ month: 5 }), 'q2');
    assert.equal(p.route({ month: 8 }), 'q3');
    assert.equal(p.route({ month: 11 }), 'q4');
  });

  it('inserts distribute across partitions', () => {
    const p = createDatePartitioner();
    for (let m = 1; m <= 12; m++) {
      p.insert({ month: m, amount: m * 100 });
    }
    
    const stats = p.stats();
    assert.equal(stats.length, 4);
    assert.equal(stats[0].rowCount, 3); // Jan, Feb, Mar
    assert.equal(stats[1].rowCount, 3); // Apr, May, Jun
  });

  it('partition pruning reduces scan', () => {
    const p = createDatePartitioner();
    for (let m = 1; m <= 12; m++) {
      p.insert({ month: m, amount: m * 100 });
    }
    
    // Query Q1 only
    const pruned = p.prunedPartitionCount(1, 3);
    assert.equal(pruned, 1); // Only q1
    
    // Query Q1-Q2
    assert.equal(p.prunedPartitionCount(1, 6), 2);
    
    // Full scan
    assert.equal(p.prunedPartitionCount(1, 12), 4);
  });

  it('query with partition pruning', () => {
    const p = createDatePartitioner();
    for (let m = 1; m <= 12; m++) {
      p.insert({ month: m, amount: m * 100 });
    }
    
    // Only scan Q1 partition
    const results = p.query(row => row.amount > 100, 1, 3);
    assert.equal(results.length, 2); // Feb(200) and Mar(300)
  });

  it('numeric partitioning by ID range', () => {
    const p = new RangePartitioner('id', [
      { name: 'p0', minValue: 0, maxValue: 999 },
      { name: 'p1', minValue: 1000, maxValue: 1999 },
      { name: 'p2', minValue: 2000, maxValue: 2999 },
    ]);
    
    assert.equal(p.route({ id: 500 }), 'p0');
    assert.equal(p.route({ id: 1500 }), 'p1');
    assert.equal(p.route({ id: 2500 }), 'p2');
  });

  it('throws for unpartitioned values', () => {
    const p = createDatePartitioner();
    assert.throws(() => p.insert({ month: 13, amount: 0 }), /No partition found/);
  });

  it('stats show distribution', () => {
    const p = createDatePartitioner();
    // Skewed data: most sales in Q4
    for (let i = 0; i < 100; i++) {
      p.insert({ month: i < 80 ? 11 : (i % 12 + 1), amount: i });
    }
    
    const stats = p.stats();
    const q4 = stats.find(s => s.name === 'q4');
    assert.ok(q4.rowCount > 50); // Q4 has most data
  });
});
