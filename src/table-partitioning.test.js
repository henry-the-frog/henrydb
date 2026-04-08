// table-partitioning.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PartitionedTable } from './table-partitioning.js';

describe('TablePartitioning', () => {
  describe('Hash Partitioning', () => {
    it('distributes rows across partitions', () => {
      const t = new PartitionedTable('users', 'id', 'hash', { numPartitions: 4 });
      for (let i = 0; i < 100; i++) t.insert({ id: i, name: `user_${i}` });
      
      assert.equal(t.totalRows, 100);
      const sizes = t.getPartitionSizes();
      // Each partition should have some rows
      assert.ok(Object.values(sizes).every(s => s > 0));
    });

    it('scanAll returns all rows', () => {
      const t = new PartitionedTable('t', 'id', 'hash', { numPartitions: 4 });
      for (let i = 0; i < 50; i++) t.insert({ id: i });
      assert.equal(t.scanAll().length, 50);
    });

    it('scan with predicate', () => {
      const t = new PartitionedTable('t', 'id', 'hash', { numPartitions: 4 });
      for (let i = 0; i < 100; i++) t.insert({ id: i });
      const result = t.scan(row => row.id > 90);
      assert.equal(result.length, 9);
    });
  });

  describe('Range Partitioning', () => {
    it('distributes by boundaries', () => {
      const t = new PartitionedTable('orders', 'amount', 'range', { boundaries: [100, 500, 1000] });
      t.insert({ amount: 50, desc: 'small' });
      t.insert({ amount: 200, desc: 'medium' });
      t.insert({ amount: 800, desc: 'large' });
      t.insert({ amount: 1500, desc: 'xlarge' });
      
      assert.equal(t.totalRows, 4);
      assert.equal(t.partitionCount, 4);
    });

    it('range scan with pruning', () => {
      const t = new PartitionedTable('orders', 'amount', 'range', { boundaries: [100, 200, 300] });
      for (let i = 0; i < 400; i++) t.insert({ amount: i });
      
      const result = t.rangeScan(150, 250);
      assert.equal(result.length, 101); // 150..250
    });

    it('range scan empty range', () => {
      const t = new PartitionedTable('t', 'x', 'range', { boundaries: [10, 20, 30] });
      for (let i = 0; i < 40; i++) t.insert({ x: i });
      assert.equal(t.rangeScan(100, 200).length, 0);
    });
  });

  it('partition sizes', () => {
    const t = new PartitionedTable('t', 'id', 'hash', { numPartitions: 3 });
    for (let i = 0; i < 30; i++) t.insert({ id: i });
    const sizes = t.getPartitionSizes();
    assert.equal(Object.values(sizes).reduce((a, b) => a + b, 0), 30);
  });
});
