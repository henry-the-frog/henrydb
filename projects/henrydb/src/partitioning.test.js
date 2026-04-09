// partitioning.test.js — Tests for table partitioning
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { PartitionedTable, createRangePartition, createListPartition, createHashPartition } from './partitioning.js';

describe('Range Partitioning', () => {
  function createDateTable() {
    const strategy = createRangePartition('date', [
      { name: 'p2024q1', from: '2024-01-01', to: '2024-04-01' },
      { name: 'p2024q2', from: '2024-04-01', to: '2024-07-01' },
      { name: 'p2024q3', from: '2024-07-01', to: '2024-10-01' },
      { name: 'p2024q4', from: '2024-10-01', to: '2025-01-01' },
    ]);
    return new PartitionedTable('events', ['id', 'date', 'name'], strategy);
  }

  test('routes rows to correct partition', () => {
    const table = createDateTable();
    assert.equal(table.insert({ id: 1, date: '2024-02-15', name: 'A' }), 'p2024q1');
    assert.equal(table.insert({ id: 2, date: '2024-05-20', name: 'B' }), 'p2024q2');
    assert.equal(table.insert({ id: 3, date: '2024-11-30', name: 'C' }), 'p2024q4');
  });

  test('prunes partitions on equality', () => {
    const table = createDateTable();
    table.insert({ id: 1, date: '2024-02-15', name: 'A' });
    table.insert({ id: 2, date: '2024-05-20', name: 'B' });
    table.insert({ id: 3, date: '2024-08-10', name: 'C' });

    const result = table.query({ op: '=', column: 'date', value: '2024-05-20' });
    assert.equal(result.rows.length, 1);
    assert.equal(result.partitionsScanned.length, 1);
    assert.equal(result.partitionsPruned, 3);
  });

  test('prunes on range condition (>)', () => {
    const table = createDateTable();
    for (let m = 1; m <= 12; m++) {
      const month = String(m).padStart(2, '0');
      table.insert({ id: m, date: `2024-${month}-15`, name: `Event ${m}` });
    }

    const result = table.query({ op: '>', column: 'date', value: '2024-09-01' });
    assert.ok(result.partitionsScanned.length <= 2);
    assert.ok(result.rows.every(r => r.date > '2024-09-01'));
  });

  test('BETWEEN prunes correctly', () => {
    const table = createDateTable();
    for (let m = 1; m <= 12; m++) {
      const month = String(m).padStart(2, '0');
      table.insert({ id: m, date: `2024-${month}-15`, name: `E${m}` });
    }

    const result = table.query({ op: 'BETWEEN', column: 'date', value: ['2024-04-01', '2024-06-30'] });
    assert.ok(result.partitionsScanned.length <= 2);
    assert.ok(result.rows.every(r => r.date >= '2024-04-01' && r.date <= '2024-06-30'));
  });

  test('rejects row with no matching partition', () => {
    const table = createDateTable();
    assert.throws(() => {
      table.insert({ id: 1, date: '2025-06-01', name: 'future' });
    }, /No matching partition/);
  });

  test('full scan when no condition', () => {
    const table = createDateTable();
    table.insert({ id: 1, date: '2024-02-15', name: 'A' });
    table.insert({ id: 2, date: '2024-08-15', name: 'B' });

    const result = table.query();
    assert.equal(result.rows.length, 2);
    assert.equal(result.partitionsPruned, 0);
  });
});

describe('List Partitioning', () => {
  function createRegionTable() {
    const strategy = createListPartition('region', [
      { name: 'p_east', values: ['NY', 'NJ', 'CT', 'MA'] },
      { name: 'p_west', values: ['CA', 'OR', 'WA'] },
      { name: 'p_central', values: ['TX', 'IL', 'OH'] },
    ]);
    return new PartitionedTable('stores', ['id', 'region', 'name'], strategy);
  }

  test('routes to correct partition', () => {
    const table = createRegionTable();
    assert.equal(table.insert({ id: 1, region: 'NY', name: 'NYC Store' }), 'p_east');
    assert.equal(table.insert({ id: 2, region: 'CA', name: 'LA Store' }), 'p_west');
    assert.equal(table.insert({ id: 3, region: 'TX', name: 'Dallas Store' }), 'p_central');
  });

  test('prunes on equality', () => {
    const table = createRegionTable();
    table.insert({ id: 1, region: 'NY', name: 'A' });
    table.insert({ id: 2, region: 'CA', name: 'B' });
    table.insert({ id: 3, region: 'TX', name: 'C' });

    const result = table.query({ op: '=', column: 'region', value: 'CA' });
    assert.equal(result.rows.length, 1);
    assert.deepEqual(result.partitionsScanned, ['p_west']);
    assert.equal(result.partitionsPruned, 2);
  });

  test('prunes on IN list', () => {
    const table = createRegionTable();
    table.insert({ id: 1, region: 'NY', name: 'A' });
    table.insert({ id: 2, region: 'CA', name: 'B' });
    table.insert({ id: 3, region: 'TX', name: 'C' });

    const result = table.query({ op: 'IN', column: 'region', value: ['NY', 'NJ'] });
    assert.equal(result.rows.length, 1);
    assert.equal(result.partitionsScanned.length, 1);
  });

  test('rejects unknown region', () => {
    const table = createRegionTable();
    assert.throws(() => {
      table.insert({ id: 1, region: 'UK', name: 'London' });
    }, /No matching partition/);
  });
});

describe('Hash Partitioning', () => {
  function createUserTable() {
    const strategy = createHashPartition('user_id', 4);
    return new PartitionedTable('sessions', ['id', 'user_id', 'data'], strategy);
  }

  test('distributes rows across partitions', () => {
    const table = createUserTable();
    for (let i = 1; i <= 100; i++) {
      table.insert({ id: i, user_id: i, data: `session_${i}` });
    }

    const stats = table.getStats();
    assert.equal(stats.totalRows, 100);
    assert.equal(stats.partitionCount, 4);
    // Each partition should have some rows (roughly 25 each)
    for (const [name, count] of Object.entries(stats.partitions)) {
      assert.ok(count > 5, `Partition ${name} has ${count} rows, expected >5`);
    }
  });

  test('same value always routes to same partition', () => {
    const table = createUserTable();
    const p1 = table.insert({ id: 1, user_id: 42, data: 'a' });
    const p2 = table.insert({ id: 2, user_id: 42, data: 'b' });
    assert.equal(p1, p2);
  });

  test('prunes on equality', () => {
    const table = createUserTable();
    for (let i = 1; i <= 20; i++) {
      table.insert({ id: i, user_id: i, data: `s${i}` });
    }

    const result = table.query({ op: '=', column: 'user_id', value: 5 });
    assert.equal(result.partitionsScanned.length, 1); // Only one partition
    assert.ok(result.partitionsPruned >= 2); // At least 2 pruned
  });

  test('cannot prune on range condition', () => {
    const table = createUserTable();
    const result = table.query({ op: '>', column: 'user_id', value: 10 });
    assert.equal(result.partitionsScanned.length, 4); // All partitions
    assert.equal(result.partitionsPruned, 0);
  });
});

describe('PartitionedTable operations', () => {
  test('delete with partition pruning', () => {
    const strategy = createListPartition('status', [
      { name: 'p_active', values: ['active'] },
      { name: 'p_archived', values: ['archived'] },
    ]);
    const table = new PartitionedTable('tasks', ['id', 'status', 'name'], strategy);

    table.insert({ id: 1, status: 'active', name: 'Task 1' });
    table.insert({ id: 2, status: 'active', name: 'Task 2' });
    table.insert({ id: 3, status: 'archived', name: 'Task 3' });

    const deleted = table.delete({ op: '=', column: 'status', value: 'archived' });
    assert.equal(deleted, 1);
    assert.equal(table.getStats().totalRows, 2);
  });

  test('getStats returns partition info', () => {
    const strategy = createHashPartition('id', 3);
    const table = new PartitionedTable('data', ['id', 'value'], strategy);

    for (let i = 0; i < 30; i++) {
      table.insert({ id: i, value: `v${i}` });
    }

    const stats = table.getStats();
    assert.equal(stats.name, 'data');
    assert.equal(stats.totalRows, 30);
    assert.equal(stats.partitionCount, 3);
    assert.equal(stats.strategy, 'HashPartition');
    assert.equal(stats.column, 'id');
  });
});
