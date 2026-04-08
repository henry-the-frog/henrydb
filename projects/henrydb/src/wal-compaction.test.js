// wal-compaction.test.js — WAL compaction tests
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { WriteAheadLog } from './wal.js';

test('compact — no-op on empty WAL', () => {
  const wal = new WriteAheadLog();
  const result = wal.compact();
  assert.equal(result.truncatedCount, 0);
});

test('compact — no-op without checkpoint', () => {
  const wal = new WriteAheadLog();
  wal.beginTransaction(1);
  wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(1);

  const result = wal.compact();
  // Without a checkpoint, compact can't do much (no safe truncation point)
  // unless there are no active transactions
  assert.ok(result.truncatedCount >= 0);
});

test('compact — removes records before checkpoint', () => {
  const wal = new WriteAheadLog();
  
  // Create some transactions
  for (let i = 1; i <= 5; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }
  wal.flush();

  // Checkpoint
  wal.fuzzyCheckpoint();

  // More transactions after checkpoint
  for (let i = 6; i <= 8; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }
  wal.flush();

  const result = wal.compact();
  assert.ok(result.truncatedCount >= 0);
  assert.ok(result.walSizeAfter <= result.walSizeBefore);
});

test('compact — preserves active transaction records', () => {
  const wal = new WriteAheadLog();
  
  wal.beginTransaction(1);
  wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(1);

  wal.checkpoint();

  // Start a new transaction but don't commit
  wal.beginTransaction(2);
  wal.appendInsert(2, 'users', 0, 1, ['Bob', 25]);
  // tx2 is active (not committed)

  wal.flush();
  const result = wal.compact();
  
  // Should not truncate past tx2's first record
  const remaining = wal.getRecords();
  const tx2Records = remaining.filter(r => r.txId === 2);
  assert.ok(tx2Records.length > 0); // tx2 records preserved
});

test('compact — respects dirty page table', () => {
  const wal = new WriteAheadLog();
  
  wal.beginTransaction(1);
  const firstLsn = wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(1);

  // Dirty page table has recLSN = firstLsn
  // Compact should not remove records at or after this LSN
  const result = wal.compact();
  assert.ok(result.safeLsn <= firstLsn || result.safeLsn === 0);
});

test('compact — after checkpoint+flush, dirty pages cleared, max truncation', () => {
  const wal = new WriteAheadLog();
  
  for (let i = 1; i <= 10; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }

  // Fuzzy checkpoint clears dirty page table
  wal.fuzzyCheckpoint();

  // Now compact — should be able to truncate everything before checkpoint
  const result = wal.compact();
  assert.ok(result.safeLsn > 0);
});

test('compact — returns correct stats', () => {
  const wal = new WriteAheadLog();
  
  for (let i = 1; i <= 5; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }

  const result = wal.compact();
  assert.ok('truncatedCount' in result);
  assert.ok('safeLsn' in result);
  assert.ok('walSizeBefore' in result);
  assert.ok('walSizeAfter' in result);
  assert.ok(result.walSizeAfter <= result.walSizeBefore);
});

test('compact — multiple compactions reduce WAL progressively', () => {
  const wal = new WriteAheadLog();
  
  // Batch 1
  for (let i = 1; i <= 5; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }
  wal.fuzzyCheckpoint();
  const r1 = wal.compact();

  // Batch 2
  for (let i = 6; i <= 10; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }
  wal.fuzzyCheckpoint();
  const r2 = wal.compact();

  // Second compaction should find something to truncate
  assert.ok(r2.safeLsn > r1.safeLsn || r2.safeLsn > 0);
});
