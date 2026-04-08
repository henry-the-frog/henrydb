// auto-checkpoint.test.js — Automatic WAL checkpointing tests
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { WriteAheadLog, WAL_TYPES } from './wal.js';

test('auto-checkpoint — disabled by default', () => {
  const wal = new WriteAheadLog();
  for (let i = 1; i <= 100; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }
  assert.equal(wal.lastCheckpointLsn, 0); // No auto-checkpoint
});

test('auto-checkpoint — triggers after threshold commits', () => {
  const wal = new WriteAheadLog();
  wal.setAutoCheckpoint(5);

  for (let i = 1; i <= 5; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }

  assert.ok(wal.lastCheckpointLsn > 0); // Auto-checkpoint fired
});

test('auto-checkpoint — does not trigger before threshold', () => {
  const wal = new WriteAheadLog();
  wal.setAutoCheckpoint(10);

  for (let i = 1; i <= 9; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }

  assert.equal(wal.lastCheckpointLsn, 0); // Not yet
});

test('auto-checkpoint — fires callback', () => {
  const wal = new WriteAheadLog();
  const checkpoints = [];
  
  wal.setAutoCheckpoint(3, (result) => {
    checkpoints.push(result);
  });

  for (let i = 1; i <= 3; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }

  assert.equal(checkpoints.length, 1);
  assert.ok(checkpoints[0].beginLsn > 0);
  assert.ok(checkpoints[0].endLsn > checkpoints[0].beginLsn);
});

test('auto-checkpoint — resets counter after checkpoint', () => {
  const wal = new WriteAheadLog();
  let checkpointCount = 0;
  
  wal.setAutoCheckpoint(3, () => { checkpointCount++; });

  // First 3 commits → checkpoint #1
  for (let i = 1; i <= 3; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }
  assert.equal(checkpointCount, 1);

  // Next 3 commits → checkpoint #2
  for (let i = 4; i <= 6; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }
  assert.equal(checkpointCount, 2);
});

test('auto-checkpoint — can be disabled', () => {
  const wal = new WriteAheadLog();
  wal.setAutoCheckpoint(3);

  wal.beginTransaction(1);
  wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(1);

  wal.setAutoCheckpoint(0); // Disable

  for (let i = 2; i <= 10; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }

  assert.equal(wal.lastCheckpointLsn, 0); // Never triggered
});

test('auto-checkpoint — stats include commitsSinceCheckpoint', () => {
  const wal = new WriteAheadLog();
  wal.setAutoCheckpoint(10);

  for (let i = 1; i <= 5; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }

  const stats = wal.getStats();
  assert.equal(stats.commitsSinceCheckpoint, 5);
});

test('auto-checkpoint — WAL growth is bounded', () => {
  const wal = new WriteAheadLog();
  wal.setAutoCheckpoint(5);

  // 20 transactions with auto-checkpoint every 5
  for (let i = 1; i <= 20; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
  }

  // WAL should have been truncated by checkpoints
  // The in-memory buffer grows but stable storage should be bounded
  const stats = wal.getStats();
  assert.ok(stats.commitsSinceCheckpoint <= 5);
  // At least 3 checkpoints should have fired (at 5, 10, 15, 20)
  assert.ok(wal.lastCheckpointLsn > 0);
});
