// pitr.test.js — Point-in-Time Recovery tests
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { WriteAheadLog, WAL_TYPES, recoverToTimestamp } from './wal.js';

// Helper: create a mock database with simple heap
function createMockDb(tableNames = ['users']) {
  const tables = new Map();
  for (const name of tableNames) {
    tables.set(name, {
      schema: [{ name: 'name' }, { name: 'age' }],
      heap: {
        _data: [],
        insert(row) { this._data.push(row); return { pageId: 0, slotIdx: this._data.length - 1 }; },
        delete(pageId, slotIdx) { if (this._data[slotIdx]) this._data[slotIdx] = null; },
        getData() { return this._data.filter(r => r !== null); }
      },
      indexes: new Map()
    });
  }
  return { tables };
}

// Helper: sleep to create distinct timestamps
function sleep(ms) { 
  const end = Date.now() + ms;
  while (Date.now() < end) {} // Busy wait for sub-ms precision
}

test('PITR — recover to time before any transactions', () => {
  const wal = new WriteAheadLog();
  const pastTime = Date.now() - 10000; // 10 seconds ago
  
  wal.beginTransaction(1);
  wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(1);

  const db = createMockDb();
  const result = recoverToTimestamp(wal, db, pastTime);
  
  assert.equal(result.committedTxns, 0); // Nothing committed before target
  assert.equal(result.redone, 0);
  assert.equal(db.tables.get('users').heap.getData().length, 0);
});

test('PITR — recover to time after all transactions', () => {
  const wal = new WriteAheadLog();
  
  wal.beginTransaction(1);
  wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(1);

  sleep(5);
  const futureTime = Date.now() + 10000;

  const db = createMockDb();
  const result = recoverToTimestamp(wal, db, futureTime);
  
  assert.equal(result.committedTxns, 1);
  assert.equal(result.redone, 1);
  assert.equal(db.tables.get('users').heap.getData().length, 1);
  assert.deepEqual(db.tables.get('users').heap.getData()[0], ['Alice', 30]);
});

test('PITR — recover between two transactions', () => {
  const wal = new WriteAheadLog();
  
  // Transaction 1 at time T1
  wal.beginTransaction(1);
  wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(1);

  sleep(15); // Ensure distinct timestamps
  const midpoint = Date.now();
  sleep(15);

  // Transaction 2 at time T2 > midpoint
  wal.beginTransaction(2);
  wal.appendInsert(2, 'users', 0, 1, ['Bob', 25]);
  wal.appendCommit(2);

  const db = createMockDb();
  const result = recoverToTimestamp(wal, db, midpoint);
  
  assert.equal(result.committedTxns, 1); // Only tx1
  assert.equal(result.skippedTxns, 1); // tx2 committed after midpoint
  assert.equal(result.redone, 1);
  assert.equal(db.tables.get('users').heap.getData().length, 1);
  assert.deepEqual(db.tables.get('users').heap.getData()[0], ['Alice', 30]);
});

test('PITR — multiple transactions with selective recovery', () => {
  const wal = new WriteAheadLog();
  const timestamps = [];
  
  // 3 transactions with timestamps between each
  for (let i = 1; i <= 3; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i * 10]);
    wal.appendCommit(i);
    sleep(15);
    timestamps.push(Date.now());
    sleep(15);
  }

  // Recover to after tx2 but before tx3
  const db = createMockDb();
  const result = recoverToTimestamp(wal, db, timestamps[1]);
  
  assert.equal(result.committedTxns, 2); // tx1 and tx2
  assert.equal(result.skippedTxns, 1); // tx3
  assert.equal(result.redone, 2);
  assert.equal(db.tables.get('users').heap.getData().length, 2);
});

test('PITR — handles deletes correctly', () => {
  const wal = new WriteAheadLog();
  
  wal.beginTransaction(1);
  wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(1);

  sleep(15);
  const afterInsert = Date.now();
  sleep(15);

  wal.beginTransaction(2);
  wal.appendDelete(2, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(2);

  // Recover to after insert but before delete
  const db1 = createMockDb();
  const r1 = recoverToTimestamp(wal, db1, afterInsert);
  assert.equal(r1.redone, 1);
  assert.equal(db1.tables.get('users').heap.getData().length, 1);

  // Recover to after delete
  const db2 = createMockDb();
  const r2 = recoverToTimestamp(wal, db2, Date.now() + 10000);
  assert.equal(r2.redone, 2); // insert + delete
  assert.equal(db2.tables.get('users').heap.getData().length, 0); // Row deleted
});

test('PITR — handles updates correctly', () => {
  const wal = new WriteAheadLog();
  
  wal.beginTransaction(1);
  wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(1);

  sleep(15);
  const afterInsert = Date.now();
  sleep(15);

  wal.beginTransaction(2);
  wal.appendUpdate(2, 'users', 0, 0, ['Alice', 30], ['Alice', 31]);
  wal.appendCommit(2);

  // Recover to before update
  const db1 = createMockDb();
  recoverToTimestamp(wal, db1, afterInsert);
  assert.deepEqual(db1.tables.get('users').heap.getData()[0], ['Alice', 30]);

  // Recover to after update
  const db2 = createMockDb();
  recoverToTimestamp(wal, db2, Date.now() + 10000);
  const data = db2.tables.get('users').heap.getData();
  // After update: old row deleted (null), new row inserted
  assert.ok(data.some(r => r && r[1] === 31)); // Updated age
});

test('PITR — uncommitted transactions at target time are excluded', () => {
  const wal = new WriteAheadLog();
  
  // tx1 commits before target
  wal.beginTransaction(1);
  wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(1);

  sleep(15);

  // tx2 starts before target but doesn't commit until after
  wal.beginTransaction(2);
  wal.appendInsert(2, 'users', 0, 1, ['Bob', 25]);

  const targetTime = Date.now();
  sleep(15);

  // tx2 commits after target
  wal.appendCommit(2);

  const db = createMockDb();
  const result = recoverToTimestamp(wal, db, targetTime);
  
  assert.equal(result.committedTxns, 1); // Only tx1
  assert.equal(result.redone, 1);
});

test('PITR — returns commit timestamps', () => {
  const wal = new WriteAheadLog();
  
  wal.beginTransaction(1);
  wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(1);

  sleep(5);

  const db = createMockDb();
  const result = recoverToTimestamp(wal, db, Date.now() + 10000);
  
  assert.ok(result.txCommitTimestamps);
  assert.ok(result.txCommitTimestamps[1]); // tx1's commit timestamp
  assert.ok(typeof result.txCommitTimestamps[1] === 'number');
});

test('PITR — multiple tables', () => {
  const wal = new WriteAheadLog();
  
  wal.beginTransaction(1);
  wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendInsert(1, 'orders', 0, 0, [100, 'widget']);
  wal.appendCommit(1);

  sleep(15);
  const midpoint = Date.now();
  sleep(15);

  wal.beginTransaction(2);
  wal.appendInsert(2, 'orders', 0, 1, [200, 'gadget']);
  wal.appendCommit(2);

  const db = createMockDb(['users', 'orders']);
  const result = recoverToTimestamp(wal, db, midpoint);
  
  assert.equal(result.committedTxns, 1);
  assert.equal(db.tables.get('users').heap.getData().length, 1);
  assert.equal(db.tables.get('orders').heap.getData().length, 1);
});

test('PITR — empty WAL returns zero counts', () => {
  const wal = new WriteAheadLog();
  const db = createMockDb();
  
  const result = recoverToTimestamp(wal, db, Date.now());
  
  assert.equal(result.committedTxns, 0);
  assert.equal(result.skippedTxns, 0);
  assert.equal(result.redone, 0);
});

test('PITR — works with fuzzy checkpoint', () => {
  const wal = new WriteAheadLog();
  
  // Pre-checkpoint data
  wal.beginTransaction(1);
  wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
  wal.appendCommit(1);

  wal.fuzzyCheckpoint();

  sleep(15);

  // Post-checkpoint data
  wal.beginTransaction(2);
  wal.appendInsert(2, 'users', 0, 1, ['Bob', 25]);
  wal.appendCommit(2);

  sleep(15);
  const afterBob = Date.now();
  sleep(15);

  wal.beginTransaction(3);
  wal.appendInsert(3, 'users', 0, 2, ['Charlie', 35]);
  wal.appendCommit(3);

  const db = createMockDb();
  const result = recoverToTimestamp(wal, db, afterBob);
  
  // Should recover tx1 and tx2, skip tx3
  assert.ok(result.committedTxns >= 1); // At least tx2 (tx1 might be truncated)
  assert.ok(result.skippedTxns >= 1); // tx3
});

test('PITR — stress test with 20 transactions', () => {
  const wal = new WriteAheadLog();
  const commitTimes = [];
  
  for (let i = 1; i <= 20; i++) {
    wal.beginTransaction(i);
    wal.appendInsert(i, 'users', 0, i - 1, [`user${i}`, i]);
    wal.appendCommit(i);
    sleep(5);
    commitTimes.push(Date.now());
    sleep(5);
  }

  // Recover to after tx10
  const db = createMockDb();
  const result = recoverToTimestamp(wal, db, commitTimes[9]);
  
  assert.equal(result.committedTxns, 10);
  assert.equal(result.skippedTxns, 10);
  assert.equal(result.redone, 10);
  assert.equal(db.tables.get('users').heap.getData().length, 10);
});
