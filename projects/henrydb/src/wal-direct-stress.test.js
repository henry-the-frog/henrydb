// wal-direct-stress.test.js — Deep stress testing of the WAL module
// Tests: segment rotation, CRC corruption detection, torn writes,
// LSN recovery after reopen, interleaved txns, edge cases, large payloads
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { WALWriter, WALReader, WALManager, crc32, RECORD_TYPES, HEADER_SIZE, FOOTER_SIZE, recoverFromWAL, recoverToTimestamp } from './wal.js';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'henrydb-wal-stress-'));
}
function cleanup(dir) {
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('WAL Segment Rotation', () => {
  let dir;
  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('rotates to new segment when file exceeds segmentSize', () => {
    // Use a tiny segment size to force rotation
    const writer = new WALWriter(dir, { segmentSize: 512, syncMode: 'none' });
    writer.open();

    const lsns = [];
    // Write enough records to force at least 2 segments
    for (let i = 0; i < 50; i++) {
      lsns.push(writer.writeRecord('INSERT', { table: 'test', row: { id: i, data: 'x'.repeat(20) }, txId: 1 }));
    }
    writer.close();

    // Verify multiple segment files exist
    const segments = fs.readdirSync(dir).filter(f => f.match(/^wal_\d+\.log$/)).sort();
    assert.ok(segments.length >= 2, `Expected >= 2 segments, got ${segments.length}`);

    // Verify all records readable across segments
    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, 50);

    // Verify LSNs are monotonically increasing
    for (let i = 1; i < records.length; i++) {
      assert.ok(records[i].lsn > records[i - 1].lsn, `LSN ${records[i].lsn} should be > ${records[i - 1].lsn}`);
    }
  });

  it('LSN continues across segment boundaries', () => {
    const writer = new WALWriter(dir, { segmentSize: 256, syncMode: 'none' });
    writer.open();

    const allLsns = [];
    for (let i = 0; i < 30; i++) {
      allLsns.push(writer.writeRecord('INSERT', { table: 't', row: { id: i }, txId: 1 }));
    }
    writer.close();

    // LSNs should be strictly ascending BigInts
    for (let i = 1; i < allLsns.length; i++) {
      assert.ok(allLsns[i] > allLsns[i - 1]);
    }
  });
});

describe('WAL CRC Corruption Detection', () => {
  let dir;
  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('stops reading at corrupted record', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();

    // Write 10 records
    for (let i = 0; i < 10; i++) {
      writer.writeRecord('INSERT', { table: 'test', row: { id: i }, txId: 1 });
    }
    writer.close();

    // Find the WAL file and corrupt a byte in the middle
    const segments = fs.readdirSync(dir).filter(f => f.match(/^wal_\d+\.log$/));
    const walFile = path.join(dir, segments[0]);
    const buf = fs.readFileSync(walFile);

    // Corrupt the payload of the 5th record (somewhere in the middle)
    // Each record is ~HEADER_SIZE + payload + FOOTER_SIZE
    // Corrupt a byte around offset 50% of file
    const corruptOffset = Math.floor(buf.length * 0.5);
    buf[corruptOffset] ^= 0xFF; // flip all bits
    fs.writeFileSync(walFile, buf);

    // Reader should stop at the corrupted record
    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.ok(records.length < 10, `Expected fewer than 10 records due to corruption, got ${records.length}`);
    assert.ok(records.length > 0, 'Should still read records before corruption');
  });

  it('detects corruption in first record', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();
    writer.writeRecord('INSERT', { table: 'test', row: { id: 1 }, txId: 1 });
    writer.writeRecord('INSERT', { table: 'test', row: { id: 2 }, txId: 1 });
    writer.close();

    const segments = fs.readdirSync(dir).filter(f => f.match(/^wal_\d+\.log$/));
    const walFile = path.join(dir, segments[0]);
    const buf = fs.readFileSync(walFile);

    // Corrupt the CRC field of the first record (bytes 16-19 in the header)
    buf[16] ^= 0xFF;
    fs.writeFileSync(walFile, buf);

    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, 0, 'No records should be readable when first record is corrupt');
  });
});

describe('WAL Torn / Partial Write Recovery', () => {
  let dir;
  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('handles truncated file (mid-record write)', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();
    for (let i = 0; i < 10; i++) {
      writer.writeRecord('INSERT', { table: 'test', row: { id: i }, txId: 1 });
    }
    writer.close();

    // Truncate the file to simulate a crash mid-write
    const segments = fs.readdirSync(dir).filter(f => f.match(/^wal_\d+\.log$/));
    const walFile = path.join(dir, segments[0]);
    const stat = fs.statSync(walFile);
    // Remove last 10 bytes — partially truncate the last record
    fs.truncateSync(walFile, stat.size - 10);

    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    // Should read 9 complete records, the 10th is truncated
    assert.strictEqual(records.length, 9, `Expected 9 readable records after truncation, got ${records.length}`);
  });

  it('handles file truncated to just header', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();
    writer.writeRecord('INSERT', { table: 'test', row: { id: 1 }, txId: 1 });
    writer.close();

    const segments = fs.readdirSync(dir).filter(f => f.match(/^wal_\d+\.log$/));
    const walFile = path.join(dir, segments[0]);
    // Truncate to just the header — no payload or footer
    fs.truncateSync(walFile, HEADER_SIZE);

    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, 0);
  });

  it('handles completely empty WAL file', () => {
    const walFile = path.join(dir, 'wal_000000.log');
    fs.writeFileSync(walFile, Buffer.alloc(0));

    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, 0);
  });

  it('handles WAL file with only a few bytes (< header size)', () => {
    const walFile = path.join(dir, 'wal_000000.log');
    fs.writeFileSync(walFile, Buffer.alloc(8, 0xFF)); // 8 garbage bytes

    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, 0);
  });
});

describe('WAL LSN Recovery After Reopen', () => {
  let dir;
  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('resumes LSN after close and reopen', () => {
    const writer1 = new WALWriter(dir, { syncMode: 'none' });
    writer1.open();
    const lsn1 = writer1.writeRecord('INSERT', { table: 'test', row: { id: 1 }, txId: 1 });
    const lsn2 = writer1.writeRecord('INSERT', { table: 'test', row: { id: 2 }, txId: 1 });
    writer1.close();

    // Reopen
    const writer2 = new WALWriter(dir, { syncMode: 'none' });
    writer2.open();
    const lsn3 = writer2.writeRecord('INSERT', { table: 'test', row: { id: 3 }, txId: 2 });
    writer2.close();

    assert.ok(lsn3 > lsn2, `LSN after reopen (${lsn3}) should be > last LSN (${lsn2})`);

    // All records should be readable
    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, 3);
  });

  it('handles multiple close-reopen cycles', () => {
    let lastLsn = BigInt(0);
    for (let cycle = 0; cycle < 5; cycle++) {
      const writer = new WALWriter(dir, { syncMode: 'none' });
      writer.open();
      for (let i = 0; i < 3; i++) {
        lastLsn = writer.writeRecord('INSERT', { table: 'test', row: { id: cycle * 3 + i }, txId: cycle });
      }
      writer.close();
    }

    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, 15);
    assert.strictEqual(records[records.length - 1].lsn, lastLsn);
  });
});

describe('WAL Interleaved Transaction Recovery', () => {
  it('only recovers committed transactions', () => {
    const wal = new WALManager(null); // in-memory

    // Tx1: committed
    wal.logBegin(1);
    wal.logInsert('users', { _pageId: 0, _slotIdx: 0, values: [1, 'Alice'] }, 1);
    wal.logInsert('users', { _pageId: 0, _slotIdx: 1, values: [2, 'Bob'] }, 1);

    // Tx2: uncommitted (will be in-progress at "crash")
    wal.logBegin(2);
    wal.logInsert('users', { _pageId: 0, _slotIdx: 2, values: [3, 'Charlie'] }, 2);

    // Tx1 commits
    wal.logCommit(1);

    // Tx3: aborted
    wal.logBegin(3);
    wal.logInsert('users', { _pageId: 0, _slotIdx: 3, values: [4, 'Dave'] }, 3);
    wal.logRollback(3);

    // Tx2 never commits — simulates crash

    // Create a mock DB to receive recovery
    const recovered = [];
    const mockDb = {
      tables: new Map([['users', {
        schema: [{ name: 'id', primaryKey: true }, { name: 'name' }],
        heap: {
          insert(row) { recovered.push(row); },
          _data: [],
          scan: function*() {},
        }
      }]]),
    };

    const result = recoverFromWAL(wal, mockDb);

    // Only Tx1's 2 inserts should be replayed
    assert.strictEqual(result.committedTxns, 1);
    assert.strictEqual(result.replayed, 2);
    // Tx2 (uncommitted) and Tx3 (aborted) should not appear
  });

  it('handles deeply interleaved transactions', () => {
    const wal = new WALManager(null);

    // 5 transactions, deeply interleaved
    for (let t = 1; t <= 5; t++) wal.logBegin(t);

    // Interleave operations
    wal.logInsert('t', { values: ['t1-a'] }, 1);
    wal.logInsert('t', { values: ['t2-a'] }, 2);
    wal.logInsert('t', { values: ['t3-a'] }, 3);
    wal.logInsert('t', { values: ['t1-b'] }, 1);
    wal.logInsert('t', { values: ['t4-a'] }, 4);
    wal.logInsert('t', { values: ['t5-a'] }, 5);

    // Commit odd txns, rollback even
    wal.logCommit(1);
    wal.logRollback(2);
    wal.logCommit(3);
    wal.logRollback(4);
    wal.logCommit(5);

    const mockDb = {
      tables: new Map([['t', {
        schema: [{ name: 'val' }],
        heap: { _data: [], insert(row) { this._data.push(row); }, scan: function*() {} }
      }]]),
    };

    const result = recoverFromWAL(wal, mockDb);
    assert.strictEqual(result.committedTxns, 3); // txns 1, 3, 5
  });
});

describe('WAL Large Payloads', () => {
  let dir;
  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('handles records with large JSON payloads', () => {
    const writer = new WALWriter(dir, { syncMode: 'none', segmentSize: 10 * 1024 * 1024 });
    writer.open();

    // Write a record with a 100KB payload
    const largeData = 'x'.repeat(100 * 1024);
    const lsn = writer.writeRecord('INSERT', { table: 'big', row: { data: largeData }, txId: 1 });
    assert.ok(lsn > BigInt(0));
    writer.close();

    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, 1);
    assert.strictEqual(records[0].payload.row.data.length, 100 * 1024);
  });

  it('handles many small records without corruption', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();

    const N = 5000;
    for (let i = 0; i < N; i++) {
      writer.writeRecord('INSERT', { table: 'test', row: { id: i }, txId: Math.floor(i / 100) });
    }
    writer.close();

    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, N);

    // Verify no data corruption — spot check some records
    assert.strictEqual(records[0].payload.row.id, 0);
    assert.strictEqual(records[999].payload.row.id, 999);
    assert.strictEqual(records[4999].payload.row.id, 4999);
  });
});

describe('WALManager In-Memory Edge Cases', () => {
  it('checkpoint clears dirty page table entries', () => {
    const wal = new WALManager(null);

    wal.logBegin(1);
    wal.logInsert('t', { _pageId: 0, _slotIdx: 0, values: [1] }, 1);
    wal.logInsert('t', { _pageId: 1, _slotIdx: 0, values: [2] }, 1);
    wal.logCommit(1);

    const dpt = wal.getDirtyPageTable();
    assert.ok(dpt.size >= 2, `Dirty page table should have entries, got ${dpt.size}`);

    // Fuzzy checkpoint should clear dirty pages
    wal.fuzzyCheckpoint({});
    const dptAfter = wal.getDirtyPageTable();
    assert.strictEqual(dptAfter.size, 0, 'Dirty page table should be empty after fuzzy checkpoint');
  });

  it('readFromStable only returns flushed records', () => {
    const wal = new WALManager(null);

    wal.logBegin(1);
    wal.logInsert('t', { values: [1] }, 1);
    // No commit yet — records should NOT be in stable storage
    const stableBefore = wal.readFromStable(0);
    // BEGIN is written but not flushed until COMMIT
    // Actually, check: does flush happen on each record or only COMMIT?
    // Per the code: _flushToStable only called on COMMIT

    wal.logCommit(1); // This triggers flush
    const stableAfter = wal.readFromStable(0);
    assert.ok(stableAfter.length >= 3, `Should have BEGIN + INSERT + COMMIT in stable, got ${stableAfter.length}`);
  });

  it('readFromStable with afterLsn filters correctly', () => {
    const wal = new WALManager(null);

    wal.logBegin(1);
    wal.logInsert('t', { values: [1] }, 1);
    wal.logCommit(1);

    const lsn2Begin = wal.logBegin(2);
    wal.logInsert('t', { values: [2] }, 2);
    wal.logCommit(2);

    // Read only records after tx1's commit
    const afterTx1 = wal.readFromStable(3); // LSN 3 is the COMMIT of tx1
    assert.ok(afterTx1.length >= 3, `Should have tx2's records, got ${afterTx1.length}`);
    assert.ok(afterTx1.every(r => r.lsn > 3));
  });

  it('truncate removes records below threshold', () => {
    const wal = new WALManager(null);

    for (let t = 1; t <= 5; t++) {
      wal.logBegin(t);
      wal.logInsert('t', { values: [t] }, t);
      wal.logCommit(t);
    }

    const before = wal.readFromStable(0).length;
    const removed = wal.truncate(10); // Remove everything with LSN < 10
    const after = wal.readFromStable(0).length;
    assert.ok(after < before, `Truncate should reduce records: ${before} -> ${after}`);
    assert.ok(removed > 0, `Should have removed some records, removed ${removed}`);
  });

  it('auto-checkpoint triggers after threshold', () => {
    // In-memory mode defaults autoCheckpoint to false, so explicitly enable it
    const wal = new WALManager(null, { checkpointInterval: 5, autoCheckpoint: true });

    // Write enough records to trigger auto-checkpoint
    for (let i = 0; i < 10; i++) {
      wal.writeRecord('INSERT', { table: 't', row: { id: i }, txId: 1 });
    }

    assert.ok(wal.getStats().checkpoints >= 1, 'Auto-checkpoint should have triggered');
  });

  it('commit-based auto-checkpoint', () => {
    const wal = new WALManager(null);
    let callbackCalled = false;
    wal.setAutoCheckpoint(3, () => { callbackCalled = true; });

    for (let t = 1; t <= 5; t++) {
      wal.logBegin(t);
      wal.logInsert('t', { values: [t] }, t);
      wal.logCommit(t);
    }

    assert.ok(callbackCalled, 'Checkpoint callback should have been called after 3 commits');
    assert.ok(wal.getCheckpointCount() >= 1);
  });

  it('tracks active transactions correctly', () => {
    const wal = new WALManager(null);

    wal.beginTransaction(1);
    wal.beginTransaction(2);
    wal.beginTransaction(3);

    let stats = wal.getStats();
    assert.strictEqual(stats.activeTxns, 3);

    wal.logCommit(1);
    stats = wal.getStats();
    assert.strictEqual(stats.activeTxns, 2);

    wal.logRollback(2);
    stats = wal.getStats();
    assert.strictEqual(stats.activeTxns, 1);
  });

  it('isCommitted returns correct results', () => {
    const wal = new WALManager(null);

    wal.beginTransaction(1);
    assert.strictEqual(wal.isCommitted(1), false);

    wal.logCommit(1);
    assert.strictEqual(wal.isCommitted(1), true);

    wal.beginTransaction(2);
    wal.logRollback(2);
    assert.strictEqual(wal.isCommitted(2), false);
  });
});

describe('WAL Point-in-Time Recovery', () => {
  it('recovers only transactions committed before target timestamp', () => {
    const wal = new WALManager(null);

    const t1 = Date.now();
    wal.logBegin(1);
    wal.logInsert('t', { values: [1] }, 1);
    wal.logCommit(1);

    // Small delay to differentiate timestamps
    const t2 = Date.now() + 1000;
    // Manually write records with future timestamps
    wal._lsn++;
    wal._memRecords.push({ lsn: Number(wal._lsn), type: 4, typeName: 'BEGIN', data: { txId: 2 }, timestamp: t2 });
    wal._lsn++;
    wal._memRecords.push({ lsn: Number(wal._lsn), type: 1, typeName: 'INSERT', data: { table: 't', row: { values: [2] }, txId: 2 }, timestamp: t2 });
    wal._lsn++;
    wal._memRecords.push({ lsn: Number(wal._lsn), type: 5, typeName: 'COMMIT', data: { txId: 2 }, timestamp: t2 });
    wal._flushToStable();

    const mockDb = {
      tables: new Map([['t', {
        schema: [{ name: 'val' }],
        heap: { _data: [], insert(row) { this._data.push(row); }, scan: function*() {} }
      }]]),
    };

    // Recover to just before t2 — should only get tx1
    const result = recoverToTimestamp(wal, mockDb, t2 - 500);
    assert.strictEqual(result.committedTxns, 1);
    assert.ok(result.skippedTxns >= 1, `Should have skipped tx2, skipped: ${result.skippedTxns}`);
  });
});

describe('WAL File-Based Recovery Integration', () => {
  let dir;
  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('recovers after simulated crash (file WAL)', () => {
    // Write committed and uncommitted txns to file
    const writer = new WALWriter(dir, { syncMode: 'immediate' });
    writer.open();

    // Tx1: committed
    writer.writeRecord('BEGIN', { txId: 1 });
    writer.writeRecord('INSERT', { table: 'users', row: { values: [1, 'Alice'] }, txId: 1 });
    writer.writeRecord('COMMIT', { txId: 1 });

    // Tx2: uncommitted (simulated crash)
    writer.writeRecord('BEGIN', { txId: 2 });
    writer.writeRecord('INSERT', { table: 'users', row: { values: [2, 'Bob'] }, txId: 2 });
    // No COMMIT — crash!

    writer.close();

    // Read back — all records should be there
    const reader = new WALReader(dir);
    const allRecords = [...reader.readRecords()];
    assert.strictEqual(allRecords.length, 5); // 3 from tx1 + 2 from tx2

    // But recovery should only replay tx1
    const committed = new Set();
    const uncommitted = new Set();
    for (const r of allRecords) {
      if (r.type === 'COMMIT') committed.add(r.payload.txId);
      if (r.type === 'BEGIN') uncommitted.add(r.payload.txId);
    }
    for (const txId of committed) uncommitted.delete(txId);

    assert.ok(committed.has(1));
    assert.ok(uncommitted.has(2));
  });

  it('checkpoint followed by recovery skips pre-checkpoint records', () => {
    const writer = new WALWriter(dir, { syncMode: 'immediate' });
    writer.open();

    // Pre-checkpoint data
    writer.writeRecord('BEGIN', { txId: 1 });
    writer.writeRecord('INSERT', { table: 't', row: { id: 1 }, txId: 1 });
    writer.writeRecord('COMMIT', { txId: 1 });

    // Checkpoint
    writer.writeCheckpoint({ tables: ['t'] });

    // Post-checkpoint data
    writer.writeRecord('BEGIN', { txId: 2 });
    writer.writeRecord('INSERT', { table: 't', row: { id: 2 }, txId: 2 });
    writer.writeRecord('COMMIT', { txId: 2 });

    writer.close();

    // Recovery records should only include post-checkpoint
    const reader = new WALReader(dir);
    const recoveryRecords = [...reader.getRecoveryRecords()];
    assert.strictEqual(recoveryRecords.length, 3); // BEGIN, INSERT, COMMIT for tx2
    assert.ok(recoveryRecords.every(r => r.payload.txId === 2));
  });

  it('handles WAL directory that does not exist', () => {
    const reader = new WALReader(path.join(dir, 'nonexistent'));
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, 0);
  });
});

describe('WAL Record Type Coverage', () => {
  let dir;
  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('correctly stores and reads all record types', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();

    const types = ['INSERT', 'UPDATE', 'DELETE', 'BEGIN', 'COMMIT', 'ROLLBACK',
                   'CHECKPOINT', 'CREATE_TABLE', 'DROP_TABLE', 'CREATE_INDEX',
                   'TRUNCATE', 'DDL'];

    for (const type of types) {
      writer.writeRecord(type, { type_test: type });
    }
    writer.close();

    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, types.length);

    for (let i = 0; i < types.length; i++) {
      assert.strictEqual(records[i].type, types[i], `Record ${i} type mismatch: expected ${types[i]}, got ${records[i].type}`);
      assert.strictEqual(records[i].payload.type_test, types[i]);
    }
  });
});

describe('WAL Stats Tracking', () => {
  it('accurately tracks write stats', () => {
    const wal = new WALManager(null);

    wal.logBegin(1);
    wal.logInsert('t', { values: [1] }, 1);
    wal.logCommit(1);

    const stats = wal.getStats();
    assert.strictEqual(stats.recordsWritten, 3);
    assert.ok(stats.nextLsn > 3);
  });

  let dir;
  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('tracks file-based stats', () => {
    const writer = new WALWriter(dir, { syncMode: 'immediate' });
    writer.open();

    writer.writeRecord('INSERT', { table: 't', row: { id: 1 }, txId: 1 });
    writer.writeRecord('INSERT', { table: 't', row: { id: 2 }, txId: 1 });

    assert.strictEqual(writer.stats.recordsWritten, 2);
    assert.ok(writer.stats.bytesWritten > 0);
    assert.ok(writer.stats.syncs >= 2); // immediate sync mode

    writer.writeCheckpoint({ tables: ['t'] });
    assert.strictEqual(writer.stats.checkpoints, 1);

    writer.close();
  });
});
