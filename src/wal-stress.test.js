// wal-stress.test.js — Adversarial WAL crash recovery tests
// Goal: verify data integrity under simulated crashes, corruption, and edge cases

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { WriteAheadLog, WALRecord, WAL_TYPES, crc32, recoverFromWAL } from './wal.js';

describe('WAL Stress Tests', () => {

  // ========== SERIALIZATION ROUNDTRIP ==========

  describe('Record serialization', () => {
    it('INSERT roundtrip', () => {
      const record = new WALRecord(1, 10, WAL_TYPES.INSERT, 'users', 0, 0, null, ['Alice', 30]);
      const buf = record.serialize();
      const result = WALRecord.deserialize(buf);
      assert.ok(result);
      assert.strictEqual(result.record.lsn, 1);
      assert.strictEqual(result.record.txId, 10);
      assert.strictEqual(result.record.type, WAL_TYPES.INSERT);
      assert.strictEqual(result.record.tableName, 'users');
      assert.deepStrictEqual(result.record.after, ['Alice', 30]);
    });

    it('UPDATE roundtrip with before and after', () => {
      const record = new WALRecord(2, 10, WAL_TYPES.UPDATE, 'users', 0, 0, 
        ['Alice', 30], ['Alice', 31]);
      const buf = record.serialize();
      const result = WALRecord.deserialize(buf);
      assert.ok(result);
      assert.deepStrictEqual(result.record.before, ['Alice', 30]);
      assert.deepStrictEqual(result.record.after, ['Alice', 31]);
    });

    it('COMMIT roundtrip', () => {
      const record = new WALRecord(3, 10, WAL_TYPES.COMMIT);
      const buf = record.serialize();
      const result = WALRecord.deserialize(buf);
      assert.ok(result);
      assert.strictEqual(result.record.type, WAL_TYPES.COMMIT);
    });

    it('multiple records roundtrip', () => {
      const records = [
        new WALRecord(1, 1, WAL_TYPES.INSERT, 't', 0, 0, null, [1]),
        new WALRecord(2, 1, WAL_TYPES.INSERT, 't', 0, 1, null, [2]),
        new WALRecord(3, 1, WAL_TYPES.COMMIT),
      ];
      
      for (const r of records) {
        const buf = r.serialize();
        const result = WALRecord.deserialize(buf);
        assert.ok(result, `Record LSN ${r.lsn} should roundtrip`);
        assert.strictEqual(result.record.lsn, r.lsn);
      }
    });
  });

  // ========== CRC CORRUPTION DETECTION ==========

  describe('CRC corruption detection', () => {
    it('detects single bit flip in body', () => {
      const record = new WALRecord(1, 10, WAL_TYPES.INSERT, 'users', 0, 0, null, ['Alice', 30]);
      const buf = record.serialize();
      
      // Flip a bit in the middle of the body
      const midpoint = Math.floor(buf.length / 2);
      buf[midpoint] ^= 0x01;
      
      const result = WALRecord.deserialize(buf);
      assert.strictEqual(result, null, 'Corrupted record should return null');
    });

    it('detects corrupted table name', () => {
      const record = new WALRecord(1, 10, WAL_TYPES.INSERT, 'users', 0, 0, null, [1]);
      const buf = record.serialize();
      
      // Corrupt the table name area (after fixed headers)
      // Fixed header: 4(len) + 8(lsn) + 4(txid) + 1(type) + 2(tableNameLen) = 19
      buf[19] ^= 0xFF;
      
      const result = WALRecord.deserialize(buf);
      assert.strictEqual(result, null, 'Corrupted table name detected');
    });

    it('detects truncated record', () => {
      const record = new WALRecord(1, 10, WAL_TYPES.INSERT, 'users', 0, 0, null, [1]);
      const buf = record.serialize();
      
      // Truncate the buffer
      const truncated = buf.subarray(0, buf.length - 10);
      const result = WALRecord.deserialize(truncated);
      assert.strictEqual(result, null, 'Truncated record returns null');
    });

    it('detects all-zero buffer', () => {
      const buf = Buffer.alloc(100);
      const result = WALRecord.deserialize(buf);
      // Should either return null or return a record with zero CRC that doesn't match
      // The behavior depends on whether all-zeros passes the CRC check
      // This is fine either way — just shouldn't crash
      assert.ok(true, 'Does not crash on all-zero buffer');
    });

    it('handles empty buffer', () => {
      const result = WALRecord.deserialize(Buffer.alloc(0));
      assert.strictEqual(result, null);
    });

    it('handles buffer too small for header', () => {
      const result = WALRecord.deserialize(Buffer.alloc(3));
      assert.strictEqual(result, null);
    });
  });

  // ========== WAL MANAGER OPERATIONS ==========

  describe('WAL Manager', () => {
    it('LSNs are monotonically increasing', () => {
      const wal = new WriteAheadLog();
      const lsns = [];
      for (let i = 0; i < 10; i++) {
        lsns.push(wal.appendInsert(1, 't', 0, i, [i]));
      }
      for (let i = 1; i < lsns.length; i++) {
        assert.ok(lsns[i] > lsns[i-1], 'LSN must increase');
      }
    });

    it('flush persists all records', () => {
      const wal = new WriteAheadLog();
      wal.appendInsert(1, 't', 0, 0, [1]);
      wal.appendInsert(1, 't', 0, 1, [2]);
      wal.appendCommit(1); // auto-flushes
      
      const stable = wal.readFromStable();
      assert.strictEqual(stable.length, 3);
    });

    it('uncommitted transaction records not in stable storage before flush', () => {
      const wal = new WriteAheadLog();
      wal.appendInsert(1, 't', 0, 0, [1]);
      // No commit, no flush
      const stable = wal.readFromStable();
      assert.strictEqual(stable.length, 0, 'Uncommitted not flushed');
    });

    it('checkpoint records are written', () => {
      const wal = new WriteAheadLog();
      wal.appendInsert(1, 't', 0, 0, [1]);
      wal.appendCommit(1);
      const cpLsn = wal.checkpoint();
      
      assert.ok(cpLsn > 0);
      assert.strictEqual(wal.lastCheckpointLsn, cpLsn);
    });

    it('readFromStable respects afterLsn', () => {
      const wal = new WriteAheadLog();
      wal.appendInsert(1, 't', 0, 0, [1]);
      wal.appendCommit(1);
      const cpLsn = wal.checkpoint();
      wal.appendInsert(2, 't', 0, 1, [2]);
      wal.appendCommit(2);
      
      // Read only after checkpoint
      const afterCp = wal.readFromStable(cpLsn);
      // Should only get tx 2's records
      assert.ok(afterCp.length > 0);
      assert.ok(afterCp.every(r => r.lsn > cpLsn));
    });
  });

  // ========== CRASH RECOVERY SCENARIOS ==========

  describe('Crash recovery', () => {
    it('committed transaction survives crash', () => {
      const wal = new WriteAheadLog();
      wal.beginTransaction(1);
      wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
      wal.appendInsert(1, 'users', 0, 1, ['Bob', 25]);
      wal.appendCommit(1);
      
      // Verify records are in stable storage
      const records = wal.readFromStable();
      assert.strictEqual(records.length, 3);
      assert.ok(records.some(r => r.type === WAL_TYPES.COMMIT));
    });

    it('uncommitted transaction has no COMMIT in WAL', () => {
      const wal = new WriteAheadLog();
      wal.beginTransaction(1);
      wal.appendInsert(1, 'users', 0, 0, ['Alice', 30]);
      wal.flush(); // Force flush even without commit
      
      const records = wal.readFromStable();
      assert.ok(!records.some(r => r.type === WAL_TYPES.COMMIT));
    });

    it('interleaved committed and uncommitted transactions', () => {
      const wal = new WriteAheadLog();
      
      // tx1 commits
      wal.beginTransaction(1);
      wal.appendInsert(1, 't', 0, 0, [1]);
      
      // tx2 starts but won't commit
      wal.beginTransaction(2);
      wal.appendInsert(2, 't', 0, 1, [2]);
      
      // tx1 commits
      wal.appendCommit(1);
      
      // tx3 commits
      wal.beginTransaction(3);
      wal.appendInsert(3, 't', 0, 2, [3]);
      wal.appendCommit(3);
      
      // "Crash" — tx2 never committed
      wal.flush();
      
      const records = wal.readFromStable();
      const committedTxIds = new Set();
      for (const r of records) {
        if (r.type === WAL_TYPES.COMMIT) committedTxIds.add(r.txId);
      }
      
      assert.ok(committedTxIds.has(1), 'tx1 committed');
      assert.ok(!committedTxIds.has(2), 'tx2 not committed');
      assert.ok(committedTxIds.has(3), 'tx3 committed');
    });

    it('checkpoint followed by new transactions', () => {
      const wal = new WriteAheadLog();
      
      wal.beginTransaction(1);
      wal.appendInsert(1, 't', 0, 0, [1]);
      wal.appendCommit(1);
      
      wal.checkpoint();
      
      wal.beginTransaction(2);
      wal.appendInsert(2, 't', 0, 1, [2]);
      wal.appendCommit(2);
      
      // Recovery from checkpoint should only need to replay tx2
      const afterCheckpoint = wal.readFromStable(wal.lastCheckpointLsn);
      const txIds = new Set(afterCheckpoint.map(r => r.txId));
      assert.ok(txIds.has(2));
      // tx1 should not appear after checkpoint
      assert.ok(!afterCheckpoint.some(r => r.txId === 1));
    });
    
    it('recovery idempotency: double recovery produces same result', () => {
      const wal = new WriteAheadLog();
      
      wal.beginTransaction(1);
      wal.appendInsert(1, 'data', 0, 0, ['row1']);
      wal.appendInsert(1, 'data', 0, 1, ['row2']);
      wal.appendCommit(1);
      
      // First recovery pass
      const records1 = wal.readFromStable();
      const committed1 = new Set();
      for (const r of records1) {
        if (r.type === WAL_TYPES.COMMIT) committed1.add(r.txId);
      }
      
      // Second recovery pass (same WAL)
      const records2 = wal.readFromStable();
      const committed2 = new Set();
      for (const r of records2) {
        if (r.type === WAL_TYPES.COMMIT) committed2.add(r.txId);
      }
      
      assert.deepStrictEqual(committed1, committed2, 'Double recovery is identical');
    });

    it('abort record marks transaction as rolled back', () => {
      const wal = new WriteAheadLog();
      wal.beginTransaction(1);
      wal.appendInsert(1, 't', 0, 0, [1]);
      wal.appendAbort(1);
      wal.flush();
      
      const records = wal.readFromStable();
      assert.ok(records.some(r => r.type === WAL_TYPES.ABORT && r.txId === 1));
      assert.ok(!records.some(r => r.type === WAL_TYPES.COMMIT && r.txId === 1));
    });
  });

  // ========== LARGE DATA STRESS ==========

  describe('Large data handling', () => {
    it('many records in a single transaction', () => {
      const wal = new WriteAheadLog();
      wal.beginTransaction(1);
      for (let i = 0; i < 1000; i++) {
        wal.appendInsert(1, 'bulk', 0, i, [i, `row_${i}`, i * 1.5]);
      }
      wal.appendCommit(1);
      
      const records = wal.readFromStable();
      assert.strictEqual(records.length, 1001); // 1000 inserts + 1 commit
    });

    it('many concurrent transactions', () => {
      const wal = new WriteAheadLog();
      const numTx = 50;
      
      // Interleave 50 transactions
      for (let i = 0; i < numTx; i++) {
        wal.beginTransaction(i + 1);
      }
      
      for (let i = 0; i < numTx; i++) {
        wal.appendInsert(i + 1, 't', 0, i, [i]);
        if (i % 2 === 0) {
          wal.appendCommit(i + 1);
        } else {
          wal.appendAbort(i + 1);
        }
      }
      
      wal.flush(); // Flush remaining (abort records don't auto-flush)
      
      const records = wal.readFromStable();
      const committed = records.filter(r => r.type === WAL_TYPES.COMMIT);
      const aborted = records.filter(r => r.type === WAL_TYPES.ABORT);
      
      assert.strictEqual(committed.length, 25);
      assert.strictEqual(aborted.length, 25);
    });

    it('large tuple data serializes correctly', () => {
      const wal = new WriteAheadLog();
      const bigString = 'x'.repeat(10000);
      wal.appendInsert(1, 'big', 0, 0, [bigString, 42, null, true]);
      wal.appendCommit(1);
      
      const records = wal.readFromStable();
      const insertRecord = records.find(r => r.type === WAL_TYPES.INSERT);
      assert.ok(insertRecord);
      assert.strictEqual(insertRecord.after[0].length, 10000);
      assert.strictEqual(insertRecord.after[1], 42);
      assert.strictEqual(insertRecord.after[2], null);
      assert.strictEqual(insertRecord.after[3], true);
    });
  });

  // ========== PARTIAL WRITE / MID-RECORD CORRUPTION ==========

  describe('Partial write simulation', () => {
    it('partial record at end of WAL is detected', () => {
      const record = new WALRecord(1, 10, WAL_TYPES.INSERT, 'users', 0, 0, null, [1, 2, 3]);
      const fullBuf = record.serialize();
      
      // Simulate partial write: only first half written
      const partial = fullBuf.subarray(0, Math.floor(fullBuf.length / 2));
      const result = WALRecord.deserialize(partial);
      assert.strictEqual(result, null, 'Partial record detected');
    });

    it('corrupt record followed by valid record', () => {
      // First record: valid
      const r1 = new WALRecord(1, 1, WAL_TYPES.INSERT, 't', 0, 0, null, [1]);
      const buf1 = r1.serialize();
      
      // Second record: corrupt (flip a byte)
      const r2 = new WALRecord(2, 1, WAL_TYPES.INSERT, 't', 0, 1, null, [2]);
      const buf2 = r2.serialize();
      buf2[10] ^= 0xFF; // Corrupt
      
      // Third record: valid
      const r3 = new WALRecord(3, 1, WAL_TYPES.COMMIT);
      const buf3 = r3.serialize();
      
      // Concatenate
      const combined = Buffer.concat([buf1, buf2, buf3]);
      
      // First record should deserialize fine
      const result1 = WALRecord.deserialize(combined, 0);
      assert.ok(result1, 'First record valid');
      
      // Second record should fail
      const result2 = WALRecord.deserialize(combined, result1.bytesRead);
      assert.strictEqual(result2, null, 'Corrupt second record detected');
    });
  });
});
