// wal.test.js — Write-Ahead Log tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { WriteAheadLog, WALRecord, WAL_TYPES, crc32 } from './wal.js';

describe('WAL Record Serialization', () => {
  it('serialize and deserialize INSERT record', () => {
    const record = new WALRecord(1, 100, WAL_TYPES.INSERT, 'users', 0, 3, null, [1, 'Alice', 30]);
    const buf = record.serialize();
    const result = WALRecord.deserialize(buf);
    
    assert.ok(result);
    assert.equal(result.record.lsn, 1);
    assert.equal(result.record.txId, 100);
    assert.equal(result.record.type, WAL_TYPES.INSERT);
    assert.equal(result.record.typeName, 'INSERT');
    assert.equal(result.record.tableName, 'users');
    assert.equal(result.record.pageId, 0);
    assert.equal(result.record.slotIdx, 3);
    assert.equal(result.record.before, null);
    assert.deepEqual(result.record.after, [1, 'Alice', 30]);
  });

  it('serialize and deserialize UPDATE record with before/after', () => {
    const record = new WALRecord(5, 200, WAL_TYPES.UPDATE, 'products', 2, 1, 
      [1, 'Widget', 10], [1, 'Widget', 15]);
    const buf = record.serialize();
    const result = WALRecord.deserialize(buf);
    
    assert.ok(result);
    assert.deepEqual(result.record.before, [1, 'Widget', 10]);
    assert.deepEqual(result.record.after, [1, 'Widget', 15]);
  });

  it('serialize and deserialize COMMIT record', () => {
    const record = new WALRecord(10, 300, WAL_TYPES.COMMIT);
    const buf = record.serialize();
    const result = WALRecord.deserialize(buf);
    
    assert.ok(result);
    assert.equal(result.record.type, WAL_TYPES.COMMIT);
    assert.equal(result.record.typeName, 'COMMIT');
    assert.equal(result.record.txId, 300);
  });

  it('detects corruption via CRC', () => {
    const record = new WALRecord(1, 100, WAL_TYPES.INSERT, 'test', 0, 0, null, [42]);
    const buf = record.serialize();
    // Corrupt a byte in the middle
    buf[10] ^= 0xFF;
    const result = WALRecord.deserialize(buf);
    assert.equal(result, null); // Should detect corruption
  });

  it('handles large LSN values', () => {
    const record = new WALRecord(999999, 1, WAL_TYPES.INSERT, 't', 0, 0, null, [1]);
    const buf = record.serialize();
    const result = WALRecord.deserialize(buf);
    assert.ok(result);
    assert.equal(result.record.lsn, 999999);
  });
});

describe('WAL Manager', () => {
  it('appends records and assigns sequential LSNs', () => {
    const wal = new WriteAheadLog();
    const lsn1 = wal.appendInsert(1, 'users', 0, 0, [1, 'Alice']);
    const lsn2 = wal.appendInsert(1, 'users', 0, 1, [2, 'Bob']);
    const lsn3 = wal.appendCommit(1);
    
    assert.equal(lsn1, 1);
    assert.equal(lsn2, 2);
    assert.equal(lsn3, 3);
  });

  it('force-at-commit flushes to stable storage', () => {
    const wal = new WriteAheadLog();
    wal.appendInsert(1, 'users', 0, 0, [1, 'Alice']);
    assert.equal(wal.flushedLsn, 0); // Not yet flushed
    
    wal.appendCommit(1);
    assert.equal(wal.flushedLsn, 2); // Flushed up to commit
  });

  it('tracks committed transactions', () => {
    const wal = new WriteAheadLog();
    wal.beginTransaction(1);
    wal.appendInsert(1, 'users', 0, 0, [1, 'Alice']);
    assert.equal(wal.isCommitted(1), false);
    
    wal.appendCommit(1);
    assert.equal(wal.isCommitted(1), true);
  });

  it('getTransactionRecords returns only that tx', () => {
    const wal = new WriteAheadLog();
    wal.appendInsert(1, 't', 0, 0, [1]);
    wal.appendInsert(2, 't', 0, 1, [2]);
    wal.appendInsert(1, 't', 0, 2, [3]);
    wal.appendCommit(1);
    wal.appendCommit(2);
    
    const tx1Records = wal.getTransactionRecords(1);
    assert.equal(tx1Records.length, 3); // 2 inserts + commit
    assert.ok(tx1Records.every(r => r.txId === 1));
  });

  it('readFromStable after flush returns all records', () => {
    const wal = new WriteAheadLog();
    wal.appendInsert(1, 'users', 0, 0, [1, 'Alice']);
    wal.appendInsert(1, 'users', 0, 1, [2, 'Bob']);
    wal.appendCommit(1);
    
    const records = wal.readFromStable();
    assert.equal(records.length, 3);
    assert.equal(records[0].typeName, 'INSERT');
    assert.equal(records[2].typeName, 'COMMIT');
  });

  it('readFromStable with afterLsn filters', () => {
    const wal = new WriteAheadLog();
    wal.appendInsert(1, 'users', 0, 0, [1]);
    wal.appendInsert(1, 'users', 0, 1, [2]);
    wal.appendCommit(1);
    
    const records = wal.readFromStable(1); // After LSN 1
    assert.equal(records.length, 2); // LSN 2 and 3
  });

  it('checkpoint records checkpoint LSN', () => {
    const wal = new WriteAheadLog();
    wal.appendInsert(1, 't', 0, 0, [1]);
    wal.appendCommit(1);
    
    const cpLsn = wal.checkpoint();
    assert.equal(wal.lastCheckpointLsn, cpLsn);
    assert.equal(cpLsn, 3);
  });

  it('multiple transactions interleaved', () => {
    const wal = new WriteAheadLog();
    wal.beginTransaction(1);
    wal.beginTransaction(2);
    
    wal.appendInsert(1, 'a', 0, 0, ['tx1_row1']);
    wal.appendInsert(2, 'b', 0, 0, ['tx2_row1']);
    wal.appendInsert(1, 'a', 0, 1, ['tx1_row2']);
    wal.appendCommit(2);
    wal.appendAbort(1);
    
    assert.equal(wal.isCommitted(2), true);
    assert.equal(wal.isCommitted(1), false);
    
    const records = wal.getRecords();
    assert.equal(records.length, 5);
  });

  it('DELETE record stores before image', () => {
    const wal = new WriteAheadLog();
    const lsn = wal.appendDelete(1, 'users', 0, 3, [1, 'Alice', 30]);
    wal.appendCommit(1);
    
    const records = wal.readFromStable();
    assert.equal(records[0].type, WAL_TYPES.DELETE);
    assert.deepEqual(records[0].before, [1, 'Alice', 30]);
    assert.equal(records[0].after, null);
  });

  it('UPDATE record stores before and after', () => {
    const wal = new WriteAheadLog();
    wal.appendUpdate(1, 'products', 2, 1, [1, 'Widget', 10], [1, 'Widget', 15]);
    wal.appendCommit(1);
    
    const records = wal.readFromStable();
    assert.equal(records[0].type, WAL_TYPES.UPDATE);
    assert.deepEqual(records[0].before, [1, 'Widget', 10]);
    assert.deepEqual(records[0].after, [1, 'Widget', 15]);
  });
});

describe('CRC32', () => {
  it('produces consistent checksums', () => {
    const data = Buffer.from('hello world');
    assert.equal(crc32(data), crc32(data));
  });

  it('different data produces different checksums', () => {
    const a = Buffer.from('hello');
    const b = Buffer.from('world');
    assert.notEqual(crc32(a), crc32(b));
  });
});
