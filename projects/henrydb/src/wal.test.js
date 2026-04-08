// wal.test.js — Tests for HenryDB Write-Ahead Log
import { describe, it, before, after, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { WALWriter, WALReader, WALManager, crc32, RECORD_TYPES, HEADER_SIZE, FOOTER_SIZE } from './wal.js';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'henrydb-wal-test-'));
}

function cleanup(dir) {
  fs.rmSync(dir, { recursive: true, force: true });
}

describe('CRC32', () => {
  it('produces consistent checksums', () => {
    const data = Buffer.from('Hello, World!', 'utf8');
    const c1 = crc32(data);
    const c2 = crc32(data);
    assert.strictEqual(c1, c2);
  });

  it('different data produces different checksums', () => {
    const c1 = crc32(Buffer.from('hello'));
    const c2 = crc32(Buffer.from('world'));
    assert.notStrictEqual(c1, c2);
  });

  it('empty buffer produces a checksum', () => {
    const c = crc32(Buffer.alloc(0));
    assert.strictEqual(typeof c, 'number');
  });
});

describe('WALWriter', () => {
  let dir;

  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('creates WAL directory if missing', () => {
    const walDir = path.join(dir, 'subdir', 'wal');
    const writer = new WALWriter(walDir);
    writer.open();
    assert.ok(fs.existsSync(walDir));
    writer.close();
  });

  it('writes records and tracks LSN', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();

    const lsn1 = writer.writeRecord('INSERT', { table: 'users', row: { id: 1, name: 'Alice' } });
    const lsn2 = writer.writeRecord('INSERT', { table: 'users', row: { id: 2, name: 'Bob' } });

    assert.strictEqual(lsn1, BigInt(1));
    assert.strictEqual(lsn2, BigInt(2));
    assert.strictEqual(writer.getCurrentLSN(), BigInt(2));

    writer.close();
  });

  it('writes to file on disk', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();

    writer.writeRecord('INSERT', { table: 'test', row: { id: 1 } });
    writer.close();

    const files = fs.readdirSync(dir);
    assert.strictEqual(files.length, 1);
    assert.ok(files[0].startsWith('wal_'));

    const fileSize = fs.statSync(path.join(dir, files[0])).size;
    assert.ok(fileSize > 0);
  });

  it('tracks stats', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();

    for (let i = 0; i < 10; i++) {
      writer.writeRecord('INSERT', { table: 'test', row: { id: i } });
    }

    assert.strictEqual(writer.stats.recordsWritten, 10);
    assert.ok(writer.stats.bytesWritten > 0);

    writer.close();
  });

  it('convenience methods work', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();

    writer.logInsert('users', { id: 1, name: 'Alice' }, 1);
    writer.logUpdate('users', { id: 1, name: 'Alice' }, { id: 1, name: 'Alicia' }, 1);
    writer.logDelete('users', { id: 1 }, 1);
    writer.logBegin(2);
    writer.logCommit(2);
    writer.logRollback(3);
    writer.logCreateTable('orders', ['id', 'amount']);
    writer.logDropTable('temp');
    writer.logCreateIndex('idx_name', 'users', ['name']);
    writer.writeCheckpoint({ tables: ['users', 'orders'] });

    assert.strictEqual(writer.stats.recordsWritten, 10);
    assert.strictEqual(writer.stats.checkpoints, 1);

    writer.close();
  });

  it('segment rotation', () => {
    const writer = new WALWriter(dir, { syncMode: 'none', segmentSize: 1024 });
    writer.open();

    // Write enough records to trigger rotation
    for (let i = 0; i < 50; i++) {
      writer.writeRecord('INSERT', { table: 'test', row: { id: i, data: 'x'.repeat(50) } });
    }

    const files = fs.readdirSync(dir).filter(f => f.endsWith('.log'));
    assert.ok(files.length > 1, `Expected multiple segments, got ${files.length}`);

    writer.close();
  });

  it('immediate sync mode', () => {
    const writer = new WALWriter(dir, { syncMode: 'immediate' });
    writer.open();

    writer.writeRecord('INSERT', { table: 'test', row: { id: 1 } });
    assert.strictEqual(writer.stats.syncs, 1);

    writer.writeRecord('INSERT', { table: 'test', row: { id: 2 } });
    assert.strictEqual(writer.stats.syncs, 2);

    writer.close();
  });

  it('recovers LSN from existing WAL file', () => {
    // Write some records
    const writer1 = new WALWriter(dir, { syncMode: 'none' });
    writer1.open();
    writer1.writeRecord('INSERT', { table: 'test', row: { id: 1 } });
    writer1.writeRecord('INSERT', { table: 'test', row: { id: 2 } });
    writer1.writeRecord('INSERT', { table: 'test', row: { id: 3 } });
    writer1.close();

    // Open a new writer on the same directory
    const writer2 = new WALWriter(dir, { syncMode: 'none' });
    writer2.open();
    assert.strictEqual(writer2.getCurrentLSN(), BigInt(3));

    // Next record should have LSN 4
    const lsn = writer2.writeRecord('INSERT', { table: 'test', row: { id: 4 } });
    assert.strictEqual(lsn, BigInt(4));

    writer2.close();
  });
});

describe('WALReader', () => {
  let dir;

  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('reads back all records', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();
    writer.logInsert('users', { id: 1, name: 'Alice' }, 1);
    writer.logInsert('users', { id: 2, name: 'Bob' }, 1);
    writer.logCommit(1);
    writer.close();

    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];

    assert.strictEqual(records.length, 3);
    assert.strictEqual(records[0].type, 'INSERT');
    assert.strictEqual(records[0].payload.table, 'users');
    assert.strictEqual(records[0].payload.row.name, 'Alice');
    assert.strictEqual(records[1].type, 'INSERT');
    assert.strictEqual(records[2].type, 'COMMIT');
  });

  it('reads from specific LSN', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();
    for (let i = 1; i <= 10; i++) {
      writer.logInsert('test', { id: i }, 1);
    }
    writer.close();

    const reader = new WALReader(dir);
    const records = [...reader.readRecords(BigInt(5))];

    assert.strictEqual(records.length, 5);
    assert.strictEqual(records[0].lsn, BigInt(6));
    assert.strictEqual(records[0].payload.row.id, 6);
  });

  it('detects corrupted records', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();
    writer.logInsert('test', { id: 1 }, 1);
    writer.logInsert('test', { id: 2 }, 1);
    writer.logInsert('test', { id: 3 }, 1);
    writer.close();

    // Corrupt the second record by flipping a byte in the payload
    const files = fs.readdirSync(dir).filter(f => f.endsWith('.log'));
    const filePath = path.join(dir, files[0]);
    const data = fs.readFileSync(filePath);
    
    // First record size
    const firstLen = data.readUInt32BE(0);
    // Corrupt a byte in the second record's payload
    data[firstLen + HEADER_SIZE + 2] ^= 0xFF;
    fs.writeFileSync(filePath, data);

    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];

    // Should only read the first record before corruption stops it
    assert.strictEqual(records.length, 1);
    assert.strictEqual(records[0].payload.row.id, 1);
  });

  it('reads across segments', () => {
    const writer = new WALWriter(dir, { syncMode: 'none', segmentSize: 512 });
    writer.open();
    for (let i = 1; i <= 20; i++) {
      writer.logInsert('test', { id: i }, 1);
    }
    writer.close();

    const files = fs.readdirSync(dir).filter(f => f.endsWith('.log'));
    assert.ok(files.length > 1);

    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, 20);
  });

  it('finds last checkpoint', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();
    writer.logInsert('test', { id: 1 }, 1);
    writer.writeCheckpoint({ tables: ['test'] });
    writer.logInsert('test', { id: 2 }, 2);
    writer.logInsert('test', { id: 3 }, 2);
    writer.writeCheckpoint({ tables: ['test', 'users'] });
    writer.logInsert('test', { id: 4 }, 3);
    writer.close();

    const reader = new WALReader(dir);
    const checkpoint = reader.findLastCheckpoint();

    assert.ok(checkpoint);
    assert.strictEqual(checkpoint.type, 'CHECKPOINT');
    assert.deepStrictEqual(checkpoint.payload.tables, ['test', 'users']);
  });

  it('recovery records start after last checkpoint', () => {
    const writer = new WALWriter(dir, { syncMode: 'none' });
    writer.open();
    writer.logInsert('test', { id: 1 }, 1);
    writer.logInsert('test', { id: 2 }, 1);
    writer.writeCheckpoint({ tables: ['test'] });
    writer.logInsert('test', { id: 3 }, 2);
    writer.logInsert('test', { id: 4 }, 2);
    writer.logCommit(2);
    writer.close();

    const reader = new WALReader(dir);
    const recoveryRecords = [...reader.getRecoveryRecords()];

    // Should only have records after the checkpoint
    assert.strictEqual(recoveryRecords.length, 3);
    assert.strictEqual(recoveryRecords[0].payload.row.id, 3);
  });

  it('handles empty WAL directory', () => {
    const reader = new WALReader(dir);
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, 0);
  });

  it('handles missing WAL directory', () => {
    const reader = new WALReader(path.join(dir, 'nonexistent'));
    const records = [...reader.readRecords()];
    assert.strictEqual(records.length, 0);
  });
});

describe('WALManager', () => {
  let dir;

  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('manages write and recovery cycle', () => {
    // Write phase
    const mgr1 = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    mgr1.open();
    mgr1.logBegin(1);
    mgr1.logCreateTable('users', ['id', 'name']);
    mgr1.logInsert('users', { id: 1, name: 'Alice' }, 1);
    mgr1.logInsert('users', { id: 2, name: 'Bob' }, 1);
    mgr1.logCommit(1);
    mgr1.checkpoint({ tables: ['users'] });
    mgr1.logBegin(2);
    mgr1.logInsert('users', { id: 3, name: 'Charlie' }, 2);
    mgr1.logCommit(2);
    mgr1.close();

    // Recovery phase (simulates crash restart)
    const mgr2 = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    mgr2.open();
    const recovered = [...mgr2.recover()];

    // Should only replay records after the checkpoint
    assert.strictEqual(recovered.length, 3); // BEGIN, INSERT Charlie, COMMIT
    assert.strictEqual(recovered[0].type, 'BEGIN');
    assert.strictEqual(recovered[1].type, 'INSERT');
    assert.strictEqual(recovered[1].payload.row.name, 'Charlie');
    assert.strictEqual(recovered[2].type, 'COMMIT');

    mgr2.close();
  });

  it('auto-checkpoint after N records', () => {
    const mgr = new WALManager(dir, { syncMode: 'none', checkpointInterval: 5, autoCheckpoint: true });
    mgr.open();

    for (let i = 0; i < 12; i++) {
      mgr.logInsert('test', { id: i }, 1);
    }

    // Should have auto-checkpointed twice (at 5 and 10 records)
    const stats = mgr.getStats();
    assert.ok(stats.checkpoints >= 2, `Expected at least 2 checkpoints, got ${stats.checkpoints}`);

    mgr.close();
  });

  it('full crash recovery simulation', () => {
    // === Phase 1: Normal operations ===
    const mgr1 = new WALManager(dir, { syncMode: 'immediate', autoCheckpoint: false });
    mgr1.open();

    // Transaction 1: Create table and insert
    mgr1.logBegin(1);
    mgr1.logCreateTable('accounts', ['id', 'name', 'balance']);
    mgr1.logInsert('accounts', { id: 1, name: 'Alice', balance: 1000 }, 1);
    mgr1.logInsert('accounts', { id: 2, name: 'Bob', balance: 500 }, 1);
    mgr1.logCommit(1);

    // Checkpoint
    mgr1.checkpoint({ accountCount: 2, totalBalance: 1500 });

    // Transaction 2: Transfer money (committed before "crash")
    mgr1.logBegin(2);
    mgr1.logUpdate('accounts', { id: 1, balance: 1000 }, { id: 1, balance: 800 }, 2);
    mgr1.logUpdate('accounts', { id: 2, balance: 500 }, { id: 2, balance: 700 }, 2);
    mgr1.logCommit(2);

    // Transaction 3: Started but NOT committed (simulates in-flight transaction at crash)
    mgr1.logBegin(3);
    mgr1.logInsert('accounts', { id: 3, name: 'Charlie', balance: 100 }, 3);
    // NO COMMIT — "crash" here

    mgr1.close();

    // === Phase 2: Recovery ===
    const mgr2 = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    mgr2.open();

    const records = [...mgr2.recover()];

    // Should replay: BEGIN(2), UPDATE, UPDATE, COMMIT(2), BEGIN(3), INSERT(3)
    // Transaction 2 is committed and should be replayed
    // Transaction 3 is NOT committed and should be rolled back during recovery
    
    const committed = new Set();
    const activeTransactions = new Set();
    
    for (const record of records) {
      if (record.type === 'BEGIN') activeTransactions.add(record.payload.txId);
      if (record.type === 'COMMIT') {
        committed.add(record.payload.txId);
        activeTransactions.delete(record.payload.txId);
      }
      if (record.type === 'ROLLBACK') {
        activeTransactions.delete(record.payload.txId);
      }
    }

    // Transaction 2 should be committed
    assert.ok(committed.has(2), 'Transaction 2 should be committed');
    // Transaction 3 should still be active (needs rollback)
    assert.ok(activeTransactions.has(3), 'Transaction 3 should be uncommitted');

    mgr2.close();
  });

  it('handles large payloads', () => {
    const mgr = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    mgr.open();

    const largeData = 'x'.repeat(10000);
    mgr.logInsert('test', { id: 1, data: largeData }, 1);

    const stats = mgr.getStats();
    assert.ok(stats.bytesWritten > 10000);

    // Read it back
    mgr.close();
    const mgr2 = new WALManager(dir, { syncMode: 'none' });
    mgr2.open();
    const records = [...mgr2.recover()];
    assert.strictEqual(records.length, 1);
    assert.strictEqual(records[0].payload.row.data.length, 10000);
    mgr2.close();
  });

  it('performance: 10K records', () => {
    const mgr = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    mgr.open();

    const start = Date.now();
    for (let i = 0; i < 10000; i++) {
      mgr.logInsert('perf_test', { id: i, value: `item_${i}` }, 1);
    }
    const writeMs = Date.now() - start;

    mgr.close();

    // Read back
    const readStart = Date.now();
    const mgr2 = new WALManager(dir, { syncMode: 'none' });
    mgr2.open();
    let count = 0;
    for (const _ of mgr2.recover()) count++;
    const readMs = Date.now() - readStart;

    assert.strictEqual(count, 10000);
    console.log(`WAL perf: write ${writeMs}ms, read ${readMs}ms (10K records)`);
    assert.ok(writeMs < 5000, `Write too slow: ${writeMs}ms`);
    assert.ok(readMs < 5000, `Read too slow: ${readMs}ms`);

    mgr2.close();
  });
});
