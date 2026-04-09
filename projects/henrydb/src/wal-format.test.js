// wal-format.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { WALWriter, WALReader, RECORD_TYPES, crc32 } from './wal-format.js';

describe('WAL Format', () => {
  it('write and read records', () => {
    const w = new WALWriter();
    w.writeRecord(RECORD_TYPES.BEGIN, 1, { table: 'users' });
    w.writeRecord(RECORD_TYPES.INSERT, 1, { row: [1, 'Alice'] });
    w.writeRecord(RECORD_TYPES.COMMIT, 1, {});
    
    const r = new WALReader(w.getBuffer());
    const records = r.readAll();
    
    assert.equal(records.length, 3);
    assert.equal(records[0].type, RECORD_TYPES.BEGIN);
    assert.equal(records[0].txId, 1);
    assert.equal(records[1].data.row[1], 'Alice');
    assert.equal(records[2].type, RECORD_TYPES.COMMIT);
  });

  it('CRC32 detects corruption', () => {
    const w = new WALWriter();
    w.writeRecord(RECORD_TYPES.INSERT, 1, { data: 'important' });
    
    const buf = Buffer.from(w.getBuffer());
    // Corrupt one byte in the data section
    buf[14] ^= 0xFF;
    
    const r = new WALReader(buf);
    const rec = r.readRecord();
    assert.equal(rec.type, 'CORRUPTED');
  });

  it('multiple transactions', () => {
    const w = new WALWriter();
    // TX1: committed
    w.writeRecord(RECORD_TYPES.BEGIN, 1, {});
    w.writeRecord(RECORD_TYPES.INSERT, 1, { row: 'a' });
    w.writeRecord(RECORD_TYPES.COMMIT, 1, {});
    // TX2: rolled back
    w.writeRecord(RECORD_TYPES.BEGIN, 2, {});
    w.writeRecord(RECORD_TYPES.INSERT, 2, { row: 'b' });
    w.writeRecord(RECORD_TYPES.ROLLBACK, 2, {});
    
    const records = new WALReader(w.getBuffer()).readAll();
    assert.equal(records.length, 6);
    
    // Replay: only committed txns
    const committed = new Set();
    const rolled = new Set();
    for (const r of records) {
      if (r.type === RECORD_TYPES.COMMIT) committed.add(r.txId);
      if (r.type === RECORD_TYPES.ROLLBACK) rolled.add(r.txId);
    }
    assert.ok(committed.has(1));
    assert.ok(rolled.has(2));
  });

  it('large data', () => {
    const w = new WALWriter();
    const bigData = { rows: Array.from({ length: 1000 }, (_, i) => [i, `name-${i}`]) };
    w.writeRecord(RECORD_TYPES.INSERT, 1, bigData);
    
    const records = new WALReader(w.getBuffer()).readAll();
    assert.equal(records.length, 1);
    assert.equal(records[0].data.rows.length, 1000);
  });

  it('crc32 function', () => {
    const a = crc32(Buffer.from('hello'));
    const b = crc32(Buffer.from('hello'));
    const c = crc32(Buffer.from('world'));
    assert.equal(a, b); // Deterministic
    assert.notEqual(a, c); // Different for different data
  });

  it('performance: 10K records', () => {
    const w = new WALWriter(10 * 1024 * 1024);
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) {
      w.writeRecord(RECORD_TYPES.INSERT, i % 100, { id: i, value: `row-${i}` });
    }
    const writeMs = performance.now() - t0;
    
    const t1 = performance.now();
    const records = new WALReader(w.getBuffer()).readAll();
    const readMs = performance.now() - t1;
    
    assert.equal(records.length, 10000);
    console.log(`  10K write: ${writeMs.toFixed(1)}ms | 10K read: ${readMs.toFixed(1)}ms | Size: ${(w.offset/1024).toFixed(0)}KB`);
  });

  it('empty WAL', () => {
    const w = new WALWriter();
    const records = new WALReader(w.getBuffer()).readAll();
    assert.equal(records.length, 0);
  });
});
