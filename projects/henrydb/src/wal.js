// wal.js — Write-Ahead Log for HenryDB
// Provides crash recovery by logging all mutations before they're applied.
//
// WAL Record Format (binary):
//   [4 bytes] record length (total, including header)
//   [8 bytes] LSN (Log Sequence Number)
//   [4 bytes] record type
//   [4 bytes] CRC32 checksum of payload
//   [N bytes] payload (JSON-encoded for simplicity)
//   [4 bytes] record length again (for backward scanning)
//
// Record Types:
//   1 = INSERT
//   2 = UPDATE
//   3 = DELETE
//   4 = BEGIN
//   5 = COMMIT
//   6 = ROLLBACK
//   7 = CHECKPOINT
//   8 = CREATE_TABLE
//   9 = DROP_TABLE
//  10 = CREATE_INDEX

import fs from 'node:fs';
import path from 'node:path';

const RECORD_TYPES = {
  INSERT: 1,
  UPDATE: 2,
  DELETE: 3,
  BEGIN: 4,
  COMMIT: 5,
  ROLLBACK: 6,
  CHECKPOINT: 7,
  CREATE_TABLE: 8,
  DROP_TABLE: 9,
  CREATE_INDEX: 10,
};

const RECORD_TYPE_NAMES = Object.fromEntries(
  Object.entries(RECORD_TYPES).map(([k, v]) => [v, k])
);

const HEADER_SIZE = 4 + 8 + 4 + 4; // length + LSN + type + CRC
const FOOTER_SIZE = 4; // trailing length

// CRC32 (simple implementation for checksumming)
const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let j = 0; j < 8; j++) {
      crc = (crc & 1) ? (0xEDB88320 ^ (crc >>> 1)) : (crc >>> 1);
    }
    table[i] = crc;
  }
  return table;
})();

function crc32(buf) {
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) {
    crc = CRC32_TABLE[(crc ^ buf[i]) & 0xFF] ^ (crc >>> 8);
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

/**
 * WAL Writer — appends log records to the WAL file.
 */
export class WALWriter {
  constructor(walDir, options = {}) {
    this.walDir = walDir;
    this.currentLSN = BigInt(0);
    this.fd = null;
    this.fileSize = 0;
    this.segmentSize = options.segmentSize || 16 * 1024 * 1024; // 16MB segments
    this.currentSegment = 0;
    this.syncMode = options.syncMode || 'batch'; // 'immediate' | 'batch' | 'none'
    this._pendingSync = false;
    this._batchTimer = null;
    this._batchIntervalMs = options.batchIntervalMs || 100;
    this.stats = {
      recordsWritten: 0,
      bytesWritten: 0,
      syncs: 0,
      checkpoints: 0,
    };
  }

  /**
   * Open or create the WAL directory and current segment.
   */
  open() {
    if (!fs.existsSync(this.walDir)) {
      fs.mkdirSync(this.walDir, { recursive: true });
    }

    // Find the latest segment
    const segments = fs.readdirSync(this.walDir)
      .filter(f => f.match(/^wal_\d+\.log$/))
      .sort();

    if (segments.length > 0) {
      const lastSeg = segments[segments.length - 1];
      this.currentSegment = parseInt(lastSeg.match(/wal_(\d+)\.log/)[1]);
      const filePath = path.join(this.walDir, lastSeg);
      this.fd = fs.openSync(filePath, 'a+');
      this.fileSize = fs.fstatSync(this.fd).size;

      // Recover LSN from last record
      this._recoverLSN();
    } else {
      this._openNewSegment();
    }

    return this;
  }

  /**
   * Close the WAL writer.
   */
  close() {
    if (this._batchTimer) {
      clearInterval(this._batchTimer);
      this._batchTimer = null;
    }
    if (this._pendingSync) {
      this.sync();
    }
    if (this.fd !== null) {
      fs.closeSync(this.fd);
      this.fd = null;
    }
  }

  /**
   * Write a WAL record.
   * @returns {bigint} The LSN of the written record.
   */
  writeRecord(type, payload) {
    const typeCode = typeof type === 'string' ? RECORD_TYPES[type] : type;
    if (!typeCode) throw new Error(`Unknown WAL record type: ${type}`);

    const payloadBuf = Buffer.from(JSON.stringify(payload), 'utf8');
    const totalLen = HEADER_SIZE + payloadBuf.length + FOOTER_SIZE;

    // Check if we need a new segment
    if (this.fileSize + totalLen > this.segmentSize) {
      this._rotateSegment();
    }

    const lsn = ++this.currentLSN;
    const record = Buffer.alloc(totalLen);

    // Header
    record.writeUInt32BE(totalLen, 0);
    record.writeBigUInt64BE(lsn, 4);
    record.writeUInt32BE(typeCode, 12);
    
    // Payload
    payloadBuf.copy(record, HEADER_SIZE);

    // CRC of payload
    const checksum = crc32(payloadBuf);
    record.writeUInt32BE(checksum, 16);

    // Footer (length for backward scanning)
    record.writeUInt32BE(totalLen, totalLen - FOOTER_SIZE);

    // Write to file
    fs.writeSync(this.fd, record);
    this.fileSize += totalLen;

    this.stats.recordsWritten++;
    this.stats.bytesWritten += totalLen;

    // Sync policy
    if (this.syncMode === 'immediate') {
      this.sync();
    } else if (this.syncMode === 'batch') {
      this._pendingSync = true;
      if (!this._batchTimer) {
        this._batchTimer = setInterval(() => {
          if (this._pendingSync) this.sync();
        }, this._batchIntervalMs);
        // Don't keep process alive for timer
        if (this._batchTimer.unref) this._batchTimer.unref();
      }
    }

    return lsn;
  }

  /**
   * Force fsync of the WAL file.
   */
  sync() {
    if (this.fd !== null) {
      fs.fsyncSync(this.fd);
      this._pendingSync = false;
      this.stats.syncs++;
    }
  }

  /**
   * Write a checkpoint record and return the checkpoint LSN.
   */
  writeCheckpoint(checkpointData) {
    const lsn = this.writeRecord('CHECKPOINT', {
      ...checkpointData,
      timestamp: Date.now(),
    });
    this.sync(); // Always sync checkpoints
    this.stats.checkpoints++;
    return lsn;
  }

  // Convenience methods
  logInsert(table, row, txId) {
    return this.writeRecord('INSERT', { table, row, txId });
  }

  logUpdate(table, oldRow, newRow, txId) {
    return this.writeRecord('UPDATE', { table, old: oldRow, new: newRow, txId });
  }

  logDelete(table, row, txId) {
    return this.writeRecord('DELETE', { table, row, txId });
  }

  logBegin(txId) {
    return this.writeRecord('BEGIN', { txId });
  }

  logCommit(txId) {
    return this.writeRecord('COMMIT', { txId });
  }

  logRollback(txId) {
    return this.writeRecord('ROLLBACK', { txId });
  }

  logCreateTable(tableName, columns) {
    return this.writeRecord('CREATE_TABLE', { table: tableName, columns });
  }

  logDropTable(tableName) {
    return this.writeRecord('DROP_TABLE', { table: tableName });
  }

  logCreateIndex(indexName, tableName, columns) {
    return this.writeRecord('CREATE_INDEX', { index: indexName, table: tableName, columns });
  }

  getCurrentLSN() {
    return this.currentLSN;
  }

  _openNewSegment() {
    const fileName = `wal_${String(this.currentSegment).padStart(6, '0')}.log`;
    const filePath = path.join(this.walDir, fileName);
    this.fd = fs.openSync(filePath, 'a+');
    this.fileSize = 0;
  }

  _rotateSegment() {
    if (this.fd !== null) {
      this.sync();
      fs.closeSync(this.fd);
    }
    this.currentSegment++;
    this._openNewSegment();
  }

  _recoverLSN() {
    if (this.fileSize === 0) return;

    // Read the last record's LSN by reading the footer (length), then the record
    try {
      const footerBuf = Buffer.alloc(4);
      fs.readSync(this.fd, footerBuf, 0, 4, this.fileSize - 4);
      const lastRecordLen = footerBuf.readUInt32BE(0);

      if (lastRecordLen > 0 && lastRecordLen <= this.fileSize) {
        const headerBuf = Buffer.alloc(20);
        fs.readSync(this.fd, headerBuf, 0, 20, this.fileSize - lastRecordLen);
        this.currentLSN = headerBuf.readBigUInt64BE(4);
      }
    } catch (e) {
      // Could not recover LSN — start fresh
      this.currentLSN = BigInt(0);
    }
  }
}

/**
 * WAL Reader — reads and replays log records for crash recovery.
 */
export class WALReader {
  constructor(walDir) {
    this.walDir = walDir;
  }

  /**
   * Read all WAL records from the specified LSN onwards.
   * @param {bigint} fromLSN — Start reading from this LSN (exclusive). 0 = read all.
   * @yields {object} WAL records
   */
  *readRecords(fromLSN = BigInt(0)) {
    if (!fs.existsSync(this.walDir)) return;

    const segments = fs.readdirSync(this.walDir)
      .filter(f => f.match(/^wal_\d+\.log$/))
      .sort();

    for (const seg of segments) {
      const filePath = path.join(this.walDir, seg);
      const fd = fs.openSync(filePath, 'r');
      const fileSize = fs.fstatSync(fd).size;
      let offset = 0;

      while (offset < fileSize) {
        // Read header
        if (offset + HEADER_SIZE > fileSize) break;
        const headerBuf = Buffer.alloc(HEADER_SIZE);
        fs.readSync(fd, headerBuf, 0, HEADER_SIZE, offset);

        const totalLen = headerBuf.readUInt32BE(0);
        const lsn = headerBuf.readBigUInt64BE(4);
        const typeCode = headerBuf.readUInt32BE(12);
        const storedCrc = headerBuf.readUInt32BE(16);

        // Validate
        if (totalLen < HEADER_SIZE + FOOTER_SIZE || offset + totalLen > fileSize) break;

        // Read payload
        const payloadLen = totalLen - HEADER_SIZE - FOOTER_SIZE;
        const payloadBuf = Buffer.alloc(payloadLen);
        if (payloadLen > 0) {
          fs.readSync(fd, payloadBuf, 0, payloadLen, offset + HEADER_SIZE);
        }

        // Verify CRC
        const computedCrc = crc32(payloadBuf);
        if (computedCrc !== storedCrc) {
          // Corrupted record — stop reading
          fs.closeSync(fd);
          return;
        }

        offset += totalLen;

        // Skip records before fromLSN
        if (lsn <= fromLSN) continue;

        const payload = payloadLen > 0 ? JSON.parse(payloadBuf.toString('utf8')) : {};

        yield {
          lsn,
          type: RECORD_TYPE_NAMES[typeCode] || `UNKNOWN_${typeCode}`,
          typeCode,
          payload,
        };
      }

      fs.closeSync(fd);
    }
  }

  /**
   * Find the last checkpoint record and return its LSN and data.
   */
  findLastCheckpoint() {
    let lastCheckpoint = null;
    for (const record of this.readRecords()) {
      if (record.type === 'CHECKPOINT') {
        lastCheckpoint = record;
      }
    }
    return lastCheckpoint;
  }

  /**
   * Get all records after the last checkpoint (for replay).
   */
  *getRecoveryRecords() {
    const checkpoint = this.findLastCheckpoint();
    const fromLSN = checkpoint ? checkpoint.lsn : BigInt(0);
    yield* this.readRecords(fromLSN);
  }
}

/**
 * WAL Manager — coordinates writing, checkpointing, and recovery.
 */
export class WALManager {
  constructor(walDir, options = {}) {
    if (!walDir) {
      // No-op mode: WAL calls are silently ignored (in-memory only)
      this._noop = true;
      this.writer = { fd: null, stats: { recordsWritten: 0, bytesWritten: 0, syncs: 0, checkpoints: 0 }, close() {}, sync() {} };
      this.reader = { readRecords: function*(){}, getRecoveryRecords: function*(){}, findLastCheckpoint: () => null };
    } else {
      this._noop = false;
      this.writer = new WALWriter(walDir, options);
      this.reader = new WALReader(walDir);
    }
    this.checkpointInterval = options.checkpointInterval || 1000;
    this._recordsSinceCheckpoint = 0;
    this._autoCheckpoint = options.autoCheckpoint !== false;
  }

  open() {
    if (!this._noop) this.writer.open();
    return this;
  }

  close() {
    this.writer.close();
  }

  writeRecord(type, payload) {
    if (this._noop) return BigInt(0);
    const lsn = this.writer.writeRecord(type, payload);
    this._recordsSinceCheckpoint++;

    if (this._autoCheckpoint && this._recordsSinceCheckpoint >= this.checkpointInterval) {
      this.checkpoint({});
    }

    return lsn;
  }

  checkpoint(data) {
    if (this._noop) return BigInt(0);
    const lsn = this.writer.writeCheckpoint(data);
    this._recordsSinceCheckpoint = 0;
    return lsn;
  }

  *recover() {
    yield* this.reader.getRecoveryRecords();
  }

  getStats() {
    return { ...this.writer.stats };
  }

  // Delegate convenience methods
  logInsert(table, row, txId) { return this.writeRecord('INSERT', { table, row, txId }); }
  logUpdate(table, oldRow, newRow, txId) { return this.writeRecord('UPDATE', { table, old: oldRow, new: newRow, txId }); }
  logDelete(table, row, txId) { return this.writeRecord('DELETE', { table, row, txId }); }
  logBegin(txId) { return this.writeRecord('BEGIN', { txId }); }
  logCommit(txId) { return this.writeRecord('COMMIT', { txId }); }
  logRollback(txId) { return this.writeRecord('ROLLBACK', { txId }); }
  logCreateTable(name, cols) { return this.writeRecord('CREATE_TABLE', { table: name, columns: cols }); }
}

export { RECORD_TYPES, RECORD_TYPE_NAMES, crc32, HEADER_SIZE, FOOTER_SIZE };

// Compatibility alias — db.js imports WriteAheadLog
export { WALManager as WriteAheadLog };

// The Database class uses these legacy method names:
// appendInsert(txId, table, pageId, slotIdx, values)
// appendCommit(txId)
// appendUpdate(txId, table, pageId, slotIdx, oldValues, newValues)
// appendDelete(txId, table, pageId, slotIdx, values)
// checkpoint()
// getRecords()
// Add them to WALManager prototype for backward compatibility.

const _proto = WALManager.prototype;

_proto.appendInsert = function(txId, table, pageId, slotIdx, values) {
  // Don't write to disk if not opened (in-memory fallback)
  if (!this.writer.fd && this.writer.fd !== 0) return;
  return this.logInsert(table, { _pageId: pageId, _slotIdx: slotIdx, values }, txId);
};

_proto.appendCommit = function(txId) {
  if (!this.writer.fd && this.writer.fd !== 0) return;
  return this.logCommit(txId);
};

_proto.appendUpdate = function(txId, table, pageId, slotIdx, oldValues, newValues) {
  if (!this.writer.fd && this.writer.fd !== 0) return;
  return this.logUpdate(table, { _pageId: pageId, _slotIdx: slotIdx, values: oldValues }, { _pageId: pageId, _slotIdx: slotIdx, values: newValues }, txId);
};

_proto.appendDelete = function(txId, table, pageId, slotIdx, values) {
  if (!this.writer.fd && this.writer.fd !== 0) return;
  return this.logDelete(table, { _pageId: pageId, _slotIdx: slotIdx, values }, txId);
};

_proto.getRecords = function() {
  return [...this.reader.readRecords()];
};
