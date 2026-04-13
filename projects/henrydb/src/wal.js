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
  TRUNCATE: 11,
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

  logTruncate(tableName, txId) {
    return this.writeRecord('TRUNCATE', { table: tableName, txId });
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
    this._lsn = 0n;
    this._lastCheckpointLsn = 0n;
    this._memRecords = [];
    this._stableRecords = [];
    this._dirtyPageTable = new Map(); // pageKey -> recLSN (first write LSN)
    this._activeTxns = new Map(); // txId -> { status: 'active'|'committed'|'aborted', startLsn }
    this._commitsSinceCheckpoint = 0;
    this._autoCheckpointByCommits = false;
    this._checkpointCallback = null;
    this._lastFlushedIdx = 0; // Track last flushed index for O(1) flush
    this._flushedLsn = 0; // Highest LSN that has been flushed to stable storage

    if (!walDir) {
      this._noop = false;
      this._inMemory = true;
      this.writer = { fd: null, stats: { recordsWritten: 0, bytesWritten: 0, syncs: 0, checkpoints: 0 }, close() {}, sync() {} };
      this.reader = { readRecords: function*(){}, getRecoveryRecords: function*(){}, findLastCheckpoint: () => null };
    } else {
      this._noop = false;
      this._inMemory = false;
      this.writer = new WALWriter(walDir, options);
      this.reader = new WALReader(walDir);
    }
    this.checkpointInterval = options.checkpointInterval || 10000;
    this._recordsSinceCheckpoint = 0;
    this._autoCheckpoint = options.autoCheckpoint !== undefined ? options.autoCheckpoint : !this._inMemory;
    // Alias for tests that access _stableStorage
    Object.defineProperty(this, '_stableStorage', {
      get() { return this._stableRecords; },
      set(v) { this._stableRecords = v; },
    });
  }

  get lastCheckpointLsn() { return Number(this._lastCheckpointLsn); }

  open() {
    if (!this._inMemory) this.writer.open();
    return this;
  }

  close() {
    if (!this._inMemory) this.writer.close();
  }

  writeRecord(type, payload) {
    this._lsn++;
    const lsn = this._lsn;

    if (this._inMemory) {
      const typeCode = typeof type === 'string' ? (RECORD_TYPES[type] || type) : type;
      const rec = { lsn: Number(lsn), type: typeCode, typeName: typeof type === 'string' ? type : (RECORD_TYPE_NAMES[type] || type), data: payload, txId: payload?.txId, table: payload?.table, timestamp: Date.now() };
      // Expose row data for INSERT/UPDATE/DELETE for test compatibility
      if (payload?.row) {
        rec.after = payload.row.values || payload.row;
      }
      if (payload?.old) {
        rec.before = payload.old.values || payload.old;
      }
      if (payload?.new) {
        rec.after = payload.new.values || payload.new;
      }
      this._memRecords.push(rec);

      // Track dirty pages for INSERT/UPDATE/DELETE
      const typeName = typeof type === 'string' ? type : (RECORD_TYPE_NAMES[typeCode] || '');
      if (['INSERT', 'UPDATE', 'DELETE'].includes(typeName) && payload?.table !== undefined) {
        const pageId = payload?.row?._pageId ?? payload?.old?._pageId ?? 0;
        const pageKey = `${payload.table}:${pageId}`;
        if (!this._dirtyPageTable.has(pageKey)) {
          this._dirtyPageTable.set(pageKey, Number(lsn)); // First-write-wins
        }
      }

      // Auto-flush on COMMIT
      if (type === 'COMMIT' || type === RECORD_TYPES.COMMIT) {
        this._flushToStable();
        // Mark txn as committed
        if (payload?.txId !== undefined) {
          this._activeTxns.set(payload.txId, { status: 'committed', startLsn: this._activeTxns.get(payload.txId)?.startLsn || 0 });
        }
        // Track commits for auto-checkpoint
        if (this._autoCheckpointByCommits) {
          this._commitsSinceCheckpoint = (this._commitsSinceCheckpoint || 0) + 1;
          if (this._commitsSinceCheckpoint >= this.checkpointInterval) {
            const beginLsn = Number(this._lsn);
            this.checkpoint({});
            const endLsn = Number(this._lsn);
            this._commitsSinceCheckpoint = 0;
            if (this._checkpointCallback) this._checkpointCallback({ beginLsn, endLsn });
          }
        }
      } else if (type === 'ROLLBACK' || type === RECORD_TYPES.ROLLBACK) {
        if (payload?.txId !== undefined) {
          this._activeTxns.set(payload.txId, { status: 'aborted', startLsn: this._activeTxns.get(payload.txId)?.startLsn || 0 });
        }
      }
      this.writer.stats.recordsWritten++;
      this._recordsSinceCheckpoint++;
      // Only auto-checkpoint by record count if not using commit-based checkpointing
      if (this._autoCheckpoint && !this._autoCheckpointByCommits && this._recordsSinceCheckpoint >= this.checkpointInterval) {
        this.checkpoint({});
      }
      return Number(lsn);
    }

    const fileLsn = this.writer.writeRecord(type, payload);
    this._recordsSinceCheckpoint++;
    if (this._autoCheckpoint && this._recordsSinceCheckpoint >= this.checkpointInterval) {
      this.checkpoint({});
    }
    return fileLsn;
  }

  _flushToStable() {
    // Append only new records since last flush — O(delta) not O(n²)
    for (let i = this._lastFlushedIdx; i < this._memRecords.length; i++) {
      this._stableRecords.push(this._memRecords[i]);
      // Track the highest flushed LSN
      const rec = this._memRecords[i];
      if (rec.lsn > this._flushedLsn) this._flushedLsn = rec.lsn;
    }
    this._lastFlushedIdx = this._memRecords.length;
  }

  checkpoint(data) {
    this._lsn++;
    const lsn = this._lsn;
    if (this._inMemory) {
      const rec = { lsn: Number(lsn), type: RECORD_TYPES.CHECKPOINT, typeName: 'CHECKPOINT', data: data || {}, timestamp: Date.now() };
      this._memRecords.push(rec);
      this._stableRecords.push(rec);
      this._lastCheckpointLsn = lsn;
      this._recordsSinceCheckpoint = 0;
      this.writer.stats.checkpoints++;
      return Number(lsn);
    }
    const fileLsn = this.writer.writeCheckpoint(data);
    this._recordsSinceCheckpoint = 0;
    return fileLsn;
  }

  readFromStable(afterLsn) {
    if (this._inMemory) {
      if (afterLsn !== undefined && afterLsn > 0) {
        return this._stableRecords.filter(r => r.lsn > afterLsn);
      }
      return [...this._stableRecords];
    }
    return [...this.reader.readRecords()];
  }

  getDirtyPageTable() {
    return new Map(this._dirtyPageTable);
  }

  fuzzyCheckpoint(options = {}) {
    const dptSnapshot = new Map(this._dirtyPageTable);
    const activeTxnSnapshot = new Map(
      [...this._activeTxns].filter(([_, v]) => v.status === 'active')
    );

    // Write BEGIN_CHECKPOINT record with dirty page table snapshot
    this._lsn++;
    const beginLsn = Number(this._lsn);
    const beginRec = {
      lsn: beginLsn,
      type: WAL_TYPES.BEGIN_CHECKPOINT,
      typeName: 'BEGIN_CHECKPOINT',
      data: {},
      txId: null,
      table: null,
      after: {
        dirtyPageTable: [...dptSnapshot].map(([pageKey, recLSN]) => ({ pageKey, recLSN })),
        activeTxns: [...activeTxnSnapshot].map(([txId, info]) => ({ txId, startLsn: info.startLsn })),
      },
      timestamp: Date.now(),
    };
    this._memRecords.push(beginRec);
    this._stableRecords.push(beginRec);

    // Callback to flush dirty pages (for buffer pool integration)
    if (options.flushDirtyPages) {
      options.flushDirtyPages(dptSnapshot);
    }

    // Clear dirty page table for pages in the snapshot
    for (const pageKey of dptSnapshot.keys()) {
      this._dirtyPageTable.delete(pageKey);
    }

    // Write END_CHECKPOINT
    this._lsn++;
    const endLsn = Number(this._lsn);
    const endRec = {
      lsn: endLsn,
      type: WAL_TYPES.END_CHECKPOINT,
      typeName: 'END_CHECKPOINT',
      data: {},
      txId: null,
      table: null,
      after: { beginCheckpointLsn: beginLsn },
      timestamp: Date.now(),
    };
    this._memRecords.push(endRec);
    this._stableRecords.push(endRec);

    this._lastCheckpointLsn = BigInt(endLsn);
    this.writer.stats.checkpoints = (this.writer.stats.checkpoints || 0) + 1;

    // Truncate WAL records before the minimum recLSN in the snapshot
    let truncatedCount = 0;
    if (dptSnapshot.size > 0) {
      const minRecLsn = Math.min(...dptSnapshot.values());
      const before = this._stableRecords.length;
      this._stableRecords = this._stableRecords.filter(r =>
        r.lsn >= minRecLsn || r.type === WAL_TYPES.BEGIN_CHECKPOINT || r.type === WAL_TYPES.END_CHECKPOINT
      );
      truncatedCount = before - this._stableRecords.length;
    }

    return {
      beginLsn,
      endLsn,
      dirtyPages: dptSnapshot.size,
      activeTxns: activeTxnSnapshot.size,
      truncatedCount,
    };
  }

  truncate(beforeLsn) {
    if (this._inMemory) {
      const before = this._stableRecords.length;
      this._stableRecords = this._stableRecords.filter(r => r.lsn >= beforeLsn);
      return before - this._stableRecords.length;
    }
    return 0;
  }

  *recover() {
    if (this._inMemory) {
      yield* this._stableRecords;
      return;
    }
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
  logDropTable(name) { return this.writeRecord('DROP_TABLE', { table: name }); }
  logTruncate(name, txId) { return this.writeRecord('TRUNCATE', { table: name, txId }); }
}

// Extended type codes used by checkpoint/recovery subsystems
const WAL_TYPES = {
  ...RECORD_TYPES,
  ABORT: 6,  // Same as ROLLBACK for backward compat
  BEGIN_CHECKPOINT: 12,
  END_CHECKPOINT: 13,
};

const WAL_TYPE_NAMES = Object.fromEntries(
  Object.entries(WAL_TYPES).map(([k, v]) => [v, k])
);

// Structured WAL record for serialize/deserialize (used by stress tests and recovery)
class WALRecord {
  constructor(lsn, txId, type, table, pageId, slotIdx, before, after) {
    this.lsn = lsn;
    this.txId = txId;
    this.type = type;
    this.tableName = table || null;
    this.table = table || null; // alias
    this.pageId = pageId || 0;
    this.slotIdx = slotIdx || 0;
    this.before = before || null;
    this.after = after || null;
    this.timestamp = Date.now();
  }

  serialize() {
    const payload = JSON.stringify({
      lsn: this.lsn,
      txId: this.txId,
      type: this.type,
      tableName: this.tableName,
      pageId: this.pageId,
      slotIdx: this.slotIdx,
      before: this.before,
      after: this.after,
      timestamp: this.timestamp,
    });
    const payloadBuf = Buffer.from(payload, 'utf8');
    const lenBuf = Buffer.alloc(4);
    lenBuf.writeUInt32BE(payloadBuf.length, 0);
    // CRC32 of payload for corruption detection
    const crcVal = crc32(payloadBuf);
    const crcBuf = Buffer.alloc(4);
    crcBuf.writeUInt32BE(crcVal, 0);
    return Buffer.concat([lenBuf, payloadBuf, crcBuf]);
  }

  static deserialize(buf, offset = 0) {
    if (!buf || buf.length < offset + 4) {
      return null;
    }
    const len = buf.readUInt32BE(offset);
    if (len === 0 || len > 10 * 1024 * 1024) {
      return null;
    }
    if (buf.length < offset + 4 + len + 4) {
      return null;
    }
    const payloadBuf = buf.subarray(offset + 4, offset + 4 + len);
    const storedCrc = buf.readUInt32BE(offset + 4 + len);
    const computedCrc = crc32(payloadBuf);
    if (storedCrc !== computedCrc) {
      return null;
    }
    try {
      const payload = JSON.parse(payloadBuf.toString('utf8'));
      const record = new WALRecord(
        payload.lsn, payload.txId, payload.type,
        payload.tableName || payload.table, payload.pageId, payload.slotIdx,
        payload.before, payload.after
      );
      if (payload.timestamp) record.timestamp = payload.timestamp;
      return { record, bytesRead: 4 + len + 4 };
    } catch (e) {
      return null;
    }
  }
}

// Recovery: replay committed transactions from WAL into database
function recoverFromWAL(wal, db) {
  const records = wal.readFromStable ? wal.readFromStable(0) : (wal.getRecords ? wal.getRecords() : [...wal.reader.readRecords()]);

  // Find latest checkpoint — skip records before it
  let startIdx = 0;
  let usedFuzzyCheckpoint = false;
  for (let i = records.length - 1; i >= 0; i--) {
    if (records[i].type === WAL_TYPES.END_CHECKPOINT || records[i].type === RECORD_TYPES.CHECKPOINT) {
      startIdx = i + 1;
      usedFuzzyCheckpoint = records[i].type === WAL_TYPES.END_CHECKPOINT;
      break;
    }
  }
  const replayRecords = records.slice(startIdx);

  const txOps = new Map(); // txId -> [records]
  const committedTxs = new Set();
  const activeTxIds = new Set();
  let replayed = 0;

  for (const rec of replayRecords) {
    const txId = rec.data?.txId || rec.txId;
    const type = typeof rec.type === 'number'
      ? (RECORD_TYPE_NAMES[rec.type] || rec.type)
      : rec.type;

    if (type === 'BEGIN' || type === WAL_TYPES.BEGIN) {
      txOps.set(txId, []);
      activeTxIds.add(txId);
    } else if (type === 'COMMIT' || type === WAL_TYPES.COMMIT) {
      committedTxs.add(txId);
      activeTxIds.delete(txId);
    } else if (type === 'ROLLBACK' || type === WAL_TYPES.ROLLBACK || type === 'ABORT') {
      txOps.delete(txId);
      activeTxIds.delete(txId);
    } else if (rec.type !== WAL_TYPES.BEGIN_CHECKPOINT && rec.type !== WAL_TYPES.END_CHECKPOINT && rec.type !== RECORD_TYPES.CHECKPOINT) {
      if (!txOps.has(txId)) txOps.set(txId, []);
      txOps.get(txId).push(rec);
      if (txId !== undefined && txId !== null) activeTxIds.add(txId);
    }
  }

  // Remove committed from active set
  for (const txId of committedTxs) activeTxIds.delete(txId);

  for (const txId of committedTxs) {
    const ops = txOps.get(txId) || [];
    for (const op of ops) {
      const type = typeof op.type === 'number' ? (RECORD_TYPE_NAMES[op.type] || op.type) : op.type;
      const table = op.data?.table || op.table;
      try {
        if (type === 'INSERT' && table && db.tables?.get(table)) {
          const row = op.data?.row?.values || op.data?.row || op.after;
          if (row) {
            const tableObj = db.tables.get(table);
            if (tableObj.heap) { tableObj.heap.insert(row); replayed++; }
            else if (db.execute) {
              // Database object: use SQL to insert
              const colNames = tableObj.schema.map(c => c.name);
              const vals = (Array.isArray(row) ? row : Object.values(row)).map(v =>
                v === null ? 'NULL' : typeof v === 'number' ? String(v) : `'${String(v).replace(/'/g, "''")}'`
              );
              db.execute(`INSERT INTO ${table} (${colNames.join(', ')}) VALUES (${vals.join(', ')})`);
              replayed++;
            }
          }
        } else if (type === 'UPDATE' && table && db.tables?.get(table)) {
          const tableObj = db.tables.get(table);
          const oldRow = op.data?.old?.values || op.data?.old || op.before;
          const newRow = op.data?.new?.values || op.data?.new || op.after;
          if (oldRow && newRow && db.execute) {
            const colNames = tableObj.schema.map(c => c.name);
            // Build SET clause
            const setClauses = colNames.map((c, i) => {
              const val = newRow[i];
              return `${c} = ${val === null ? 'NULL' : typeof val === 'number' ? val : `'${String(val).replace(/'/g, "''")}'`}`;
            }).join(', ');
            // Build WHERE clause from old values (use PK if available)
            const pkIdx = tableObj.schema.findIndex(c => c.primaryKey);
            let where;
            if (pkIdx >= 0) {
              const pkVal = oldRow[pkIdx];
              where = `${colNames[pkIdx]} = ${typeof pkVal === 'number' ? pkVal : `'${pkVal}'`}`;
            } else {
              where = colNames.map((c, i) => {
                const v = oldRow[i];
                return v === null ? `${c} IS NULL` : `${c} = ${typeof v === 'number' ? v : `'${String(v).replace(/'/g, "''")}'`}`;
              }).join(' AND ');
            }
            db.execute(`UPDATE ${table} SET ${setClauses} WHERE ${where}`);
            replayed++;
          } else if (tableObj.heap) {
            replayed++;
          }
        } else if (type === 'DELETE' && table && db.tables?.get(table)) {
          const tableObj = db.tables.get(table);
          const row = op.data?.row?.values || op.data?.row || op.before || op.after;
          if (row && db.execute) {
            const colNames = tableObj.schema.map(c => c.name);
            const pkIdx = tableObj.schema.findIndex(c => c.primaryKey);
            let where;
            if (pkIdx >= 0) {
              const pkVal = Array.isArray(row) ? row[pkIdx] : row[colNames[pkIdx]];
              where = `${colNames[pkIdx]} = ${typeof pkVal === 'number' ? pkVal : `'${pkVal}'`}`;
            } else {
              where = colNames.map((c, i) => {
                const v = Array.isArray(row) ? row[i] : row[c];
                return v === null ? `${c} IS NULL` : `${c} = ${typeof v === 'number' ? v : `'${String(v).replace(/'/g, "''")}'`}`;
              }).join(' AND ');
            }
            db.execute(`DELETE FROM ${table} WHERE ${where}`);
            replayed++;
          } else if (tableObj.heap) {
            replayed++;
          }
        } else if (type === 'TRUNCATE' && table && db.tables?.get(table)) {
          const tableObj = db.tables.get(table);
          if (db.execute) {
            db.execute(`DELETE FROM ${table} WHERE 1=1`);
            replayed++;
          } else if (tableObj.heap) {
            // Direct heap access: clear it
            tableObj.heap = db._heapFactory ? db._heapFactory() : { _data: [], scan: function*(){}, insert(){}, rowCount: 0 };
            replayed++;
          }
        } else if (type === 'DROP_TABLE' && table) {
          if (db.execute) {
            try { db.execute(`DROP TABLE IF EXISTS ${table}`); } catch {}
          } else if (db.tables) {
            db.tables.delete(table);
          }
          replayed++;
        }
      } catch { /* skip replay errors */ }
    }
  }

  return {
    replayed,
    redone: replayed,
    committedTxns: committedTxs.size,
    committedTransactions: committedTxs.size,
    activeTxns: activeTxIds.size,
    totalRecords: records.length,
    usedFuzzyCheckpoint,
  };
}

// Point-in-time recovery: replay WAL up to a given timestamp
function recoverToTimestamp(wal, db, targetTimestamp) {
  const records = wal.readFromStable ? wal.readFromStable(0) : (wal.getRecords ? wal.getRecords() : [...wal.reader.readRecords()]);
  const targetMs = targetTimestamp instanceof Date ? targetTimestamp.getTime() : targetTimestamp;
  const filteredRecords = records.filter(r => {
    const ts = r.timestamp || r.data?.timestamp || 0;
    return ts <= targetMs;
  });

  const txOps = new Map();
  const committedTxs = new Set();
  const txCommitTimestamps = {};
  let replayed = 0;

  for (const rec of filteredRecords) {
    const txId = rec.data?.txId || rec.txId;
    const type = typeof rec.type === 'number' ? (RECORD_TYPE_NAMES[rec.type] || rec.type) : rec.type;

    if (type === 'BEGIN') { txOps.set(txId, []); }
    else if (type === 'COMMIT') {
      committedTxs.add(txId);
      txCommitTimestamps[txId] = rec.timestamp || Date.now();
    }
    else if (type === 'ROLLBACK' || type === 'ABORT') { txOps.delete(txId); }
    else if (rec.type !== WAL_TYPES.BEGIN_CHECKPOINT && rec.type !== WAL_TYPES.END_CHECKPOINT && rec.type !== RECORD_TYPES.CHECKPOINT) {
      if (!txOps.has(txId)) txOps.set(txId, []);
      txOps.get(txId).push(rec);
    }
  }

  for (const txId of committedTxs) {
    const ops = txOps.get(txId) || [];
    for (const op of ops) {
      const type = typeof op.type === 'number' ? (RECORD_TYPE_NAMES[op.type] || op.type) : op.type;
      const table = op.data?.table || op.table;
      try {
        if (type === 'INSERT' && table && db.tables?.get(table)) {
          const row = op.data?.row?.values || op.data?.row || op.after;
          if (row) {
            const tableObj = db.tables.get(table);
            if (tableObj.heap?.insert) { tableObj.heap.insert(row); replayed++; }
          }
        } else if (type === 'DELETE' && table && db.tables?.get(table)) {
          const tableObj = db.tables.get(table);
          const row = op.data?.row || {};
          if (tableObj.heap?.delete) {
            tableObj.heap.delete(row._pageId || 0, row._slotIdx || 0);
            replayed++;
          }
        } else if (type === 'UPDATE' && table && db.tables?.get(table)) {
          const tableObj = db.tables.get(table);
          const oldRow = op.data?.old?.values || op.data?.old || op.before;
          const newRow = op.data?.new?.values || op.data?.new || op.after;
          if (oldRow && newRow && tableObj.heap) {
            // For mock heaps: find and replace
            if (tableObj.heap._data) {
              const idx = tableObj.heap._data.findIndex(r => r && JSON.stringify(r) === JSON.stringify(oldRow));
              if (idx >= 0) tableObj.heap._data[idx] = newRow;
              else tableObj.heap.insert(newRow); // fallback: insert new value
            } else if (db.execute) {
              // Full Database: use SQL
              const colNames = tableObj.schema.map(c => c.name);
              const setClauses = colNames.map((c, i) => {
                const val = newRow[i];
                return `${c} = ${val === null ? 'NULL' : typeof val === 'number' ? val : `'${String(val).replace(/'/g, "''")}'`}`;
              }).join(', ');
              const pkIdx = tableObj.schema.findIndex(c => c.primaryKey);
              let where;
              if (pkIdx >= 0) {
                const pkVal = oldRow[pkIdx];
                where = `${colNames[pkIdx]} = ${typeof pkVal === 'number' ? pkVal : `'${pkVal}'`}`;
              } else {
                where = '1=1';
              }
              db.execute(`UPDATE ${table} SET ${setClauses} WHERE ${where}`);
            }
            replayed++;
          }
        } else if (type === 'TRUNCATE' && table && db.tables?.get(table)) {
          if (db.execute) {
            db.execute(`DELETE FROM ${table} WHERE 1=1`);
          } else {
            const tableObj = db.tables.get(table);
            if (tableObj.heap) {
              tableObj.heap = db._heapFactory ? db._heapFactory() : { _data: [], scan: function*(){}, insert(){}, rowCount: 0 };
            }
          }
          replayed++;
        } else if (type === 'DROP_TABLE' && table) {
          if (db.execute) {
            try { db.execute(`DROP TABLE IF EXISTS ${table}`); } catch {}
          } else if (db.tables) {
            db.tables.delete(table);
          }
          replayed++;
        }
      } catch { /* skip */ }
    }
  }

  // Count skipped txns (committed after target timestamp)
  const allRecords = wal.readFromStable ? wal.readFromStable(0) : records;
  const allCommittedTxs = new Set();
  for (const rec of allRecords) {
    const type = typeof rec.type === 'number' ? (RECORD_TYPE_NAMES[rec.type] || rec.type) : rec.type;
    if (type === 'COMMIT') allCommittedTxs.add(rec.data?.txId || rec.txId);
  }
  const skippedTxns = allCommittedTxs.size - committedTxs.size;

  return {
    replayed,
    redone: replayed,
    committedTxns: committedTxs.size,
    committedTransactions: committedTxs.size,
    skippedTxns,
    txCommitTimestamps,
    totalRecords: filteredRecords.length,
    targetTimestamp: new Date(targetMs).toISOString(),
  };
}

export { RECORD_TYPES, RECORD_TYPE_NAMES, WAL_TYPES, WAL_TYPE_NAMES, WALRecord, crc32, HEADER_SIZE, FOOTER_SIZE, recoverFromWAL, recoverToTimestamp };

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
  if (!this._inMemory && (!this.writer.fd && this.writer.fd !== 0)) return;
  return this.logInsert(table, { _pageId: pageId, _slotIdx: slotIdx, values }, txId);
};

_proto.appendCommit = function(txId) {
  if (!this._inMemory && (!this.writer.fd && this.writer.fd !== 0)) return;
  return this.logCommit(txId);
};

_proto.appendUpdate = function(txId, table, pageId, slotIdx, oldValues, newValues) {
  if (!this._inMemory && (!this.writer.fd && this.writer.fd !== 0)) return;
  return this.logUpdate(table, { _pageId: pageId, _slotIdx: slotIdx, values: oldValues }, { _pageId: pageId, _slotIdx: slotIdx, values: newValues }, txId);
};

_proto.appendDelete = function(txId, table, pageId, slotIdx, values) {
  if (!this._inMemory && (!this.writer.fd && this.writer.fd !== 0)) return;
  return this.logDelete(table, { _pageId: pageId, _slotIdx: slotIdx, values }, txId);
};

_proto.appendAbort = function(txId) {
  if (!this._inMemory && (!this.writer.fd && this.writer.fd !== 0)) return;
  return this.writeRecord('ROLLBACK', { txId });
};

_proto.appendTruncate = function(txId, table) {
  if (!this._inMemory && (!this.writer.fd && this.writer.fd !== 0)) return;
  return this.logTruncate(table, txId);
};

_proto.beginTransaction = function(txId) {
  // Track active transaction (no WAL record — recovery uses COMMIT presence)
  this._activeTxns.set(txId, { status: 'active', startLsn: Number(this._lsn) });
  return txId;
};

_proto.isCommitted = function(txId) {
  // Check active transaction tracking
  const tx = this._activeTxns && this._activeTxns.get(txId);
  if (tx && tx.status === 'committed') return true;
  // Check in-memory WAL buffer for COMMIT record
  if (this._buffer) {
    for (const rec of this._buffer) {
      if ((rec.type === 'COMMIT' || rec.type === 5) && (rec.txId === txId || (rec.data && rec.data.txId === txId))) return true;
    }
  }
  if (this._records) {
    for (const rec of this._records) {
      if ((rec.type === 'COMMIT' || rec.type === 5) && (rec.txId === txId || (rec.data && rec.data.txId === txId))) return true;
    }
  }
  return false;
};

_proto.flush = function() {
  if (this._inMemory) {
    this._flushToStable();
    return;
  }
  if (this.writer.sync) this.writer.sync();
};

_proto.forceToLsn = function(lsn) {
  // Force WAL to be stable up to the given LSN
  this.flush();
  // Track the forced LSN
  this._lastForcedLsn = Math.max(this._lastForcedLsn || 0, lsn);
  this._flushedLsn = Math.max(this._flushedLsn || 0, lsn);
};

_proto.getLastForcedLsn = function() {
  return this._lastForcedLsn || 0;
};

Object.defineProperty(_proto, 'flushedLsn', {
  get() { return this._flushedLsn || 0; }
});

_proto.setAutoCheckpoint = function(threshold, callback) {
  if (threshold <= 0) {
    this._autoCheckpoint = false;
    this._autoCheckpointByCommits = false;
    this._checkpointCallback = null;
    return;
  }
  this._autoCheckpoint = true;
  this.checkpointInterval = threshold;
  this._commitsSinceCheckpoint = 0;
  this._checkpointCallback = callback || null;
  this._autoCheckpointByCommits = true;
};

_proto.getCheckpointCount = function() {
  return this.writer.stats.checkpoints || 0;
};

// Override getStats for in-memory mode
const _origGetStats = WALManager.prototype.getStats;
_proto.getStats = function() {
  const stats = _origGetStats ? _origGetStats.call(this) : { ...this.writer.stats };
  stats.commitsSinceCheckpoint = this._commitsSinceCheckpoint || 0;
  stats.activeTxns = [...this._activeTxns].filter(([_, v]) => v.status === 'active').length;
  stats.dirtyPages = this._dirtyPageTable.size;
  stats.nextLsn = Number(this._lsn) + 1;
  stats.stableRecords = this._stableRecords.length;
  return stats;
};

_proto.getRecords = function() {
  return [...this.reader.readRecords()];
};
