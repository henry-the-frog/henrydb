// wal.js — Write-Ahead Log for HenryDB
// Implements sequential, append-only logging for durability.
//
// WAL Record Format (in-memory):
//   { lsn, txId, type, tableName, pageId, slotIdx, before, after }
//
// Types:
//   INSERT  — after contains new tuple data
//   DELETE  — before contains removed tuple data
//   UPDATE  — before + after contain old/new tuple data
//   COMMIT  — marks transaction as committed
//   ABORT   — marks transaction as aborted
//   CHECKPOINT — marks a checkpoint (all dirty pages flushed)
//
// Binary format (for serialization):
//   [4 bytes: record length]
//   [8 bytes: LSN]
//   [4 bytes: txId]
//   [1 byte: type enum]
//   [2 bytes: tableName length] [N bytes: tableName]
//   [4 bytes: pageId]
//   [2 bytes: slotIdx]
//   [4 bytes: before length] [N bytes: before (JSON)]
//   [4 bytes: after length] [N bytes: after (JSON)]
//   [4 bytes: CRC32 checksum]

const WAL_TYPES = {
  INSERT: 1,
  DELETE: 2,
  UPDATE: 3,
  COMMIT: 4,
  ABORT: 5,
  CHECKPOINT: 6,
};

const WAL_TYPE_NAMES = Object.fromEntries(
  Object.entries(WAL_TYPES).map(([k, v]) => [v, k])
);

// Simple CRC32 for integrity checking
function crc32(data) {
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < data.length; i++) {
    crc ^= data[i];
    for (let j = 0; j < 8; j++) {
      crc = (crc >>> 1) ^ (crc & 1 ? 0xEDB88320 : 0);
    }
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

export class WALRecord {
  constructor(lsn, txId, type, tableName = '', pageId = -1, slotIdx = -1, before = null, after = null) {
    this.lsn = lsn;
    this.txId = txId;
    this.type = type;
    this.tableName = tableName;
    this.pageId = pageId;
    this.slotIdx = slotIdx;
    this.before = before;
    this.after = after;
  }

  get typeName() {
    return WAL_TYPE_NAMES[this.type] || 'UNKNOWN';
  }

  /**
   * Serialize to a Buffer for stable storage.
   */
  serialize() {
    const tableNameBuf = Buffer.from(this.tableName, 'utf8');
    const beforeBuf = this.before != null ? Buffer.from(JSON.stringify(this.before), 'utf8') : Buffer.alloc(0);
    const afterBuf = this.after != null ? Buffer.from(JSON.stringify(this.after), 'utf8') : Buffer.alloc(0);

    // Calculate total size (excluding the 4-byte length prefix)
    const bodySize = 8 + 4 + 1 + 2 + tableNameBuf.length + 4 + 2 + 4 + beforeBuf.length + 4 + afterBuf.length;
    const totalSize = 4 + bodySize + 4; // length prefix + body + CRC

    const buf = Buffer.alloc(totalSize);
    let offset = 0;

    // Record length (not including the length field itself)
    buf.writeUInt32LE(bodySize + 4, offset); offset += 4; // +4 for CRC

    // LSN (8 bytes as two 32-bit ints for simplicity — JS doesn't have native 64-bit)
    buf.writeUInt32LE(this.lsn & 0xFFFFFFFF, offset); offset += 4;
    buf.writeUInt32LE(Math.floor(this.lsn / 0x100000000), offset); offset += 4;

    // txId
    buf.writeUInt32LE(this.txId, offset); offset += 4;

    // type
    buf.writeUInt8(this.type, offset); offset += 1;

    // tableName
    buf.writeUInt16LE(tableNameBuf.length, offset); offset += 2;
    tableNameBuf.copy(buf, offset); offset += tableNameBuf.length;

    // pageId
    buf.writeInt32LE(this.pageId, offset); offset += 4;

    // slotIdx
    buf.writeInt16LE(this.slotIdx, offset); offset += 2;

    // before data
    buf.writeUInt32LE(beforeBuf.length, offset); offset += 4;
    beforeBuf.copy(buf, offset); offset += beforeBuf.length;

    // after data
    buf.writeUInt32LE(afterBuf.length, offset); offset += 4;
    afterBuf.copy(buf, offset); offset += afterBuf.length;

    // CRC32 over the body (everything between length prefix and CRC)
    const bodyData = buf.subarray(4, offset);
    buf.writeUInt32LE(crc32(bodyData), offset); offset += 4;

    return buf;
  }

  /**
   * Deserialize from a Buffer. Returns { record, bytesRead } or null on corruption.
   */
  static deserialize(buf, startOffset = 0) {
    if (buf.length - startOffset < 4) return null;
    let offset = startOffset;

    const recordLen = buf.readUInt32LE(offset); offset += 4;
    if (buf.length - offset < recordLen) return null;

    const bodyStart = offset;

    // LSN
    const lsnLow = buf.readUInt32LE(offset); offset += 4;
    const lsnHigh = buf.readUInt32LE(offset); offset += 4;
    const lsn = lsnHigh * 0x100000000 + lsnLow;

    // txId
    const txId = buf.readUInt32LE(offset); offset += 4;

    // type
    const type = buf.readUInt8(offset); offset += 1;

    // tableName
    const tableNameLen = buf.readUInt16LE(offset); offset += 2;
    const tableName = buf.subarray(offset, offset + tableNameLen).toString('utf8'); offset += tableNameLen;

    // pageId
    const pageId = buf.readInt32LE(offset); offset += 4;

    // slotIdx
    const slotIdx = buf.readInt16LE(offset); offset += 2;

    // before
    const beforeLen = buf.readUInt32LE(offset); offset += 4;
    const before = beforeLen > 0 ? JSON.parse(buf.subarray(offset, offset + beforeLen).toString('utf8')) : null;
    offset += beforeLen;

    // after
    const afterLen = buf.readUInt32LE(offset); offset += 4;
    const after = afterLen > 0 ? JSON.parse(buf.subarray(offset, offset + afterLen).toString('utf8')) : null;
    offset += afterLen;

    // Verify CRC
    const bodyData = buf.subarray(bodyStart, offset);
    const expectedCrc = buf.readUInt32LE(offset); offset += 4;
    const actualCrc = crc32(bodyData);

    if (expectedCrc !== actualCrc) {
      return null; // Corrupted record
    }

    const record = new WALRecord(lsn, txId, type, tableName, pageId, slotIdx, before, after);
    return { record, bytesRead: offset - startOffset };
  }
}

/**
 * Write-Ahead Log manager.
 * Maintains an append-only log of all database modifications.
 */
export class WriteAheadLog {
  constructor() {
    this._nextLsn = 1;
    this._records = [];        // In-memory log buffer
    this._flushedLsn = 0;      // Last LSN written to stable storage
    this._stableStorage = [];   // Simulates disk — array of serialized buffers
    this._committedTxns = new Set();
    this._activeTxns = new Set();
    this._lastCheckpointLsn = 0;
  }

  get nextLsn() { return this._nextLsn; }
  get flushedLsn() { return this._flushedLsn; }
  get lastCheckpointLsn() { return this._lastCheckpointLsn; }

  /**
   * Append a log record. Returns the LSN.
   */
  appendInsert(txId, tableName, pageId, slotIdx, afterData) {
    return this._append(txId, WAL_TYPES.INSERT, tableName, pageId, slotIdx, null, afterData);
  }

  appendDelete(txId, tableName, pageId, slotIdx, beforeData) {
    return this._append(txId, WAL_TYPES.DELETE, tableName, pageId, slotIdx, beforeData, null);
  }

  appendUpdate(txId, tableName, pageId, slotIdx, beforeData, afterData) {
    return this._append(txId, WAL_TYPES.UPDATE, tableName, pageId, slotIdx, beforeData, afterData);
  }

  appendCommit(txId) {
    const lsn = this._append(txId, WAL_TYPES.COMMIT);
    this._committedTxns.add(txId);
    this._activeTxns.delete(txId);
    return lsn;
  }

  appendAbort(txId) {
    const lsn = this._append(txId, WAL_TYPES.ABORT);
    this._activeTxns.delete(txId);
    return lsn;
  }

  checkpoint() {
    const lsn = this._append(0, WAL_TYPES.CHECKPOINT);
    this._lastCheckpointLsn = lsn;
    this.flush(); // Force flush on checkpoint
    return lsn;
  }

  beginTransaction(txId) {
    this._activeTxns.add(txId);
  }

  /**
   * Force all buffered records to stable storage (simulated).
   * In a real DB this would be fsync().
   */
  flush() {
    for (const record of this._records) {
      if (record.lsn > this._flushedLsn) {
        this._stableStorage.push(record.serialize());
        this._flushedLsn = record.lsn;
      }
    }
  }

  /**
   * Force-at-commit: flush WAL up to the commit record's LSN.
   * Called automatically by appendCommit.
   */
  forceToLsn(targetLsn) {
    for (const record of this._records) {
      if (record.lsn > this._flushedLsn && record.lsn <= targetLsn) {
        this._stableStorage.push(record.serialize());
        this._flushedLsn = record.lsn;
      }
    }
  }

  /**
   * Read all records from stable storage (for recovery).
   * Returns records from afterLsn onwards.
   */
  readFromStable(afterLsn = 0) {
    const records = [];
    for (const buf of this._stableStorage) {
      const result = WALRecord.deserialize(buf);
      if (result && result.record.lsn > afterLsn) {
        records.push(result.record);
      }
    }
    return records;
  }

  /**
   * Get all records in the in-memory buffer.
   */
  getRecords() {
    return [...this._records];
  }

  /**
   * Get records for a specific transaction.
   */
  getTransactionRecords(txId) {
    return this._records.filter(r => r.txId === txId);
  }

  isCommitted(txId) {
    return this._committedTxns.has(txId);
  }

  _append(txId, type, tableName = '', pageId = -1, slotIdx = -1, before = null, after = null) {
    const lsn = this._nextLsn++;
    const record = new WALRecord(lsn, txId, type, tableName, pageId, slotIdx, before, after);
    this._records.push(record);
    
    // Force-at-commit: automatically flush when committing
    if (type === WAL_TYPES.COMMIT) {
      this.forceToLsn(lsn);
    }
    
    return lsn;
  }
}

export { WAL_TYPES, WAL_TYPE_NAMES, crc32 };

/**
 * ARIES-style crash recovery.
 * Given a WriteAheadLog and a Database, replays committed transactions
 * from the last checkpoint to reconstruct consistent state.
 * 
 * Three phases:
 * 1. Analysis: scan WAL to find committed and active transactions
 * 2. Redo: replay all operations from committed transactions
 * 3. Undo: nothing to undo for committed transactions (in-memory model)
 */
export function recoverFromWAL(wal, db) {
  const lastCheckpoint = wal.lastCheckpointLsn;
  const records = wal.readFromStable(lastCheckpoint);
  
  // Phase 1: Analysis — find committed transactions
  const committedTxns = new Set();
  const abortedTxns = new Set();
  const activeTxns = new Set();
  
  for (const record of records) {
    if (record.type === WAL_TYPES.COMMIT) {
      committedTxns.add(record.txId);
      activeTxns.delete(record.txId);
    } else if (record.type === WAL_TYPES.ABORT) {
      abortedTxns.add(record.txId);
      activeTxns.delete(record.txId);
    } else if (record.type !== WAL_TYPES.CHECKPOINT) {
      activeTxns.add(record.txId);
    }
  }
  
  // Phase 2: Redo — replay committed transaction operations
  let redone = 0;
  for (const record of records) {
    // Only replay committed transactions
    if (!committedTxns.has(record.txId)) continue;
    
    const table = db.tables.get(record.tableName);
    if (!table) continue; // Table might not exist yet in recovery
    
    switch (record.type) {
      case WAL_TYPES.INSERT: {
        if (record.after) {
          table.heap.insert(record.after);
          // Update indexes
          for (const [colName, index] of table.indexes) {
            const colIdx = table.schema.findIndex(c => c.name === colName);
            if (colIdx >= 0) {
              const rid = { pageId: record.pageId, slotIdx: record.slotIdx };
              index.insert(record.after[colIdx], rid);
            }
          }
        }
        redone++;
        break;
      }
      case WAL_TYPES.DELETE: {
        if (record.pageId >= 0 && record.slotIdx >= 0) {
          table.heap.delete(record.pageId, record.slotIdx);
        }
        redone++;
        break;
      }
      case WAL_TYPES.UPDATE: {
        if (record.after && record.pageId >= 0) {
          // Delete old, insert new (same as normal update path)
          table.heap.delete(record.pageId, record.slotIdx);
          const newRid = table.heap.insert(record.after);
          // Update indexes
          for (const [colName, index] of table.indexes) {
            const colIdx = table.schema.findIndex(c => c.name === colName);
            if (colIdx >= 0) {
              index.insert(record.after[colIdx], newRid);
            }
          }
        }
        redone++;
        break;
      }
    }
  }
  
  return {
    committedTxns: committedTxns.size,
    abortedTxns: abortedTxns.size,
    activeTxns: activeTxns.size, // These were in-flight at crash — data lost
    redone,
  };
}
