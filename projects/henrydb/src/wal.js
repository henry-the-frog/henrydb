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
  BEGIN_CHECKPOINT: 7,
  END_CHECKPOINT: 8,
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
    this.timestamp = Date.now(); // Wall-clock time for PITR
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
    this._dirtyPageTable = new Map(); // pageKey -> firstDirtyLsn (recLSN)
    this._checkpointInProgress = false;
    this._commitsSinceCheckpoint = 0;
    this._autoCheckpointThreshold = 0; // 0 = disabled
    this._onAutoCheckpoint = null; // callback for auto-checkpoint
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

  /**
   * ARIES-style fuzzy checkpoint.
   * 1. Write BEGIN_CHECKPOINT with active transaction table + dirty page table
   * 2. Caller flushes dirty pages (async-safe — new writes can continue)
   * 3. Write END_CHECKPOINT referencing the BEGIN
   * 4. Truncate WAL records before the min(recLSN) in dirty page table
   *
   * The dirty page table tracks {pageKey -> recLSN} where recLSN is the
   * LSN of the first modification to that page since the last checkpoint.
   *
   * @param {Object} options
   * @param {Function} [options.flushDirtyPages] - callback to flush dirty pages to disk
   * @returns {{ beginLsn, endLsn, truncatedBefore, dirtyPages, activeTxns }}
   */
  fuzzyCheckpoint(options = {}) {
    const { flushDirtyPages } = options;

    // Snapshot current state
    const activeTxnSnapshot = [...this._activeTxns];
    const dirtyPageSnapshot = new Map(this._dirtyPageTable);

    // Phase 1: BEGIN_CHECKPOINT — record state
    const beginLsn = this._append(0, WAL_TYPES.BEGIN_CHECKPOINT, '', -1, -1,
      null,
      {
        activeTxns: activeTxnSnapshot,
        dirtyPageTable: [...dirtyPageSnapshot.entries()].map(([k, v]) => ({ pageKey: k, recLSN: v })),
      }
    );
    this._checkpointInProgress = true;
    this.flush();

    // Phase 2: Flush dirty pages (if callback provided)
    if (flushDirtyPages) {
      flushDirtyPages(dirtyPageSnapshot);
    }

    // Phase 3: END_CHECKPOINT — reference the begin
    const endLsn = this._append(0, WAL_TYPES.END_CHECKPOINT, '', -1, -1,
      null,
      { beginCheckpointLsn: beginLsn }
    );
    this._lastCheckpointLsn = endLsn;
    this._checkpointInProgress = false;
    this.flush();

    // Phase 4: Truncate WAL — safe to discard records before min recLSN
    let truncateBefore = beginLsn; // At minimum, we can truncate up to begin
    if (dirtyPageSnapshot.size > 0) {
      const minRecLSN = Math.min(...dirtyPageSnapshot.values());
      truncateBefore = Math.min(truncateBefore, minRecLSN);
    }
    const truncated = this.truncate(truncateBefore);

    // Clear dirty page table for flushed pages
    for (const [pageKey] of dirtyPageSnapshot) {
      // Only clear if recLSN hasn't been updated since snapshot
      if (this._dirtyPageTable.get(pageKey) === dirtyPageSnapshot.get(pageKey)) {
        this._dirtyPageTable.delete(pageKey);
      }
    }

    return {
      beginLsn,
      endLsn,
      truncatedBefore: truncateBefore,
      truncatedCount: truncated,
      dirtyPages: dirtyPageSnapshot.size,
      activeTxns: activeTxnSnapshot.length,
    };
  }

  /**
   * Mark a page as dirty with the given LSN as its recLSN.
   * recLSN = LSN of the first modification since last checkpoint.
   * Only sets if not already tracked (first-write wins).
   */
  markPageDirty(tableName, pageId, lsn) {
    const pageKey = `${tableName}:${pageId}`;
    if (!this._dirtyPageTable.has(pageKey)) {
      this._dirtyPageTable.set(pageKey, lsn);
    }
  }

  /**
   * Get current dirty page table (for inspection/testing).
   */
  getDirtyPageTable() {
    return new Map(this._dirtyPageTable);
  }

  /**
   * Truncate WAL records with LSN < beforeLsn.
   * Removes from both in-memory buffer and stable storage.
   * Returns number of records truncated.
   */
  truncate(beforeLsn) {
    const beforeCount = this._records.length + this._stableStorage.length;

    // Truncate in-memory buffer
    this._records = this._records.filter(r => r.lsn >= beforeLsn);

    // Truncate stable storage
    this._stableStorage = this._stableStorage.filter(buf => {
      const result = WALRecord.deserialize(buf);
      return result && result.record.lsn >= beforeLsn;
    });

    const afterCount = this._records.length + this._stableStorage.length;
    return beforeCount - afterCount;
  }

  /**
   * Get WAL size stats (for monitoring).
   */
  getStats() {
    return {
      inMemoryRecords: this._records.length,
      stableRecords: this._stableStorage.length,
      nextLsn: this._nextLsn,
      flushedLsn: this._flushedLsn,
      lastCheckpointLsn: this._lastCheckpointLsn,
      dirtyPages: this._dirtyPageTable.size,
      activeTxns: this._activeTxns.size,
      commitsSinceCheckpoint: this._commitsSinceCheckpoint,
    };
  }

  /**
   * Configure automatic checkpointing after N commits.
   * Set threshold to 0 to disable.
   * @param {number} threshold - Number of commits between checkpoints
   * @param {Function} [callback] - Optional callback invoked on auto-checkpoint
   */
  setAutoCheckpoint(threshold, callback = null) {
    this._autoCheckpointThreshold = threshold;
    this._onAutoCheckpoint = callback;
  }

  /**
   * Check if auto-checkpoint should fire and do it.
   * Called internally after each COMMIT.
   * @returns {Object|null} Checkpoint result if triggered, null otherwise
   */
  _maybeAutoCheckpoint() {
    if (this._autoCheckpointThreshold <= 0) return null;
    if (this._checkpointInProgress) return null;
    if (this._commitsSinceCheckpoint < this._autoCheckpointThreshold) return null;

    const result = this.fuzzyCheckpoint();
    this._commitsSinceCheckpoint = 0;
    if (this._onAutoCheckpoint) {
      this._onAutoCheckpoint(result);
    }
    return result;
  }

  /**
   * Compact the WAL: remove records that are no longer needed for recovery.
   * 
   * Safe truncation point is determined by:
   * 1. The earliest active transaction's first record LSN
   * 2. The min recLSN in the dirty page table
   * 3. The last checkpoint LSN
   *
   * Records before min(above) can be safely removed.
   * 
   * @returns {{ truncatedCount, safeLsn, walSizeBefore, walSizeAfter }}
   */
  compact() {
    const candidates = [];

    // Last checkpoint establishes a baseline
    if (this._lastCheckpointLsn > 0) {
      candidates.push(this._lastCheckpointLsn);
    }

    // Can't truncate past any active transaction's records
    if (this._activeTxns.size > 0) {
      for (const record of this._records) {
        if (this._activeTxns.has(record.txId)) {
          candidates.push(record.lsn);
          break; // First record of any active txn
        }
      }
    }

    // Can't truncate past dirty page recLSNs
    if (this._dirtyPageTable.size > 0) {
      candidates.push(Math.min(...this._dirtyPageTable.values()));
    }

    if (candidates.length === 0) {
      return { truncatedCount: 0, safeLsn: 0, walSizeBefore: this._records.length, walSizeAfter: this._records.length };
    }

    const safeLsn = Math.min(...candidates);
    const walSizeBefore = this._records.length + this._stableStorage.length;
    const truncatedCount = this.truncate(safeLsn);
    const walSizeAfter = this._records.length + this._stableStorage.length;

    return { truncatedCount, safeLsn, walSizeBefore, walSizeAfter };
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
    
    // Auto-track dirty pages for data modification records
    if ((type === WAL_TYPES.INSERT || type === WAL_TYPES.DELETE || type === WAL_TYPES.UPDATE) && 
        tableName && pageId >= 0) {
      this.markPageDirty(tableName, pageId, lsn);
    }
    
    // Force-at-commit: automatically flush when committing
    if (type === WAL_TYPES.COMMIT) {
      this.forceToLsn(lsn);
      this._commitsSinceCheckpoint++;
      this._maybeAutoCheckpoint();
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
 * 1. Analysis: scan WAL from last checkpoint, rebuild active txn table + dirty page table
 * 2. Redo: replay all operations from committed transactions (from min recLSN)
 * 3. Undo: nothing to undo for committed transactions (in-memory model)
 *
 * Fuzzy checkpoint support:
 * - If END_CHECKPOINT found, reads BEGIN_CHECKPOINT for saved state
 * - Initializes analysis from checkpoint's active txn + dirty page tables
 * - Only redoes records from min(recLSN) forward, skipping already-flushed pages
 */
export function recoverFromWAL(wal, db) {
  const lastCheckpoint = wal.lastCheckpointLsn;
  const records = wal.readFromStable(0); // Read all records for checkpoint scanning
  
  // Find the effective start point
  let analysisStartLsn = 0;
  let initialActiveTxns = new Set();
  let initialDirtyPages = new Map(); // pageKey -> recLSN
  
  // Look for fuzzy checkpoint (END_CHECKPOINT -> BEGIN_CHECKPOINT)
  let endCheckpoint = null;
  for (let i = records.length - 1; i >= 0; i--) {
    if (records[i].type === WAL_TYPES.END_CHECKPOINT) {
      endCheckpoint = records[i];
      break;
    }
  }
  
  if (endCheckpoint && endCheckpoint.after) {
    const beginLsn = endCheckpoint.after.beginCheckpointLsn;
    // Find the matching BEGIN_CHECKPOINT
    for (const r of records) {
      if (r.type === WAL_TYPES.BEGIN_CHECKPOINT && r.lsn === beginLsn && r.after) {
        // Initialize from checkpoint state
        if (r.after.activeTxns) {
          for (const txId of r.after.activeTxns) initialActiveTxns.add(txId);
        }
        if (r.after.dirtyPageTable) {
          for (const entry of r.after.dirtyPageTable) {
            initialDirtyPages.set(entry.pageKey, entry.recLSN);
          }
        }
        analysisStartLsn = beginLsn;
        break;
      }
    }
  } else if (lastCheckpoint > 0) {
    // Fall back to simple checkpoint
    analysisStartLsn = lastCheckpoint;
  }
  
  // Phase 1: Analysis — scan from checkpoint, find committed and active transactions
  const committedTxns = new Set();
  const abortedTxns = new Set();
  const activeTxns = new Set(initialActiveTxns);
  const dirtyPages = new Map(initialDirtyPages);
  
  const analysisRecords = records.filter(r => r.lsn > analysisStartLsn);
  
  for (const record of analysisRecords) {
    if (record.type === WAL_TYPES.COMMIT) {
      committedTxns.add(record.txId);
      activeTxns.delete(record.txId);
    } else if (record.type === WAL_TYPES.ABORT) {
      abortedTxns.add(record.txId);
      activeTxns.delete(record.txId);
    } else if (record.type !== WAL_TYPES.CHECKPOINT && 
               record.type !== WAL_TYPES.BEGIN_CHECKPOINT && 
               record.type !== WAL_TYPES.END_CHECKPOINT) {
      activeTxns.add(record.txId);
      // Track dirty pages
      if (record.tableName && record.pageId >= 0) {
        const pageKey = `${record.tableName}:${record.pageId}`;
        if (!dirtyPages.has(pageKey)) {
          dirtyPages.set(pageKey, record.lsn);
        }
      }
    }
  }
  
  // Phase 2: Redo — replay committed transaction operations
  // Start from min(recLSN) in dirty page table (or analysisStartLsn)
  let redoStartLsn = analysisStartLsn;
  if (dirtyPages.size > 0) {
    const minRecLSN = Math.min(...dirtyPages.values());
    redoStartLsn = Math.min(redoStartLsn, minRecLSN);
  }
  
  const redoRecords = records.filter(r => r.lsn > redoStartLsn);
  let redone = 0;
  
  for (const record of redoRecords) {
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
    usedFuzzyCheckpoint: !!endCheckpoint,
    dirtyPagesAtCheckpoint: dirtyPages.size,
  };
}

/**
 * Point-in-Time Recovery (PITR).
 * Replays WAL records up to a target timestamp, recovering the database
 * to its state at that moment. Only transactions that committed before
 * the target timestamp are replayed.
 *
 * This enables "recover my database to how it was at 3pm yesterday"
 * — a critical feature for data recovery and disaster mitigation.
 *
 * @param {WriteAheadLog} wal - The WAL to replay from
 * @param {Object} db - Database with tables map
 * @param {number} targetTimestamp - Unix timestamp (ms) to recover to
 * @param {Object} [options]
 * @param {number} [options.fromLsn=0] - Start replay from this LSN (for checkpoint-based PITR)
 * @returns {{ committedTxns, skippedTxns, redone, targetTimestamp, actualTimestamp }}
 */
export function recoverToTimestamp(wal, db, targetTimestamp, options = {}) {
  const { fromLsn = 0 } = options;
  
  // Get all records (in-memory buffer has timestamps)
  // Note: in a real DB, timestamps would be serialized to disk. In our simulation,
  // the in-memory records preserve timestamps from when they were created.
  const allRecords = wal.getRecords().filter(r => r.lsn > fromLsn);
  allRecords.sort((a, b) => a.lsn - b.lsn);
  const records = allRecords;
  
  // Phase 1: Analysis — find transactions that COMMITTED before target time
  const committedTxns = new Set();
  const skippedTxns = new Set(); // Committed after target
  const txCommitTimestamps = new Map(); // txId -> commit timestamp
  
  for (const record of records) {
    if (record.type === WAL_TYPES.COMMIT) {
      if (record.timestamp <= targetTimestamp) {
        committedTxns.add(record.txId);
        txCommitTimestamps.set(record.txId, record.timestamp);
      } else {
        skippedTxns.add(record.txId);
      }
    }
  }
  
  // Phase 2: Redo — replay only committed-before-target transactions
  let redone = 0;
  let lastReplayedTimestamp = 0;
  
  for (const record of records) {
    // Stop processing records after target timestamp
    if (record.timestamp > targetTimestamp) break;
    
    // Only replay committed transactions
    if (!committedTxns.has(record.txId)) continue;
    
    const table = db.tables.get(record.tableName);
    if (!table) continue;
    
    switch (record.type) {
      case WAL_TYPES.INSERT: {
        if (record.after) {
          table.heap.insert(record.after);
          for (const [colName, index] of table.indexes) {
            const colIdx = table.schema.findIndex(c => c.name === colName);
            if (colIdx >= 0) {
              index.insert(record.after[colIdx], { pageId: record.pageId, slotIdx: record.slotIdx });
            }
          }
        }
        redone++;
        lastReplayedTimestamp = record.timestamp;
        break;
      }
      case WAL_TYPES.DELETE: {
        if (record.pageId >= 0 && record.slotIdx >= 0) {
          table.heap.delete(record.pageId, record.slotIdx);
        }
        redone++;
        lastReplayedTimestamp = record.timestamp;
        break;
      }
      case WAL_TYPES.UPDATE: {
        if (record.after && record.pageId >= 0) {
          table.heap.delete(record.pageId, record.slotIdx);
          const newRid = table.heap.insert(record.after);
          for (const [colName, index] of table.indexes) {
            const colIdx = table.schema.findIndex(c => c.name === colName);
            if (colIdx >= 0) {
              index.insert(record.after[colIdx], newRid);
            }
          }
        }
        redone++;
        lastReplayedTimestamp = record.timestamp;
        break;
      }
    }
  }
  
  return {
    committedTxns: committedTxns.size,
    skippedTxns: skippedTxns.size,
    redone,
    targetTimestamp,
    actualTimestamp: lastReplayedTimestamp || targetTimestamp,
    txCommitTimestamps: Object.fromEntries(txCommitTimestamps),
  };
}
