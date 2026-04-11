// file-wal.js — File-backed WAL for crash recovery
// Wraps the existing WAL serialization but writes records to a file
// so they survive process crashes.

import { openSync, readSync, writeSync, fsyncSync, closeSync, existsSync, statSync, ftruncateSync } from 'node:fs';
import { WALRecord, WAL_TYPES, WAL_TYPE_NAMES } from './wal.js';
import { encodeTuple, decodeTuple, FreeSpaceMap } from './page.js';
import { PAGE_SIZE } from './disk-manager.js';

export class FileWAL {
  /**
   * @param {string} filePath — path to the WAL file
   * @param {object} options
   * @param {'immediate'|'batch'|'none'} options.syncMode — fsync strategy
   *   - 'immediate': fsync on every commit (safe, slow, default)
   *   - 'batch': fsync every batchIntervalMs (group commit, fast)
   *   - 'none': no fsync (fastest, data at risk on power loss)
   * @param {number} options.batchIntervalMs — batch fsync interval (default: 5ms)
   */
  constructor(filePath, options = {}) {
    this._filePath = filePath;
    this._nextLsn = 1;
    this._nextTxId = 1;
    this._flushedLsn = 0;
    this._writeBuffer = [];  // Records buffered before flush
    this._lastCheckpointLsn = 0;
    
    // Group commit settings
    this._syncMode = options.syncMode || 'immediate';
    this._batchIntervalMs = options.batchIntervalMs || 5;
    this._batchTimer = null;
    this._pendingSync = false; // true if writes happened since last fsync
    
    const exists = existsSync(filePath);
    if (exists) {
      this._fd = openSync(filePath, 'r+');
      this._fileSize = statSync(filePath).size;
      // Scan existing records to find max LSN
      const records = this._readAllRecords();
      for (const r of records) {
        if (r.lsn >= this._nextLsn) this._nextLsn = r.lsn + 1;
        if (r.txId >= this._nextTxId) this._nextTxId = r.txId + 1;
        if (r.type === WAL_TYPES.CHECKPOINT) this._lastCheckpointLsn = r.lsn;
      }
      this._flushedLsn = this._nextLsn - 1;
    } else {
      this._fd = openSync(filePath, 'w+');
      this._fileSize = 0;
    }
  }

  get flushedLsn() { return this._flushedLsn; }
  get lastCheckpointLsn() { return this._lastCheckpointLsn; }

  beginTransaction(txId) {
    if (txId >= this._nextTxId) this._nextTxId = txId + 1;
  }

  allocateTxId() {
    return this._nextTxId++;
  }

  appendInsert(txId, tableName, pageId, slotIdx, afterData) {
    const lsn = this._nextLsn++;
    const record = new WALRecord(lsn, txId, WAL_TYPES.INSERT, tableName, pageId, slotIdx, null, afterData);
    this._writeBuffer.push(record);
    return lsn;
  }

  appendDelete(txId, tableName, pageId, slotIdx, beforeData) {
    const lsn = this._nextLsn++;
    const record = new WALRecord(lsn, txId, WAL_TYPES.DELETE, tableName, pageId, slotIdx, beforeData, null);
    this._writeBuffer.push(record);
    return lsn;
  }

  appendUpdate(txId, tableName, pageId, slotIdx, beforeData, afterData) {
    const lsn = this._nextLsn++;
    const record = new WALRecord(lsn, txId, WAL_TYPES.UPDATE, tableName, pageId, slotIdx, beforeData, afterData);
    this._writeBuffer.push(record);
    return lsn;
  }

  appendCommit(txId) {
    const lsn = this._nextLsn++;
    const record = new WALRecord(lsn, txId, WAL_TYPES.COMMIT);
    this._writeBuffer.push(record);
    
    if (this._syncMode === 'immediate') {
      // Write + fsync immediately (safe, slow)
      this._flushToFile();
      fsyncSync(this._fd);
    } else if (this._syncMode === 'batch') {
      // Write immediately but defer fsync
      this._flushToFile();
      this._pendingSync = true;
      this._ensureBatchTimer();
    } else {
      // 'none': defer write to next flush/checkpoint (fastest, unsafe)
      // Records stay in _writeBuffer until explicit flush
      this._ensureDeferredFlushTimer();
    }
    
    return lsn;
  }

  appendAbort(txId) {
    const lsn = this._nextLsn++;
    const record = new WALRecord(lsn, txId, WAL_TYPES.ABORT);
    this._writeBuffer.push(record);
    return lsn;
  }

  checkpoint() {
    const lsn = this._nextLsn++;
    const record = new WALRecord(lsn, 0, WAL_TYPES.CHECKPOINT);
    this._writeBuffer.push(record);
    this.flush();
    this._lastCheckpointLsn = lsn;
    return lsn;
  }

  /**
   * Truncate the WAL file after a successful checkpoint.
   * Only safe to call after all dirty pages have been flushed to data files.
   * Resets the WAL to empty — next recovery will start fresh.
   */
  truncate() {
    if (this._fd !== null && this._fd !== undefined) {
      ftruncateSync(this._fd, 0);
      this._flushedLsn = 0;
      this._writeBuffer = [];
      this._pendingSync = false;
    }
  }

  /** Get WAL file size in bytes */
  get fileSize() {
    if (this._fd !== null && this._fd !== undefined) {
      return statSync(this._filePath).size;
    }
    return 0;
  }

  /** Flush buffered records to the file. */
  flush() {
    this._flushToFile();
    // Always fsync on explicit flush
    if (this._fd !== null && this._fd !== undefined) {
      fsyncSync(this._fd);
      this._pendingSync = false;
    }
  }

  /** Write buffered records to file without fsync. */
  _flushToFile() {
    if (this._writeBuffer.length === 0) return;
    
    const buffers = this._writeBuffer.map(r => r.serialize());
    const totalSize = buffers.reduce((acc, b) => acc + b.length, 0);
    const combined = Buffer.alloc(totalSize);
    let offset = 0;
    for (const buf of buffers) {
      buf.copy(combined, offset);
      offset += buf.length;
    }
    
    writeSync(this._fd, combined, 0, combined.length, this._fileSize);
    
    this._fileSize += totalSize;
    this._flushedLsn = this._writeBuffer[this._writeBuffer.length - 1].lsn;
    this._writeBuffer = [];
  }

  /** Start batch timer for group commit. */
  _ensureBatchTimer() {
    if (this._batchTimer) return;
    this._batchTimer = setInterval(() => {
      if (this._pendingSync && this._fd !== null && this._fd !== undefined) {
        fsyncSync(this._fd);
        this._pendingSync = false;
      }
    }, this._batchIntervalMs);
    if (this._batchTimer.unref) this._batchTimer.unref();
  }

  /** Deferred flush timer for syncMode='none': write buffered records periodically. */
  _ensureDeferredFlushTimer() {
    if (this._deferredFlushTimer) return;
    this._deferredFlushTimer = setInterval(() => {
      if (this._writeBuffer.length > 0 && this._fd !== null && this._fd !== undefined) {
        this._flushToFile();
      }
    }, 50); // flush every 50ms
    if (this._deferredFlushTimer.unref) this._deferredFlushTimer.unref();
  }

  /** Stop batch timer. */
  _stopBatchTimer() {
    if (this._batchTimer) {
      clearInterval(this._batchTimer);
      this._batchTimer = null;
    }
  }

  /** Force flush up to a specific LSN. */
  forceToLsn(targetLsn) {
    // Flush any buffered records up to targetLsn
    if (this._writeBuffer.length > 0 && this._writeBuffer[this._writeBuffer.length - 1].lsn >= targetLsn) {
      this.flush();
    }
  }

  /** Read all records from the WAL file. */
  readFromStable(afterLsn = 0) {
    const records = this._readAllRecords();
    return records.filter(r => r.lsn > afterLsn);
  }

  /** Close the WAL file. */
  close() {
    this._stopBatchTimer();
    if (this._deferredFlushTimer) {
      clearInterval(this._deferredFlushTimer);
      this._deferredFlushTimer = null;
    }
    if (this._writeBuffer.length > 0) this.flush();
    // Final fsync to ensure all data is on disk
    if (this._pendingSync && this._fd >= 0) {
      fsyncSync(this._fd);
      this._pendingSync = false;
    }
    if (this._fd >= 0) {
      closeSync(this._fd);
      this._fd = -1;
    }
  }

  // --- Internal ---

  _readAllRecords() {
    if (this._fileSize === 0) return [];
    
    const buf = Buffer.alloc(this._fileSize);
    readSync(this._fd, buf, 0, this._fileSize, 0);
    
    const records = [];
    let offset = 0;
    while (offset < buf.length) {
      if (offset + 4 > buf.length) break;
      
      const result = WALRecord.deserialize(buf, offset);
      if (!result) break; // CRC error or corrupt — stop here
      
      records.push(result.record);
      offset += result.bytesRead;
    }
    
    return records;
  }
}

/**
 * Recover a FileBackedHeap from a FileWAL.
 * Reads all WAL records, identifies committed transactions,
 * and redoes their operations to make the heap consistent.
 * 
 * @param {FileBackedHeap} heap
 * @param {FileWAL} wal
 * @returns {{ redone: number, skipped: number }}
 */
export function recoverFromFileWAL(heap, wal) {
  // Check last applied LSN — skip recovery if data is up to date
  const lastAppliedLSN = heap._dm ? heap._dm.lastAppliedLSN : 0;
  
  const allRecords = wal.readFromStable(0);
  if (allRecords.length === 0) return { redone: 0, skipped: 0, committedTxns: 0 };
  
  const maxWalLSN = allRecords[allRecords.length - 1].lsn;
  
  // If all WAL records are already applied, skip recovery
  if (lastAppliedLSN >= maxWalLSN) {
    return { redone: 0, skipped: 0, committedTxns: 0 };
  }
  
  // Phase 1: Analysis — identify committed transactions
  const committedTxns = new Set();
  const abortedTxns = new Set();
  let lastCheckpointLsn = 0;
  for (const r of allRecords) {
    if (r.type === WAL_TYPES.COMMIT) committedTxns.add(r.txId);
    if (r.type === WAL_TYPES.ABORT) abortedTxns.add(r.txId);
    if (r.type === WAL_TYPES.CHECKPOINT) lastCheckpointLsn = r.lsn;
  }
  
  const dm = heap._dm;
  const bp = heap._bp;
  
  // Evict all pages from buffer pool first, then invalidate cache
  if (bp && bp.flushAll) {
    bp.flushAll((pid, data) => dm.writePage(pid, data));
  }
  if (bp && bp.invalidateAll) {
    bp.invalidateAll();
  }
  
  // Determine recovery strategy:
  // - If WAL has records from before the last checkpoint (or no checkpoint at all),
  //   we need to wipe pages and replay from scratch (full redo).
  // - If all WAL records are AFTER a checkpoint, page files have the pre-checkpoint
  //   data and we only need to replay the new records on top.
  const hasPreCheckpointData = lastCheckpointLsn === 0 || 
    allRecords.some(r => r.lsn < lastCheckpointLsn && 
      (r.type === WAL_TYPES.INSERT || r.type === WAL_TYPES.UPDATE || r.type === WAL_TYPES.DELETE));
  
  // Only records after lastAppliedLSN need replay
  const recordsToReplay = allRecords.filter(r => r.lsn > lastAppliedLSN);
  
  if (hasPreCheckpointData && lastAppliedLSN === 0) {
    // Full redo: WAL contains full history, clear pages and replay everything
    // Clear all data pages (write zeroed pages)
    for (let i = 0; i < dm.pageCount; i++) {
      const zeroBuf = Buffer.alloc(PAGE_SIZE);
      dm.writePage(i, zeroBuf);
    }
    // Reset page count to 0
    dm._pageCount = 0;
    dm._freeListHead = -1;
    dm._writeHeader();
    
    // Re-initialize FSM and row count
    heap._fsm = new FreeSpaceMap();
    heap._rowCount = 0;
  } else {
    // Incremental redo: page files have data from prior checkpoint/flush.
    // Just replay WAL records that haven't been applied yet.
    // Re-initialize FSM from current pages (they have valid data)
    heap._fsm = new FreeSpaceMap();
    heap._rowCount = 0;
    for (let i = 0; i < dm.pageCount; i++) {
      try {
        const page = heap._fetchPage(i);
        heap._fsm.update(i, page.freeSpace());
        for (const _ of page.scanTuples()) heap._rowCount++;
        heap._unpinPage(i, false);
      } catch (e) {
        // Page may not exist, skip
      }
    }
  }
  
  // Phase 3: Redo — replay committed operations for this heap's table only
  let redone = 0;
  let skipped = 0;
  
  // Disable WAL on heap during recovery to avoid recursive logging
  const savedWal = heap._wal;
  heap._wal = null;
  
  const replaySet = hasPreCheckpointData && lastAppliedLSN === 0 ? allRecords : recordsToReplay;
  
  for (const r of replaySet) {
    if (!committedTxns.has(r.txId)) {
      skipped++;
      continue;
    }
    
    // Only replay records matching this heap's table name
    if (r.tableName && r.tableName !== heap.name) {
      skipped++;
      continue;
    }
    
    if (r.type === WAL_TYPES.INSERT && r.after) {
      try {
        heap.insert(r.after);
        redone++;
      } catch (e) {
        skipped++;
      }
    } else if (r.type === WAL_TYPES.DELETE && r.pageId !== undefined) {
      try {
        heap.delete(r.pageId, r.slotIdx);
        redone++;
      } catch (e) {
        skipped++;
      }
    } else if (r.type === WAL_TYPES.UPDATE && r.after) {
      try {
        heap.delete(r.pageId, r.slotIdx);
        heap.insert(r.after);
        redone++;
      } catch (e) {
        skipped++;
      }
    }
  }
  
  // Restore WAL reference
  heap._wal = savedWal;
  
  heap.flush();
  
  // Update lastAppliedLSN
  if (dm) {
    dm.lastAppliedLSN = maxWalLSN;
  }
  
  return { redone, skipped, committedTxns: committedTxns.size };
}
