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
   */
  constructor(filePath) {
    this._filePath = filePath;
    this._nextLsn = 1;
    this._nextTxId = 1;
    this._flushedLsn = 0;
    this._writeBuffer = [];  // Records buffered before flush
    this._lastCheckpointLsn = 0;
    
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
    // COMMIT forces flush to stable storage
    this.flush();
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

  /** Log a DDL statement (ALTER TABLE, etc.) */
  logDDL(sql) {
    const lsn = this._nextLsn++;
    const record = new WALRecord(lsn, 0, WAL_TYPES.DDL);
    record.after = sql; // Store as plain string, will be JSON-serialized by WALRecord.serialize()
    this._writeBuffer.push(record);
    this.flush();
    return lsn;
  }

  /** Flush buffered records to the file. */
  flush() {
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
    fsyncSync(this._fd);
    
    this._fileSize += totalSize;
    this._flushedLsn = this._writeBuffer[this._writeBuffer.length - 1].lsn;
    this._writeBuffer = [];
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
    if (this._writeBuffer.length > 0) this.flush();
    if (this._fd >= 0) {
      closeSync(this._fd);
      this._fd = -1;
    }
  }

  /**
   * Truncate WAL records before a given LSN.
   * Rewrites the WAL file keeping only records >= keepFromLsn.
   * Must be called after checkpoint when dirty pages are flushed.
   * @param {number} keepFromLsn - Keep records with LSN >= this value
   * @returns {{ removed: number, kept: number, savedBytes: number }}
   */
  truncate(keepFromLsn) {
    // Flush any pending writes first
    if (this._writeBuffer.length > 0) this.flush();
    
    const allRecords = this._readAllRecords();
    const kept = allRecords.filter(r => r.lsn >= keepFromLsn);
    const removed = allRecords.length - kept.length;
    
    if (removed === 0) return { removed: 0, kept: kept.length, savedBytes: 0 };
    
    // Serialize kept records
    const buffers = kept.map(r => r.serialize());
    const totalSize = buffers.reduce((acc, b) => acc + b.length, 0);
    const combined = Buffer.alloc(totalSize);
    let offset = 0;
    for (const buf of buffers) {
      buf.copy(combined, offset);
      offset += buf.length;
    }
    
    const oldSize = this._fileSize;
    
    // Rewrite file from beginning
    writeSync(this._fd, combined, 0, combined.length, 0);
    ftruncateSync(this._fd, totalSize);
    fsyncSync(this._fd);
    
    this._fileSize = totalSize;
    this._flushedLsn = kept.length > 0 ? kept[kept.length - 1].lsn : this._flushedLsn;
    
    return { removed, kept: kept.length, savedBytes: oldSize - totalSize };
  }

  /** Get size info for the WAL file */
  getSize() {
    return { fileSize: this._fileSize, recordCount: this._readAllRecords().length };
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
  
  // Check if WAL was truncated: no data records exist before checkpoint
  // In that case, heap file is already consistent — skip destructive replay
  const hasDataRecordsBeforeCheckpoint = lastCheckpointLsn > 0 && allRecords.some(
    r => r.lsn < lastCheckpointLsn && (r.type === WAL_TYPES.INSERT || r.type === WAL_TYPES.UPDATE || r.type === WAL_TYPES.DELETE)
  );
  const walTruncated = lastCheckpointLsn > 0 && !hasDataRecordsBeforeCheckpoint;
  
  if (walTruncated) {
    // WAL was truncated after checkpoint — heap file has all data
    // Only replay data records after checkpoint (if any)
    const recordsAfterCheckpoint = allRecords.filter(
      r => r.lsn > lastCheckpointLsn && (r.type === WAL_TYPES.INSERT || r.type === WAL_TYPES.UPDATE || r.type === WAL_TYPES.DELETE)
    );
    
    if (recordsAfterCheckpoint.length === 0) {
      // No new data — heap is fully consistent
      const hDm = heap._dm;
      if (hDm) hDm.lastAppliedLSN = maxWalLSN;
      return { redone: 0, skipped: 0, committedTxns: committedTxns.size };
    }
    // Otherwise, fall through to incremental replay (no clear)
  }
  
  // Phase 2: Clear heap data and rebuild from WAL
  // Skip destructive clear if WAL was truncated (data already on disk)
  const dm = heap._dm;
  const bp = heap._bp;
  
  if (!walTruncated) {
    // Full replay: clear heap and rebuild from WAL
    // Evict all pages from buffer pool first
    if (bp && bp.flushAll) {
      bp.flushAll((pid, data) => dm.writePage(pid, data));
    }
    
    // Clear all data pages (write zeroed pages)
    for (let i = 0; i < dm.pageCount; i++) {
      const zeroBuf = Buffer.alloc(PAGE_SIZE);
      dm.writePage(i, zeroBuf);
    }
    // Reset page count to 0
    dm._pageCount = 0;
    dm._freeListHead = -1;
    dm._writeHeader();
    
    // Re-initialize FSM
    heap._fsm = new FreeSpaceMap();
  }
  
  // Phase 3: Redo — replay committed operations for this heap's table only
  let redone = 0;
  let skipped = 0;
  
  // Disable WAL on heap during recovery to avoid recursive logging
  const savedWal = heap._wal;
  heap._wal = null;
  
  const replayRecords = allRecords;
  for (const r of replayRecords) {
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
