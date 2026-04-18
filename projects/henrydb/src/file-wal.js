// file-wal.js — File-backed WAL for crash recovery
// Wraps the existing WAL serialization but writes records to a file
// so they survive process crashes.

import { openSync, readSync, writeSync, fsyncSync, closeSync, existsSync, statSync, ftruncateSync } from 'node:fs';
import { WALRecord, WAL_TYPES, WAL_TYPE_NAMES, RECORD_TYPES } from './wal.js';
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

  /**
   * Log a DDL statement (ALTER TABLE, etc.) as an auto-committed record.
   * These records have no txId and are replayed directly during recovery.
   */
  logDDL(sql) {
    const lsn = this._nextLsn++;
    // Use a special WALRecord: txId=0 (auto-committed), type=DDL, store SQL in 'after'
    const record = new WALRecord(lsn, 0, RECORD_TYPES.DDL);
    record.sql = sql;
    record.after = { sql };
    this._writeBuffer.push(record);
    this.flush();
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
      this._fileSize = 0;
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
export function recoverFromFileWAL(heap, wal, fromLsn = 0) {
  const allRecords = wal.readFromStable(fromLsn);
  if (allRecords.length === 0) return { redone: 0, skipped: 0, committedTxns: 0 };
  
  // Phase 0: Build table name alias map from DDL rename records
  // Maps old names → new names so we can associate old WAL records with renamed tables
  const nameAliases = new Map(); // oldName → newName
  for (const r of allRecords) {
    if (r.type === RECORD_TYPES.DDL && r.after?.sql) {
      const m = r.after.sql.match(/ALTER\s+TABLE\s+(\w+)\s+RENAME\s+TO\s+(\w+)/i);
      if (m) nameAliases.set(m[1], m[2]);
    }
  }
  
  // Helper: check if a record's tableName matches this heap (considering renames)
  const matchesHeap = (tableName) => {
    if (!tableName) return true; // No table specified = applicable to all
    if (tableName === heap.name) return true;
    // Check if tableName was renamed to heap.name
    return nameAliases.get(tableName) === heap.name;
  };
  
  // Phase 1: Analysis — identify committed transactions
  const committedTxns = new Set();
  const abortedTxns = new Set();
  for (const r of allRecords) {
    if (r.type === WAL_TYPES.COMMIT) committedTxns.add(r.txId);
    if (r.type === WAL_TYPES.ABORT) abortedTxns.add(r.txId);
  }
  
  const dm = heap._dm;
  const bp = heap._bp;
  
  // Flush all dirty pages to disk, then invalidate buffer pool cache
  if (bp && bp.flushAll) {
    bp.flushAll((pid, data) => dm.writePage(pid, data));
  }
  if (bp && bp.invalidateAll) {
    bp.invalidateAll();
  }
  
  // Phase 2: Build page LSN map from on-disk pages
  // Read each page header to get its pageLSN (the LSN of the last applied record)
  const pageLSNMap = new Map();
  for (let i = 0; i < dm.pageCount; i++) {
    try {
      const buf = dm.readPage(i);
      const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
      // pageLSN at offset 8 (4 bytes, uint32)
      const pageLSN = buf.length >= 12 ? view.getUint32(8, true) : 0;
      pageLSNMap.set(i, pageLSN);
    } catch {
      pageLSNMap.set(i, 0);
    }
  }
  
  // Check if ANY committed record needs replay (per-page LSN check)
  // A record needs replay if it targets a page whose pageLSN < record.lsn
  // For INSERT records that allocate new pages, pageLSN will be 0
  let needsReplay = false;
  for (const r of allRecords) {
    if (r.txId !== 0 && !committedTxns.has(r.txId)) continue;
    if (!matchesHeap(r.tableName)) continue;
    if (r.type === WAL_TYPES.INSERT || r.type === WAL_TYPES.UPDATE || r.type === WAL_TYPES.DELETE) {
      const pageLSN = pageLSNMap.get(r.pageId) || 0;
      if (r.lsn > pageLSN) {
        needsReplay = true;
        break;
      }
    }
  }
  
  // Check if there are any uncommitted/aborted transactions that touched pages.
  // If so, pages may contain uncommitted data — we must do full redo.
  const allTxIds = new Set();
  for (const r of allRecords) {
    if (r.txId !== undefined) allTxIds.add(r.txId);
  }
  const hasUncommitted = [...allTxIds].some(txId => txId !== 0 && !committedTxns.has(txId) && !abortedTxns.has(txId)) ||
    abortedTxns.size > 0;
  
  if (!needsReplay && !hasUncommitted) {
    // All committed records already applied to pages — just rebuild FSM and rowCount
    heap._fsm = new FreeSpaceMap();
    heap._rowCount = 0;
    for (let i = 0; i < dm.pageCount; i++) {
      try {
        const page = heap._fetchPage(i);
        heap._fsm.update(i, page.freeSpace());
        for (const _ of page.scanTuples()) heap._rowCount++;
        heap._unpinPage(i, false);
      } catch { /* skip */ }
    }
    return { redone: 0, skipped: allRecords.length, committedTxns: committedTxns.size };
  }
  
  // Phase 3: Determine if we need full redo or incremental
  // If there are uncommitted/aborted transactions, pages may contain dirty data.
  // In that case, we must do full redo (clear + replay committed only).
  if (hasUncommitted) {
    // Full redo: pages may contain uncommitted data.
    // Strategy: check if pages actually have data. If yes, just remove uncommitted
    // records. If pages are empty/corrupt, do traditional clear + replay.
    
    // Check if pages have any data
    let existingTupleCount = 0;
    for (let i = 0; i < dm.pageCount; i++) {
      try {
        const page = heap._fetchPage(i);
        for (const _ of page.scanTuples()) existingTupleCount++;
        heap._unpinPage(i, false);
      } catch { /* skip */ }
    }
    
    if (existingTupleCount > 0) {
      // Pages have data — delete only uncommitted records
      const uncommittedInserts = [];
      for (const r of allRecords) {
        if (!matchesHeap(r.tableName)) continue;
        if (r.type === WAL_TYPES.INSERT && r.txId !== 0 && !committedTxns.has(r.txId)) {
          uncommittedInserts.push({ pageId: r.pageId, slotIdx: r.slotIdx });
        }
      }
      
      for (const { pageId, slotIdx } of uncommittedInserts) {
        try { heap.delete(pageId, slotIdx); } catch { /* may already be deleted */ }
      }
      
      // Rebuild FSM and rowCount
      heap._fsm = new FreeSpaceMap();
      heap._rowCount = 0;
      for (let i = 0; i < dm.pageCount; i++) {
        try {
          const page = heap._fetchPage(i);
          heap._fsm.update(i, page.freeSpace());
          for (const _ of page.scanTuples()) heap._rowCount++;
          heap._unpinPage(i, false);
        } catch { /* skip */ }
      }
      
      heap.flush();
      const maxLSN = allRecords[allRecords.length - 1].lsn;
      if (dm) dm.lastAppliedLSN = maxLSN;
      
      return { redone: uncommittedInserts.length, skipped: allRecords.length - uncommittedInserts.length, committedTxns: committedTxns.size };
    }
    
    // Pages are empty — traditional full redo (clear + replay committed only)
    for (let i = 0; i < dm.pageCount; i++) {
      dm.writePage(i, Buffer.alloc(PAGE_SIZE));
    }
    dm._nextPageId = 0;
    dm._numPages = 0;
    heap._fsm = new FreeSpaceMap();
    heap._rowCount = 0;
    return _replayRecords(heap, allRecords, committedTxns, allRecords, dm, matchesHeap);
  }
  
  // Check the minimum pageLSN across all pages. If 0 for all pages and WAL
  // has committed inserts, we're in "full redo from scratch" mode (first boot or old format).
  // Otherwise, use per-page LSN for incremental redo.
  const allPagesHaveLSN = [...pageLSNMap.values()].some(lsn => lsn > 0);
  
  if (!allPagesHaveLSN && dm.pageCount > 0) {
    // Pages exist but have no LSNs — could be old format or needs full redo
    // Check if lastAppliedLSN is set (backward compat with pre-pageLSN code)
    const lastAppliedLSN = dm.lastAppliedLSN || 0;
    if (lastAppliedLSN > 0) {
      // Old-style recovery: rebuild from existing pages + replay only new records
      heap._fsm = new FreeSpaceMap();
      heap._rowCount = 0;
      for (let i = 0; i < dm.pageCount; i++) {
        try {
          const page = heap._fetchPage(i);
          heap._fsm.update(i, page.freeSpace());
          for (const _ of page.scanTuples()) heap._rowCount++;
          heap._unpinPage(i, false);
        } catch { /* skip */ }
      }
      // Only replay records after lastAppliedLSN
      const replaySet = allRecords.filter(r => r.lsn > lastAppliedLSN);
      return _replayRecords(heap, replaySet, committedTxns, allRecords, dm, matchesHeap);
    }
    
    // Full redo from scratch: clear pages, replay all committed records
    for (let i = 0; i < dm.pageCount; i++) {
      dm.writePage(i, Buffer.alloc(PAGE_SIZE));
    }
    dm._pageCount = 0;
    dm._freeListHead = -1;
    dm._writeHeader();
    heap._fsm = new FreeSpaceMap();
    heap._rowCount = 0;
    return _replayRecords(heap, allRecords, committedTxns, allRecords, dm, matchesHeap);
  }
  
  // Per-page LSN recovery: rebuild state from existing pages, then replay only needed records
  heap._fsm = new FreeSpaceMap();
  heap._rowCount = 0;
  
  // Find the minimum pageLSN — we need to replay all records after this
  let minPageLSN = Infinity;
  for (const lsn of pageLSNMap.values()) {
    if (lsn < minPageLSN) minPageLSN = lsn;
  }
  if (minPageLSN === Infinity) minPageLSN = 0;
  
  // For pages that already have correct data (pageLSN >= all targeting records), keep them
  // For pages that are stale, we'll need to either:
  // a) Clear and replay from scratch (if the page has pageLSN=0 but WAL has inserts for it)
  // b) Apply only the missing records (pageLSN > 0 but < max record LSN for that page)
  
  // Group WAL records by page
  const recordsByPage = new Map();
  for (const r of allRecords) {
    if (r.txId !== 0 && !committedTxns.has(r.txId)) continue;
    if (!matchesHeap(r.tableName)) continue;
    if (r.type !== WAL_TYPES.INSERT && r.type !== WAL_TYPES.UPDATE && r.type !== WAL_TYPES.DELETE) continue;
    
    if (!recordsByPage.has(r.pageId)) recordsByPage.set(r.pageId, []);
    recordsByPage.get(r.pageId).push(r);
  }
  
  // For each page, decide: skip (all applied) or replay (some need replay)
  // IMPORTANT: Do NOT clear pages that have pageLSN > 0 — they contain valid data
  // from before the last checkpoint. Only replay records with LSN > pageLSN.
  // Clearing would destroy checkpointed data not in the current WAL.
  
  // Find the checkpoint LSN (if any) — records before this are already on disk
  let checkpointLSN = 0;
  for (const r of allRecords) {
    if (r.type === WAL_TYPES.CHECKPOINT) checkpointLSN = r.lsn;
  }
  
  for (const [pageId, records] of recordsByPage) {
    const pageLSN = pageLSNMap.get(pageId) || 0;
    const maxRecordLSN = Math.max(...records.map(r => r.lsn));
    
    if (pageLSN >= maxRecordLSN) {
      continue; // All records for this page already applied
    }
    
    // If page has no LSN (pageLSN === 0) and no checkpoint, safe to clear and replay
    if (pageLSN === 0 && checkpointLSN === 0 && pageId < dm.pageCount) {
      dm.writePage(pageId, Buffer.alloc(PAGE_SIZE));
    }
    // If page has a valid pageLSN, keep existing data and only replay newer records
  }
  
  // Rebuild state from surviving pages
  for (let i = 0; i < dm.pageCount; i++) {
    try {
      const page = heap._fetchPage(i);
      heap._fsm.update(i, page.freeSpace());
      for (const _ of page.scanTuples()) heap._rowCount++;
      heap._unpinPage(i, false);
    } catch { /* skip */ }
  }
  
  // Replay records for stale pages
  let redone = 0;
  let skipped = 0;
  const savedWal = heap._wal;
  heap._wal = null;
  
  for (const [pageId, records] of recordsByPage) {
    const pageLSN = pageLSNMap.get(pageId) || 0;
    const maxRecordLSN = Math.max(...records.map(r => r.lsn));
    
    if (pageLSN >= maxRecordLSN) {
      skipped += records.length;
      continue;
    }
    
    for (const r of records) {
      // Skip records already applied to this page (LSN <= pageLSN)
      if (r.lsn <= pageLSN) { skipped++; continue; }
      
      if (r.type === WAL_TYPES.INSERT && r.after) {
        try { heap.insert(r.after); redone++; } catch { skipped++; }
      } else if (r.type === WAL_TYPES.DELETE && r.pageId !== undefined) {
        try { heap.delete(r.pageId, r.slotIdx); redone++; } catch { skipped++; }
      } else if (r.type === WAL_TYPES.UPDATE && r.after) {
        try { heap.delete(r.pageId, r.slotIdx); heap.insert(r.after); redone++; } catch { skipped++; }
      }
    }
  }
  
  heap._wal = savedWal;
  heap.flush();
  
  // Update pageLSNs on all pages
  const maxWalLSN = allRecords[allRecords.length - 1].lsn;
  if (dm) dm.lastAppliedLSN = maxWalLSN;
  
  return { redone, skipped, committedTxns: committedTxns.size };
}

/** Helper: replay WAL records onto heap */
function _replayRecords(heap, replaySet, committedTxns, allRecords, dm, matchesHeap) {
  let redone = 0;
  let skipped = 0;
  const savedWal = heap._wal;
  heap._wal = null;
  
  const nameMatch = matchesHeap || ((name) => !name || name === heap.name);
  
  for (const r of replaySet) {
    if (r.txId !== 0 && !committedTxns.has(r.txId)) { skipped++; continue; }
    if (!nameMatch(r.tableName)) { skipped++; continue; }
    
    if (r.type === WAL_TYPES.INSERT && r.after) {
      try { heap.insert(r.after); redone++; } catch { skipped++; }
    } else if (r.type === WAL_TYPES.DELETE && r.pageId !== undefined) {
      try { heap.delete(r.pageId, r.slotIdx); redone++; } catch { skipped++; }
    } else if (r.type === WAL_TYPES.UPDATE && r.after) {
      try { heap.delete(r.pageId, r.slotIdx); heap.insert(r.after); redone++; } catch { skipped++; }
    }
  }
  
  heap._wal = savedWal;
  heap.flush();
  
  const maxWalLSN = allRecords[allRecords.length - 1].lsn;
  if (dm) dm.lastAppliedLSN = maxWalLSN;
  
  return { redone, skipped, committedTxns: committedTxns.size };
}
