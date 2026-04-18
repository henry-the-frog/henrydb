// file-backed-heap.js — HeapFile backed by disk storage via buffer pool
// Provides the same interface as HeapFile (page.js) but uses DiskManager
// for persistence and BufferPool for caching.

import { DiskManager, PAGE_SIZE } from './disk-manager.js';
import { BufferPool } from './buffer-pool.js';
import { encodeTuple, decodeTuple, FreeSpaceMap } from './page.js';

// Page layout constants (must match page.js)
const HEADER_SIZE = 12;  // [4 bytes: pageId] [2 bytes: numSlots] [2 bytes: freeSpaceEnd] [4 bytes: pageLSN]
const SLOT_SIZE = 4;     // [2 bytes: offset] [2 bytes: length]

/**
 * A page wrapper that reads/writes from a Buffer (backed by buffer pool frame).
 * Similar to Page in page.js but operates on a raw Buffer instead of Uint8Array.
 */
class BufferedPage {
  constructor(pageId, buf) {
    this.id = pageId;
    this.buf = buf;
    this._view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }

  getNumSlots() { return this._view.getUint16(4, true); }
  setNumSlots(n) { this._view.setUint16(4, n, true); }
  
  freeSpaceEnd() { return this._view.getUint16(6, true); }
  setFreeSpaceEnd(v) { this._view.setUint16(6, v, true); }

  getPageLSN() { return this._view.getUint32(8, true); }
  setPageLSN(lsn) { this._view.setUint32(8, lsn >>> 0, true); }
  
  slotsEnd() { return HEADER_SIZE + this.getNumSlots() * SLOT_SIZE; }

  freeSpace() {
    return this.freeSpaceEnd() - this.slotsEnd();
  }

  initialize() {
    // Write page header
    this._view.setUint32(0, this.id, true);    // pageId
    this._view.setUint16(4, 0, true);          // numSlots = 0
    this._view.setUint16(6, PAGE_SIZE, true);  // freeSpaceEnd = PAGE_SIZE
    this._view.setUint32(8, 0, true);          // pageLSN = 0
  }

  insertTuple(data) {
    const needed = data.length + SLOT_SIZE;
    if (needed > this.freeSpace()) return -1;

    const numSlots = this.getNumSlots();
    const tupleOffset = this.freeSpaceEnd() - data.length;

    // Write tuple data
    this.buf.set(data, tupleOffset);
    this.setFreeSpaceEnd(tupleOffset);

    // Write slot entry
    const slotOffset = HEADER_SIZE + numSlots * SLOT_SIZE;
    this._view.setUint16(slotOffset, tupleOffset, true);
    this._view.setUint16(slotOffset + 2, data.length, true);
    this.setNumSlots(numSlots + 1);

    return numSlots;
  }

  getTuple(slotIdx) {
    const numSlots = this.getNumSlots();
    if (slotIdx < 0 || slotIdx >= numSlots) return null;
    
    const slotOffset = HEADER_SIZE + slotIdx * SLOT_SIZE;
    const tupleOffset = this._view.getUint16(slotOffset, true);
    const tupleLength = this._view.getUint16(slotOffset + 2, true);
    
    if (tupleLength === 0) return null; // deleted
    return new Uint8Array(this.buf.buffer, this.buf.byteOffset + tupleOffset, tupleLength);
  }

  deleteTuple(slotIdx) {
    const numSlots = this.getNumSlots();
    if (slotIdx < 0 || slotIdx >= numSlots) return false;
    
    const slotOffset = HEADER_SIZE + slotIdx * SLOT_SIZE;
    this._view.setUint16(slotOffset + 2, 0, true); // Set length to 0 (deleted)
    return true;
  }

  /**
   * Update a tuple in-place: delete old slot, insert new data at current free space.
   * Returns true on success, false if no space.
   */
  updateTuple(slotIdx, newData) {
    const numSlots = this.getNumSlots();
    if (slotIdx < 0 || slotIdx >= numSlots) return false;
    // Mark old slot as deleted
    const slotOffset = HEADER_SIZE + slotIdx * SLOT_SIZE;
    this._view.setUint16(slotOffset + 2, 0, true);
    // Insert new data at free space end
    const freeEnd = this.freeSpaceEnd();
    const offset = freeEnd - newData.length;
    if (offset < HEADER_SIZE + numSlots * SLOT_SIZE) return false; // No space
    this.buf.set(newData, offset);
    this._view.setUint16(slotOffset, offset, true);
    this._view.setUint16(slotOffset + 2, newData.length, true);
    this.setFreeSpaceEnd(offset);
    return true;
  }

  *scanTuples() {
    const numSlots = this.getNumSlots();
    for (let i = 0; i < numSlots; i++) {
      const data = this.getTuple(i);
      if (data && data.length > 0) {
        yield { slotIdx: i, data };
      }
    }
  }
}

/**
 * File-backed heap file.
 * Same interface as HeapFile but backed by DiskManager + BufferPool.
 */
export class FileBackedHeap {
  /**
   * @param {string} name — table name
   * @param {DiskManager} diskManager
   * @param {BufferPool} bufferPool
   */
  constructor(name, diskManager, bufferPool, wal = null) {
    this.name = name;
    this._dm = diskManager;
    this._bp = bufferPool;
    this._wal = wal;
    this._fsm = new FreeSpaceMap();
    this._pageLSNs = new Map(); // pageId → latest LSN that modified this page
    
    // Set up eviction callback — enforce write-ahead constraint, then flush
    this._bp.setEvictCallback((pageId, data) => {
      this._enforceWriteAhead(pageId);
      this._dm.writePage(pageId, data);
      // Track the max LSN written to disk so recovery skips already-applied records
      const pageLsn = this._pageLSNs.get(pageId) || 0;
      if (pageLsn > this._dm.lastAppliedLSN) {
        this._dm.lastAppliedLSN = pageLsn;
      }
    });
    
    // Initialize FSM from existing pages
    this._rowCount = 0;
    for (let i = 0; i < diskManager.pageCount; i++) {
      const page = this._fetchPage(i);
      this._fsm.update(i, page.freeSpace());
      // Count existing tuples for rowCount
      for (const _ of page.scanTuples()) this._rowCount++;
      this._unpinPage(i, false);
    }
  }

  /** Number of live tuples. */
  get rowCount() { return this._rowCount; }

  /** Insert a tuple (array of JS values). Returns { pageId, slotIdx }. */
  insert(values) {
    const tupleBytes = encodeTuple(values);
    
    // Try FSM first
    const targetPageId = this._fsm.findPage(tupleBytes.length + SLOT_SIZE);
    if (targetPageId >= 0) {
      const page = this._fetchPage(targetPageId);
      const slotIdx = page.insertTuple(tupleBytes);
      if (slotIdx >= 0) {
        this._fsm.update(targetPageId, page.freeSpace());
        // WAL: log insert before marking page dirty
        if (this._wal) {
          const lsn = this._wal.appendInsert(this._currentTxId || 0, this.name, targetPageId, slotIdx, values);
          this.setPageLSN(targetPageId, lsn);
        }
        this._unpinPage(targetPageId, true);
        this._rowCount++;
        return { pageId: targetPageId, slotIdx };
      }
      this._unpinPage(targetPageId, false);
    }
    
    // Scan existing pages
    for (let i = 0; i < this._dm.pageCount; i++) {
      const page = this._fetchPage(i);
      const slotIdx = page.insertTuple(tupleBytes);
      if (slotIdx >= 0) {
        this._fsm.update(i, page.freeSpace());
        if (this._wal) {
          const lsn = this._wal.appendInsert(this._currentTxId || 0, this.name, i, slotIdx, values);
          this.setPageLSN(i, lsn);
        }
        this._unpinPage(i, true);
        this._rowCount++; return { pageId: i, slotIdx };
      }
      this._unpinPage(i, false);
    }
    
    // Allocate new page
    const pageId = this._dm.allocatePage();
    const page = this._fetchPage(pageId);
    page.initialize();
    const slotIdx = page.insertTuple(tupleBytes);
    this._fsm.update(pageId, page.freeSpace());
    if (this._wal) {
      const lsn = this._wal.appendInsert(this._currentTxId || 0, this.name, pageId, slotIdx, values);
      this.setPageLSN(pageId, lsn);
    }
    this._unpinPage(pageId, true);
    this._rowCount++; return { pageId, slotIdx };
  }

  /** Get a tuple by page/slot. Returns decoded values array or null. */
  get(pageId, slotIdx) {
    const page = this._fetchPage(pageId);
    const tuple = page.getTuple(slotIdx);
    this._unpinPage(pageId, false);
    if (!tuple) return null;
    return decodeTuple(tuple);
  }

  /**
   * Update a tuple in-place without WAL logging.
   * Used by ALTER TABLE backfill — schema changes are logged via DDL WAL records,
   * and recovery replays schema-only changes, so the data doesn't need separate WAL entries.
   * Updates pageLSN to prevent stale-page redo.
   */
  updateInPlace(pageId, slotIdx, newValues) {
    const encoded = encodeTuple(newValues);
    const page = this._fetchPage(pageId);
    const ok = page.updateTuple(slotIdx, encoded);
    if (ok && this._wal) {
      // Advance pageLSN without adding a WAL record — this ensures recovery
      // knows the page is up-to-date and won't re-insert the old tuple
      const lsn = this._wal._nextLsn || 0;
      this.setPageLSN(pageId, lsn);
    }
    this._unpinPage(pageId, ok);
    return ok;
  }

  /**
   * Truncate all data — clear all pages. Used by REFRESH MATERIALIZED VIEW.
   */
  truncate() {
    for (let i = 0; i < this._dm.pageCount; i++) {
      this._dm.writePage(i, Buffer.alloc(PAGE_SIZE));
    }
    this._dm._pageCount = 0;
    this._bp.invalidateAll();
    this._fsm = new FreeSpaceMap();
    this._rowCount = 0;
    this._pageLSNs.clear();
  }

  /** Delete a tuple by marking its slot as empty. */
  delete(pageId, slotIdx) {
    // WAL: log delete before modifying page
    if (this._wal) {
      const page = this._fetchPage(pageId);
      const tuple = page.getTuple(slotIdx);
      const beforeData = tuple ? decodeTuple(tuple) : null;
      this._unpinPage(pageId, false);
      if (beforeData) {
        const lsn = this._wal.appendDelete(this._currentTxId || 0, this.name, pageId, slotIdx, beforeData);
        this.setPageLSN(pageId, lsn);
      }
    }
    const page = this._fetchPage(pageId);
    const result = page.deleteTuple(slotIdx);
    this._unpinPage(pageId, result);
    if (result) this._rowCount--;
    return result;
  }

  /** Scan all live tuples. Yields { pageId, slotIdx, values }. */
  *scan() {
    // Scan all pages known to the disk manager (may include in-buffer-pool pages)
    const pageCount = this._dm.pageCount || 0;
    for (let i = 0; i < pageCount; i++) {
      const page = this._fetchPage(i);
      if (!page) continue;
      for (const { slotIdx, data } of page.scanTuples()) {
        yield { pageId: i, slotIdx, values: decodeTuple(data) };
      }
      this._unpinPage(i, false);
    }
  }

  /** Count of pages. */
  get pageCount() { return this._dm.pageCount; }

  /** Count of live tuples (requires full scan). */
  get tupleCount() {
    let count = 0;
    for (const _ of this.scan()) count++;
    return count;
  }

  /** Track the LSN of the latest modification to a page (in-memory + page header). */
  setPageLSN(pageId, lsn) {
    this._pageLSNs.set(pageId, lsn);
    // Also write to the on-disk page header via buffer pool
    try {
      const page = this._fetchPage(pageId);
      page.setPageLSN(lsn);
      this._unpinPage(pageId, true); // mark dirty so it gets flushed
    } catch {
      // Page might not be in buffer pool yet, that's ok
    }
  }

  /** Get the LSN of the latest modification to a page. */
  getPageLSN(pageId) {
    // Try in-memory first (most recent), fall back to page header
    const memLSN = this._pageLSNs.get(pageId);
    if (memLSN !== undefined) return memLSN;
    try {
      const page = this._fetchPage(pageId);
      const diskLSN = page.getPageLSN();
      this._unpinPage(pageId, false);
      return diskLSN;
    } catch {
      return 0;
    }
  }

  // --- Internal ---

  _fetchPage(pageId) {
    const frame = this._bp.fetchPage(pageId, (pid) => {
      return this._dm.readPage(pid);
    });
    return new BufferedPage(pageId, frame.data);
  }

  _unpinPage(pageId, isDirty) {
    this._bp.unpinPage(pageId, isDirty);
  }

  /** Flush all dirty pages through the buffer pool. */
  flush() {
    this._bp.flushAll((pid, data) => {
      this._enforceWriteAhead(pid);
      this._dm.writePage(pid, data);
    });
    // Update lastAppliedLSN in the data file
    const maxLSN = Math.max(0, ...[...this._pageLSNs.values()]);
    if (maxLSN > this._dm.lastAppliedLSN) {
      this._dm.lastAppliedLSN = maxLSN;
    }
  }

  /**
   * Enforce the write-ahead constraint:
   * Before writing a dirty page to disk, ensure all WAL records
   * up to that page's LSN have been flushed to stable storage.
   */
  _enforceWriteAhead(pageId) {
    if (!this._wal) return;
    const pageLsn = this._pageLSNs.get(pageId) || 0;
    if (pageLsn > this._wal.flushedLsn) {
      this._wal.forceToLsn(pageLsn);
    }
  }
}
