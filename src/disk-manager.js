// disk-manager.js — File-backed page I/O for HenryDB
// Manages a database file as a sequence of fixed-size pages.
// Each page is PAGE_SIZE bytes, addressed by page ID (0-indexed).

import { openSync, readSync, writeSync, ftruncateSync, fsyncSync, closeSync, statSync, existsSync } from 'node:fs';

export const PAGE_SIZE = 4096;

// File header: stored in the first PAGE_SIZE bytes of the file
// [4 bytes: magic "HNDB"]
// [4 bytes: version (1)]
// [4 bytes: page size]
// [4 bytes: total page count (not including header page)]
// [4 bytes: free list head (page ID of first free page, or -1)]
// [rest: reserved]
const MAGIC = 0x48_4E_44_42; // 'HNDB'
const VERSION = 1;
const HEADER_PAGE = 0;

export class DiskManager {
  /**
   * @param {string} filePath — path to the database file
   * @param {object} [options]
   * @param {boolean} [options.create=true] — create file if it doesn't exist
   * @param {boolean} [options.sync=false] — fsync after every write (slower, safer)
   */
  constructor(filePath, { create = true, sync = false } = {}) {
    this._filePath = filePath;
    this._sync = sync;
    this._pageCount = 0;
    this._freeListHead = -1;
    this._lastAppliedLSN = 0;
    this._fd = -1;

    const exists = existsSync(filePath);
    if (!exists && !create) {
      throw new Error(`Database file not found: ${filePath}`);
    }

    // Open file (read+write, create if needed)
    this._fd = openSync(filePath, exists ? 'r+' : 'w+');

    if (exists) {
      this._readHeader();
    } else {
      this._initHeader();
    }
  }

  /** Total number of data pages (excluding the header page). */
  get pageCount() { return this._pageCount; }

  /** Last WAL LSN that was fully applied to this file. */
  get lastAppliedLSN() { return this._lastAppliedLSN; }
  set lastAppliedLSN(lsn) { 
    this._lastAppliedLSN = lsn; 
    this._writeHeader();
  }

  /**
   * Allocate a new page. Returns the page ID.
   * If there's a free page, reuse it. Otherwise, extend the file.
   */
  allocatePage() {
    let pageId;

    if (this._freeListHead >= 0) {
      // Reuse a free page
      pageId = this._freeListHead;
      // Read the free page to get the next free pointer
      const buf = this.readPage(pageId);
      this._freeListHead = buf.readInt32LE(0);
      this._writeHeader();
    } else {
      // Extend the file
      pageId = this._pageCount;
      this._pageCount++;
      // Extend file to include the new page
      const offset = this._pageOffset(pageId);
      const zeroBuf = Buffer.alloc(PAGE_SIZE);
      writeSync(this._fd, zeroBuf, 0, PAGE_SIZE, offset);
      this._writeHeader();
    }

    return pageId;
  }

  /**
   * Deallocate a page (add to free list).
   */
  deallocatePage(pageId) {
    // Write the current free list head into the deallocated page
    const buf = Buffer.alloc(PAGE_SIZE);
    buf.writeInt32LE(this._freeListHead, 0);
    this.writePage(pageId, buf);
    this._freeListHead = pageId;
    this._writeHeader();
  }

  /**
   * Read a page from disk.
   * @param {number} pageId
   * @returns {Buffer} — PAGE_SIZE bytes
   */
  readPage(pageId) {
    if (pageId < 0 || pageId >= this._pageCount) {
      throw new Error(`Invalid page ID: ${pageId} (total: ${this._pageCount})`);
    }
    const buf = Buffer.alloc(PAGE_SIZE);
    const offset = this._pageOffset(pageId);
    const bytesRead = readSync(this._fd, buf, 0, PAGE_SIZE, offset);
    if (bytesRead < PAGE_SIZE) {
      throw new Error(`Short read for page ${pageId}: got ${bytesRead} bytes`);
    }
    return buf;
  }

  /**
   * Write a page to disk.
   * @param {number} pageId
   * @param {Buffer} data — exactly PAGE_SIZE bytes
   */
  writePage(pageId, data) {
    if (data.length !== PAGE_SIZE) {
      throw new Error(`Page data must be exactly ${PAGE_SIZE} bytes, got ${data.length}`);
    }
    if (pageId < 0 || pageId >= this._pageCount) {
      throw new Error(`Invalid page ID: ${pageId} (total: ${this._pageCount})`);
    }
    const offset = this._pageOffset(pageId);
    writeSync(this._fd, data, 0, PAGE_SIZE, offset);
    if (this._sync) fsyncSync(this._fd);
  }

  /**
   * Force all pending writes to disk (fsync).
   */
  sync() {
    fsyncSync(this._fd);
  }

  /**
   * Close the database file.
   */
  close() {
    if (this._fd >= 0) {
      this._writeHeader();
      fsyncSync(this._fd);
      closeSync(this._fd);
      this._fd = -1;
    }
  }

  // --- Internal ---

  _pageOffset(pageId) {
    // Page 0 is the header, data pages start at offset PAGE_SIZE
    return (pageId + 1) * PAGE_SIZE;
  }

  _initHeader() {
    const buf = Buffer.alloc(PAGE_SIZE);
    buf.writeUInt32LE(MAGIC, 0);
    buf.writeUInt32LE(VERSION, 4);
    buf.writeUInt32LE(PAGE_SIZE, 8);
    buf.writeUInt32LE(0, 12);  // page count
    buf.writeInt32LE(-1, 16);  // free list head
    // lastAppliedLSN: 8 bytes at offset 20 (as two 32-bit halves)
    buf.writeUInt32LE(0, 20);  // LSN low
    buf.writeUInt32LE(0, 24);  // LSN high
    writeSync(this._fd, buf, 0, PAGE_SIZE, 0);
    fsyncSync(this._fd);
    this._pageCount = 0;
    this._freeListHead = -1;
    this._lastAppliedLSN = 0;
  }

  _readHeader() {
    const buf = Buffer.alloc(PAGE_SIZE);
    const bytesRead = readSync(this._fd, buf, 0, PAGE_SIZE, 0);
    
    if (bytesRead < 20) {
      // File too small to be a valid database — reinitialize
      this._initHeader();
      return;
    }

    const magic = buf.readUInt32LE(0);
    if (magic !== MAGIC) {
      throw new Error(`Invalid database file: bad magic number (expected 0x${MAGIC.toString(16)}, got 0x${magic.toString(16)})`);
    }

    const version = buf.readUInt32LE(4);
    if (version !== VERSION) {
      throw new Error(`Unsupported database version: ${version}`);
    }

    const pageSize = buf.readUInt32LE(8);
    if (pageSize !== PAGE_SIZE) {
      throw new Error(`Page size mismatch: file has ${pageSize}, expected ${PAGE_SIZE}`);
    }

    this._pageCount = buf.readUInt32LE(12);
    this._freeListHead = buf.readInt32LE(16);
    
    // Read lastAppliedLSN (8 bytes)
    if (bytesRead >= 28) {
      const lsnLow = buf.readUInt32LE(20);
      const lsnHigh = buf.readUInt32LE(24);
      this._lastAppliedLSN = lsnHigh * 0x100000000 + lsnLow;
    } else {
      this._lastAppliedLSN = 0;
    }
  }

  _writeHeader() {
    const buf = Buffer.alloc(PAGE_SIZE);
    buf.writeUInt32LE(MAGIC, 0);
    buf.writeUInt32LE(VERSION, 4);
    buf.writeUInt32LE(PAGE_SIZE, 8);
    buf.writeUInt32LE(this._pageCount, 12);
    buf.writeInt32LE(this._freeListHead, 16);
    buf.writeUInt32LE(this._lastAppliedLSN & 0xFFFFFFFF, 20);  // LSN low
    buf.writeUInt32LE(Math.floor(this._lastAppliedLSN / 0x100000000), 24);  // LSN high
    writeSync(this._fd, buf, 0, PAGE_SIZE, 0);
  }
}
