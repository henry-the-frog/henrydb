// disk-manager.js — File-backed page I/O for HenryDB
//
// Manages page storage on disk using a single database file.
// Pages are fixed-size and addressed by page ID.
// Layout: [Page 0][Page 1][Page 2]...
//
// File format:
//   Bytes 0..PAGE_SIZE-1: Page 0
//   Bytes PAGE_SIZE..2*PAGE_SIZE-1: Page 1
//   ...
//
// Design matches CMU 15-445 DiskManager interface.

import { openSync, readSync, writeSync, closeSync, fstatSync, ftruncateSync, unlinkSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

/**
 * DiskManager — File-backed page storage.
 * Each page is a fixed-size block at offset pageId * pageSize.
 */
export class DiskManager {
  /**
   * @param {string} filePath - Path to the database file
   * @param {number} pageSize - Size of each page in bytes (default: 4096)
   */
  constructor(filePath, pageSize = 4096) {
    this.filePath = filePath;
    this.pageSize = pageSize;
    this._fd = openSync(filePath, 'a+'); // Create if not exists, read+write
    // Reopen with r+ for proper random access
    closeSync(this._fd);
    this._fd = openSync(filePath, existsSync(filePath) ? 'r+' : 'w+');
    
    const stat = fstatSync(this._fd);
    this._fileSize = stat.size;
    this._numPages = Math.ceil(this._fileSize / this.pageSize);
    this._nextPageId = this._numPages;
    
    // Stats
    this._readCount = 0;
    this._writeCount = 0;
    this._bytesRead = 0;
    this._bytesWritten = 0;
  }

  /**
   * Allocate a new page. Returns the page ID.
   * The page is zero-filled on disk.
   */
  allocatePage() {
    const pageId = this._nextPageId++;
    const zeros = Buffer.alloc(this.pageSize);
    const offset = pageId * this.pageSize;
    writeSync(this._fd, zeros, 0, this.pageSize, offset);
    this._fileSize = Math.max(this._fileSize, offset + this.pageSize);
    this._numPages = pageId + 1;
    this._writeCount++;
    this._bytesWritten += this.pageSize;
    return pageId;
  }

  /**
   * Read a page from disk into a buffer.
   * @param {number} pageId
   * @returns {Buffer} Page data
   */
  readPage(pageId) {
    const offset = pageId * this.pageSize;
    if (offset + this.pageSize > this._fileSize) {
      throw new Error(`Page ${pageId} does not exist (offset ${offset} exceeds file size ${this._fileSize})`);
    }
    
    const buf = Buffer.alloc(this.pageSize);
    const bytesRead = readSync(this._fd, buf, 0, this.pageSize, offset);
    if (bytesRead < this.pageSize) {
      throw new Error(`Short read for page ${pageId}: got ${bytesRead} bytes, expected ${this.pageSize}`);
    }
    
    this._readCount++;
    this._bytesRead += this.pageSize;
    return buf;
  }

  /**
   * Write a page to disk.
   * @param {number} pageId
   * @param {Buffer} data - Must be exactly pageSize bytes
   */
  writePage(pageId, data) {
    if (!Buffer.isBuffer(data) || data.length !== this.pageSize) {
      throw new Error(`Data must be a Buffer of exactly ${this.pageSize} bytes`);
    }
    
    const offset = pageId * this.pageSize;
    writeSync(this._fd, data, 0, this.pageSize, offset);
    this._fileSize = Math.max(this._fileSize, offset + this.pageSize);
    
    this._writeCount++;
    this._bytesWritten += this.pageSize;
  }

  /**
   * Deallocate a page. In this simple implementation,
   * we zero out the page but don't reclaim space.
   */
  deallocatePage(pageId) {
    const zeros = Buffer.alloc(this.pageSize);
    const offset = pageId * this.pageSize;
    if (offset < this._fileSize) {
      writeSync(this._fd, zeros, 0, this.pageSize, offset);
    }
  }

  /**
   * Get the number of pages currently on disk.
   */
  get numPages() {
    return this._numPages;
  }

  /**
   * Get I/O statistics.
   */
  get stats() {
    return {
      totalPages: this._numPages,
      fileSize: this._fileSize,
      reads: this._readCount,
      writes: this._writeCount,
      bytesRead: this._bytesRead,
      bytesWritten: this._bytesWritten,
    };
  }

  /**
   * Close the file descriptor.
   */
  close() {
    closeSync(this._fd);
    this._fd = -1;
  }

  /**
   * Delete the database file.
   */
  destroy() {
    if (this._fd >= 0) this.close();
    try { unlinkSync(this.filePath); } catch (e) { /* ignore */ }
  }

  /**
   * Create a temporary DiskManager for testing.
   */
  static createTemp(pageSize = 4096) {
    const filePath = join(tmpdir(), `henrydb-test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
    return new DiskManager(filePath, pageSize);
  }
}
