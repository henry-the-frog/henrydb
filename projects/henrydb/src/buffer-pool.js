// buffer-pool.js — Buffer Pool Manager for HenryDB
//
// Manages a fixed-size pool of page frames in memory.
// Pages are loaded from a DiskManager (or mock) on demand.
// When the pool is full, the LRU replacer evicts a page.
//
// Key operations:
//   fetchPage(pageId)  → Pin and return page data (load from disk if needed)
//   unpinPage(pageId, isDirty)  → Unpin, mark dirty if modified
//   flushPage(pageId)  → Write dirty page to disk
//   newPage()          → Allocate a new page
//   deletePage(pageId) → Remove page from pool and disk
//   flushAll()         → Flush all dirty pages

import { LRUReplacer } from './lru-replacer.js';
import { ClockReplacer } from './clock-replacer.js';

/**
 * Simple in-memory disk manager for testing.
 * In production, this would read/write to actual files.
 */
export class InMemoryDiskManager {
  constructor(pageSize = 4096) {
    this.pageSize = pageSize;
    this._pages = new Map(); // pageId → Buffer
    this._nextPageId = 0;
    this._readCount = 0;
    this._writeCount = 0;
  }

  allocatePage() {
    const pageId = this._nextPageId++;
    this._pages.set(pageId, Buffer.alloc(this.pageSize));
    return pageId;
  }

  readPage(pageId) {
    this._readCount++;
    const data = this._pages.get(pageId);
    if (!data) throw new Error(`Page ${pageId} does not exist on disk`);
    return Buffer.from(data); // Return a copy
  }

  writePage(pageId, data) {
    this._writeCount++;
    if (!Buffer.isBuffer(data)) throw new Error('Data must be a Buffer');
    this._pages.set(pageId, Buffer.from(data)); // Store a copy
  }

  deallocatePage(pageId) {
    this._pages.delete(pageId);
  }

  get stats() {
    return {
      totalPages: this._pages.size,
      reads: this._readCount,
      writes: this._writeCount,
    };
  }
}

/**
 * Frame metadata tracked by the buffer pool.
 */
class FrameInfo {
  constructor() {
    this.pageId = -1;     // -1 means frame is free
    this.dirty = false;
    this.pinCount = 0;
    this.data = null;     // Buffer holding page data
  }

  reset() {
    this.pageId = -1;
    this.dirty = false;
    this.pinCount = 0;
    this.data = null;
  }
}

/**
 * BufferPoolManager — Fixed-size buffer pool with LRU eviction.
 * 
 * Architecture:
 *   - Pool of `poolSize` frames, each can hold one page
 *   - Page table: maps pageId → frameId
 *   - LRU replacer decides which frame to evict when pool is full
 *   - Pin count tracks concurrent users; unpinned frames are evictable
 *   - Dirty pages are flushed to disk before eviction
 */
export class BufferPoolManager {
  /**
   * @param {number} poolSize - Number of frames in the buffer pool
   * @param {Object} diskManager - Disk manager for page I/O
   * @param {Object} options
   * @param {string} options.replacer - 'lru' or 'clock' (default: 'clock')
   */
  constructor(poolSize, diskManager, options = {}) {
    this.poolSize = poolSize;
    this.disk = diskManager || null;
    
    const replacerType = (options.replacer || 'clock').toLowerCase();
    if (replacerType === 'lru') {
      this.replacer = new LRUReplacer(poolSize);
    } else {
      this.replacer = new ClockReplacer(poolSize);
    }
    this.replacerType = replacerType;
    
    // Frame storage
    this._frames = Array.from({ length: poolSize }, () => new FrameInfo());
    
    // Page table: pageId → frameId
    this._pageTable = new Map();
    
    // Free list: available frame IDs
    this._freeList = [];
    for (let i = poolSize - 1; i >= 0; i--) {
      this._freeList.push(i);
    }
    
    // Stats
    this._hits = 0;
    this._misses = 0;
    this._evictions = 0;
    this._evictCallback = null;
  }

  /**
   * Set a callback invoked when a dirty page is evicted.
   * @param {Function} cb - (pageId, data) => void
   */
  setEvictCallback(cb) {
    this._evictCallback = cb;
  }

  /**
   * Fetch a page. Returns the page data buffer (pinned).
   * If page is in pool, returns it directly (cache hit).
   * If not, loads from disk and potentially evicts an LRU page.
   * 
   * @param {number} pageId
   * @param {Function} [readFn] - Optional callback (pageId) => Buffer to read page from disk
   * @returns {Buffer|null} Page data, or null if fetch failed
   */
  fetchPage(pageId, readFn) {
    // Check if page is already in the pool
    if (this._pageTable.has(pageId)) {
      const frameId = this._pageTable.get(pageId);
      const frame = this._frames[frameId];
      frame.pinCount++;
      this.replacer.pin(frameId);
      this._hits++;
      return readFn ? frame : frame.data;
    }
    
    // Page not in pool — need to load from disk
    this._misses++;
    
    // Get a free frame (or evict)
    const frameId = this._getFrame();
    if (frameId === -1) return null; // No available frames (all pinned)
    
    // Load page from disk (use callback if provided)
    const frame = this._frames[frameId];
    try {
      frame.data = readFn ? readFn(pageId) : (this.disk ? this.disk.readPage(pageId) : null);
    } catch (e) {
      // Page doesn't exist on disk
      this._freeList.push(frameId);
      return null;
    }
    frame.pageId = pageId;
    frame.pinCount = 1;
    frame.dirty = false;
    
    this._pageTable.set(pageId, frameId);
    this.replacer.pin(frameId); // Pinned (active use)
    
    return readFn ? frame : frame.data;
  }

  /**
   * Unpin a page. Decrements pin count.
   * If pin count reaches 0, the page becomes evictable.
   * 
   * @param {number} pageId
   * @param {boolean} isDirty - Whether the page was modified
   * @returns {boolean} Success
   */
  unpinPage(pageId, isDirty = false) {
    if (!this._pageTable.has(pageId)) return false;
    
    const frameId = this._pageTable.get(pageId);
    const frame = this._frames[frameId];
    
    if (frame.pinCount <= 0) return false; // Already unpinned
    
    frame.pinCount--;
    if (isDirty) frame.dirty = true;
    
    if (frame.pinCount === 0) {
      this.replacer.record(frameId); // Mark as recently used
      this.replacer.unpin(frameId);  // Make evictable
    }
    
    return true;
  }

  /**
   * Flush a specific page to disk.
   * @param {number} pageId
   * @returns {boolean} Success
   */
  flushPage(pageId) {
    if (!this._pageTable.has(pageId)) return false;
    
    const frameId = this._pageTable.get(pageId);
    const frame = this._frames[frameId];
    
    if (frame.dirty) {
      this.disk.writePage(pageId, frame.data);
      frame.dirty = false;
    }
    
    return true;
  }

  /**
   * Allocate a new page. Returns {pageId, data} or null if pool is full.
   */
  newPage() {
    if (!this.disk) return null; // Can't allocate without disk manager
    const frameId = this._getFrame();
    if (frameId === -1) return null;
    
    const pageId = this.disk.allocatePage();
    const frame = this._frames[frameId];
    frame.data = Buffer.alloc(this.disk.pageSize || 4096);
    frame.pageId = pageId;
    frame.pinCount = 1;
    frame.dirty = true; // New page needs to be written
    
    this._pageTable.set(pageId, frameId);
    this.replacer.pin(frameId);
    
    return { pageId, data: frame.data };
  }

  /**
   * Delete a page from the buffer pool and disk.
   * @param {number} pageId
   * @returns {boolean} Success
   */
  deletePage(pageId) {
    if (this._pageTable.has(pageId)) {
      const frameId = this._pageTable.get(pageId);
      const frame = this._frames[frameId];
      
      if (frame.pinCount > 0) return false; // Can't delete pinned page
      
      this.replacer.remove(frameId);
      this._pageTable.delete(pageId);
      frame.reset();
      this._freeList.push(frameId);
    }
    
    if (this.disk) this.disk.deallocatePage(pageId);
    return true;
  }

  /**
   * Flush all dirty pages to disk.
   */
  flushAll(writeFn) {
    for (const [pageId, frameId] of this._pageTable) {
      const frame = this._frames[frameId];
      if (frame.dirty) {
        if (writeFn) {
          writeFn(pageId, frame.data);
        } else if (this.disk) {
          this.disk.writePage(pageId, frame.data);
        }
        frame.dirty = false;
      }
    }
  }

  /**
   * Get buffer pool statistics.
   */
  get stats() {
    let pinned = 0, dirty = 0, used = 0;
    for (const frame of this._frames) {
      if (frame.pageId !== -1) {
        used++;
        if (frame.pinCount > 0) pinned++;
        if (frame.dirty) dirty++;
      }
    }
    return {
      poolSize: this.poolSize,
      replacer: this.replacerType,
      used,
      free: this._freeList.length,
      pinned,
      dirty,
      evictable: this.replacer.size(),
      hits: this._hits,
      misses: this._misses,
      evictions: this._evictions,
      hitRate: this._hits + this._misses > 0
        ? (this._hits / (this._hits + this._misses) * 100).toFixed(1) + '%'
        : 'N/A',
      disk: this.disk ? this.disk.stats : null,
    };
  }

  // --- Internal ---

  /**
   * Get a free frame, evicting if necessary.
   * @returns {number} frameId, or -1 if all frames are pinned
   */
  _getFrame() {
    // Try free list first
    if (this._freeList.length > 0) {
      return this._freeList.pop();
    }
    
    // No free frames — evict LRU
    const victimFrameId = this.replacer.evict();
    if (victimFrameId === -1) return -1; // All frames pinned
    this._evictions++;
    
    const frame = this._frames[victimFrameId];
    
    // Flush dirty page before eviction
    if (frame.dirty) {
      if (this._evictCallback) {
        this._evictCallback(frame.pageId, frame.data);
      }
      if (this.disk) {
        this.disk.writePage(frame.pageId, frame.data);
      }
      frame.dirty = false;
    }
    
    // Remove old page from page table
    this._pageTable.delete(frame.pageId);
    frame.reset();
    
        return victimFrameId;
  }

  /** Alias: stats() as method (for compatibility with code calling bp.stats()) */
  getStats() { return this.stats; }
}

export { BufferPoolManager as BufferPool };
