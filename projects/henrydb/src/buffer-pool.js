// buffer-pool.js — Buffer Pool Manager with LRU eviction
// Manages a fixed-size cache of pages in memory.
// Pages can be pinned (in use) or unpinned (evictable).
// Dirty pages are tracked for write-back on eviction.
// This is the core I/O abstraction for a disk-based database.

/**
 * BufferPoolManager — LRU page cache with pin/unpin semantics.
 */
export class BufferPoolManager {
  constructor(poolSize = 64, pageSize = 4096) {
    this.poolSize = poolSize;
    this.pageSize = pageSize;

    // Frame table: frameId → { pageId, data, pinCount, dirty, refBit }
    this._frames = new Array(poolSize);
    for (let i = 0; i < poolSize; i++) {
      this._frames[i] = { pageId: null, data: null, pinCount: 0, dirty: false, refBit: false };
    }

    // Page table: pageId → frameId
    this._pageTable = new Map();

    // Free list (unused frames)
    this._freeList = [];
    for (let i = poolSize - 1; i >= 0; i--) this._freeList.push(i);

    // LRU list for eviction (doubly linked list via Map for O(1) operations)
    this._lruOrder = []; // frameIds in LRU order (front = least recently used)
    this._lruSet = new Set();

    // Disk storage simulation
    this._disk = new Map(); // pageId → Buffer

    // Stats
    this.stats = { hits: 0, misses: 0, evictions: 0, dirtyEvictions: 0, fetches: 0, flushes: 0 };
  }

  /**
   * Fetch a page into the buffer pool and pin it.
   * Returns the frame with the page data.
   */
  fetchPage(pageId) {
    // Check if page is already in buffer pool
    if (this._pageTable.has(pageId)) {
      const frameId = this._pageTable.get(pageId);
      const frame = this._frames[frameId];
      frame.pinCount++;
      frame.refBit = true;
      this._removeLru(frameId); // Pinned pages aren't in LRU
      this.stats.hits++;
      return { frameId, data: frame.data, pageId };
    }

    // Page not in pool — need to fetch from "disk"
    this.stats.misses++;
    const frameId = this._getFrame();
    if (frameId === null) return null; // All frames pinned

    const frame = this._frames[frameId];
    
    // Load page data
    frame.pageId = pageId;
    frame.data = this._readFromDisk(pageId);
    frame.pinCount = 1;
    frame.dirty = false;
    frame.refBit = true;

    this._pageTable.set(pageId, frameId);
    this.stats.fetches++;

    return { frameId, data: frame.data, pageId };
  }

  /**
   * Create a new page and pin it.
   */
  newPage() {
    const pageId = this._nextPageId();
    const frameId = this._getFrame();
    if (frameId === null) return null;

    const frame = this._frames[frameId];
    frame.pageId = pageId;
    frame.data = Buffer.alloc(this.pageSize);
    frame.pinCount = 1;
    frame.dirty = true;
    frame.refBit = true;

    this._pageTable.set(pageId, frameId);
    return { frameId, data: frame.data, pageId };
  }

  /**
   * Unpin a page. When pinCount reaches 0, the page becomes evictable.
   */
  unpinPage(pageId, isDirty = false) {
    if (!this._pageTable.has(pageId)) return false;

    const frameId = this._pageTable.get(pageId);
    const frame = this._frames[frameId];

    if (frame.pinCount <= 0) return false;
    frame.pinCount--;
    if (isDirty) frame.dirty = true;

    if (frame.pinCount === 0) {
      this._addLru(frameId);
    }

    return true;
  }

  /**
   * Flush a specific page to "disk".
   */
  flushPage(pageId) {
    if (!this._pageTable.has(pageId)) return false;

    const frameId = this._pageTable.get(pageId);
    const frame = this._frames[frameId];

    this._writeToDisk(frame.pageId, frame.data);
    frame.dirty = false;
    this.stats.flushes++;
    return true;
  }

  /**
   * Flush all dirty pages.
   */
  flushAll() {
    for (let i = 0; i < this.poolSize; i++) {
      const frame = this._frames[i];
      if (frame.pageId !== null && frame.dirty) {
        this._writeToDisk(frame.pageId, frame.data);
        frame.dirty = false;
        this.stats.flushes++;
      }
    }
  }

  /**
   * Delete a page from the buffer pool and disk.
   */
  deletePage(pageId) {
    if (this._pageTable.has(pageId)) {
      const frameId = this._pageTable.get(pageId);
      const frame = this._frames[frameId];
      if (frame.pinCount > 0) return false; // Can't delete pinned page

      this._removeLru(frameId);
      this._pageTable.delete(pageId);
      frame.pageId = null;
      frame.data = null;
      frame.dirty = false;
      this._freeList.push(frameId);
    }

    this._disk.delete(pageId);
    return true;
  }

  /**
   * Get a free or evictable frame.
   */
  _getFrame() {
    // Try free list first
    if (this._freeList.length > 0) {
      return this._freeList.pop();
    }

    // Evict from LRU
    while (this._lruOrder.length > 0) {
      const frameId = this._lruOrder.shift();
      this._lruSet.delete(frameId);
      const frame = this._frames[frameId];

      if (frame.pinCount > 0) continue; // Skip pinned (shouldn't happen in LRU)

      // Write dirty page back
      if (frame.dirty) {
        this._writeToDisk(frame.pageId, frame.data);
        this.stats.dirtyEvictions++;
      }

      this._pageTable.delete(frame.pageId);
      this.stats.evictions++;
      return frameId;
    }

    return null; // All frames pinned
  }

  _addLru(frameId) {
    if (!this._lruSet.has(frameId)) {
      this._lruOrder.push(frameId);
      this._lruSet.add(frameId);
    }
  }

  _removeLru(frameId) {
    if (this._lruSet.has(frameId)) {
      this._lruOrder = this._lruOrder.filter(id => id !== frameId);
      this._lruSet.delete(frameId);
    }
  }

  _readFromDisk(pageId) {
    if (this._disk.has(pageId)) {
      return Buffer.from(this._disk.get(pageId));
    }
    return Buffer.alloc(this.pageSize);
  }

  _writeToDisk(pageId, data) {
    this._disk.set(pageId, Buffer.from(data));
  }

  _nextPageId() {
    if (!this._pageIdCounter) this._pageIdCounter = 0;
    return this._pageIdCounter++;
  }

  getStats() {
    const inUse = this._frames.filter(f => f.pageId !== null).length;
    const pinned = this._frames.filter(f => f.pinCount > 0).length;
    const dirty = this._frames.filter(f => f.dirty).length;
    return {
      ...this.stats,
      poolSize: this.poolSize,
      inUse,
      pinned,
      dirty,
      freeFrames: this._freeList.length,
      lruSize: this._lruOrder.length,
      hitRate: this.stats.hits + this.stats.misses > 0
        ? ((this.stats.hits / (this.stats.hits + this.stats.misses)) * 100).toFixed(1) + '%'
        : '0%',
    };
  }
}

export { BufferPoolManager as BufferPool };
