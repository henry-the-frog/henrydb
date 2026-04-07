// buffer-pool.js — Buffer pool manager for HenryDB
// Manages a fixed-size cache of pages with LRU eviction.
// Classic database memory management component.

/**
 * Buffer frame: a slot in the buffer pool that holds one page.
 */
class BufferFrame {
  constructor(pageId = -1) {
    this.pageId = pageId;
    this.data = null;      // Page data
    this.pinCount = 0;     // Number of users currently accessing this page
    this.isDirty = false;  // Has the page been modified?
    this.lastAccess = 0;   // Timestamp of last access
  }
}

/**
 * Buffer Pool Manager.
 * Manages a fixed number of page frames in memory.
 * Uses LRU replacement for eviction.
 */
export class BufferPool {
  constructor(poolSize = 64) {
    this._poolSize = poolSize;
    this._frames = new Array(poolSize).fill(null).map(() => new BufferFrame());
    this._pageTable = new Map(); // pageId → frameIndex
    this._nextTimestamp = 0;
    this._onEvict = null; // Callback: (pageId, data) => void — called when dirty page is evicted
    
    // Statistics
    this._hits = 0;
    this._misses = 0;
    this._evictions = 0;
    this._flushes = 0;
  }

  /** Set eviction callback for dirty pages. */
  setEvictCallback(fn) {
    this._onEvict = fn;
  }

  /**
   * Fetch a page. Returns the frame containing the page data.
   * If not in pool, loads it and possibly evicts another page.
   * 
   * @param {number} pageId - Page identifier
   * @param {Function} loader - Function to load page data from disk: (pageId) => data
   * @returns {BufferFrame} The frame containing the page
   */
  fetchPage(pageId, loader) {
    // Check if page is already in the buffer pool
    if (this._pageTable.has(pageId)) {
      const frameIdx = this._pageTable.get(pageId);
      const frame = this._frames[frameIdx];
      frame.pinCount++;
      frame.lastAccess = this._nextTimestamp++;
      this._hits++;
      return frame;
    }

    this._misses++;

    // Find a free frame or evict one
    let frameIdx = this._findFreeFrame();
    if (frameIdx === -1) {
      frameIdx = this._evict();
      if (frameIdx === -1) {
        throw new Error('Buffer pool: all frames are pinned, cannot evict');
      }
    }

    const frame = this._frames[frameIdx];
    
    // Load page data
    frame.pageId = pageId;
    frame.data = loader(pageId);
    frame.pinCount = 1;
    frame.isDirty = false;
    frame.lastAccess = this._nextTimestamp++;

    this._pageTable.set(pageId, frameIdx);
    return frame;
  }

  /**
   * Unpin a page. Decrements pin count.
   * A page can only be evicted when pinCount = 0.
   * 
   * @param {number} pageId - Page to unpin
   * @param {boolean} isDirty - Whether the page was modified
   */
  unpinPage(pageId, isDirty = false) {
    if (!this._pageTable.has(pageId)) return false;
    const frameIdx = this._pageTable.get(pageId);
    const frame = this._frames[frameIdx];
    
    if (frame.pinCount <= 0) return false;
    frame.pinCount--;
    if (isDirty) frame.isDirty = true;
    return true;
  }

  /**
   * Flush a specific page to disk.
   * 
   * @param {number} pageId - Page to flush
   * @param {Function} writer - Function to write page data: (pageId, data) => void
   */
  flushPage(pageId, writer) {
    if (!this._pageTable.has(pageId)) return false;
    const frameIdx = this._pageTable.get(pageId);
    const frame = this._frames[frameIdx];
    
    if (frame.isDirty) {
      writer(frame.pageId, frame.data);
      frame.isDirty = false;
      this._flushes++;
    }
    return true;
  }

  /**
   * Flush all dirty pages.
   */
  flushAll(writer) {
    let flushed = 0;
    for (const frame of this._frames) {
      if (frame.isDirty && frame.pageId >= 0) {
        writer(frame.pageId, frame.data);
        frame.isDirty = false;
        flushed++;
      }
    }
    this._flushes += flushed;
    return flushed;
  }

  /**
   * Find a free (unused) frame.
   */
  _findFreeFrame() {
    for (let i = 0; i < this._poolSize; i++) {
      if (this._frames[i].pageId === -1) return i;
    }
    return -1;
  }

  /**
   * Evict the LRU unpinned page.
   * Returns the frame index of the evicted page, or -1 if all are pinned.
   */
  _evict() {
    let lruIdx = -1;
    let lruTime = Infinity;

    for (let i = 0; i < this._poolSize; i++) {
      const frame = this._frames[i];
      if (frame.pinCount === 0 && frame.lastAccess < lruTime) {
        lruIdx = i;
        lruTime = frame.lastAccess;
      }
    }

    if (lruIdx === -1) return -1;

    const frame = this._frames[lruIdx];
    
    // Flush dirty page before eviction
    if (frame.isDirty && this._onEvict) {
      this._onEvict(frame.pageId, frame.data);
      this._flushes++;
    }
    
    // Remove from page table
    this._pageTable.delete(frame.pageId);
    
    // Reset frame
    frame.pageId = -1;
    frame.data = null;
    frame.isDirty = false;
    frame.pinCount = 0;
    
    this._evictions++;
    return lruIdx;
  }

  /**
   * Get buffer pool statistics.
   */
  stats() {
    let pinned = 0, dirty = 0, used = 0;
    for (const frame of this._frames) {
      if (frame.pageId >= 0) {
        used++;
        if (frame.pinCount > 0) pinned++;
        if (frame.isDirty) dirty++;
      }
    }
    const total = this._hits + this._misses;
    return {
      poolSize: this._poolSize,
      used,
      pinned,
      dirty,
      hits: this._hits,
      misses: this._misses,
      hitRate: total > 0 ? Math.round(this._hits / total * 1000) / 1000 : 0,
      evictions: this._evictions,
      flushes: this._flushes,
    };
  }
}
