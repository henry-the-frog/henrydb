// clock-sweep.js — Page cache with Clock Sweep eviction (PostgreSQL-style)
// Each frame has a usage counter; sweep decrements until finding a 0.

export class ClockSweepCache {
  constructor(capacity) {
    this.capacity = capacity;
    this.frames = new Array(capacity).fill(null).map(() => ({ pageId: null, data: null, usage: 0, dirty: false, pinned: false }));
    this.pageToFrame = new Map();
    this.hand = 0;
    this.stats = { hits: 0, misses: 0, evictions: 0 };
  }

  /** Fetch a page from cache or load it */
  get(pageId, loadFn) {
    if (this.pageToFrame.has(pageId)) {
      this.stats.hits++;
      const frameIdx = this.pageToFrame.get(pageId);
      this.frames[frameIdx].usage = Math.min(this.frames[frameIdx].usage + 1, 5);
      return this.frames[frameIdx].data;
    }

    this.stats.misses++;
    const data = loadFn ? loadFn(pageId) : null;
    this._insert(pageId, data);
    return data;
  }

  /** Pin a page (prevent eviction) */
  pin(pageId) {
    if (this.pageToFrame.has(pageId)) {
      this.frames[this.pageToFrame.get(pageId)].pinned = true;
    }
  }

  /** Unpin a page */
  unpin(pageId) {
    if (this.pageToFrame.has(pageId)) {
      this.frames[this.pageToFrame.get(pageId)].pinned = false;
    }
  }

  /** Mark a page as dirty */
  markDirty(pageId) {
    if (this.pageToFrame.has(pageId)) {
      this.frames[this.pageToFrame.get(pageId)].dirty = true;
    }
  }

  _insert(pageId, data) {
    // Find empty frame first
    for (let i = 0; i < this.capacity; i++) {
      if (this.frames[i].pageId == null) {
        this.frames[i] = { pageId, data, usage: 1, dirty: false, pinned: false };
        this.pageToFrame.set(pageId, i);
        return;
      }
    }

    // Clock sweep to find victim
    const victim = this._sweep();
    const old = this.frames[victim];
    if (old.pageId != null) {
      this.pageToFrame.delete(old.pageId);
      this.stats.evictions++;
    }
    this.frames[victim] = { pageId, data, usage: 1, dirty: false, pinned: false };
    this.pageToFrame.set(pageId, victim);
  }

  _sweep() {
    let attempts = 0;
    while (attempts < this.capacity * 10) {
      const frame = this.frames[this.hand];
      if (!frame.pinned) {
        if (frame.usage === 0) {
          const victim = this.hand;
          this.hand = (this.hand + 1) % this.capacity;
          return victim;
        }
        frame.usage--;
      }
      this.hand = (this.hand + 1) % this.capacity;
      attempts++;
    }
    // Fallback: evict first unpinned
    for (let i = 0; i < this.capacity; i++) {
      if (!this.frames[i].pinned) return i;
    }
    throw new Error('All pages pinned — cannot evict');
  }

  /** Get dirty pages */
  getDirtyPages() {
    return this.frames.filter(f => f.dirty && f.pageId != null).map(f => f.pageId);
  }

  /** Flush all dirty pages */
  flush(writeFn) {
    for (const frame of this.frames) {
      if (frame.dirty && frame.pageId != null) {
        if (writeFn) writeFn(frame.pageId, frame.data);
        frame.dirty = false;
      }
    }
  }

  get size() { return this.pageToFrame.size; }
  get hitRate() { const total = this.stats.hits + this.stats.misses; return total > 0 ? this.stats.hits / total : 0; }
}
