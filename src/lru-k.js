// lru-k.js — LRU-K replacement policy
// Instead of evicting the least recently used, evict the page whose
// Kth-to-last access is the oldest. This handles scan resistance better
// than plain LRU (K=2 is the most common choice: LRU-2).

export class LRUK {
  constructor(capacity, k = 2) {
    this.capacity = capacity;
    this.k = k;
    this._pages = new Map(); // pageId → { accessHistory: [timestamps], data }
    this._clock = 0;
  }

  access(pageId, data) {
    this._clock++;
    if (this._pages.has(pageId)) {
      const page = this._pages.get(pageId);
      page.accessHistory.push(this._clock);
      if (page.accessHistory.length > this.k) page.accessHistory.shift();
      if (data !== undefined) page.data = data;
      return { hit: true, data: page.data };
    }

    // Miss — may need to evict
    if (this._pages.size >= this.capacity) this._evict();

    this._pages.set(pageId, { accessHistory: [this._clock], data });
    return { hit: false, data };
  }

  get(pageId) {
    const page = this._pages.get(pageId);
    return page ? page.data : undefined;
  }

  _evict() {
    let victim = null, oldestKthAccess = Infinity;
    
    for (const [pageId, page] of this._pages) {
      // K-th to last access timestamp (or 0 if fewer than K accesses)
      const kthAccess = page.accessHistory.length >= this.k
        ? page.accessHistory[page.accessHistory.length - this.k]
        : 0; // Pages with < K accesses are evicted first
      
      if (kthAccess < oldestKthAccess) {
        oldestKthAccess = kthAccess;
        victim = pageId;
      }
    }

    if (victim !== null) this._pages.delete(victim);
  }

  get size() { return this._pages.size; }

  getStats() {
    return { size: this._pages.size, capacity: this.capacity, k: this.k };
  }
}
