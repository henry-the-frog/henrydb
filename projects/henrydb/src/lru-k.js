// lru-k.js — LRU-K Replacement Policy
// Tracks K-th most recent access timestamp for eviction decisions.
// Unlike LRU (K=1), LRU-K considers access frequency, not just recency.
// Used in: DB2, Oracle (K=2 is most common).
// Key insight: a page accessed twice recently is more valuable than one
// accessed once very recently (avoids "sequential flooding" problem).

export class LRUK {
  /**
   * @param {number} k - Number of accesses to track (2 is standard)
   * @param {number} maxSize - Maximum tracked frames
   */
  constructor(k = 2, maxSize = Infinity) {
    this._k = k;
    this._maxSize = maxSize;
    this._history = new Map(); // frameId → [timestamp_1, ..., timestamp_k]
    this._pinned = new Set();
    this._time = 0; // Logical clock
  }

  get size() { return this._history.size; }

  /**
   * Record an access to a frame. O(K).
   */
  record(frameId) {
    this._time++;
    let history = this._history.get(frameId);
    if (!history) {
      history = [];
      this._history.set(frameId, history);
    }
    history.push(this._time);
    if (history.length > this._k) {
      history.shift(); // Keep only last K accesses
    }
  }

  /**
   * Pin a frame (cannot be evicted).
   */
  pin(frameId) { this._pinned.add(frameId); }

  /**
   * Unpin a frame.
   */
  unpin(frameId) { this._pinned.delete(frameId); }

  /**
   * Evict the frame with the oldest K-th access.
   * Frames with < K accesses are evicted first (cold pages).
   * Among those with K accesses, evict the one with oldest K-th access.
   */
  evict() {
    let bestFrame = null;
    let bestScore = Infinity;
    let bestIsInf = false; // Prefer evicting pages with <K accesses

    for (const [frameId, history] of this._history) {
      if (this._pinned.has(frameId)) continue;

      if (history.length < this._k) {
        // Cold page: hasn't been accessed K times yet
        // Among cold pages, evict the one with oldest first access
        if (!bestIsInf || history[0] < bestScore) {
          bestFrame = frameId;
          bestScore = history[0];
          bestIsInf = true;
        }
      } else if (!bestIsInf) {
        // Warm page: has K accesses, use K-th oldest timestamp
        const kthAccess = history[0]; // oldest of K timestamps
        if (kthAccess < bestScore) {
          bestFrame = frameId;
          bestScore = kthAccess;
        }
      }
    }

    if (bestFrame !== null) {
      this._history.delete(bestFrame);
      this._pinned.delete(bestFrame);
    }
    return bestFrame;
  }

  /**
   * Remove a specific frame.
   */
  remove(frameId) {
    this._history.delete(frameId);
    this._pinned.delete(frameId);
  }

  /**
   * Get stats about all tracked frames.
   */
  getStats() {
    let cold = 0, warm = 0;
    for (const [, h] of this._history) {
      if (h.length < this._k) cold++;
      else warm++;
    }
    return { size: this._history.size, cold, warm, pinned: this._pinned.size };
  }
}
