// clock-replacer.js — Clock (Second Chance) page replacement policy
//
// PostgreSQL-style clock sweep: instead of a binary ref bit,
// uses a usage_count (capped at 5) that's incremented on access
// and decremented by the clock hand. Pages with count > 0 get
// a "second chance" — only evicted when count reaches 0.
//
// Advantages over LRU:
//   - Lower overhead (no linked list manipulation)
//   - Better resistance to sequential flooding
//   - Frequently-accessed pages build up "heat" (usage count)

/**
 * ClockReplacer — PostgreSQL-style clock sweep eviction.
 * 
 * Each frame has a usage_count (0..maxUsage). On access, count is bumped.
 * On eviction, the clock hand sweeps circularly:
 *   - If count > 0: decrement and skip (second chance)
 *   - If count == 0: evict this frame
 * 
 * This means frequently-accessed pages require multiple sweeps to evict,
 * while cold pages (accessed once) are evicted quickly.
 */
export class ClockReplacer {
  constructor(capacity, maxUsage = 5) {
    this.capacity = capacity;
    this.maxUsage = maxUsage;
    
    // Circular buffer for clock
    this._frames = new Array(capacity);
    for (let i = 0; i < capacity; i++) {
      this._frames[i] = {
        occupied: false,  // Is this slot tracking a frame?
        frameId: -1,
        usageCount: 0,
        pinned: false,
      };
    }
    
    // Free slot tracking
    this._freeSlots = [];
    for (let i = capacity - 1; i >= 0; i--) {
      this._freeSlots.push(i);
    }
    
    // frameId → slot index
    this._frameToSlot = new Map();
    this._clockHand = 0;
    this._evictableCount = 0;
  }

  size() {
    return this._evictableCount;
  }

  /**
   * Record access to a frame. Bumps usage count (capped at maxUsage).
   */
  record(frameId) {
    if (!this._frameToSlot.has(frameId)) {
      // New frame — find an empty slot
      const slot = this._findEmptySlot();
      if (slot === -1) return; // Shouldn't happen if managed correctly
      
      this._frames[slot].occupied = true;
      this._frames[slot].frameId = frameId;
      this._frames[slot].usageCount = 1;
      this._frames[slot].pinned = false;
      this._frameToSlot.set(frameId, slot);
      this._evictableCount++;
    } else {
      const slot = this._frameToSlot.get(frameId);
      const frame = this._frames[slot];
      if (!frame.pinned) {
        frame.usageCount = Math.min(frame.usageCount + 1, this.maxUsage);
      }
    }
  }

  /**
   * Evict the next victim using clock sweep.
   * Returns frameId, or -1 if no evictable frames.
   */
  evict() {
    if (this._evictableCount === 0) return -1;
    
    let scanned = 0;
    const maxScans = this.capacity * (this.maxUsage + 1); // Guaranteed to find a victim
    
    while (scanned < maxScans) {
      const frame = this._frames[this._clockHand];
      
      if (frame.occupied && !frame.pinned) {
        if (frame.usageCount === 0) {
          // Found a victim!
          const victimId = frame.frameId;
          frame.occupied = false;
          frame.frameId = -1;
          this._frameToSlot.delete(victimId);
          this._freeSlots.push(this._clockHand);
          this._evictableCount--;
          this._clockHand = (this._clockHand + 1) % this.capacity;
          return victimId;
        } else {
          // Give second chance: decrement and move on
          frame.usageCount--;
        }
      }
      
      this._clockHand = (this._clockHand + 1) % this.capacity;
      scanned++;
    }
    
    return -1; // Should not reach here
  }

  /**
   * Remove a frame entirely.
   */
  remove(frameId) {
    if (!this._frameToSlot.has(frameId)) return false;
    
    const slot = this._frameToSlot.get(frameId);
    const frame = this._frames[slot];
    
    if (!frame.pinned) this._evictableCount--;
    
    frame.occupied = false;
    frame.frameId = -1;
    frame.usageCount = 0;
    frame.pinned = false;
    this._frameToSlot.delete(frameId);
    this._freeSlots.push(slot);
    return true;
  }

  /**
   * Pin a frame — prevent eviction.
   */
  pin(frameId) {
    if (!this._frameToSlot.has(frameId)) {
      // Frame not tracked — just record as pinned
      const slot = this._findEmptySlot();
      if (slot === -1) return;
      this._frames[slot].occupied = true;
      this._frames[slot].frameId = frameId;
      this._frames[slot].usageCount = 1;
      this._frames[slot].pinned = true;
      this._frameToSlot.set(frameId, slot);
      return;
    }
    
    const slot = this._frameToSlot.get(frameId);
    const frame = this._frames[slot];
    if (!frame.pinned) {
      frame.pinned = true;
      this._evictableCount--;
    }
  }

  /**
   * Unpin a frame — make evictable.
   */
  unpin(frameId) {
    if (!this._frameToSlot.has(frameId)) return;
    
    const slot = this._frameToSlot.get(frameId);
    const frame = this._frames[slot];
    if (frame.pinned) {
      frame.pinned = false;
      this._evictableCount++;
    }
  }

  isEvictable(frameId) {
    if (!this._frameToSlot.has(frameId)) return false;
    const slot = this._frameToSlot.get(frameId);
    return this._frames[slot].occupied && !this._frames[slot].pinned;
  }

  isPinned(frameId) {
    if (!this._frameToSlot.has(frameId)) return false;
    const slot = this._frameToSlot.get(frameId);
    return this._frames[slot].pinned;
  }

  /**
   * Get the usage count for a frame (for debugging/testing).
   */
  getUsageCount(frameId) {
    if (!this._frameToSlot.has(frameId)) return -1;
    const slot = this._frameToSlot.get(frameId);
    return this._frames[slot].usageCount;
  }

  _findEmptySlot() {
    return this._freeSlots.length > 0 ? this._freeSlots.pop() : -1;
  }
}
