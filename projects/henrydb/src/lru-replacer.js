// lru-replacer.js — LRU page replacement policy for buffer pool
//
// Implements the "Least Recently Used" eviction strategy using a
// doubly-linked list + hash map for O(1) operations:
//   - record(frameId): Mark frame as recently accessed (move to MRU end)
//   - evict(): Remove and return the LRU frame (least recently used)
//   - remove(frameId): Remove a specific frame from consideration
//   - pin(frameId): Pin a frame (prevent eviction)
//   - unpin(frameId): Unpin a frame (allow eviction)
//   - size(): Number of evictable frames
//
// The replacer tracks which frames are evictable. Pinned frames are
// NOT evictable — they must be explicitly unpinned first.

/**
 * Doubly-linked list node.
 */
class DLLNode {
  constructor(frameId) {
    this.frameId = frameId;
    this.prev = null;
    this.next = null;
  }
}

/**
 * LRUReplacer — Tracks evictable frames in LRU order.
 * 
 * Internal structure:
 *   sentinel.next → MRU → ... → LRU → sentinel (circular doubly-linked)
 *   Actually: head sentinel <-> node1 <-> node2 <-> ... <-> tail sentinel
 *   Head side = MRU, Tail side = LRU
 *   Eviction removes from tail side (LRU).
 *   Recording moves/adds to head side (MRU).
 */
export class LRUReplacer {
  /**
   * @param {number} capacity - Maximum number of frames the replacer can track
   */
  constructor(capacity) {
    this.capacity = capacity;
    this._map = new Map();    // frameId → DLLNode
    this._pinned = new Set(); // frameIds that are pinned (not evictable)
    
    // Sentinel nodes for doubly-linked list
    this._head = new DLLNode(-1); // MRU side
    this._tail = new DLLNode(-2); // LRU side
    this._head.next = this._tail;
    this._tail.prev = this._head;
  }

  /**
   * Number of evictable frames (unpinned frames in the list).
   */
  size() {
    return this._map.size;
  }

  /**
   * Record access to a frame. Moves it to MRU position.
   * If frame is pinned, it stays pinned but position is updated for when it's unpinned.
   */
  record(frameId) {
    if (this._pinned.has(frameId)) {
      // Frame is pinned — don't add to evictable list
      // But track it so unpin can add it at MRU position
      return;
    }
    
    if (this._map.has(frameId)) {
      // Already in list — move to MRU position
      const node = this._map.get(frameId);
      this._detach(node);
      this._addToHead(node);
    } else {
      // New frame — add to MRU position
      const node = new DLLNode(frameId);
      this._addToHead(node);
      this._map.set(frameId, node);
    }
  }

  /**
   * Evict the least recently used frame.
   * Returns the frameId, or -1 if no evictable frames.
   */
  evict() {
    if (this._map.size === 0) return -1;
    
    // LRU frame is at tail side
    const lruNode = this._tail.prev;
    if (lruNode === this._head) return -1; // Empty list
    
    this._detach(lruNode);
    this._map.delete(lruNode.frameId);
    return lruNode.frameId;
  }

  /**
   * Remove a frame from the replacer entirely.
   * Used when a page is deleted from the buffer pool.
   */
  remove(frameId) {
    this._pinned.delete(frameId);
    if (this._map.has(frameId)) {
      const node = this._map.get(frameId);
      this._detach(node);
      this._map.delete(frameId);
      return true;
    }
    return false;
  }

  /**
   * Pin a frame — remove from evictable set.
   * Pinned frames cannot be evicted.
   */
  pin(frameId) {
    this._pinned.add(frameId);
    if (this._map.has(frameId)) {
      const node = this._map.get(frameId);
      this._detach(node);
      this._map.delete(frameId);
    }
  }

  /**
   * Unpin a frame — add back to evictable set at MRU position.
   */
  unpin(frameId) {
    if (!this._pinned.has(frameId)) return;
    this._pinned.delete(frameId);
    
    // Add to MRU position
    if (!this._map.has(frameId)) {
      const node = new DLLNode(frameId);
      this._addToHead(node);
      this._map.set(frameId, node);
    }
  }

  /**
   * Check if a frame is evictable (in the list and not pinned).
   */
  isEvictable(frameId) {
    return this._map.has(frameId);
  }

  /**
   * Check if a frame is pinned.
   */
  isPinned(frameId) {
    return this._pinned.has(frameId);
  }

  // --- Internal doubly-linked list operations ---

  _addToHead(node) {
    node.next = this._head.next;
    node.prev = this._head;
    this._head.next.prev = node;
    this._head.next = node;
  }

  _detach(node) {
    node.prev.next = node.next;
    node.next.prev = node.prev;
    node.prev = null;
    node.next = null;
  }
}
