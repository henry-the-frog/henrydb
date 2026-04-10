// visibility-map.js — Visibility Map for MVCC optimization
// Tracks which pages have all-visible tuples (no MVCC check needed)
// Updated by VACUUM, invalidated by INSERT/UPDATE/DELETE

/**
 * VisibilityMap: a bitmap where each bit represents a page.
 * 1 = all tuples on this page are visible to all transactions
 * 0 = some tuples may not be visible (need MVCC check)
 */
export class VisibilityMap {
  constructor() {
    this._pages = new Set(); // Set of page numbers that are all-visible
    this._stats = { hits: 0, misses: 0 };
  }

  /**
   * Check if a page is all-visible.
   */
  isAllVisible(pageId) {
    const visible = this._pages.has(pageId);
    if (visible) this._stats.hits++;
    else this._stats.misses++;
    return visible;
  }

  /**
   * Mark a page as all-visible (called by VACUUM after confirming all tuples visible).
   */
  setAllVisible(pageId) {
    this._pages.add(pageId);
  }

  /**
   * Clear the all-visible bit for a page (called when a tuple is modified).
   * Must be called on INSERT, UPDATE, DELETE to the page.
   */
  clearPage(pageId) {
    this._pages.delete(pageId);
  }

  /**
   * Clear all pages (full invalidation).
   */
  clearAll() {
    this._pages.clear();
  }

  /**
   * Get the number of all-visible pages.
   */
  get visibleCount() {
    return this._pages.size;
  }

  /**
   * Get hit/miss statistics.
   */
  getStats() {
    const total = this._stats.hits + this._stats.misses;
    return {
      ...this._stats,
      hitRate: total > 0 ? this._stats.hits / total : 0,
      visiblePages: this._pages.size
    };
  }

  /**
   * Reset statistics.
   */
  resetStats() {
    this._stats = { hits: 0, misses: 0 };
  }
}

/**
 * TableVisibilityMap: manages visibility maps for all tables.
 */
export class TableVisibilityMap {
  constructor() {
    this._maps = new Map(); // tableName → VisibilityMap
  }

  /**
   * Get or create the visibility map for a table.
   */
  getMap(tableName) {
    if (!this._maps.has(tableName)) {
      this._maps.set(tableName, new VisibilityMap());
    }
    return this._maps.get(tableName);
  }

  /**
   * Check if a page in a table is all-visible.
   */
  isAllVisible(tableName, pageId) {
    return this.getMap(tableName).isAllVisible(pageId);
  }

  /**
   * Mark a page as all-visible.
   */
  setAllVisible(tableName, pageId) {
    this.getMap(tableName).setAllVisible(pageId);
  }

  /**
   * Invalidate a page when it's modified.
   */
  onPageModified(tableName, pageId) {
    this.getMap(tableName).clearPage(pageId);
  }

  /**
   * Get aggregate statistics across all tables.
   */
  getStats() {
    const result = {};
    for (const [name, map] of this._maps) {
      result[name] = map.getStats();
    }
    return result;
  }
}
