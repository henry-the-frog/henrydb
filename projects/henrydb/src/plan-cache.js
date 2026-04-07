// plan-cache.js — Query plan cache with LRU eviction for HenryDB

/**
 * LRU Cache for parsed SQL ASTs.
 * Avoids re-parsing identical SQL strings.
 */
export class PlanCache {
  constructor(maxSize = 256) {
    this._maxSize = maxSize;
    this._cache = new Map(); // sql → { ast, hits, lastAccess }
    this._hits = 0;
    this._misses = 0;
  }

  /**
   * Get cached AST for a SQL string.
   * Returns the cached AST or null if not cached.
   */
  get(sql) {
    const entry = this._cache.get(sql);
    if (entry) {
      entry.hits++;
      entry.lastAccess = Date.now();
      this._hits++;
      // Move to end (most recently used) by deleting and re-adding
      this._cache.delete(sql);
      this._cache.set(sql, entry);
      return entry.ast;
    }
    this._misses++;
    return null;
  }

  /**
   * Store a parsed AST in the cache.
   */
  put(sql, ast) {
    if (this._cache.has(sql)) {
      this._cache.delete(sql);
    }
    
    // Evict LRU if at capacity
    if (this._cache.size >= this._maxSize) {
      // Map iterates in insertion order, first entry is LRU
      const lruKey = this._cache.keys().next().value;
      this._cache.delete(lruKey);
    }

    this._cache.set(sql, {
      ast,
      hits: 0,
      lastAccess: Date.now(),
      created: Date.now(),
    });
  }

  /**
   * Invalidate cache (e.g., after DDL changes).
   */
  clear() {
    this._cache.clear();
  }

  /**
   * Get cache statistics.
   */
  stats() {
    const total = this._hits + this._misses;
    return {
      size: this._cache.size,
      maxSize: this._maxSize,
      hits: this._hits,
      misses: this._misses,
      hitRate: total > 0 ? Math.round(this._hits / total * 1000) / 1000 : 0,
      entries: [...this._cache.entries()].map(([sql, entry]) => ({
        sql: sql.length > 50 ? sql.slice(0, 50) + '...' : sql,
        hits: entry.hits,
      })),
    };
  }
}
