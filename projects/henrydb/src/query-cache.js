// query-cache.js — Server-side query result cache for HenryDB
// LRU cache with table-level invalidation on mutations.

/**
 * Query result cache with LRU eviction and automatic invalidation.
 */
export class QueryCache {
  constructor(options = {}) {
    this.maxSize = options.maxSize || 1000;
    this.maxAgeMs = options.maxAgeMs || 60000; // 1 minute default TTL
    this.enabled = options.enabled !== false;
    
    // LRU cache: key → { result, tables, timestamp, size, accessCount }
    this._cache = new Map();
    this._order = []; // LRU order (most recently used last)
    
    // Stats
    this.stats = {
      hits: 0,
      misses: 0,
      evictions: 0,
      invalidations: 0,
      sets: 0,
      totalSizeBytes: 0,
    };
  }

  /**
   * Get a cached result for a query.
   * @param {string} sql — The SQL query
   * @returns {object|null} Cached result or null
   */
  get(sql) {
    if (!this.enabled) return null;
    
    const key = this._normalizeKey(sql);
    const entry = this._cache.get(key);
    
    if (!entry) {
      this.stats.misses++;
      return null;
    }

    // Check TTL
    if (Date.now() - entry.timestamp > this.maxAgeMs) {
      this._cache.delete(key);
      this._removeFromOrder(key);
      this.stats.misses++;
      return null;
    }

    // Move to end (most recently used)
    this._removeFromOrder(key);
    this._order.push(key);
    entry.accessCount++;
    
    this.stats.hits++;
    return entry.result;
  }

  /**
   * Cache a query result.
   * @param {string} sql — The SQL query
   * @param {object} result — The query result
   * @param {string[]} tables — Tables referenced by the query (for invalidation)
   */
  set(sql, result, tables = []) {
    if (!this.enabled) return;
    
    const key = this._normalizeKey(sql);
    
    // Evict if at capacity
    while (this._cache.size >= this.maxSize) {
      this._evictLRU();
    }

    const size = JSON.stringify(result).length;
    
    this._cache.set(key, {
      result,
      tables: new Set(tables.map(t => t.toLowerCase())),
      timestamp: Date.now(),
      size,
      accessCount: 0,
    });
    
    this._removeFromOrder(key);
    this._order.push(key);
    this.stats.sets++;
    this.stats.totalSizeBytes += size;
  }

  /**
   * Invalidate all cached results that reference a table.
   * Called when a table is mutated (INSERT/UPDATE/DELETE/DROP).
   */
  invalidate(tableName) {
    const lowerName = tableName.toLowerCase();
    const keysToRemove = [];
    
    for (const [key, entry] of this._cache) {
      if (entry.tables.has(lowerName)) {
        keysToRemove.push(key);
      }
    }

    for (const key of keysToRemove) {
      const entry = this._cache.get(key);
      if (entry) this.stats.totalSizeBytes -= entry.size;
      this._cache.delete(key);
      this._removeFromOrder(key);
      this.stats.invalidations++;
    }
  }

  /**
   * Invalidate all cached results.
   */
  invalidateAll() {
    const count = this._cache.size;
    this._cache.clear();
    this._order = [];
    this.stats.invalidations += count;
    this.stats.totalSizeBytes = 0;
  }

  /**
   * Get cache statistics.
   */
  getStats() {
    const hitRate = this.stats.hits + this.stats.misses > 0
      ? this.stats.hits / (this.stats.hits + this.stats.misses)
      : 0;
    
    return {
      ...this.stats,
      entries: this._cache.size,
      hitRate: Math.round(hitRate * 10000) / 100, // percent with 2 decimals
      maxSize: this.maxSize,
      maxAgeMs: this.maxAgeMs,
    };
  }

  /**
   * Extract table names from a SQL query (best effort).
   */
  static extractTables(sql) {
    const tables = new Set();
    const upper = sql.toUpperCase();
    
    // FROM clause
    const fromMatches = sql.match(/FROM\s+(\w+)/gi);
    if (fromMatches) {
      for (const m of fromMatches) {
        tables.add(m.replace(/FROM\s+/i, '').toLowerCase());
      }
    }
    
    // JOIN clause
    const joinMatches = sql.match(/JOIN\s+(\w+)/gi);
    if (joinMatches) {
      for (const m of joinMatches) {
        tables.add(m.replace(/JOIN\s+/i, '').toLowerCase());
      }
    }
    
    return [...tables];
  }

  _normalizeKey(sql) {
    // Normalize whitespace for better cache hit rate
    return sql.trim().replace(/\s+/g, ' ').toLowerCase();
  }

  _evictLRU() {
    if (this._order.length === 0) return;
    const key = this._order.shift();
    const entry = this._cache.get(key);
    if (entry) this.stats.totalSizeBytes -= entry.size;
    this._cache.delete(key);
    this.stats.evictions++;
  }

  _removeFromOrder(key) {
    const idx = this._order.indexOf(key);
    if (idx !== -1) this._order.splice(idx, 1);
  }
}

export default QueryCache;
