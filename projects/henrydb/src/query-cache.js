// query-cache.js — Query result cache with TTL and auto-invalidation
// Caches SELECT query results, invalidates on table mutations.

/**
 * QueryCache — LRU cache for query results with TTL and table dependency tracking.
 * 
 * Usage:
 *   const cache = new QueryCache({ maxEntries: 100, defaultTTL: 60000 });
 *   
 *   // Cache a query result
 *   cache.set('SELECT * FROM users', ['users'], [{ id: 1, name: 'Alice' }]);
 *   
 *   // Get cached result
 *   const result = cache.get('SELECT * FROM users'); // returns rows or null
 *   
 *   // Invalidate when table changes
 *   cache.invalidate('users'); // Removes all queries that depend on 'users'
 */
export class QueryCache {
  constructor(options = {}) {
    this.maxEntries = options.maxEntries || 100;
    this.defaultTTL = options.defaultTTL || 60000; // 60 seconds
    this._cache = new Map();
    this._tableDeps = new Map(); // table → Set<sql>
    this._stats = {
      hits: 0,
      misses: 0,
      sets: 0,
      invalidations: 0,
      evictions: 0,
    };
  }

  /**
   * Normalize SQL for cache key (basic: lowercase, trim, collapse whitespace).
   */
  _normalize(sql) {
    return sql.trim().replace(/\s+/g, ' ').toLowerCase();
  }

  /**
   * Get cached query result.
   * @param {string} sql - SQL query
   * @returns {Array|null} Cached rows or null if miss/expired
   */
  get(sql) {
    const key = this._normalize(sql);
    const entry = this._cache.get(key);
    
    if (!entry) {
      this._stats.misses++;
      return null;
    }
    
    // Check TTL
    if (Date.now() > entry.expiresAt) {
      this._cache.delete(key);
      this._removeTableDeps(key);
      this._stats.misses++;
      return null;
    }
    
    // Update access time for LRU
    entry.lastAccessed = Date.now();
    entry.hitCount++;
    this._stats.hits++;
    
    return entry.rows;
  }

  /**
   * Cache a query result.
   * @param {string} sql - SQL query
   * @param {string[]} tables - Tables this query depends on
   * @param {Array} rows - Query result rows
   * @param {number} [ttl] - TTL in ms (default: defaultTTL)
   */
  set(sql, tables, rows, ttl) {
    const key = this._normalize(sql);
    
    // Evict if at capacity
    if (this._cache.size >= this.maxEntries && !this._cache.has(key)) {
      this._evictLRU();
    }
    
    const now = Date.now();
    this._cache.set(key, {
      sql: key,
      tables,
      rows,
      createdAt: now,
      lastAccessed: now,
      expiresAt: now + (ttl || this.defaultTTL),
      hitCount: 0,
    });
    
    // Track table dependencies
    for (const table of tables) {
      const t = table.toLowerCase();
      if (!this._tableDeps.has(t)) this._tableDeps.set(t, new Set());
      this._tableDeps.get(t).add(key);
    }
    
    this._stats.sets++;
  }

  /**
   * Invalidate all cached queries that depend on a table.
   * @param {string} table - Table name
   * @returns {number} Number of cache entries invalidated
   */
  invalidate(table) {
    const t = table.toLowerCase();
    const deps = this._tableDeps.get(t);
    if (!deps) return 0;
    
    let count = 0;
    for (const key of deps) {
      if (this._cache.delete(key)) count++;
    }
    this._tableDeps.delete(t);
    this._stats.invalidations += count;
    return count;
  }

  /**
   * Invalidate all cached queries.
   */
  invalidateAll() {
    const count = this._cache.size;
    this._cache.clear();
    this._tableDeps.clear();
    this._stats.invalidations += count;
    return count;
  }

  /**
   * Remove expired entries.
   */
  prune() {
    const now = Date.now();
    let pruned = 0;
    for (const [key, entry] of this._cache) {
      if (now > entry.expiresAt) {
        this._cache.delete(key);
        this._removeTableDeps(key);
        pruned++;
      }
    }
    return pruned;
  }

  /**
   * Evict the least recently used entry.
   */
  _evictLRU() {
    let oldest = null;
    let oldestKey = null;
    
    for (const [key, entry] of this._cache) {
      if (!oldest || entry.lastAccessed < oldest.lastAccessed) {
        oldest = entry;
        oldestKey = key;
      }
    }
    
    if (oldestKey) {
      this._cache.delete(oldestKey);
      this._removeTableDeps(oldestKey);
      this._stats.evictions++;
    }
  }

  /**
   * Remove a cache key from all table dependency sets.
   */
  _removeTableDeps(key) {
    for (const deps of this._tableDeps.values()) {
      deps.delete(key);
    }
  }

  /**
   * Get cache statistics.
   */
  stats() {
    const total = this._stats.hits + this._stats.misses;
    return {
      ...this._stats,
      entries: this._cache.size,
      hitRate: total > 0 ? +(this._stats.hits / total * 100).toFixed(1) : 0,
      tables: this._tableDeps.size,
    };
  }

  /**
   * Get cache size.
   */
  get size() { return this._cache.size; }

  /**
   * Extract table names from a SQL query (static utility).
   */
  static extractTables(sql) {
    const tables = new Set();
    const patterns = [
      /\bFROM\s+(\w+)/gi,
      /\bJOIN\s+(\w+)/gi,
      /\bINTO\s+(\w+)/gi,
      /\bUPDATE\s+(\w+)/gi,
      /\bTABLE\s+(\w+)/gi,
    ];
    for (const pat of patterns) {
      let m;
      while ((m = pat.exec(sql)) !== null) {
        tables.add(m[1].toLowerCase());
      }
    }
    return [...tables];
  }
}
