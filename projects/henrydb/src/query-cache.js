// query-cache.js — Query result cache with TTL and write invalidation
// Caches the results of SELECT queries. Invalidated when:
// 1. TTL expires (time-based)
// 2. Any INSERT/UPDATE/DELETE touches a cached table (write invalidation)
// 3. Cache is full (LRU eviction)

/**
 * QueryCache — LRU cache with TTL and table-based invalidation.
 */
export class QueryCache {
  constructor(options = {}) {
    this.maxEntries = options.maxEntries || 1000;
    this.defaultTTLMs = options.defaultTTLMs || 60000; // 60s default
    this._cache = new Map(); // sql → { result, tables, insertedAt, ttl, hits }
    this._tableIndex = new Map(); // table → Set<sql> (for invalidation)
    this._accessOrder = []; // LRU tracking
    this.stats = { hits: 0, misses: 0, invalidations: 0, evictions: 0, sets: 0 };
  }

  /**
   * Get a cached result for a SQL query.
   * Returns the result or null if not cached/expired.
   */
  get(sql) {
    const entry = this._cache.get(sql);
    if (!entry) {
      this.stats.misses++;
      return null;
    }

    // Check TTL
    if (Date.now() - entry.insertedAt > entry.ttl) {
      this._remove(sql);
      this.stats.misses++;
      return null;
    }

    // LRU: move to end
    this._touch(sql);
    entry.hits++;
    this.stats.hits++;
    return entry.result;
  }

  /**
   * Cache a SELECT result.
   * @param {string} sql — the SQL query
   * @param {object} result — the query result { rows, ... }
   * @param {string[]} tables — tables referenced by this query
   * @param {number} ttl — TTL in ms (optional)
   */
  set(sql, result, tables = [], ttl = this.defaultTTLMs) {
    // Evict if full
    while (this._cache.size >= this.maxEntries) {
      this._evictLRU();
    }

    this._cache.set(sql, {
      result,
      tables,
      insertedAt: Date.now(),
      ttl,
      hits: 0,
    });

    // Update table index for invalidation
    for (const table of tables) {
      if (!this._tableIndex.has(table)) this._tableIndex.set(table, new Set());
      this._tableIndex.get(table).add(sql);
    }

    this._touch(sql);
    this.stats.sets++;
  }

  /**
   * Invalidate all cached results that reference a specific table.
   * Called after INSERT/UPDATE/DELETE on that table.
   */
  invalidateTable(table) {
    const queries = this._tableIndex.get(table);
    if (!queries) return 0;

    let count = 0;
    for (const sql of queries) {
      this._remove(sql);
      count++;
    }
    this._tableIndex.delete(table);
    this.stats.invalidations += count;
    return count;
  }

  /**
   * Invalidate all cached results.
   */
  invalidateAll() {
    const count = this._cache.size;
    this._cache.clear();
    this._tableIndex.clear();
    this._accessOrder = [];
    this.stats.invalidations += count;
    return count;
  }

  /**
   * Extract table names from a SQL query (simple heuristic).
   */
  static extractTables(sql) {
    const tables = new Set();
    const upper = sql.toUpperCase();
    
    // FROM table
    const fromMatch = upper.match(/FROM\s+(\w+)/g);
    if (fromMatch) {
      for (const m of fromMatch) {
        tables.add(m.replace(/FROM\s+/i, '').toLowerCase());
      }
    }

    // JOIN table
    const joinMatch = upper.match(/JOIN\s+(\w+)/g);
    if (joinMatch) {
      for (const m of joinMatch) {
        tables.add(m.replace(/JOIN\s+/i, '').toLowerCase());
      }
    }

    // INSERT INTO / UPDATE / DELETE FROM
    const writeMatch = upper.match(/(?:INTO|UPDATE|FROM)\s+(\w+)/g);
    if (writeMatch) {
      for (const m of writeMatch) {
        tables.add(m.replace(/(?:INTO|UPDATE|FROM)\s+/i, '').toLowerCase());
      }
    }

    return [...tables];
  }

  _remove(sql) {
    const entry = this._cache.get(sql);
    if (!entry) return;

    // Remove from table index
    for (const table of entry.tables) {
      const queries = this._tableIndex.get(table);
      if (queries) {
        queries.delete(sql);
        if (queries.size === 0) this._tableIndex.delete(table);
      }
    }

    this._cache.delete(sql);
    this._accessOrder = this._accessOrder.filter(s => s !== sql);
  }

  _touch(sql) {
    this._accessOrder = this._accessOrder.filter(s => s !== sql);
    this._accessOrder.push(sql);
  }

  _evictLRU() {
    if (this._accessOrder.length === 0) return;
    const oldest = this._accessOrder.shift();
    this._remove(oldest);
    this.stats.evictions++;
  }

  get size() { return this._cache.size; }
  
  getStats() {
    const hitRate = this.stats.hits + this.stats.misses > 0
      ? (this.stats.hits / (this.stats.hits + this.stats.misses) * 100).toFixed(1)
      : '0';
    return {
      ...this.stats,
      size: this._cache.size,
      hitRate: hitRate + '%',
    };
  }
}
