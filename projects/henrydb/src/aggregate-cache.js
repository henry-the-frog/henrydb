// aggregate-cache.js — Server-side aggregate result cache
// Caches GROUP BY / aggregate query results and invalidates on table mutations.
// Designed for read-heavy workloads where aggregate results don't change frequently.

/**
 * AggregateCache — caches aggregate query results with table-level invalidation.
 * 
 * When a table is mutated (INSERT/UPDATE/DELETE), all cached results
 * that depend on that table are invalidated.
 */
export class AggregateCache {
  constructor(options = {}) {
    this.maxEntries = options.maxEntries || 200;
    this.maxAgeMs = options.maxAgeMs || 60000; // 1 minute default TTL
    this._cache = new Map(); // normalized SQL → CacheEntry
    this._tableDeps = new Map(); // table name → Set<cache keys>
    this._stats = {
      hits: 0,
      misses: 0,
      invalidations: 0,
      evictions: 0,
    };
  }

  /**
   * Check if a query result is cached.
   * Returns { hit: true, result } or { hit: false }.
   */
  get(sql) {
    const key = this._normalizeSQL(sql);
    const entry = this._cache.get(key);
    
    if (!entry) {
      this._stats.misses++;
      return { hit: false };
    }

    // Check TTL
    if (Date.now() - entry.createdAt > this.maxAgeMs) {
      this._remove(key);
      this._stats.misses++;
      return { hit: false };
    }

    entry.lastAccessed = Date.now();
    entry.hitCount++;
    this._stats.hits++;
    return { hit: true, result: entry.result };
  }

  /**
   * Cache an aggregate query result.
   * @param {string} sql - The SQL query
   * @param {object} result - The query result
   * @param {string[]} tables - Tables this query depends on
   */
  put(sql, result, tables = []) {
    const key = this._normalizeSQL(sql);

    // Evict if at capacity (LRU)
    if (this._cache.size >= this.maxEntries && !this._cache.has(key)) {
      this._evictLRU();
    }

    // Remove old entry's table deps if updating
    if (this._cache.has(key)) {
      this._removeDeps(key);
    }

    this._cache.set(key, {
      result,
      tables,
      createdAt: Date.now(),
      lastAccessed: Date.now(),
      hitCount: 0,
    });

    // Register table dependencies
    for (const table of tables) {
      const lowerTable = table.toLowerCase();
      if (!this._tableDeps.has(lowerTable)) {
        this._tableDeps.set(lowerTable, new Set());
      }
      this._tableDeps.get(lowerTable).add(key);
    }
  }

  /**
   * Invalidate all cached results that depend on a given table.
   * Called when the table is mutated (INSERT/UPDATE/DELETE/TRUNCATE).
   */
  invalidateTable(tableName) {
    const lowerTable = tableName.toLowerCase();
    const keys = this._tableDeps.get(lowerTable);
    if (!keys || keys.size === 0) return 0;

    let count = 0;
    for (const key of [...keys]) {
      this._remove(key);
      count++;
    }
    this._stats.invalidations += count;
    return count;
  }

  /**
   * Invalidate all entries.
   */
  invalidateAll() {
    const count = this._cache.size;
    this._cache.clear();
    this._tableDeps.clear();
    this._stats.invalidations += count;
    return count;
  }

  /**
   * Check if a SQL query is an aggregate/GROUP BY query worth caching.
   */
  isAggregateQuery(sql) {
    const upper = sql.toUpperCase();
    return /\bGROUP\s+BY\b/.test(upper) ||
           /\b(COUNT|SUM|AVG|MIN|MAX)\s*\(/.test(upper);
  }

  /**
   * Extract table names from a simple SELECT query.
   * Handles FROM and JOIN clauses.
   */
  extractTables(sql) {
    const tables = [];
    const upper = sql.toUpperCase();
    
    // FROM clause
    const fromMatch = upper.match(/\bFROM\s+(\w+)/);
    if (fromMatch) tables.push(fromMatch[1].toLowerCase());
    
    // JOIN clauses
    const joinRegex = /\bJOIN\s+(\w+)/g;
    let match;
    while ((match = joinRegex.exec(upper)) !== null) {
      tables.push(match[1].toLowerCase());
    }
    
    return [...new Set(tables)];
  }

  getStats() {
    const total = this._stats.hits + this._stats.misses;
    return {
      ...this._stats,
      entries: this._cache.size,
      hitRate: total > 0 ? +(this._stats.hits / total * 100).toFixed(1) : 0,
      tables: this._tableDeps.size,
    };
  }

  _normalizeSQL(sql) {
    return sql.trim().replace(/\s+/g, ' ').toLowerCase();
  }

  _remove(key) {
    this._removeDeps(key);
    this._cache.delete(key);
  }

  _removeDeps(key) {
    const entry = this._cache.get(key);
    if (!entry) return;
    for (const table of entry.tables) {
      const lowerTable = table.toLowerCase();
      const deps = this._tableDeps.get(lowerTable);
      if (deps) {
        deps.delete(key);
        if (deps.size === 0) this._tableDeps.delete(lowerTable);
      }
    }
  }

  _evictLRU() {
    let oldestKey = null;
    let oldestTime = Infinity;
    for (const [key, entry] of this._cache) {
      if (entry.lastAccessed < oldestTime) {
        oldestTime = entry.lastAccessed;
        oldestKey = key;
      }
    }
    if (oldestKey) {
      this._remove(oldestKey);
      this._stats.evictions++;
    }
  }
}

/**
 * CachingDatabase — wraps a Database to automatically cache aggregate results.
 */
export class CachingDatabase {
  constructor(db, options = {}) {
    this.db = db;
    this.cache = new AggregateCache(options);
  }

  execute(sql) {
    const upper = sql.trim().toUpperCase();

    // Mutations invalidate cache
    if (upper.startsWith('INSERT') || upper.startsWith('UPDATE') || 
        upper.startsWith('DELETE') || upper.startsWith('TRUNCATE')) {
      const tables = this.cache.extractTables(sql);
      for (const table of tables) {
        this.cache.invalidateTable(table);
      }
      // For INSERT/UPDATE/DELETE, extract target table from first word
      const targetMatch = sql.match(/^\s*(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE)\s+(\w+)/i);
      if (targetMatch) {
        this.cache.invalidateTable(targetMatch[1]);
      }
      return this.db.execute(sql);
    }

    // Check cache for aggregate queries
    if (this.cache.isAggregateQuery(sql)) {
      const cached = this.cache.get(sql);
      if (cached.hit) return cached.result;

      const result = this.db.execute(sql);
      const tables = this.cache.extractTables(sql);
      this.cache.put(sql, result, tables);
      return result;
    }

    return this.db.execute(sql);
  }
}
