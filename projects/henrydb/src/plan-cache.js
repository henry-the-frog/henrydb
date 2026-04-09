// plan-cache.js — Query plan cache with LRU eviction and DDL invalidation
// Caches compiled query plans keyed on normalized SQL.
// PostgreSQL-inspired: generic vs custom plans, auto-eviction, DDL invalidation.

/**
 * PlanCacheEntry — a cached query plan.
 */
class PlanCacheEntry {
  constructor(normalizedSQL, plan, tables) {
    this.normalizedSQL = normalizedSQL;
    this.plan = plan;
    this.tables = tables; // Tables referenced in the plan
    this.hitCount = 0;
    this.totalExecTime = 0;
    this.createdAt = Date.now();
    this.lastUsed = 0; // Set by PlanCache on insert
    this.estimatedCost = plan?.cost || 0;
    this.isGeneric = false; // True if plan uses generic parameters
  }
}

/**
 * PlanCache — LRU cache for query plans.
 */
export class PlanCache {
  constructor(options = {}) {
    if (typeof options === 'number') options = { maxEntries: options };
    this.maxEntries = options.maxEntries || 500;
    this.maxMemoryBytes = options.maxMemoryBytes || 50 * 1024 * 1024; // 50MB
    this._cache = new Map(); // normalizedSQL → PlanCacheEntry
    this._tableDeps = new Map(); // table → Set<normalizedSQL>
    this._stats = {
      hits: 0,
      misses: 0,
      evictions: 0,
      invalidations: 0,
      inserts: 0,
    };
    this._estimatedMemory = 0;
    this._accessCounter = 0; // Monotonic counter for LRU ordering
  }

  /**
   * Look up a cached plan.
   */
  get(sql) {
    const key = this._normalize(sql);
    const entry = this._cache.get(key);
    
    if (!entry) {
      this._stats.misses++;
      return null;
    }

    entry.hitCount++;
    entry.lastUsed = ++this._accessCounter;
    this._stats.hits++;
    return entry.plan;
  }

  /**
   * Cache a query plan.
   */
  put(sql, plan, tables = []) {
    const key = this._normalize(sql);

    // Remove old entry if exists
    if (this._cache.has(key)) {
      this._removeEntry(key);
    }

    // Evict if necessary
    while (this._cache.size >= this.maxEntries) {
      this._evictLRU();
    }

    const entry = new PlanCacheEntry(key, plan, tables);
    entry.lastUsed = ++this._accessCounter;
    this._cache.set(key, entry);

    // Register table dependencies
    for (const table of tables) {
      const lowerTable = table.toLowerCase();
      if (!this._tableDeps.has(lowerTable)) {
        this._tableDeps.set(lowerTable, new Set());
      }
      this._tableDeps.get(lowerTable).add(key);
    }

    this._stats.inserts++;
    return entry;
  }

  /**
   * Invalidate all plans that reference a given table.
   * Called on DDL (CREATE/ALTER/DROP TABLE, CREATE/DROP INDEX).
   */
  invalidateTable(tableName) {
    const lowerTable = tableName.toLowerCase();
    const keys = this._tableDeps.get(lowerTable);
    if (!keys || keys.size === 0) return 0;

    let count = 0;
    for (const key of [...keys]) {
      this._removeEntry(key);
      count++;
    }
    this._stats.invalidations += count;
    return count;
  }

  /**
   * Invalidate all cached plans.
   */
  invalidateAll() {
    const count = this._cache.size;
    this._cache.clear();
    this._tableDeps.clear();
    this._stats.invalidations += count;
    return count;
  }

  /**
   * Get cache statistics.
   */
  getStats() {
    const total = this._stats.hits + this._stats.misses;
    return {
      ...this._stats,
      entries: this._cache.size,
      hitRate: total > 0 ? +(this._stats.hits / total * 100).toFixed(1) : 0,
      tables: this._tableDeps.size,
      size: this._cache.size,
    };
  }

  stats() { return this.getStats(); }

  /**
   * Get detailed info about cached plans.
   */
  getEntries(options = {}) {
    const entries = [...this._cache.values()].map(e => ({
      sql: e.normalizedSQL.substring(0, 100),
      hitCount: e.hitCount,
      avgExecMs: e.hitCount > 0 ? +(e.totalExecTime / e.hitCount).toFixed(3) : 0,
      tables: e.tables,
      createdAt: e.createdAt,
      lastUsed: e.lastUsed,
    }));

    if (options.sortBy === 'hits') {
      entries.sort((a, b) => b.hitCount - a.hitCount);
    } else if (options.sortBy === 'recent') {
      entries.sort((a, b) => b.lastUsed - a.lastUsed);
    }

    return options.limit ? entries.slice(0, options.limit) : entries;
  }

  _normalize(sql) {
    return sql.trim().replace(/\s+/g, ' ').toLowerCase();
  }

  _removeEntry(key) {
    const entry = this._cache.get(key);
    if (!entry) return;

    // Remove table dependencies
    for (const table of entry.tables) {
      const deps = this._tableDeps.get(table.toLowerCase());
      if (deps) {
        deps.delete(key);
        if (deps.size === 0) this._tableDeps.delete(table.toLowerCase());
      }
    }

    this._cache.delete(key);
  }

  _evictLRU() {
    let oldestKey = null;
    let oldestTime = Infinity;
    for (const [key, entry] of this._cache) {
      if (entry.lastUsed < oldestTime) {
        oldestTime = entry.lastUsed;
        oldestKey = key;
      }
    }
    if (oldestKey) {
      this._removeEntry(oldestKey);
      this._stats.evictions++;
    }
  }
}

/**
 * SQL Normalizer — normalizes SQL for plan cache keys.
 * Replaces literal values with $N placeholders.
 */
export function normalizeSQL(sql) {
  let paramIdx = 1;
  // Replace number literals
  let normalized = sql.replace(/\b\d+(\.\d+)?\b/g, () => `$${paramIdx++}`);
  // Replace string literals
  normalized = normalized.replace(/'[^']*'/g, () => `$${paramIdx++}`);
  return normalized.trim().replace(/\s+/g, ' ').toLowerCase();
}
