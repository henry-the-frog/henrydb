// query-cache.js — Query plan cache with compiled execution
// Caches compiled query functions for repeated queries.
// First execution compiles and caches; subsequent executions use the cache.

import { compileScanFilterProject } from './query-compiler.js';
import { parse } from './sql.js';

export class QueryCache {
  constructor(maxSize = 100) {
    this._cache = new Map();
    this._maxSize = maxSize;
    this._hits = 0;
    this._misses = 0;
    this._compilations = 0;
  }

  /** Look up a cached compiled query function. */
  get(sql) {
    const entry = this._cache.get(sql);
    if (entry) {
      entry.lastUsed = Date.now();
      entry.useCount++;
      this._hits++;
      return entry;
    }
    this._misses++;
    return null;
  }

  /**
   * Cache a compiled query.
   * @param {string} sql — the SQL query string
   * @param {Function} compiledFn — compiled scan-filter-project function
   * @param {object} metadata — { columns, schema, etc. }
   */
  put(sql, compiledFn, metadata) {
    if (this._cache.size >= this._maxSize) {
      this._evictLRU();
    }
    this._cache.set(sql, {
      compiledFn,
      metadata,
      lastUsed: Date.now(),
      useCount: 1,
      created: Date.now(),
    });
    this._compilations++;
  }

  /** Get cache statistics. */
  stats() {
    return {
      entries: this._cache.size,
      maxSize: this._maxSize,
      hits: this._hits,
      misses: this._misses,
      compilations: this._compilations,
      hitRate: this._hits + this._misses > 0 ? this._hits / (this._hits + this._misses) : 0,
    };
  }

  /** Clear the cache. */
  clear() {
    this._cache.clear();
  }

  /** Evict the least recently used entry. */
  _evictLRU() {
    let oldest = Infinity;
    let oldestKey = null;
    for (const [key, entry] of this._cache) {
      if (entry.lastUsed < oldest) {
        oldest = entry.lastUsed;
        oldestKey = key;
      }
    }
    if (oldestKey) this._cache.delete(oldestKey);
  }
}

/**
 * Try to execute a SELECT query using compiled code.
 * Returns null if the query can't be compiled (complex query).
 * 
 * @param {string} sql — SQL query
 * @param {Database} db — database instance
 * @param {QueryCache} cache — query cache
 * @returns {{ rows: object[] } | null}
 */
export function tryCompiledExecution(sql, db, cache) {
  // Only compile simple SELECT queries (no subqueries, no JOINs for now)
  const trimmed = sql.trim().toUpperCase();
  if (!trimmed.startsWith('SELECT')) return null;
  
  // Check cache
  const cached = cache.get(sql);
  if (cached) {
    // Use cached compiled function
    const tableName = cached.metadata.tableName;
    const table = db.tables.get(tableName);
    if (!table) return null;
    
    const heap = table.heap.scan();
    const rows = cached.compiledFn(heap);
    return { rows };
  }
  
  // Try to parse and compile
  try {
    const ast = parse(sql);
    
    // Only handle simple single-table SELECTs
    if (ast.type !== 'SELECT') return null;
    if (!ast.from || ast.from.join) return null; // No JOINs
    if (ast.groupBy) return null; // No GROUP BY (need aggregation)
    if (ast.having) return null;
    if (ast.union) return null;
    
    const tableName = ast.from.table || ast.from.name;
    if (!tableName) return null;
    
    const table = db.tables.get(tableName);
    if (!table) return null;
    
    const schema = table.schema;
    const compiled = compileScanFilterProject(
      ast.where,
      ast.columns,
      schema,
      { limit: ast.limit, offset: ast.offset }
    );
    
    if (!compiled) return null;
    
    // Execute and cache
    const heap = table.heap.scan();
    const rows = compiled(heap);
    
    cache.put(sql, compiled, { tableName, schema });
    
    return { rows };
  } catch (e) {
    return null; // Can't compile — fall back to interpreter
  }
}
