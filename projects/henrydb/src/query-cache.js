// query-cache.js — Result cache for repeated queries
export class QueryCache {
  constructor(maxSize = 1000) {
    this._cache = new Map();
    this._maxSize = maxSize;
    this._hits = 0;
    this._misses = 0;
  }

  get(sql, params) {
    const key = sql + '|' + JSON.stringify(params || []);
    if (this._cache.has(key)) { this._hits++; return this._cache.get(key); }
    this._misses++;
    return undefined;
  }

  set(sql, params, result) {
    const key = sql + '|' + JSON.stringify(params || []);
    if (this._cache.size >= this._maxSize) {
      this._cache.delete(this._cache.keys().next().value);
    }
    this._cache.set(key, { result, timestamp: Date.now() });
  }

  invalidate(table) {
    for (const [key] of this._cache) {
      if (key.toLowerCase().includes(table.toLowerCase())) this._cache.delete(key);
    }
  }

  /**
   * Extract table names from a SQL statement for cache invalidation.
   * @param {string} sql
   * @returns {string[]}
   */
  static extractTables(sql) {
    const tables = [];
    // FROM clause
    const fromMatch = sql.match(/\bFROM\s+(\w+)/gi);
    if (fromMatch) {
      for (const m of fromMatch) {
        tables.push(m.replace(/^FROM\s+/i, '').toLowerCase());
      }
    }
    // JOIN clause
    const joinMatch = sql.match(/\bJOIN\s+(\w+)/gi);
    if (joinMatch) {
      for (const m of joinMatch) {
        tables.push(m.replace(/^JOIN\s+/i, '').toLowerCase());
      }
    }
    // INSERT INTO
    const insertMatch = sql.match(/\bINSERT\s+INTO\s+(\w+)/i);
    if (insertMatch) tables.push(insertMatch[1].toLowerCase());
    // UPDATE
    const updateMatch = sql.match(/\bUPDATE\s+(\w+)/i);
    if (updateMatch) tables.push(updateMatch[1].toLowerCase());
    // DELETE FROM
    const deleteMatch = sql.match(/\bDELETE\s+FROM\s+(\w+)/i);
    if (deleteMatch) tables.push(deleteMatch[1].toLowerCase());
    return [...new Set(tables)];
  }

  clear() { this._cache.clear(); }
  get hitRate() { return this._hits / (this._hits + this._misses) || 0; }
  get size() { return this._cache.size; }
}
