// connection-pool.js — Simple connection pool for HenryDB
// Manages a pool of Database instances with idle timeout and health checks.

import { Database } from './db.js';

/**
 * ConnectionPool — manages a pool of database connections.
 * 
 * Usage:
 *   const pool = new ConnectionPool({ max: 10, idleTimeout: 30000 });
 *   const conn = pool.acquire();
 *   conn.execute('SELECT 1');
 *   pool.release(conn);
 *   pool.shutdown();
 */
export class ConnectionPool {
  constructor(options = {}) {
    this.max = options.max || 10;
    this.min = options.min || 0;
    this.idleTimeout = options.idleTimeout || 30000;
    this.acquireTimeout = options.acquireTimeout || 5000;
    
    this._idle = [];       // Available connections
    this._active = new Set(); // In-use connections
    this._waiting = [];    // Queued acquire requests
    this._metadata = new Map(); // Connection metadata
    this._closed = false;
    
    this._stats = {
      acquired: 0,
      released: 0,
      created: 0,
      destroyed: 0,
      timeouts: 0,
      errors: 0,
    };
    
    // Pre-warm minimum connections
    for (let i = 0; i < this.min; i++) {
      this._idle.push(this._createConnection());
    }
  }

  _createConnection() {
    const conn = new Database();
    this._metadata.set(conn, {
      created: Date.now(),
      lastUsed: Date.now(),
      queryCount: 0,
    });
    this._stats.created++;
    return conn;
  }

  _destroyConnection(conn) {
    this._metadata.delete(conn);
    this._stats.destroyed++;
  }

  /**
   * Acquire a connection from the pool.
   * @returns {Database} A database connection
   * @throws {Error} If pool is closed or timeout exceeded
   */
  acquire() {
    if (this._closed) throw new Error('Pool is closed');
    
    // Try idle connection
    while (this._idle.length > 0) {
      const conn = this._idle.pop();
      const meta = this._metadata.get(conn);
      
      // Check if idle too long
      if (meta && Date.now() - meta.lastUsed > this.idleTimeout) {
        this._destroyConnection(conn);
        continue;
      }
      
      this._active.add(conn);
      if (meta) meta.lastUsed = Date.now();
      this._stats.acquired++;
      return conn;
    }
    
    // Create new if under max
    if (this._active.size < this.max) {
      const conn = this._createConnection();
      this._active.add(conn);
      this._stats.acquired++;
      return conn;
    }
    
    // Pool exhausted
    throw new Error(`Pool exhausted (${this._active.size}/${this.max} active)`);
  }

  /**
   * Release a connection back to the pool.
   * @param {Database} conn - Connection to release
   */
  release(conn) {
    if (!this._active.has(conn)) return;
    
    this._active.delete(conn);
    this._stats.released++;
    
    const meta = this._metadata.get(conn);
    if (meta) {
      meta.lastUsed = Date.now();
      meta.queryCount++;
    }
    
    // Serve waiting requests first
    if (this._waiting.length > 0) {
      const waiter = this._waiting.shift();
      this._active.add(conn);
      this._stats.acquired++;
      waiter(conn);
      return;
    }
    
    this._idle.push(conn);
  }

  /**
   * Execute a query using a pooled connection.
   * Automatically acquires and releases.
   */
  execute(sql) {
    const conn = this.acquire();
    try {
      const result = conn.execute(sql);
      this.release(conn);
      return result;
    } catch(e) {
      this._stats.errors++;
      this.release(conn);
      throw e;
    }
  }

  /**
   * Get pool statistics.
   */
  stats() {
    return {
      ...this._stats,
      idle: this._idle.length,
      active: this._active.size,
      waiting: this._waiting.length,
      total: this._idle.length + this._active.size,
      maxConnections: this.max,
    };
  }

  /**
   * Get pool size.
   */
  get size() { return this._idle.length + this._active.size; }

  /**
   * Prune idle connections that have timed out.
   */
  prune() {
    const now = Date.now();
    let pruned = 0;
    
    this._idle = this._idle.filter(conn => {
      const meta = this._metadata.get(conn);
      if (meta && now - meta.lastUsed > this.idleTimeout) {
        this._destroyConnection(conn);
        pruned++;
        return false;
      }
      return true;
    });
    
    return pruned;
  }

  /**
   * Shut down the pool: destroy all connections.
   */
  shutdown() {
    this._closed = true;
    for (const conn of this._idle) this._destroyConnection(conn);
    for (const conn of this._active) this._destroyConnection(conn);
    this._idle.length = 0;
    this._active.clear();
    this._waiting.length = 0;
  }
}
