// connection-pool.js — Connection pool and query queue for concurrent OLTP workloads
// Manages multiple client connections, each with its own transaction state,
// and queues queries for serialized execution (maintaining isolation guarantees).

/**
 * Connection — represents a single client connection to the database.
 */
export class Connection {
  constructor(id, pool) {
    this.id = id;
    this.pool = pool;
    this.inTransaction = false;
    this.transactionId = null;
    this.createdAt = Date.now();
    this.lastActiveAt = Date.now();
    this.queriesExecuted = 0;
    this._closed = false;
  }

  /**
   * Execute a SQL query through this connection.
   */
  execute(sql) {
    if (this._closed) throw new Error(`Connection ${this.id} is closed`);
    this.lastActiveAt = Date.now();
    this.queriesExecuted++;
    return this.pool._executeOnConnection(this, sql);
  }

  /**
   * Begin a transaction on this connection.
   */
  begin() {
    return this.execute('BEGIN');
  }

  /**
   * Commit the current transaction.
   */
  commit() {
    return this.execute('COMMIT');
  }

  /**
   * Rollback the current transaction.
   */
  rollback() {
    return this.execute('ROLLBACK');
  }

  /**
   * Close this connection and return it to the pool.
   */
  close() {
    if (this._closed) return;
    this._closed = true;
    if (this.inTransaction) {
      try { this.rollback(); } catch {}
    }
    this.pool._releaseConnection(this);
  }

  get isActive() { return !this._closed; }
  get idleMs() { return Date.now() - this.lastActiveAt; }
}

/**
 * ConnectionPool — manages a pool of connections to a database.
 */
export class ConnectionPool {
  constructor(database, options = {}) {
    this.db = database;
    this.maxConnections = options.maxConnections || 10;
    this.idleTimeoutMs = options.idleTimeoutMs || 30000; // 30s idle timeout
    
    this._connections = new Map(); // id → Connection
    this._available = []; // Connection objects available for reuse
    this._waiting = []; // Pending connection requests
    this._nextId = 1;
    this._queryQueue = []; // { sql, resolve, reject, connection }
    this._processing = false;

    this.stats = {
      totalConnections: 0,
      activeConnections: 0,
      totalQueries: 0,
      queuedQueries: 0,
      peakConnections: 0,
    };
  }

  /**
   * Acquire a connection from the pool.
   * Returns a Connection, or waits if the pool is full.
   */
  async acquire() {
    // Try to reuse an available connection
    if (this._available.length > 0) {
      const conn = this._available.pop();
      conn._closed = false;
      conn.lastActiveAt = Date.now();
      this.stats.activeConnections++;
      return conn;
    }

    // Create new connection if under limit
    if (this._connections.size < this.maxConnections) {
      const conn = this._createConnection();
      this.stats.activeConnections++;
      return conn;
    }

    // Pool is full — wait for a connection to become available
    return new Promise((resolve) => {
      this._waiting.push(resolve);
      this.stats.queuedQueries++;
    });
  }

  /**
   * Execute a query on a borrowed connection (auto-acquire + release).
   */
  async query(sql) {
    const conn = await this.acquire();
    try {
      const result = conn.execute(sql);
      return result;
    } finally {
      conn.close();
    }
  }

  /**
   * Execute a function within a transaction.
   */
  async transaction(fn) {
    const conn = await this.acquire();
    try {
      conn.begin();
      const result = await fn(conn);
      conn.commit();
      return result;
    } catch (e) {
      try { conn.rollback(); } catch {}
      throw e;
    } finally {
      conn.close();
    }
  }

  _createConnection() {
    const id = this._nextId++;
    const conn = new Connection(id, this);
    this._connections.set(id, conn);
    this.stats.totalConnections++;
    if (this._connections.size > this.stats.peakConnections) {
      this.stats.peakConnections = this._connections.size;
    }
    return conn;
  }

  _releaseConnection(conn) {
    this.stats.activeConnections = Math.max(0, this.stats.activeConnections - 1);

    // If someone is waiting for a connection, give them this one
    if (this._waiting.length > 0) {
      const resolve = this._waiting.shift();
      conn._closed = false;
      conn.lastActiveAt = Date.now();
      this.stats.activeConnections++;
      resolve(conn);
      return;
    }

    // Return to available pool
    this._available.push(conn);
  }

  _executeOnConnection(conn, sql) {
    this.stats.totalQueries++;
    
    // Track transaction state
    const upper = sql.trim().toUpperCase();
    if (upper === 'BEGIN' || upper.startsWith('BEGIN ')) {
      conn.inTransaction = true;
    } else if (upper === 'COMMIT' || upper === 'ROLLBACK') {
      conn.inTransaction = false;
    }

    return this.db.execute(sql);
  }

  /**
   * Close all connections and shut down the pool.
   */
  close() {
    for (const conn of this._connections.values()) {
      if (conn.isActive) conn.close();
    }
    this._connections.clear();
    this._available = [];
    this._waiting = [];
  }

  /**
   * Evict idle connections that have been inactive too long.
   */
  evictIdle() {
    const now = Date.now();
    const toEvict = this._available.filter(c => now - c.lastActiveAt > this.idleTimeoutMs);
    for (const conn of toEvict) {
      this._available = this._available.filter(c => c !== conn);
      this._connections.delete(conn.id);
    }
    return toEvict.length;
  }

  getStats() {
    return {
      ...this.stats,
      poolSize: this._connections.size,
      available: this._available.length,
      waiting: this._waiting.length,
    };
  }
}
