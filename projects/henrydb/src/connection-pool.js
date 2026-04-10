// connection-pool.js — Production-quality database connection pool
// Features: async acquire with waiters, idle timeout, health checks, metrics

export class ConnectionPool {
  /**
   * @param {Object} opts
   * @param {number} opts.maxSize - Maximum pool size (default 10)
   * @param {number} opts.minSize - Minimum idle connections (default 0)
   * @param {Function} opts.factory - Create a new connection (may be async)
   * @param {Function} opts.destroy - Destroy a connection (may be async)
   * @param {Function} opts.validate - Health check (return true if healthy)
   * @param {number} opts.acquireTimeoutMs - Max wait time for acquire (default 5000)
   * @param {number} opts.idleTimeoutMs - Close idle connections after this (default 30000, 0=disabled)
   * @param {number} opts.maxLifetimeMs - Max connection lifetime (default 0=unlimited)
   */
  constructor(opts = {}) {
    this._maxSize = opts.maxSize ?? 10;
    this._minSize = opts.minSize ?? 0;
    this._factory = opts.factory ?? (() => ({ id: Math.random().toString(36).slice(2, 8), connected: true }));
    this._destroy = opts.destroy ?? (() => {});
    this._validate = opts.validate ?? (() => true);
    this._acquireTimeoutMs = opts.acquireTimeoutMs ?? 5000;
    this._idleTimeoutMs = opts.idleTimeoutMs ?? 30000;
    this._maxLifetimeMs = opts.maxLifetimeMs ?? 0;

    // Pool state
    this._idle = [];         // { conn, createdAt, lastUsedAt }
    this._inUse = new Map(); // conn → { createdAt, acquiredAt }
    this._waiters = [];      // { resolve, reject, timer }
    this._totalCreated = 0;
    this._closed = false;

    // Metrics
    this._metrics = {
      acquireCount: 0,
      releaseCount: 0,
      timeoutCount: 0,
      destroyCount: 0,
      waitCount: 0,
    };

    // Idle reaper
    this._reaperTimer = null;
    if (this._idleTimeoutMs > 0) {
      this._reaperTimer = setInterval(() => this._reapIdle(), this._idleTimeoutMs / 2);
      if (this._reaperTimer.unref) this._reaperTimer.unref();
    }
  }

  /**
   * Acquire a connection. Returns a promise that resolves with a connection
   * or rejects after acquireTimeoutMs.
   * @returns {Promise<any>}
   */
  async acquire() {
    if (this._closed) throw new Error('Pool is closed');

    // Try to get a healthy idle connection
    while (this._idle.length > 0) {
      const entry = this._idle.pop();
      if (this._isExpired(entry)) {
        await this._destroyEntry(entry);
        continue;
      }
      if (!this._validate(entry.conn)) {
        await this._destroyEntry(entry);
        continue;
      }
      this._inUse.set(entry.conn, { createdAt: entry.createdAt, acquiredAt: Date.now() });
      this._metrics.acquireCount++;
      return entry.conn;
    }

    // Try to create a new connection if under max
    if (this._totalSize() < this._maxSize) {
      const conn = await this._createConnection();
      this._inUse.set(conn.conn, { createdAt: conn.createdAt, acquiredAt: Date.now() });
      this._metrics.acquireCount++;
      return conn.conn;
    }

    // Wait for a connection to be released
    this._metrics.waitCount++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this._waiters.findIndex(w => w.resolve === resolve);
        if (idx !== -1) this._waiters.splice(idx, 1);
        this._metrics.timeoutCount++;
        reject(new Error(`Connection acquire timeout after ${this._acquireTimeoutMs}ms`));
      }, this._acquireTimeoutMs);

      this._waiters.push({ resolve, reject, timer });
    });
  }

  /**
   * Release a connection back to the pool.
   * @param {any} conn
   * @param {boolean} destroy - If true, destroy instead of returning to pool
   */
  async release(conn, destroy = false) {
    if (!this._inUse.has(conn)) return; // Double release protection

    const meta = this._inUse.get(conn);
    this._inUse.delete(conn);
    this._metrics.releaseCount++;

    const entry = { conn, createdAt: meta.createdAt, lastUsedAt: Date.now() };

    if (destroy || this._closed || this._isExpired(entry) || !this._validate(conn)) {
      await this._destroyEntry(entry);
      // Try to fill min connections
      if (!this._closed) await this._ensureMin();
      return;
    }

    // If waiters are pending, give directly
    if (this._waiters.length > 0) {
      const waiter = this._waiters.shift();
      clearTimeout(waiter.timer);
      this._inUse.set(conn, { createdAt: meta.createdAt, acquiredAt: Date.now() });
      this._metrics.acquireCount++;
      waiter.resolve(conn);
      return;
    }

    this._idle.push(entry);
  }

  /**
   * Close the pool and destroy all connections.
   */
  async close() {
    this._closed = true;

    if (this._reaperTimer) {
      clearInterval(this._reaperTimer);
      this._reaperTimer = null;
    }

    // Reject all waiters
    for (const w of this._waiters) {
      clearTimeout(w.timer);
      w.reject(new Error('Pool closed'));
    }
    this._waiters = [];

    // Destroy idle connections
    for (const entry of this._idle) {
      await this._destroyEntry(entry);
    }
    this._idle = [];

    // Destroy in-use connections
    for (const [conn, meta] of this._inUse) {
      await this._destroyEntry({ conn, createdAt: meta.createdAt });
    }
    this._inUse.clear();
  }

  /**
   * Get pool statistics.
   */
  getStats() {
    return {
      idle: this._idle.length,
      inUse: this._inUse.size,
      waiting: this._waiters.length,
      total: this._totalSize(),
      max: this._maxSize,
      ...this._metrics,
    };
  }

  // --- Internals ---

  _totalSize() {
    return this._idle.length + this._inUse.size;
  }

  async _createConnection() {
    const conn = await this._factory();
    this._totalCreated++;
    return { conn, createdAt: Date.now(), lastUsedAt: Date.now() };
  }

  async _destroyEntry(entry) {
    try {
      await this._destroy(entry.conn);
    } catch (e) { /* swallow destroy errors */ }
    this._metrics.destroyCount++;
  }

  _isExpired(entry) {
    if (this._maxLifetimeMs > 0) {
      return (Date.now() - entry.createdAt) > this._maxLifetimeMs;
    }
    return false;
  }

  _reapIdle() {
    const now = Date.now();
    const keep = [];
    for (const entry of this._idle) {
      if ((now - entry.lastUsedAt) > this._idleTimeoutMs && keep.length >= this._minSize) {
        this._destroyEntry(entry); // fire and forget
      } else {
        keep.push(entry);
      }
    }
    this._idle = keep;
  }

  async _ensureMin() {
    while (this._totalSize() < this._minSize && !this._closed) {
      const entry = await this._createConnection();
      this._idle.push(entry);
    }
  }
}
