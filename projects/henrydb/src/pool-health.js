// pool-health.js — Connection pool health checking and management
// Validation, idle timeout, health probes, pool sizing.

/**
 * PooledConnection — represents a managed connection in the pool.
 */
class PooledConnection {
  constructor(id, connection) {
    this.id = id;
    this.connection = connection;
    this.state = 'idle'; // idle, active, validating, evicting
    this.createdAt = Date.now();
    this.lastUsedAt = Date.now();
    this.lastValidatedAt = Date.now();
    this.useCount = 0;
    this.errorCount = 0;
    this.totalActiveMs = 0;
    this._activeStartTime = null;
  }

  acquire() {
    this.state = 'active';
    this.lastUsedAt = Date.now();
    this.useCount++;
    this._activeStartTime = Date.now();
  }

  release() {
    this.state = 'idle';
    this.lastUsedAt = Date.now();
    if (this._activeStartTime) {
      this.totalActiveMs += Date.now() - this._activeStartTime;
      this._activeStartTime = null;
    }
  }

  recordError() {
    this.errorCount++;
  }

  getAge() {
    return Date.now() - this.createdAt;
  }

  getIdleTime() {
    return this.state === 'idle' ? Date.now() - this.lastUsedAt : 0;
  }
}

/**
 * HealthCheckPool — connection pool with health checking.
 */
export class HealthCheckPool {
  constructor(options = {}) {
    this.minSize = options.minSize || 2;
    this.maxSize = options.maxSize || 10;
    this.idleTimeoutMs = options.idleTimeoutMs || 300000; // 5 minutes
    this.maxLifetimeMs = options.maxLifetimeMs || 1800000; // 30 minutes
    this.validationIntervalMs = options.validationIntervalMs || 30000; // 30 seconds
    this.maxErrors = options.maxErrors || 3; // Evict after this many errors
    this.validationQuery = options.validationQuery || 'SELECT 1';
    this.connectionFactory = options.connectionFactory || (() => ({ id: Date.now(), valid: true }));
    this.validator = options.validator || ((conn) => conn.valid !== false);

    this._pool = new Map(); // id → PooledConnection
    this._idle = []; // Idle connection ids
    this._nextId = 1;
    this._timer = null;
    this._stats = {
      created: 0,
      destroyed: 0,
      validated: 0,
      validationFailures: 0,
      idleEvictions: 0,
      lifetimeEvictions: 0,
      errorEvictions: 0,
      acquires: 0,
      releases: 0,
      waits: 0,
    };
  }

  /**
   * Initialize the pool with minimum connections.
   */
  async warmUp() {
    while (this._pool.size < this.minSize) {
      await this._createConnection();
    }
    return this._pool.size;
  }

  /**
   * Acquire a connection from the pool.
   */
  async acquire() {
    this._stats.acquires++;

    // Try to get an idle connection
    while (this._idle.length > 0) {
      const id = this._idle.pop();
      const conn = this._pool.get(id);
      if (!conn || conn.state !== 'idle') continue;

      // Validate if stale
      if (Date.now() - conn.lastValidatedAt > this.validationIntervalMs) {
        const valid = await this._validate(conn);
        if (!valid) {
          this._destroyConnection(conn);
          continue;
        }
      }

      conn.acquire();
      return conn;
    }

    // No idle connections — create new if under max
    if (this._pool.size < this.maxSize) {
      const conn = await this._createConnection();
      conn.acquire();
      return conn;
    }

    // Pool exhausted
    this._stats.waits++;
    throw new Error('Connection pool exhausted');
  }

  /**
   * Release a connection back to the pool.
   */
  release(conn) {
    this._stats.releases++;

    // Check if connection should be evicted
    if (conn.errorCount >= this.maxErrors) {
      this._destroyConnection(conn);
      this._stats.errorEvictions++;
      return;
    }

    if (conn.getAge() > this.maxLifetimeMs) {
      this._destroyConnection(conn);
      this._stats.lifetimeEvictions++;
      return;
    }

    conn.release();
    this._idle.push(conn.id);
  }

  /**
   * Run health check: validate idle connections, evict stale/expired ones.
   */
  async healthCheck() {
    const results = {
      validated: 0,
      evicted: 0,
      created: 0,
    };

    // Check idle connections
    const idleCopy = [...this._idle];
    this._idle = [];

    for (const id of idleCopy) {
      const conn = this._pool.get(id);
      if (!conn) continue;

      // Idle timeout
      if (conn.getIdleTime() > this.idleTimeoutMs && this._pool.size > this.minSize) {
        this._destroyConnection(conn);
        this._stats.idleEvictions++;
        results.evicted++;
        continue;
      }

      // Lifetime check
      if (conn.getAge() > this.maxLifetimeMs) {
        this._destroyConnection(conn);
        this._stats.lifetimeEvictions++;
        results.evicted++;
        continue;
      }

      // Validate
      const valid = await this._validate(conn);
      if (valid) {
        this._idle.push(id);
        results.validated++;
      } else {
        this._destroyConnection(conn);
        results.evicted++;
      }
    }

    // Ensure minimum pool size
    while (this._pool.size < this.minSize) {
      const conn = await this._createConnection();
      this._idle.push(conn.id);
      results.created++;
    }

    return results;
  }

  /**
   * Get pool sizing recommendations.
   */
  getRecommendations() {
    const recommendations = [];
    const totalConns = this._pool.size;
    const idleConns = this._idle.length;
    const activeConns = totalConns - idleConns;
    const utilizationPct = totalConns > 0 ? (activeConns / totalConns) * 100 : 0;

    if (utilizationPct > 90) {
      recommendations.push({
        type: 'increase_max',
        message: `Pool utilization is ${utilizationPct.toFixed(0)}%. Consider increasing maxSize from ${this.maxSize}.`,
        severity: 'high',
      });
    }

    if (utilizationPct < 20 && totalConns > this.minSize * 2) {
      recommendations.push({
        type: 'decrease_max',
        message: `Pool utilization is ${utilizationPct.toFixed(0)}%. Consider decreasing maxSize or minSize.`,
        severity: 'low',
      });
    }

    if (this._stats.waits > 0) {
      recommendations.push({
        type: 'pool_exhaustion',
        message: `Pool has been exhausted ${this._stats.waits} times. Increase maxSize.`,
        severity: 'high',
      });
    }

    if (this._stats.errorEvictions > totalConns * 0.1) {
      recommendations.push({
        type: 'connection_errors',
        message: `High error rate (${this._stats.errorEvictions} evictions). Check database health.`,
        severity: 'high',
      });
    }

    return recommendations;
  }

  getStats() {
    const totalConns = this._pool.size;
    const idleConns = this._idle.length;
    return {
      ...this._stats,
      total: totalConns,
      idle: idleConns,
      active: totalConns - idleConns,
      utilization: totalConns > 0
        ? +((totalConns - idleConns) / totalConns * 100).toFixed(1)
        : 0,
    };
  }

  /**
   * Destroy all connections and stop health checks.
   */
  async destroy() {
    this.stopHealthChecks();
    for (const conn of this._pool.values()) {
      this._destroyConnection(conn);
    }
    this._idle = [];
  }

  startHealthChecks() {
    if (this._timer) return;
    this._timer = setInterval(() => this.healthCheck(), this.validationIntervalMs);
  }

  stopHealthChecks() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }

  async _createConnection() {
    const rawConn = await this.connectionFactory();
    const conn = new PooledConnection(this._nextId++, rawConn);
    this._pool.set(conn.id, conn);
    this._idle.push(conn.id);
    this._stats.created++;
    return conn;
  }

  _destroyConnection(conn) {
    this._pool.delete(conn.id);
    this._idle = this._idle.filter(id => id !== conn.id);
    conn.state = 'evicting';
    this._stats.destroyed++;
  }

  async _validate(conn) {
    this._stats.validated++;
    try {
      const valid = await this.validator(conn.connection);
      conn.lastValidatedAt = Date.now();
      if (!valid) {
        this._stats.validationFailures++;
      }
      return valid;
    } catch {
      this._stats.validationFailures++;
      return false;
    }
  }
}
