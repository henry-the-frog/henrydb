// rate-limiter.js — Connection and query rate limiter for HenryDB
// Token bucket algorithm with per-IP limits, fairness queuing, and slowloris protection.

/**
 * TokenBucket — classic token bucket rate limiter.
 */
export class TokenBucket {
  constructor(capacity, refillRate) {
    this.capacity = capacity;        // Max tokens
    this.refillRate = refillRate;     // Tokens per second
    this.tokens = capacity;
    this.lastRefill = performance.now();
  }

  /**
   * Try to consume n tokens. Returns true if allowed, false if rejected.
   */
  tryConsume(n = 1) {
    this._refill();
    if (this.tokens >= n) {
      this.tokens -= n;
      return true;
    }
    return false;
  }

  /**
   * Get time (ms) until n tokens are available.
   */
  waitTime(n = 1) {
    this._refill();
    if (this.tokens >= n) return 0;
    const deficit = n - this.tokens;
    return (deficit / this.refillRate) * 1000;
  }

  _refill() {
    const now = performance.now();
    const elapsed = (now - this.lastRefill) / 1000;
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.refillRate);
    this.lastRefill = now;
  }
}

/**
 * SlidingWindowCounter — count events in a sliding time window.
 */
export class SlidingWindowCounter {
  constructor(windowMs) {
    this.windowMs = windowMs;
    this._events = []; // timestamps
  }

  add() {
    this._prune();
    this._events.push(Date.now());
  }

  count() {
    this._prune();
    return this._events.length;
  }

  _prune() {
    const cutoff = Date.now() - this.windowMs;
    while (this._events.length > 0 && this._events[0] < cutoff) {
      this._events.shift();
    }
  }
}

/**
 * ConnectionRateLimiter — per-IP rate limiting with fairness.
 */
export class ConnectionRateLimiter {
  constructor(options = {}) {
    this.maxConnectionsPerIP = options.maxConnectionsPerIP || 10;
    this.maxQueriesPerSecond = options.maxQueriesPerSecond || 100;
    this.maxConnectionRate = options.maxConnectionRate || 5; // new connections per second per IP
    this.slowlorisTimeoutMs = options.slowlorisTimeoutMs || 30000; // Max idle time
    this.globalMaxConnections = options.globalMaxConnections || 100;

    this._ipBuckets = new Map(); // IP → TokenBucket
    this._ipConnections = new Map(); // IP → Set<connectionId>
    this._connectionTimestamps = new Map(); // connectionId → lastActivity
    this._queryCounters = new Map(); // IP → SlidingWindowCounter
    this._totalConnections = 0;

    this._stats = {
      totalAllowed: 0,
      totalRejected: 0,
      totalSlowloris: 0,
      totalQPSRejected: 0,
    };
  }

  /**
   * Check if a new connection from an IP is allowed.
   */
  allowConnection(ip) {
    // Global limit
    if (this._totalConnections >= this.globalMaxConnections) {
      this._stats.totalRejected++;
      return { allowed: false, reason: 'global_limit' };
    }

    // Per-IP connection count
    const ipConns = this._ipConnections.get(ip) || new Set();
    if (ipConns.size >= this.maxConnectionsPerIP) {
      this._stats.totalRejected++;
      return { allowed: false, reason: 'per_ip_limit', current: ipConns.size };
    }

    // Per-IP connection rate
    if (!this._ipBuckets.has(ip)) {
      this._ipBuckets.set(ip, new TokenBucket(this.maxConnectionRate * 2, this.maxConnectionRate));
    }
    const bucket = this._ipBuckets.get(ip);
    if (!bucket.tryConsume(1)) {
      this._stats.totalRejected++;
      return { allowed: false, reason: 'rate_limit', retryAfterMs: bucket.waitTime(1) };
    }

    this._stats.totalAllowed++;
    return { allowed: true };
  }

  /**
   * Register a new connection.
   */
  registerConnection(ip, connectionId) {
    if (!this._ipConnections.has(ip)) {
      this._ipConnections.set(ip, new Set());
    }
    this._ipConnections.get(ip).add(connectionId);
    this._connectionTimestamps.set(connectionId, Date.now());
    this._totalConnections++;
  }

  /**
   * Record connection activity (prevents slowloris detection).
   */
  recordActivity(connectionId) {
    this._connectionTimestamps.set(connectionId, Date.now());
  }

  /**
   * Check if a query from an IP is allowed (QPS limiting).
   */
  allowQuery(ip) {
    if (!this._queryCounters.has(ip)) {
      this._queryCounters.set(ip, new SlidingWindowCounter(1000));
    }
    const counter = this._queryCounters.get(ip);
    if (counter.count() >= this.maxQueriesPerSecond) {
      this._stats.totalQPSRejected++;
      return false;
    }
    counter.add();
    return true;
  }

  /**
   * Remove a connection.
   */
  removeConnection(ip, connectionId) {
    const conns = this._ipConnections.get(ip);
    if (conns) {
      conns.delete(connectionId);
      if (conns.size === 0) this._ipConnections.delete(ip);
    }
    this._connectionTimestamps.delete(connectionId);
    this._totalConnections = Math.max(0, this._totalConnections - 1);
  }

  /**
   * Detect and kill slowloris connections (idle too long).
   */
  detectSlowloris() {
    const now = Date.now();
    const killed = [];

    for (const [connId, lastActivity] of this._connectionTimestamps) {
      if (now - lastActivity > this.slowlorisTimeoutMs) {
        killed.push(connId);
        this._stats.totalSlowloris++;
      }
    }

    // Clean up killed connections
    for (const connId of killed) {
      this._connectionTimestamps.delete(connId);
      // Find and remove from IP maps
      for (const [ip, conns] of this._ipConnections) {
        if (conns.has(connId)) {
          conns.delete(connId);
          if (conns.size === 0) this._ipConnections.delete(ip);
          this._totalConnections = Math.max(0, this._totalConnections - 1);
          break;
        }
      }
    }

    return killed;
  }

  getStats() {
    return {
      ...this._stats,
      activeConnections: this._totalConnections,
      uniqueIPs: this._ipConnections.size,
    };
  }

  /**
   * Get per-IP connection info.
   */
  getIPInfo(ip) {
    const conns = this._ipConnections.get(ip);
    const counter = this._queryCounters.get(ip);
    return {
      connections: conns ? conns.size : 0,
      queriesInLastSecond: counter ? counter.count() : 0,
    };
  }
}
