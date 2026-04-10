// rate-limiter.js — Rate limiting algorithms for database server connections
// Implements: Token Bucket, Leaky Bucket, Sliding Window Counter, Fixed Window, Per-Key

/**
 * Token Bucket: burst-friendly, refills at a steady rate.
 * Good for: API rate limiting where bursts are acceptable.
 */
export class TokenBucket {
  constructor(capacity, refillRate, nowFn = Date.now) {
    this._capacity = capacity;
    this._tokens = capacity;
    this._refillRate = refillRate; // tokens per second
    this._lastRefill = nowFn();
    this._now = nowFn;
  }

  allow(tokens = 1) {
    this._refill();
    if (this._tokens >= tokens) {
      this._tokens -= tokens;
      return true;
    }
    return false;
  }

  /** How many tokens are currently available */
  get available() {
    this._refill();
    return Math.floor(this._tokens);
  }

  /** Time in ms until `tokens` are available */
  waitTime(tokens = 1) {
    this._refill();
    if (this._tokens >= tokens) return 0;
    return Math.ceil(((tokens - this._tokens) / this._refillRate) * 1000);
  }

  _refill() {
    const now = this._now();
    const elapsed = (now - this._lastRefill) / 1000;
    this._tokens = Math.min(this._capacity, this._tokens + elapsed * this._refillRate);
    this._lastRefill = now;
  }
}

/**
 * Leaky Bucket: smooth output rate, no bursts.
 * Good for: ensuring steady throughput (e.g., WAL flush rate).
 */
export class LeakyBucket {
  constructor(capacity, drainRate, nowFn = Date.now) {
    this._capacity = capacity;
    this._water = 0;
    this._drainRate = drainRate; // units per second
    this._lastDrain = nowFn();
    this._now = nowFn;
  }

  allow(amount = 1) {
    this._drain();
    if (this._water + amount <= this._capacity) {
      this._water += amount;
      return true;
    }
    return false;
  }

  get level() {
    this._drain();
    return this._water;
  }

  _drain() {
    const now = this._now();
    const elapsed = (now - this._lastDrain) / 1000;
    this._water = Math.max(0, this._water - elapsed * this._drainRate);
    this._lastDrain = now;
  }
}

/**
 * Sliding Window Counter: weighted average of current + previous window.
 * More accurate than fixed window, prevents boundary spikes.
 */
export class SlidingWindowCounter {
  constructor(windowMs, limit, nowFn = Date.now) {
    this._windowMs = windowMs;
    this._limit = limit;
    this._now = nowFn;
    this._prevCount = 0;
    this._currCount = 0;
    this._currWindowStart = Math.floor(nowFn() / windowMs) * windowMs;
  }

  allow() {
    const now = this._now();
    this._advance(now);
    const elapsed = now - this._currWindowStart;
    const weight = elapsed / this._windowMs;
    const estimate = this._prevCount * (1 - weight) + this._currCount;
    if (estimate < this._limit) {
      this._currCount++;
      return true;
    }
    return false;
  }

  get count() {
    const now = this._now();
    this._advance(now);
    const elapsed = now - this._currWindowStart;
    const weight = elapsed / this._windowMs;
    return Math.floor(this._prevCount * (1 - weight) + this._currCount);
  }

  _advance(now) {
    const windowStart = Math.floor(now / this._windowMs) * this._windowMs;
    if (windowStart > this._currWindowStart) {
      const windowsElapsed = (windowStart - this._currWindowStart) / this._windowMs;
      if (windowsElapsed >= 2) {
        this._prevCount = 0;
      } else {
        this._prevCount = this._currCount;
      }
      this._currCount = 0;
      this._currWindowStart = windowStart;
    }
  }
}

/**
 * Fixed Window Counter: simple, low memory.
 * Good for: coarse rate limiting where boundary spikes are acceptable.
 */
export class FixedWindowCounter {
  constructor(windowMs, limit, nowFn = Date.now) {
    this._windowMs = windowMs;
    this._limit = limit;
    this._now = nowFn;
    this._count = 0;
    this._windowStart = Math.floor(nowFn() / windowMs) * windowMs;
  }

  allow() {
    this._advance();
    if (this._count < this._limit) {
      this._count++;
      return true;
    }
    return false;
  }

  get remaining() {
    this._advance();
    return Math.max(0, this._limit - this._count);
  }

  _advance() {
    const now = this._now();
    const windowStart = Math.floor(now / this._windowMs) * this._windowMs;
    if (windowStart > this._windowStart) {
      this._count = 0;
      this._windowStart = windowStart;
    }
  }
}

/**
 * Per-Key Rate Limiter: wraps any limiter with per-key isolation.
 * Good for: per-client, per-IP, per-table limiting.
 */
export class PerKeyRateLimiter {
  /**
   * @param {Function} limiterFactory - () => limiter instance
   * @param {number} cleanupIntervalMs - How often to prune stale keys (default 60s)
   * @param {number} maxIdleMs - Remove keys idle longer than this (default 5min)
   */
  constructor(limiterFactory, cleanupIntervalMs = 60000, maxIdleMs = 300000) {
    this._factory = limiterFactory;
    this._limiters = new Map(); // key → { limiter, lastUsed }
    this._maxIdleMs = maxIdleMs;
    this._cleanupTimer = setInterval(() => this._cleanup(), cleanupIntervalMs);
    if (this._cleanupTimer.unref) this._cleanupTimer.unref();
  }

  allow(key, amount = 1) {
    let entry = this._limiters.get(key);
    if (!entry) {
      entry = { limiter: this._factory(), lastUsed: Date.now() };
      this._limiters.set(key, entry);
    }
    entry.lastUsed = Date.now();
    return entry.limiter.allow(amount);
  }

  /** Number of tracked keys */
  get size() {
    return this._limiters.size;
  }

  close() {
    clearInterval(this._cleanupTimer);
    this._limiters.clear();
  }

  _cleanup() {
    const now = Date.now();
    for (const [key, entry] of this._limiters) {
      if (now - entry.lastUsed > this._maxIdleMs) {
        this._limiters.delete(key);
      }
    }
  }
}
