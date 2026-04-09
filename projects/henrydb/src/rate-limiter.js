// rate-limiter.js — Token bucket + sliding window rate limiting
export class TokenBucket {
  constructor(capacity, refillRate) {
    this._capacity = capacity;
    this._tokens = capacity;
    this._refillRate = refillRate; // tokens per second
    this._lastRefill = Date.now();
  }

  allow(tokens = 1) {
    this._refill();
    if (this._tokens >= tokens) { this._tokens -= tokens; return true; }
    return false;
  }

  _refill() {
    const now = Date.now();
    const elapsed = (now - this._lastRefill) / 1000;
    this._tokens = Math.min(this._capacity, this._tokens + elapsed * this._refillRate);
    this._lastRefill = now;
  }
}

export class SlidingWindowCounter {
  constructor(windowMs, limit) {
    this._windowMs = windowMs;
    this._limit = limit;
    this._timestamps = [];
  }

  allow() {
    const now = Date.now();
    this._timestamps = this._timestamps.filter(t => now - t < this._windowMs);
    if (this._timestamps.length < this._limit) {
      this._timestamps.push(now);
      return true;
    }
    return false;
  }
}
