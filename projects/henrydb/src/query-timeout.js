// query-timeout.js — Connection-level query timeout and cancellation
// SET statement_timeout = 5000; (ms)
// Supports statement cancellation via cancel token.

/**
 * CancelToken — cooperative cancellation mechanism.
 */
export class CancelToken {
  constructor() {
    this._cancelled = false;
    this._reason = null;
    this._callbacks = [];
  }

  get isCancelled() { return this._cancelled; }
  get reason() { return this._reason; }

  cancel(reason = 'Query cancelled') {
    if (this._cancelled) return;
    this._cancelled = true;
    this._reason = reason;
    for (const cb of this._callbacks) {
      try { cb(reason); } catch {}
    }
    this._callbacks = [];
  }

  onCancel(callback) {
    if (this._cancelled) {
      callback(this._reason);
    } else {
      this._callbacks.push(callback);
    }
  }

  /**
   * Check cancellation and throw if cancelled.
   * Call this at checkpoints during long operations.
   */
  throwIfCancelled() {
    if (this._cancelled) {
      throw new QueryCancelledError(this._reason);
    }
  }
}

/**
 * QueryCancelledError — thrown when a query is cancelled.
 */
export class QueryCancelledError extends Error {
  constructor(reason = 'Query cancelled') {
    super(reason);
    this.name = 'QueryCancelledError';
    this.code = '57014'; // PostgreSQL error code for query_canceled
  }
}

/**
 * QueryTimeoutError — thrown when a query exceeds the timeout.
 */
export class QueryTimeoutError extends QueryCancelledError {
  constructor(timeoutMs) {
    super(`Query cancelled due to statement timeout (${timeoutMs}ms)`);
    this.name = 'QueryTimeoutError';
    this.code = '57014';
    this.timeoutMs = timeoutMs;
  }
}

/**
 * TimeoutManager — manages query timeouts for connections.
 */
export class TimeoutManager {
  constructor() {
    this._activeQueries = new Map(); // queryId → { token, timer, startTime, sql }
    this._nextQueryId = 1;
    this._stats = {
      totalQueries: 0,
      totalTimeouts: 0,
      totalCancellations: 0,
    };
  }

  /**
   * Start tracking a query with an optional timeout.
   * Returns { queryId, token } for cancellation.
   */
  startQuery(sql, timeoutMs = 0) {
    const queryId = this._nextQueryId++;
    const token = new CancelToken();
    const startTime = performance.now();

    let timer = null;
    if (timeoutMs > 0) {
      timer = setTimeout(() => {
        token.cancel(`Query cancelled due to statement timeout (${timeoutMs}ms)`);
        this._stats.totalTimeouts++;
      }, timeoutMs);
    }

    this._activeQueries.set(queryId, {
      token,
      timer,
      startTime,
      sql: sql.substring(0, 100), // Truncate for display
      timeoutMs,
    });

    this._stats.totalQueries++;
    return { queryId, token };
  }

  /**
   * Complete a query (normal finish).
   */
  endQuery(queryId) {
    const entry = this._activeQueries.get(queryId);
    if (!entry) return;

    if (entry.timer) clearTimeout(entry.timer);
    const elapsed = performance.now() - entry.startTime;
    this._activeQueries.delete(queryId);

    return { elapsed, cancelled: entry.token.isCancelled };
  }

  /**
   * Cancel a specific query.
   */
  cancelQuery(queryId, reason = 'Cancelled by user') {
    const entry = this._activeQueries.get(queryId);
    if (!entry) return false;

    entry.token.cancel(reason);
    this._stats.totalCancellations++;
    return true;
  }

  /**
   * Cancel all active queries (shutdown).
   */
  cancelAll(reason = 'All queries cancelled') {
    let count = 0;
    for (const [id, entry] of this._activeQueries) {
      entry.token.cancel(reason);
      if (entry.timer) clearTimeout(entry.timer);
      count++;
    }
    this._stats.totalCancellations += count;
    this._activeQueries.clear();
    return count;
  }

  /**
   * Get list of active queries.
   */
  getActiveQueries() {
    const now = performance.now();
    return [...this._activeQueries.entries()].map(([id, entry]) => ({
      queryId: id,
      sql: entry.sql,
      elapsedMs: +(now - entry.startTime).toFixed(1),
      timeoutMs: entry.timeoutMs,
      cancelled: entry.token.isCancelled,
    }));
  }

  getStats() {
    return {
      ...this._stats,
      activeQueries: this._activeQueries.size,
    };
  }

  /**
   * Clean up (cancel all and clear timers).
   */
  destroy() {
    for (const entry of this._activeQueries.values()) {
      if (entry.timer) clearTimeout(entry.timer);
    }
    this._activeQueries.clear();
  }
}

/**
 * Execute a function with a cancellation token and timeout.
 * @param {Function} fn - async function(token) that should check token.throwIfCancelled()
 * @param {number} timeoutMs - timeout in milliseconds (0 = no timeout)
 */
export async function withTimeout(fn, timeoutMs) {
  const token = new CancelToken();
  let timer = null;

  const timeoutPromise = new Promise((_, reject) => {
    if (timeoutMs > 0) {
      timer = setTimeout(() => {
        token.cancel();
        reject(new QueryTimeoutError(timeoutMs));
      }, timeoutMs);
    }
  });

  try {
    const result = await Promise.race([
      fn(token),
      timeoutPromise,
    ]);
    return result;
  } finally {
    if (timer) clearTimeout(timer);
  }
}
