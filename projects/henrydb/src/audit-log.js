// audit-log.js — Connection and query audit logging for HenryDB
// Tracks connections, authentications, DDL, DML, and failures.

/**
 * AuditEvent types.
 */
const EVENT_TYPES = ['CONNECT', 'DISCONNECT', 'AUTH_SUCCESS', 'AUTH_FAILURE', 'DDL', 'DML', 'QUERY', 'ERROR', 'PRIVILEGE_CHECK'];

/**
 * AuditEntry — a single audit log entry.
 */
class AuditEntry {
  constructor(options) {
    this.id = options.id;
    this.timestamp = options.timestamp || Date.now();
    this.eventType = options.eventType;
    this.sessionId = options.sessionId || null;
    this.user = options.user || null;
    this.database = options.database || 'henrydb';
    this.sourceIP = options.sourceIP || null;
    this.statement = options.statement || null;
    this.objectType = options.objectType || null;
    this.objectName = options.objectName || null;
    this.success = options.success !== false;
    this.errorMessage = options.errorMessage || null;
    this.duration = options.duration || null;
    this.rowsAffected = options.rowsAffected || null;
  }

  toJSON() {
    const obj = { ...this };
    // Remove null fields for compact logging
    for (const key of Object.keys(obj)) {
      if (obj[key] === null) delete obj[key];
    }
    return obj;
  }
}

/**
 * AuditLog — manages audit log entries with rotation.
 */
export class AuditLog {
  constructor(options = {}) {
    this.maxEntries = options.maxEntries || 10000;
    this.enabled = options.enabled !== false;
    this.logLevel = options.logLevel || 'all'; // all, ddl, auth, errors
    this.excludePatterns = options.excludePatterns || []; // SQL patterns to exclude
    this._entries = [];
    this._nextId = 1;
    this._callbacks = []; // External log sinks
    this._stats = {
      totalEvents: 0,
      byType: {},
      failedEvents: 0,
      rotations: 0,
    };
  }

  /**
   * Log an audit event.
   */
  log(options) {
    if (!this.enabled) return null;

    const eventType = options.eventType?.toUpperCase();
    
    // Filter by log level
    if (!this._shouldLog(eventType, options)) return null;

    // Check exclude patterns
    if (options.statement && this.excludePatterns.some(p => options.statement.match(p))) {
      return null;
    }

    const entry = new AuditEntry({
      id: this._nextId++,
      ...options,
      eventType,
    });

    this._entries.push(entry);
    this._stats.totalEvents++;
    this._stats.byType[eventType] = (this._stats.byType[eventType] || 0) + 1;
    if (!entry.success) this._stats.failedEvents++;

    // Rotate if needed
    if (this._entries.length > this.maxEntries) {
      this._rotate();
    }

    // Notify callbacks
    for (const cb of this._callbacks) {
      try { cb(entry); } catch {}
    }

    return entry;
  }

  // Convenience methods
  logConnect(sessionId, user, sourceIP) {
    return this.log({ eventType: 'CONNECT', sessionId, user, sourceIP });
  }

  logDisconnect(sessionId, user) {
    return this.log({ eventType: 'DISCONNECT', sessionId, user });
  }

  logAuthSuccess(sessionId, user, sourceIP) {
    return this.log({ eventType: 'AUTH_SUCCESS', sessionId, user, sourceIP });
  }

  logAuthFailure(user, sourceIP, reason) {
    return this.log({ eventType: 'AUTH_FAILURE', user, sourceIP, success: false, errorMessage: reason });
  }

  logDDL(sessionId, user, statement, objectType, objectName) {
    return this.log({ eventType: 'DDL', sessionId, user, statement, objectType, objectName });
  }

  logQuery(sessionId, user, statement, options = {}) {
    return this.log({ eventType: 'QUERY', sessionId, user, statement, ...options });
  }

  logError(sessionId, user, statement, errorMessage) {
    return this.log({ eventType: 'ERROR', sessionId, user, statement, success: false, errorMessage });
  }

  /**
   * Query the audit log.
   */
  query(filters = {}) {
    let results = this._entries;

    if (filters.eventType) {
      const types = Array.isArray(filters.eventType) ? filters.eventType : [filters.eventType];
      results = results.filter(e => types.includes(e.eventType));
    }
    if (filters.user) {
      results = results.filter(e => e.user === filters.user);
    }
    if (filters.sessionId) {
      results = results.filter(e => e.sessionId === filters.sessionId);
    }
    if (filters.since) {
      results = results.filter(e => e.timestamp >= filters.since);
    }
    if (filters.until) {
      results = results.filter(e => e.timestamp <= filters.until);
    }
    if (filters.success !== undefined) {
      results = results.filter(e => e.success === filters.success);
    }
    if (filters.objectName) {
      results = results.filter(e => e.objectName === filters.objectName);
    }

    if (filters.limit) {
      results = results.slice(-filters.limit);
    }

    return results.map(e => e.toJSON());
  }

  /**
   * Add an external log sink (callback).
   */
  addSink(callback) {
    this._callbacks.push(callback);
  }

  /**
   * Get the most recent N entries.
   */
  recent(n = 10) {
    return this._entries.slice(-n).map(e => e.toJSON());
  }

  /**
   * Clear all entries.
   */
  clear() {
    const count = this._entries.length;
    this._entries = [];
    return count;
  }

  getStats() {
    return {
      ...this._stats,
      currentEntries: this._entries.length,
      enabled: this.enabled,
    };
  }

  _shouldLog(eventType, options) {
    switch (this.logLevel) {
      case 'all': return true;
      case 'ddl': return eventType === 'DDL' || eventType === 'AUTH_FAILURE' || eventType === 'ERROR';
      case 'auth': return ['AUTH_SUCCESS', 'AUTH_FAILURE', 'CONNECT', 'DISCONNECT'].includes(eventType);
      case 'errors': return !options.success || eventType === 'AUTH_FAILURE' || eventType === 'ERROR';
      default: return true;
    }
  }

  _rotate() {
    // Keep only the most recent half
    const keepCount = Math.floor(this.maxEntries / 2);
    this._entries = this._entries.slice(-keepCount);
    this._stats.rotations++;
  }
}
